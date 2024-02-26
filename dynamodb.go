package eventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	// ErrCorruptedEvent is returned when an event is unable to be unmarshal'd
	// from dynamodb
	ErrCorruptedEvent = errors.New("corrupted event")
	// ErrUnknownEvent is returned when an event is being unmarshal'd and we are
	// unable to resolve the underlying Applyable payload.
	ErrUnknownEvent = errors.New("unknown event")
)

// DynamoDBClient implements the Client interface and allows reading and
// writing aggregate events from dynamodb.
type DynamoDBClient struct {
	reader StreamReader
	writer StreamWriter
}

// NewClient returns a new dynamodb Client. Client must be
// provided table information along with a resolver which maps event names to
// underlying applyable payload structs.
func NewClient(
	db *dynamodb.Client,
	table string,
	resolver map[string]EventResolver,
) Client {
	reader := NewStreamReader(db, table, resolver)
	writer := NewStreamWriter(db, table)
	return &DynamoDBClient{
		reader: reader,
		writer: writer,
	}
}

// StreamReader is able to read events from an event stream
type StreamReader func(
	ctx context.Context,
	aggregateID string,
) ([]DomainEvent, error)

// StreamWriter is able to write a set of events onto an event stream
type StreamWriter func(
	ctx context.Context,
	events ...DomainEvent,
) error

// Load hydrates an aggregate with the events within its event stream. Must
// provide a pointer reference to a blank aggregate as the base to the load
// function.
// TODO: Add functionality to validate that we have a blank struct as our base.
// Potentially add a method on the aggregate `Dirty() bool` or something.
func (c *DynamoDBClient) Load(
	ctx context.Context,
	id string,
	base Aggregate,
) error {
	events, err := c.reader(ctx, id)

	if err != nil {
		return err
	}

	for _, ev := range events {
		base.Apply(ev)
	}

	base.setID(id)
	base.setVersion(len(events))

	return nil
}

// Commit an aggregate to dynamodb
func (c *DynamoDBClient) Commit(
	ctx context.Context,
	aggregate Aggregate,
) error {
	changes := aggregate.changes()
	err := c.writer(ctx, changes...)

	if err != nil {
		return err
	}

	aggregate.setVersion(aggregate.Version() + len(changes))

	aggregate.clean()

	return nil
}

// PersistedEvent represents a DomainEvent that is easily persisted to
// DynamoDB by making all properties exported values
type PersistedEvent struct {
	// ID is the unique identifier for this event. able to be used for
	// idempotency
	ID string

	// AggregateID is the aggregate to which this event is emitted for.
	AggregateID string

	// Metadata about the event
	Metadata map[string]any

	// StreamRevision is the sequence number of this event within it's aggregate
	// event stream.
	StreamRevision int

	// Name of the event
	Name string

	// Version is the schema version of the event
	Version int

	// Actor who triggered the event
	Actor string

	// Source of the event
	Source string

	// TS timestamp
	TS int

	// Payload of the event
	Payload string
}

// toDomainEvent will converrt a PersistedEvent to an DomainEvent
func toDomainEvent(
	resolver map[string]EventResolver,
	e *PersistedEvent,
) (DomainEvent, error) {
	applyableFN, found := resolver[e.Name]
	if !found {
		return DomainEvent{}, fmt.Errorf(
			"missing event type in resolver: %s",
			e.Name,
		)
	}

	applyable := applyableFN()

	json.Unmarshal([]byte(e.Payload), applyable)

	return NewDomainEvent(
		e.AggregateID,
		applyable,
	).
		WithID(e.ID).
		WithMetadata(e.Metadata).
		WithStreamRevision(e.StreamRevision).
		WithVersion(e.Version).
		WithActor(e.Actor).
		WithSource(e.Source).
		WithTS(e.TS), nil
}

// ToPersistedEvent will convert an DomainEvent to a
// PersistedEvent
func ToPersistedEvent(e DomainEvent) *PersistedEvent {
	payloadb, _ := json.Marshal(e.Payload())

	return &PersistedEvent{
		ID:             e.ID(),
		AggregateID:    e.AggregateID(),
		Metadata:       e.Metadata(),
		StreamRevision: e.StreamRevision(),
		Name:           e.Name(),
		Version:        e.Version(),
		Actor:          e.Actor(),
		Source:         e.Source(),
		TS:             e.TS(),
		Payload:        string(payloadb),
	}
}

// NewStreamWriter will return a function that is able to write events into the
// event system.
func NewStreamWriter(
	dynamo *dynamodb.Client,
	table string,
) func(context.Context, ...DomainEvent) error {
	return func(
		ctx context.Context,
		events ...DomainEvent,
	) error {
		transactionItems := make([]types.TransactWriteItem, len(events))

		for i, e := range events {
			pe := ToPersistedEvent(e)
			eventItem := eventToRecord(pe, table)
			transactionItems[i] = eventItem
		}

		_, err := dynamo.TransactWriteItems(
			ctx,
			&dynamodb.TransactWriteItemsInput{
				TransactItems: transactionItems,
			},
		)
		if err != nil {
			return fmt.Errorf("dynamodb streamwriter failure: %s", err.Error())
		}
		return nil
	}
}

// eventToEventRecord will convert an event to a transaction item for writing an
// event to the event database.
func eventToRecord(
	e *PersistedEvent,
	table string,
) types.TransactWriteItem {
	av, _ := attributevalue.MarshalMap(e)
	return types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String(table),
			Item:      av,
			ExpressionAttributeNames: map[string]string{
				"#ID": "ID",
			},
			ConditionExpression: aws.String(
				"attribute_not_exists(#StreamRevision)",
			),
		},
	}
}

// NewStreamReader returns a function capable of returning all events within an
// event stream.
//
// Caller must provide a resolver which is able to map event names to concrete
// instances of Applyable payloads.
//
// ```golang
// type MyEventV1 struct{}
//
// // Implement Applyable
// func (ev *MyEventV1) ApplyTo(a Aggregate) {
//		aggregate := a.(*MyAggregate)
//		// Do event things here
// }
// resolver := map[string]eventsourcing.EventResolver{
//		"MyEventV1": func() { return &MyEventV1 },
// }
// ```
func NewStreamReader(
	dynamo *dynamodb.Client,
	table string,
	resolver map[string]EventResolver,
) StreamReader {
	return func(
		ctx context.Context,
		aggregateID string,
	) ([]DomainEvent, error) {

		agAv, _ := attributevalue.Marshal(aggregateID)
		out, err := dynamo.Query(
			ctx,
			&dynamodb.QueryInput{
				TableName: aws.String(table),
				ExpressionAttributeNames: map[string]string{
					"#AggregateID": "AggregateID",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":AggregateID": agAv,
				},
				KeyConditionExpression: aws.String("#AggregateID = :AggregateID"),
			},
		)

		if err != nil {
			return nil, err
		}

		events := make([]DomainEvent, int(out.Count))
		for i, item := range out.Items {
			e := &PersistedEvent{}
			err = attributevalue.UnmarshalMap(item, e)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", ErrCorruptedEvent, err.Error())
			}

			events[i], err = toDomainEvent(resolver, e)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", ErrUnknownEvent, err.Error())
			}
		}

		return events, err
	}
}
