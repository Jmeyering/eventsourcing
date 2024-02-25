package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jmeyering/eventsourcing"
)

// Client implements the eventsourcing.Client interface and allows reading and
// writing aggregate events from dynamodb.
type Client struct {
	reader StreamReader
	writer StreamWriter
}

var _ eventsourcing.Client = (*Client)(nil)

// NewClient returns a new dynamodb eventsourcing.Client. Client must be
// provided table information along with a resolver which maps event names to
// underlying applyable payload structs.
func NewClient(
	db *dynamodb.Client,
	table string,
	resolver map[string]func() eventsourcing.Applyable,
) *Client {
	reader := NewStreamReader(db, table, resolver)
	writer := NewStreamWriter(db, table)
	return &Client{
		reader: reader,
		writer: writer,
	}
}

// StreamReader is able to read events from an event stream
type StreamReader func(
	ctx context.Context,
	aggregateID string,
) ([]eventsourcing.DomainEvent, error)

// StreamWriter is able to write a set of events onto an event stream
type StreamWriter func(
	ctx context.Context,
	events ...eventsourcing.DomainEvent,
) error

// Load hydrates an aggregate with the events within its event stream. Must
// provide a pointer reference to a blank aggregate as the base to the load
// function.
// TODO: Add functionality to validate that we have a blank struct as our base.
// Potentially add a method on the aggregate `Dirty() bool` or something.
func (c *Client) Load(
	ctx context.Context,
	id string,
	base eventsourcing.Aggregate,
) error {
	events, err := c.reader(ctx, id)

	if err != nil {
		return err
	}

	for _, ev := range events {
		base.Apply(ev)
	}

	base.SetID(id)
	base.SetVersion(len(events))
	base.SetAggregateType(structName(base))

	return nil
}

// Commit an aggregate to dynamodb
func (c *Client) Commit(
	ctx context.Context,
	aggregate eventsourcing.Aggregate,
) error {
	changes := aggregate.Changes()
	err := c.writer(ctx, changes...)

	if err != nil {
		return err
	}

	aggregate.SetVersion(aggregate.Version() + len(changes))

	aggregate.Clean()

	return nil
}

func structName(myvar any) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
