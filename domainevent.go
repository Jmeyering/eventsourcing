package eventsourcing

import (
	"errors"
	"fmt"
	"github.com/oklog/ulid/v2"
	"reflect"
	"time"
)

var (
	// ErrInvalidEvent will be returned when an OK check of a DomainEvent does
	// not validate.
	ErrInvalidEvent = errors.New("invalid event")

	// CorrelationKey is set in metadata when calling `CorrelateWith` and the
	// value is set to the correlation key, or id, of the event being
	// correlated.
	CorrelationKey = "$correlationID"

	// CausationKey is set in metadata when calling `CorrelateWith` and the
	// value is set to the originating event in the event chain.
	CausationKey = "$causationID"
)

// Event is the primary unit within the eventsourcing system. It's
// designed intentionally to not have exposed properties in order to ensure
// idempotency of the payload and metadata. This prevents unexpected side
// effects to Event data.
type Event struct {
	// id is the unique identifier for this event. able to be used for
	// idempotency
	id string

	// aggregateID is the aggregate to which this event is emitted for.
	aggregateID string

	// metadata about the event
	metadata map[string]any

	// streamRevision is the sequence number of this event within it's aggregate
	// event stream.
	streamRevision int

	// name of the event
	name string

	// version is the schema version of the event
	version int

	// actor who triggered the event
	actor string

	// source of the event
	source string

	// ts created timestamp
	ts int

	// payload of the event
	payload Applyable
}

// Applyable is intended to represent an event payload that is able to be
// applied to an aggregate
type Applyable interface {
	ApplyTo(*Aggregate)
}

// EventResolver returns an Applyable
type EventResolver func() Applyable

// NewEvent returns a new DomainEvent and sets a new ID and the
// current timestamp
func NewEvent(
	payload Applyable,
) Event {
	ts := int(time.Now().Unix())
	eventID := ulid.Make()

	return Event{
		payload: payload,
		id:      eventID,
		name:    structName(payload),
		ts:      ts,
		metadata: map[string]any{
			CorrelationKey: eventID,
			CausationKey:   eventID,
		},
	}
}

// OK validates that a DomainEvent is valid for raising into the stream.
func (e Event) OK() error {
	var err error
	if e.aggregateID == "" {
		err = fmt.Errorf("%w %s", err, "missing AggregateID")
	}

	if e.id == "" {
		err = fmt.Errorf("%w %s", err, "missing EventID")
	}

	if e.source == "" {
		err = fmt.Errorf("%w %s", err, "missing Source")
	}

	if e.payload == nil {
		err = fmt.Errorf("%w %s", err, "missing Payload")
	}

	if e.name == "" {
		err = fmt.Errorf("%w %s", err, "missing EventName")
	}

	if err != nil {
		err = fmt.Errorf("%w:%s", ErrInvalidEvent, err)
	}

	return err
}

// ID access
func (e Event) ID() string {
	return e.id
}

// WithID will set the id on the event. Helpful when hydrating a DomainEvent
// from persistence
func (e Event) WithID(id string) Event {
	e.id = id
	e.metadata = copyMap(e.metadata)

	return e
}

// AggregateID returns the id of the aggregate
func (e Event) AggregateID() string {
	return e.aggregateID
}

// WithAggregateID will set the aggregateID on the event. Helpful when hydrating
// a DomainEvent from persistence
func (e Event) WithAggregateID(id string) Event {
	e.aggregateID = id
	e.metadata = copyMap(e.metadata)
	return e
}

// Metadata returns a copy of the metadata of the event. Event metadata cannot
// be mutated directly from the returned copy. Use `WithMetadata` to set the
// entire metadata object, of `WithAddtionalMetadata` to append data to event
// metadata.
func (e Event) Metadata() map[string]any {
	return copyMap(e.metadata)
}

// WithMetadata will reset the metadata of the event with the given map. Helpful
// when hydrating a DomainEvent from persistence
func (e Event) WithMetadata(m map[string]any) Event {
	e.metadata = copyMap(m)
	return e
}

// WithAddtionalMetadata will add additional metadata to an event and return a
// copy of the event.
func (e Event) WithAddtionalMetadata(m map[string]any) Event {
	metadata := copyMap(e.metadata)

	for k, v := range m {
		metadata[k] = v
	}

	e.metadata = metadata
	return e
}

// Payload returns the applyable payload of the event
func (e Event) Payload() Applyable {
	return e.payload
}

// WithPayload adds an Applyable payload to the event.
func (e Event) WithPayload(b Applyable) Event {
	e.payload = b
	e.metadata = copyMap(e.metadata)
	return e
}

// StreamRevision is the incrementing event number within an aggregate event
// stream
func (e Event) StreamRevision() int {
	return e.streamRevision
}

// WithStreamRevision will assign the event number to a given event.
func (e Event) WithStreamRevision(n int) Event {
	e.streamRevision = n
	e.metadata = copyMap(e.metadata)
	return e
}

func (e Event) Name() string {
	return e.name
}

// WithName will assign a name to a given event. Helpful when hydrating a
// DomainEvent from persistence.
func (e Event) WithName(val string) Event {
	e.name = val
	e.metadata = copyMap(e.metadata)
	return e
}

// Version access
func (e Event) Version() int {
	return e.version
}

// WithVersion will assign the event version to the metadata.
func (e Event) WithVersion(version int) Event {
	e.version = version
	e.metadata = copyMap(e.metadata)
	return e
}

func (e Event) Source() string {
	return e.source
}

// WithSource will return a new DomainEvent with the included source information
func (e Event) WithSource(value string) Event {
	e.source = value
	e.metadata = copyMap(e.metadata)
	return e
}

func (e Event) Actor() string {
	return e.actor
}

// WithActor will return a new DomainEvent with the included actor information
func (e Event) WithActor(value string) Event {
	e.actor = value
	e.metadata = copyMap(e.metadata)
	return e
}

// TS access
func (e Event) TS() int {
	return e.ts
}

// WithTS will return a new DomainEvent with the included ts information.
// Helpful when hydrating a DomainEvent from persistence.
func (e Event) WithTS(value int) Event {
	e.ts = value
	e.metadata = copyMap(e.metadata)
	return e
}

// CorrelateWith will set the correlationID of the current event equal to
// whatever correlation id was set to in the target event. If no correlation ID
// is present in the target event, we will correlate with the ID of the target
// event.
func (e Event) CorrelateWith(ev Event) Event {
	correlation, ok := ev.metadata[CorrelationKey]
	if !ok {
		correlation = ev.ID()
	}

	causation := ev.ID()

	return e.WithAddtionalMetadata(map[string]any{
		CorrelationKey: correlation,
		CausationKey:   causation,
	})
}

// copyMap is a helper function to copy a map to create immutable events with
// all methods
func copyMap(m map[string]any) map[string]any {
	cp := make(map[string]any)
	for k, v := range m {
		cp[k] = v
	}

	return cp
}

func structName(myvar any) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
