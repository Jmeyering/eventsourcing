package eventsourcing

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// DomainEvent is the primary unit within the eventsourcing system. It's
// designed intentionally to not have exposed properties in order to ensure
// idempotency of the payload and metadata. This prevents unexpected side
// effects to DomainEvent data.
type DomainEvent struct {
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
	ApplyTo(Aggregate)
}

// NewDomainEvent returns a new DomainEvent and sets a new uuid ID and the
// current timestamp
func NewDomainEvent(
	aggregateID string,
	eventName string,
	payload Applyable,
) DomainEvent {
	ts := int(time.Now().Unix())
	eventID := uuid.NewString()

	return DomainEvent{
		payload:     payload,
		aggregateID: aggregateID,
		id:          eventID,
		name:        eventName,
		ts:          ts,
		metadata: map[string]any{
			CorrelationKey: eventID,
			CausationKey:   eventID,
		},
	}
}

// OK validates that a DomainEvent is valid for raising into the stream.
func (e DomainEvent) OK() error {
	var err error
	if e.aggregateID == "" {
		err = fmt.Errorf("%w %s", err, "missing AggregateID")
	}

	if e.id == "" {
		err = fmt.Errorf("%w %s", err, "missing EventID")
	}

	if e.name == "" {
		err = fmt.Errorf("%w %s", err, "missing EventName")
	}

	if e.source == "" {
		err = fmt.Errorf("%w %s", err, "missing Source")
	}

	if e.payload == nil {
		err = fmt.Errorf("%w %s", err, "missing Payload")
	}

	if err != nil {
		err = fmt.Errorf("%w:%s", ErrInvalidEvent, err)
	}

	return err
}

// ID access
func (e DomainEvent) ID() string {
	return e.id
}

// WithID will set the id on the event. Helpful when hydrating a DomainEvent
// from persistence
func (e DomainEvent) WithID(id string) DomainEvent {
	e.id = id
	e.metadata = CopyMap(e.metadata)

	return e
}

// AggregateID returns the id of the aggregate
func (e DomainEvent) AggregateID() string {
	return e.aggregateID
}

// WithAggregateID will set the aggregateID on the event. Helpful when hydrating
// a DomainEvent from persistence
func (e DomainEvent) WithAggregateID(id string) DomainEvent {
	e.aggregateID = id
	e.metadata = CopyMap(e.metadata)
	return e
}

// Metadata returns a copy of the metadata of the event. Event metadata cannot
// be mutated directly from the returned copy. Use `WithMetadata` to set the
// entire metadata object, of `WithAddtionalMetadata` to append data to event
// metadata.
func (e DomainEvent) Metadata() map[string]any {
	return CopyMap(e.metadata)
}

// WithMetadata will reset the metadata of the event with the given map. Helpful
// when hydrating a DomainEvent from persistence
func (e DomainEvent) WithMetadata(m map[string]any) DomainEvent {
	e.metadata = CopyMap(m)
	return e
}

// WithAddtionalMetadata will add additional metadata to an event and return a
// copy of the event.
func (e DomainEvent) WithAddtionalMetadata(m map[string]any) DomainEvent {
	metadata := CopyMap(e.metadata)

	for k, v := range m {
		metadata[k] = v
	}

	e.metadata = metadata
	return e
}

// Payload returns the applyable payload of the event
func (e DomainEvent) Payload() Applyable {
	return e.payload
}

// WithPayload adds an Applyable payload to the event.
func (e DomainEvent) WithPayload(b Applyable) DomainEvent {
	e.payload = b
	e.metadata = CopyMap(e.metadata)
	return e
}

// StreamRevision is the incrementing event number within an aggregate event
// stream
func (e DomainEvent) StreamRevision() int {
	return e.streamRevision
}

// WithStreamRevision will assign the event number to a given event.
func (e DomainEvent) WithStreamRevision(n int) DomainEvent {
	e.streamRevision = n
	e.metadata = CopyMap(e.metadata)
	return e
}

func (e DomainEvent) Name() string {
	return e.name
}

// WithName will assign a name to a given event. Helpful when hydrating a
// DomainEvent from persistence.
func (e DomainEvent) WithName(val string) DomainEvent {
	e.name = val
	e.metadata = CopyMap(e.metadata)
	return e
}

// Version access
func (e DomainEvent) Version() int {
	return e.version
}

// WithVersion will assign the event version to the metadata.
func (e DomainEvent) WithVersion(version int) DomainEvent {
	e.version = version
	e.metadata = CopyMap(e.metadata)
	return e
}

func (e DomainEvent) Source() string {
	return e.source
}

// WithSource will return a new DomainEvent with the included source information
func (e DomainEvent) WithSource(value string) DomainEvent {
	e.source = value
	e.metadata = CopyMap(e.metadata)
	return e
}

func (e DomainEvent) Actor() string {
	return e.actor
}

// WithActor will return a new DomainEvent with the included actor information
func (e DomainEvent) WithActor(value string) DomainEvent {
	e.actor = value
	e.metadata = CopyMap(e.metadata)
	return e
}

// TS access
func (e DomainEvent) TS() int {
	return e.ts
}

// WithTS will return a new DomainEvent with the included ts information.
// Helpful when hydrating a DomainEvent from persistence.
func (e DomainEvent) WithTS(value int) DomainEvent {
	e.ts = value
	e.metadata = CopyMap(e.metadata)
	return e
}

// CorrelateWith will set the correlationID of the current event equal to
// whatever correlation id was set to in the target event. If no correlation ID
// is present in the target event, we will correlate with the ID of the target
// event.
func (e DomainEvent) CorrelateWith(ev DomainEvent) DomainEvent {
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

// CopyMap is a helper function to copy a map to create immutable events with
// all methods
func CopyMap(m map[string]any) map[string]any {
	cp := make(map[string]any)
	for k, v := range m {
		cp[k] = v
	}

	return cp
}
