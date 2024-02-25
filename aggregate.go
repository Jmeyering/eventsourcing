package eventsourcing

// Aggregate represents an abstract entity stored within an individual event
// stream. Aggregates are able to be Loaded from event storage, and able to
// raise events back into that event stream.
type Aggregate interface {
	// ID returns the id of the aggregate
	ID() string
	// SetID sets the id value of the aggregate
	SetID(string)
	// Version returns the current version of the aggregate, equal to the number
	// of events that are in the aggregate event stream
	Version() int
	// SetVersion sets the version value of the aggregate
	SetVersion(int)
	// AggregateType returns the type of aggregate that is represented by the
	// implementation
	AggregateType() string
	// SetAggregateType sets the type of aggregate
	SetAggregateType(string)
	// Raise a set of events on the aggregate and apply them. Stores the changed
	// events in the set of aggregate changes which allows the new  aggregate
	// events to be committed to storage
	Raise(...DomainEvent)
	// Apply a set of events to the aggregate
	Apply(...DomainEvent)
	// Changes returns the currently uncommitted changes that have been raised
	// on the aggregate
	Changes() []DomainEvent
	// Clean resets the changes slice that have been applied. Clean does not
	// revert any changes to the aggregate it simply empties the stored changes
	// from memory.
	Clean()
}

// Aggregate is an abstraction for an aggregate. An Aggregate is defined as a
// struct that is able to return it's ID and it's version.
type BaseAggregate struct {
	id            string
	version       int
	aggregateType string
	changes       []DomainEvent
}

// ID of the base aggregate
func (b *BaseAggregate) ID() string {
	return b.id
}

// SetID sets the id of the aggregate
func (b *BaseAggregate) SetID(val string) {
	b.id = val
}

// Version of the base aggregate
func (b *BaseAggregate) Version() int {
	return b.version
}

// SetVersion sets the version of the aggregate
func (b *BaseAggregate) SetVersion(val int) {
	b.version = val
}

// AggregateType of the base aggregate
func (b *BaseAggregate) AggregateType() string {
	return b.aggregateType
}

// SetAggregateType sets the aggregate type
func (b *BaseAggregate) SetAggregateType(val string) {
	b.aggregateType = val
}

// Changes that have been raised into the aggregate
func (b *BaseAggregate) Changes() []DomainEvent {
	return b.changes
}

// Clean resets the aggregate changes to an empty slice
func (b *BaseAggregate) Clean() {
	b.changes = []DomainEvent{}
}

// Raise an event into the aggregate changes slice. Also applies the change to
// the underlying aggregate
func (b *BaseAggregate) Raise(e ...DomainEvent) {
	for _, ev := range e {
		b.changes = append(b.changes, ev)
		ev.payload.ApplyTo(b)
	}
}

// Apply a change to an aggregate. Does not add this event into the changes
// slice. Useful when hydrading an Aggregate from persistance
func (b *BaseAggregate) Apply(e ...DomainEvent) {
	for _, ev := range e {
		ev.payload.ApplyTo(b)
	}
}
