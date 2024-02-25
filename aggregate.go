package eventsourcing

// Aggregate represents an abstract entity stored within an individual event
// stream. Aggregates are able to be Loaded from event storage, and able to
// raise events back into that event stream.
type Aggregate interface {
	// ID returns the id of the aggregate
	ID() string
	// Version returns the current version of the aggregate, equal to the number
	// of events that are in the aggregate event stream
	Version() int
	// setID sets the id value of the aggregate
	setID(string)
	// setVersion sets the version value of the aggregate
	setVersion(int)
	// changes returns the currently uncommitted changes that have been raised
	// on the aggregate
	changes() []DomainEvent
	// clean resets the changes slice that have been applied. clean does not
	// revert any changes to the aggregate it simply empties the stored changes
	// from memory.
	clean()
	// raise a set of events on the aggregate and apply them. Stores the changed
	// events in the set of aggregate changes which allows the new  aggregate
	// events to be committed to storage
	raise(...DomainEvent)
	// apply a set of events to the aggregate
	apply(...DomainEvent)
}

// Aggregate is an abstraction for an aggregate. An Aggregate is defined as a
// struct that is able to return it's ID and it's version.
type BaseAggregate struct {
	id            string
	version       int
	aggregateType string
	changeEvents  []DomainEvent
}

// ID of the base aggregate
func (b *BaseAggregate) ID() string {
	return b.id
}

// setID sets the id of the aggregate
func (b *BaseAggregate) setID(val string) {
	b.id = val
}

// Version of the base aggregate
func (b *BaseAggregate) Version() int {
	return b.version
}

// setVersion sets the version of the aggregate
func (b *BaseAggregate) setVersion(val int) {
	b.version = val
}

// changes that have been raised into the aggregate
func (b *BaseAggregate) changes() []DomainEvent {
	return b.changeEvents
}

// clean resets the aggregate changes to an empty slice
func (b *BaseAggregate) clean() {
	b.changeEvents = []DomainEvent{}
}

// Raise an event into the aggregate changes slice. Also applies the change to
// the underlying aggregate
func (b *BaseAggregate) raise(e ...DomainEvent) {
	for _, ev := range e {
		b.changeEvents = append(b.changeEvents, ev)
		ev.payload.ApplyTo(b)
	}
}

// Apply a change to an aggregate. Does not add this event into the changes
// slice. Useful when hydrading an Aggregate from persistance
func (b *BaseAggregate) apply(e ...DomainEvent) {
	for _, ev := range e {
		ev.payload.ApplyTo(b)
	}
}
