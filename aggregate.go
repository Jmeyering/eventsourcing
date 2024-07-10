package eventsourcing

// IAggregate represents an abstract entity stored within an individual event
// stream. Aggregates are able to be Loaded from event storage, and able to
// raise events back into that event stream.
type IAggregate interface {
	// ID returns the id of the aggregate
	ID() string
	// Version returns the current version of the aggregate, equal to the
	// number of events that are in the aggregate event stream
	Version() int
	// Raise a set of events on the aggregate and apply them. Stores the changed
	// events in the set of aggregate changes which allows the new  aggregate
	// events to be committed to storage
	Raise(...DomainEvent)
	// Apply a set of events to the aggregate
	Apply(...DomainEvent)
	Data() any
}

func NewAggregate(id string, data any) IAggregate {
	return &Aggregate{
		id:      id,
		data:    data,
		version: 0,
	}
}

// Aggregate is defined as a struct that is able to return it's ID and it's
// version.
type Aggregate struct {
	id            string
	data          any
	version       int
	aggregateType string
	changeEvents  []DomainEvent
}

// ID of the base aggregate
func (b *Aggregate) ID() string {
	return b.id
}

// setID sets the id of the aggregate
func (b *Aggregate) setID(val string) {
	b.id = val
}

func (b *Aggregate) Data() any {
	return b.data
}

func (b *Aggregate) setData(data any) {
	b.data = data
}

// Version of the base aggregate
func (b *Aggregate) Version() int {
	return b.version
}

// setVersion sets the version of the aggregate
func (b *Aggregate) setVersion(val int) {
	b.version = val
}

// changes that have been raised into the aggregate
func (b *Aggregate) changes() []DomainEvent {
	return b.changeEvents
}

// clean resets the aggregate changes to an empty slice
func (b *Aggregate) clean() {
	b.changeEvents = []DomainEvent{}
}

// Raise an event into the aggregate changes slice. Also applies the change to
// the underlying aggregate
func (b *Aggregate) Raise(e ...DomainEvent) {
	for _, ev := range e {
		b.changeEvents = append(b.changeEvents, ev)
		ev.payload.ApplyTo(b)
	}
}

// Apply a change to an aggregate. Does not add this event into the changes
// slice. Useful when hydrading an Aggregate from persistance
func (b *Aggregate) Apply(e ...DomainEvent) {
	for _, ev := range e {
		ev.payload.ApplyTo(b)
	}
}
