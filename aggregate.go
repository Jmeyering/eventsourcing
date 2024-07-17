package eventsourcing

// Aggregate is defined as a struct that is able to return it's ID and it's
// version.
type Aggregate struct {
	id           string
	data         any
	version      int
	changeEvents []Event
}

// NewAggregate return a new aggregate with the given id.
func NewAggregate(
	id string,
	base any,
) *Aggregate {
	return &Aggregate{
		id:      id,
		data:    base,
		version: 0,
	}
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
func (b *Aggregate) changes() []Event {
	return b.changeEvents
}

// clean resets the aggregate changes to an empty slice
func (b *Aggregate) clean() {
	b.changeEvents = []Event{}
}

// Raise an event into the aggregate changes slice. Also applies the change to
// the underlying aggregate
func (b *Aggregate) Raise(e ...Event) {
	for _, ev := range e {
		agEv := ev.WithAggregateID(b.ID())
		b.changeEvents = append(b.changeEvents, agEv)
		agEv.payload.ApplyTo(b)
	}
}

// Apply a change to an aggregate. Does not add this event into the changes
// slice. Useful when hydrading an Aggregate from persistance
func (b *Aggregate) Apply(e ...Event) {
	for _, ev := range e {
		agEv := ev.WithAggregateID(b.ID())
		agEv.payload.ApplyTo(b)
	}
}
