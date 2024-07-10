package eventsourcing

import (
	"testing"
)

type MockData struct {
	Name string
}

type ChangeNameEvent struct {
	Name string
}

func (c ChangeNameEvent) ApplyTo(a IAggregate) {
	data, _ := a.Data().(*MockData)
	data.Name = c.Name
}

func TestAggregate(t *testing.T) {
	test := &MockData{
		Name: "foo",
	}

	aggregate := NewAggregate(
		"foo-id",
		test,
	)

	aggregate.Apply(
		NewDomainEvent(
			aggregate.ID(),
			ChangeNameEvent{
				Name: "bar",
			},
		),
	)

	newActual, _ := aggregate.Data().(*MockData)
	if newActual.Name != "bar" {
		t.Errorf("aggregate value not saved correctly after event")
	}
}
