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

func TestAggregateApply(t *testing.T) {
	test := &MockData{
		Name: "foo",
	}

	aggregate := NewAggregate(
		"foo-id",
		test,
	)

	aggregate.Apply(
		NewEvent(
			ChangeNameEvent{
				Name: "bar",
			},
		),
		NewEvent(
			ChangeNameEvent{
				Name: "bang",
			},
		),
	)

	newActual, _ := aggregate.Data().(*MockData)
	if newActual.Name != "bang" {
		t.Errorf(
			"aggregate value not saved correctly after event,\nexpected: %s\nactual: %s",
			"bar",
			newActual.Name,
		)
	}
}
