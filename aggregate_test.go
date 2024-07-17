package eventsourcing

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

type MockData struct {
	Name       string
	NumberData int
}

type ChangeNameEvent struct {
	Name string
}

type IncrementEvent struct{}

func (_ IncrementEvent) ApplyTo(a *Aggregate) {
	data, _ := a.Data().(*MockData)
	data.NumberData = data.NumberData + 1
}

func (c ChangeNameEvent) ApplyTo(a *Aggregate) {
	data, _ := a.Data().(*MockData)
	data.Name = c.Name
}

func TestAggregateApply(t *testing.T) {
	tcs := []struct {
		events   []Event
		expected *MockData
	}{
		{
			events: []Event{
				NewEvent(ChangeNameEvent{
					Name: "foo",
				}),
				NewEvent(IncrementEvent{}),
				NewEvent(IncrementEvent{}),
				NewEvent(ChangeNameEvent{
					Name: "bar",
				}),
				NewEvent(IncrementEvent{}),
			},
			expected: &MockData{
				Name:       "bar",
				NumberData: 3,
			},
		},
		{
			events: []Event{
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
			},
			expected: &MockData{
				Name: "bang",
			},
		},
	}

	for _, tc := range tcs {

		aggregate := NewAggregate(
			"foo-id",
			&MockData{},
		)

		aggregate.Apply(tc.events...)

		newActual, _ := aggregate.Data().(*MockData)
		if !cmp.Equal(newActual, tc.expected) {
			t.Errorf("aggregate value not saved correctly after event,\n%s",
				cmp.Diff(newActual, tc.expected),
			)
		}
	}
}
