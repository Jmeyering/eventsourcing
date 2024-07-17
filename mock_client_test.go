package eventsourcing

import "context"

type MockClient struct {
	events map[string][]Event
}

func (m *MockClient) Load(
	ctx context.Context,
	id string,
	base *Aggregate,
) error {
	events, _ := m.events[id]

	for _, ev := range events {
		base.Apply(ev)
	}

	base.setID(id)
	base.setVersion(len(events))

	return nil
}

func (m *MockClient) Commit(
	ctx context.Context,
	aggregate *Aggregate,
) error {
	changes := aggregate.changes()
	currentEvents, ok := m.events[aggregate.ID()]
	if !ok {
		currentEvents = []Event{}
	}
	m.events[aggregate.ID()] = append(currentEvents, changes...)

	aggregate.setVersion(aggregate.Version() + len(changes))

	aggregate.clean()

	return nil
}
