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
	return nil
}

func (m *MockClient) Commit(
	ctx context.Context,
	aggregate *Aggregate,
) error {
	currentEvents, ok := m.events[aggregate.ID()]
	if !ok {
		currentEvents = []Event{}
	}
	m.events[aggregate.ID()] = append(currentEvents, aggregate.changes()...)
	return nil
}
