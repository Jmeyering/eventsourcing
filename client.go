package eventsourcing

import "context"

type LoadOptions struct {
}

type Client interface {
	// Load and hydrate aggregate from events
	Load(ctx context.Context, id string, base *Aggregate) error
	// Commit aggregate changes to persistance
	Commit(ctx context.Context, aggregate *Aggregate) error
}
