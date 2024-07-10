package eventsourcing

import "context"

type Client interface {
	// Load an aggregate from persistance
	Load(context.Context, string, IAggregate) error
	// Commit aggregate changes to persistance
	Commit(context.Context, IAggregate) error
}
