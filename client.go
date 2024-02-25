package eventsourcing

import "context"

type Client interface {
	// Load an aggregate from persistance
	Load(context.Context, string, Aggregate) error
	// Commit aggregate changes to persistance
	Commit(context.Context, Aggregate) error
}
