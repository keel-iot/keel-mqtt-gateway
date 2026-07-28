// Package store defines ClusterStore, a minimal key/value + pub/sub
// abstraction over a distributed AP store, and wraps olric-data/olric as
// its concrete backend. internal/cluster/routing depends only on
// ClusterStore, never on the olric package directly — this keeps the
// backend swappable (e.g. to the tochemey/olric fork, which has fixes for
// automatic rejoin after a network partition, should resilience testing
// show a need for it) without touching call sites.
package store

import "context"

// ClusterStore is a minimal, backend-agnostic key/value + pub/sub
// interface. Implementations do not interpret values — callers own
// encoding.
type ClusterStore interface {
	// Put durably writes value for key, overwriting any existing value.
	Put(ctx context.Context, key string, value []byte) error

	// Get reads the value for key. ok is false if key does not exist.
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)

	// Delete removes zero or more keys. Missing keys are not an error.
	Delete(ctx context.Context, keys ...string) error

	// Scan returns an iterator over every key currently stored. Meant for
	// the (comparatively rare) full-reconciliation path — callers needing
	// values too should Get(Key()) explicitly per key.
	Scan(ctx context.Context) (KeyIterator, error)

	// Publish broadcasts message on channel to every current subscriber,
	// cluster-wide. Fire-and-forget: no delivery guarantee to a
	// subscriber that is down, not yet listening, or on the other side of
	// a dropped connection — callers relying on eventual convergence
	// rather than delivery of any single message must pair this with
	// periodic reconciliation via Scan.
	Publish(ctx context.Context, channel string, message []byte) error

	// Subscribe opens a cluster-wide subscription to channel, including
	// this process's own Publish calls to it. Call Subscription.Close
	// when done.
	Subscribe(ctx context.Context, channel string) (Subscription, error)

	// Close releases the store's resources — for an embedded backend this
	// shuts down the local member; for a remote-client backend it closes
	// the connection.
	Close(ctx context.Context) error
}

// KeyIterator iterates over every key in a ClusterStore.
type KeyIterator interface {
	// Next advances the iterator, returning false when exhausted.
	Next() bool
	// Key returns the current key. Only valid after a Next call that
	// returned true.
	Key() string
	// Close releases resources held by the iterator.
	Close()
}

// Subscription is a live cluster-wide pub/sub subscription.
type Subscription interface {
	// Messages delivers every payload published to the subscribed
	// channel. The channel is closed when the subscription ends (Close
	// called, or the underlying connection is lost).
	Messages() <-chan []byte
	Close() error
}
