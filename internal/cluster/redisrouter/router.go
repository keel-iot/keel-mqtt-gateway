// Package redisrouter provides a single swappable Redis client used by
// every consumer that talks to the co-located core primary+replica pair
// (QoS1/2 session persistence in internal/broker/redis_session.go, and
// tenant data-volume rate limiting in internal/forwarder/volume.go).
//
// *redis.Client is bound to a fixed address at construction — go-redis's
// only built-in failover mechanism (NewFailoverClient) requires Redis
// Sentinel, which this design deliberately doesn't use (see
// keel-design-doc.md's risk #6: failover is decided via raft.Apply, the
// same arbiter already used for session ownership, not a second consensus
// mechanism). Redirecting to a new primary after a failover therefore
// means constructing a new *redis.Client and swapping it in — Router is
// the one place that happens, instead of three separate swap sites for
// the three consumers above (a raw *redis.Client was passed to all three
// directly before this package existed).
package redisrouter

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

// clientOptions builds the redis.Options every client this package
// constructs shares. Protocol is pinned to RESP2 (go-redis defaults to
// RESP3, which negotiates via a HELLO handshake on connect): nothing this
// project's Redis usage needs (plain hash/scan commands and a Lua EVAL —
// see internal/broker/redis_session.go and internal/forwarder/volume.go)
// requires RESP3-only features (client-side caching push invalidation,
// double/big-number reply types), so the simpler, universally-supported
// wire protocol is the more conservative choice here, not just a testing
// convenience.
func clientOptions(addr, password string) *redis.Options {
	return &redis.Options{Addr: addr, Password: password, Protocol: 2}
}

// Router holds a swappable *redis.Client pointed at whichever address is
// currently the Redis primary. Safe for concurrent use: Client() may be
// called concurrently with Redirect from any goroutine.
type Router struct {
	password string

	mu     sync.RWMutex
	client *redis.Client
	addr   string
}

// New creates a Router whose initial client points at addr, verified
// reachable via Ping before returning — same eager-connectivity-check
// posture as the single *redis.Client construction it replaces (see
// cmd/server/main.go).
func New(ctx context.Context, addr, password string) (*Router, error) {
	r := &Router{password: password}
	client := redis.NewClient(clientOptions(addr, password))
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisrouter: ping %s: %w", addr, err)
	}
	r.client = client
	r.addr = addr
	return r, nil
}

// Client returns the current underlying *redis.Client. Callers should
// re-fetch via Client() for each operation rather than caching the
// returned pointer across calls that may span a failover — every existing
// consumer already calls through a field access per-request (h.rdb.HSet(...)
// etc.), so this is a drop-in replacement: change the field's type, add
// one ".Client()" at each call site.
func (r *Router) Client() *redis.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client
}

// Addr returns the address the current client is connected to.
func (r *Router) Addr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.addr
}

// Redirect points the Router at a new address, if it isn't already there.
// The new client is verified reachable via Ping before the swap — on
// failure the Router keeps using its current (old) client rather than
// switching to one that can't even connect, and the error is returned for
// the caller to log/retry. The old client is closed after the swap
// completes, asynchronously, so in-flight requests against it aren't
// aborted by the swap itself.
//
// Expected to be called by a single watcher goroutine per Router instance
// (the primary-address watcher wired in cmd/server/main.go, one per
// process) — Client() is what concurrent request-handling goroutines call,
// and that's safe against a concurrent Redirect by design (RWMutex); two
// concurrent Redirect calls racing each other isn't a scenario this design
// needs to handle, so no ordering guarantee is made beyond the mutex itself.
func (r *Router) Redirect(ctx context.Context, addr string) error {
	r.mu.RLock()
	same := addr == r.addr
	r.mu.RUnlock()
	if same {
		return nil
	}

	newClient := redis.NewClient(clientOptions(addr, r.password))
	if err := newClient.Ping(ctx).Err(); err != nil {
		_ = newClient.Close()
		return fmt.Errorf("redisrouter: ping new target %s: %w", addr, err)
	}

	r.mu.Lock()
	old := r.client
	r.client = newClient
	r.addr = addr
	r.mu.Unlock()

	go func() { _ = old.Close() }()
	return nil
}

// Close closes the current underlying client. Called once at shutdown,
// same as the single *redis.Client.Close() it replaces.
func (r *Router) Close() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client.Close()
}
