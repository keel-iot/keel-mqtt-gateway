package raft

import (
	"log/slog"
	"sync"
	"time"
)

// defaultRevocationCacheInterval mirrors ACLCache's default (10s) — same
// trade-off: revocations are low-frequency writes (an external custodian's
// webhook, not a per-connect operation), so a poll this wide is a small
// staleness window in exchange for zero network I/O on the connect hot
// path.
const defaultRevocationCacheInterval = 10 * time.Second

// RevocationCache is a local, periodically-refreshed read cache for
// device cert revocations, used by edge nodes so IsRevoked is a local
// read instead of a synchronous gRPC round-trip to a core node on every
// mTLS connect. Same explicit trade-off as ACLCache: no push/pub-sub
// invalidation channel — a revocation made via the management API's
// webhook handler can take up to Interval to reach edge nodes.
//
// Fail-closed before the first successful fetch (deny every cert connect,
// same as ACLCache's pre-ready EvaluateACL) — after that, a failed
// refresh keeps serving the last known-good snapshot rather than denying
// everything, on the same "transient core-unreachable blip should degrade
// to slightly-stale, not reject-everyone" theory ACLCache documents.
//
// Scope note (deliberate, not an oversight): this cache only affects new
// connect attempts. It does not proactively scan and evict already-
// -connected clients matching a newly-revoked identity — see the design
// doc's "revoca certificati" section for that as an explicit follow-up.
type RevocationCache struct {
	fetch    func() (map[string]int64, error)
	interval time.Duration
	log      *slog.Logger

	mu      sync.RWMutex
	revoked map[string]int64
	ready   bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewRevocationCache creates a cache that calls fetch (typically
// RemoteRegistry.RevokedSnapshot) once immediately and then every
// interval (defaultRevocationCacheInterval if zero) in the background.
func NewRevocationCache(fetch func() (map[string]int64, error), interval time.Duration, log *slog.Logger) *RevocationCache {
	if interval <= 0 {
		interval = defaultRevocationCacheInterval
	}
	c := &RevocationCache{
		fetch:    fetch,
		interval: interval,
		log:      log,
		stop:     make(chan struct{}),
	}
	c.refresh()
	c.wg.Add(1)
	go c.loop()
	return c
}

func (c *RevocationCache) loop() {
	defer c.wg.Done()
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.refresh()
		}
	}
}

func (c *RevocationCache) refresh() {
	revoked, err := c.fetch()
	if err != nil {
		c.log.Warn("revocation cache: refresh failed, serving last known snapshot", "error", err)
		return
	}
	c.mu.Lock()
	c.revoked = revoked
	c.ready = true
	c.mu.Unlock()
}

// Close stops the background refresh loop.
func (c *RevocationCache) Close() {
	close(c.stop)
	c.wg.Wait()
}

// IsRevoked serves the decision entirely from the local cache. Fails
// closed (revoked=true) before the first successful fetch — see the
// type's doc for why this mirrors ACLCache's pre-ready posture.
func (c *RevocationCache) IsRevoked(identity string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.ready {
		return true
	}
	_, ok := c.revoked[identity]
	return ok
}
