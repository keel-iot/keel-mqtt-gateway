package session

import (
	"sync"
	"time"
)

// InventoryCache keeps the latest offline-session inventory produced by the
// reconciler. Management reads use this cache instead of issuing a Redis
// scan for every dashboard refresh.
type InventoryCache struct {
	mu        sync.RWMutex
	sessions  []OfflineSession
	updatedAt time.Time
	ready     bool
}

// NewInventoryCache creates an empty cache. It becomes ready after the first
// successful inventory refresh.
func NewInventoryCache() *InventoryCache { return &InventoryCache{} }

// Set replaces the cached inventory with a defensive copy.
func (c *InventoryCache) Set(sessions []OfflineSession) {
	if c == nil {
		return
	}
	out := cloneOfflineSessions(sessions)
	c.mu.Lock()
	c.sessions = out
	c.updatedAt = time.Now().UTC()
	c.ready = true
	c.mu.Unlock()
}

// Snapshot returns a defensive copy, the refresh time, and whether at least
// one successful inventory pass has completed.
func (c *InventoryCache) Snapshot() ([]OfflineSession, time.Time, bool) {
	if c == nil {
		return nil, time.Time{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneOfflineSessions(c.sessions), c.updatedAt, c.ready
}

func cloneOfflineSessions(in []OfflineSession) []OfflineSession {
	out := make([]OfflineSession, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Subscriptions = append([]OfflineSubscription(nil), item.Subscriptions...)
	}
	return out
}
