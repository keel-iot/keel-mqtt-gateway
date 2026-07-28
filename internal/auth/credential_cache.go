package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// credentialCache caches recent successful password validations to avoid
// repeated bcrypt operations during reconnect storms. It is security-safe:
//   - Only caches successful validations
//   - Cache key is deviceID + SHA-256(provided_token), never storing plaintext
//   - Wrong credentials always hit bcrypt (no false positives)
//   - Expired entries fall back to bcrypt verification (fail-safe)
type credentialCache struct {
	mu    sync.RWMutex
	cache map[string]time.Time // key → timestamp of successful validation
	ttl   time.Duration
}

// newCredentialCache creates a cache with the given TTL.
// Recommended TTL: 30-60 seconds — long enough to cover a reconnect storm,
// short enough that revoked credentials force re-verification within TTL.
func newCredentialCache(ttl time.Duration) *credentialCache {
	return &credentialCache{
		cache: make(map[string]time.Time),
		ttl:   ttl,
	}
}

// check returns true if the (deviceID, token) pair was successfully validated
// within the TTL window. The token is hashed with SHA-256 to avoid storing
// plaintext credentials in memory.
func (c *credentialCache) check(deviceID, token string) bool {
	key := cacheKey(deviceID, token)

	c.mu.RLock()
	ts, ok := c.cache[key]
	c.mu.RUnlock()

	if !ok {
		return false
	}

	// Check if entry is still within TTL
	return time.Since(ts) < c.ttl
}

// set records a successful validation for the (deviceID, token) pair.
// Only called after bcrypt verification succeeds.
func (c *credentialCache) set(deviceID, token string) {
	key := cacheKey(deviceID, token)

	c.mu.Lock()
	c.cache[key] = time.Now()
	c.mu.Unlock()
}

// invalidate removes all cache entries, forcing all subsequent validations
// to go through full bcrypt verification. Called when the credential file
// is reloaded.
func (c *credentialCache) invalidate() {
	c.mu.Lock()
	c.cache = make(map[string]time.Time)
	c.mu.Unlock()
}

// cacheKey computes the cache key as deviceID + ":" + SHA-256(token).
// SHA-256 is fast (~1µs) and collision-resistant, sufficient for cache lookup.
// The hash is NOT a security substitute for bcrypt — it's only used to avoid
// storing the plaintext token in memory.
func cacheKey(deviceID, token string) string {
	h := sha256.Sum256([]byte(token))
	return deviceID + ":" + hex.EncodeToString(h[:])
}
