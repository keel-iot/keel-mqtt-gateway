package broker

import (
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// rateLimiterTTL is how long an idle per-key bucket survives before the
// periodic sweep reclaims it. Without this, a connect-attempt flood from
// ever-changing source IPs (or tenant churn on the publish side) would
// turn the limiter's own bookkeeping into unbounded memory growth — the
// exact opposite of what an anti-abuse control is for.
const rateLimiterTTL = 10 * time.Minute

const rateLimiterSweepInterval = time.Minute

type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// keyedRateLimiter enforces an independent token-bucket limit per key —
// source IP for connect attempts, tenant ID for publishes — entirely
// in-memory and local to this node. See ROADMAP.md's rate-limiting entry
// for why this is deliberately not cluster-coordinated: it's a
// security/operational protection, not a billing-grade exact global
// quota, so no Raft/Redis on the publish hot path.
type keyedRateLimiter struct {
	limit rate.Limit
	burst int
	ttl   time.Duration

	mu      sync.Mutex
	entries map[string]*rateLimiterEntry

	stop chan struct{}
}

// newKeyedRateLimiter starts the periodic sweep goroutine immediately;
// call close to stop it. Callers only construct one once the pair of
// config values has already been validated as "enabled" (see
// config.Load) — this trusts that, the same posture MaxKeepAliveHook
// takes toward its own caller-validated input.
func newKeyedRateLimiter(perSec float64, burst int, ttl, sweepInterval time.Duration) *keyedRateLimiter {
	l := &keyedRateLimiter{
		limit:   rate.Limit(perSec),
		burst:   burst,
		ttl:     ttl,
		entries: make(map[string]*rateLimiterEntry),
		stop:    make(chan struct{}),
	}
	go l.sweepLoop(sweepInterval)
	return l
}

func (l *keyedRateLimiter) sweepLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.sweep(time.Now())
		}
	}
}

func (l *keyedRateLimiter) sweep(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, e := range l.entries {
		if now.Sub(e.lastSeen) > l.ttl {
			delete(l.entries, key)
		}
	}
}

// allow reports whether an event for key is within its rate limit,
// creating a fresh bucket on first use. A nil receiver always allows —
// lets call sites skip a separate "is this limiter even configured"
// check, matching the "absent means untouched" convention used
// throughout this package.
func (l *keyedRateLimiter) allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[key]
	if !ok {
		e = &rateLimiterEntry{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.entries[key] = e
	}
	e.lastSeen = time.Now()
	return e.limiter.Allow()
}

// entryCount reports the number of live per-key buckets. Test-only
// visibility into sweep behavior.
func (l *keyedRateLimiter) entryCount() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *keyedRateLimiter) close() {
	if l == nil {
		return
	}
	close(l.stop)
}

// remoteIP extracts the host portion of a net.Conn-style "ip:port"
// address (e.g. cl.Net.Remote). Falls back to the raw string on parse
// failure — still a usable, bounded rate-limit key, just potentially one
// bucket per distinct port instead of per IP; RemoteAddr().String() on a
// real connection never actually fails to parse, this is only a
// defensive fallback.
func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
