package raft

import (
	"log/slog"
	"sync"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

// defaultACLCacheInterval mirrors routing.Router's default reconcile
// interval (10s) — same trade-off, same order of magnitude: ACL rules are
// low-frequency writes (see fsm.go's state docstring), so a poll this
// wide is a small staleness window in exchange for zero network I/O on
// the OnACLCheck hot path.
const defaultACLCacheInterval = 10 * time.Second

// ACLCache is a local, periodically-refreshed read cache for ACL state
// (custom roles, bindings, enabled standard rulesets), used by edge nodes
// so EvaluateACL is a local read instead of a synchronous gRPC round-trip
// to a core node on every publish/subscribe. There is no push/pub-sub
// invalidation channel (unlike internal/cluster/routing.Router) — this is
// a deliberate, explicitly accepted trade-off, not an oversight: ACL
// changes made via the management API can take up to Interval to reach
// edge nodes. See NewACLCache's doc for the fail-closed behavior this is
// paired with.
type ACLCache struct {
	fetch    func() (roles map[string]acl.Role, bindings map[string][]string, enabledRulesets []string, err error)
	interval time.Duration
	log      *slog.Logger

	mu       sync.RWMutex
	roles    map[string]acl.Role
	bindings map[string][]string
	enabled  []string
	ready    bool // true once fetch has ever succeeded

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewACLCache creates a cache that calls fetch (typically
// RemoteRegistry.ACLSnapshot) once immediately and then every interval
// (defaultACLCacheInterval if zero) in the background. Before the first
// successful fetch — and only then — EvaluateACL fails closed (deny)
// exactly like an FSM lookup that finds nothing would; once a snapshot
// has been fetched at least once, a subsequent failed refresh keeps
// serving the last known-good snapshot rather than flipping to deny, on
// the theory that a transient core-unreachable blip should degrade to
// "slightly stale ACL" rather than "reject everything" — this is the
// opposite trade-off from EvaluateACL's own transport-error handling
// (which does fail closed), called out explicitly here because it is a
// real trade-off, not a hidden one: a core outage longer than an operator
// expects means edge nodes keep enforcing a stale ruleset rather than
// locking out every device.
func NewACLCache(fetch func() (map[string]acl.Role, map[string][]string, []string, error), interval time.Duration, log *slog.Logger) *ACLCache {
	if interval <= 0 {
		interval = defaultACLCacheInterval
	}
	c := &ACLCache{
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

func (c *ACLCache) loop() {
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

func (c *ACLCache) refresh() {
	roles, bindings, enabled, err := c.fetch()
	if err != nil {
		c.log.Warn("acl cache: refresh failed, serving last known snapshot", "error", err)
		return
	}
	c.mu.Lock()
	c.roles, c.bindings, c.enabled = roles, bindings, enabled
	c.ready = true
	c.mu.Unlock()
}

// Close stops the background refresh loop.
func (c *ACLCache) Close() {
	close(c.stop)
	c.wg.Wait()
}

// EvaluateACL serves the decision entirely from the local cache — see
// FSM.evaluateACL, whose role/binding-resolution logic this mirrors
// exactly, just reading from a cached snapshot instead of live FSM state.
func (c *ACLCache) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.ready {
		return acl.Decision{Effect: acl.EffectDeny}
	}

	var enabledRoles []acl.Role
	for _, name := range c.enabled {
		if role, ok := acl.StandardRulesets[name]; ok {
			enabledRoles = append(enabledRoles, role)
		}
	}

	var custom []acl.ACLRule
	for principal, roleNames := range c.bindings {
		if principal != clientID && principal != username {
			continue
		}
		for _, rn := range roleNames {
			if role, ok := c.roles[rn]; ok {
				custom = append(custom, role.Rules...)
			}
		}
	}

	return acl.Evaluate(clientID, username, topic, action, enabledRoles, custom)
}
