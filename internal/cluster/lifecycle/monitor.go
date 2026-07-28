package lifecycle

import (
	"context"
	"log/slog"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
)

// Monitor watches for core nodes that stop appearing in gossip membership
// and logs once their absence exceeds Threshold. It intentionally takes
// no corrective action on raft quorum membership (no raft.RemoveServer) —
// the design defers automatic quorum shrinkage to a later phase, once the
// "core node gone for good" case has been exercised manually.
//
// Routing-table cleanup for the dead node (PurgeNode) is a separate,
// lower-risk concern from quorum shrinkage — removing stale routing
// entries can't strand a live node the way a wrong RemoveServer could, so
// it's safe to wire up now, ahead of RemoveServer automation.
type Monitor struct {
	Members   func() []membership.NodeMeta
	Threshold time.Duration
	Interval  time.Duration
	Log       *slog.Logger

	// PurgeNode removes a dead core node's routing-table entries once its
	// absence exceeds Threshold (see raft.NodePurger). Optional — nil
	// preserves today's log-only behaviour.
	PurgeNode func(nodeID string) error

	lastSeen map[string]time.Time
	warned   map[string]bool
}

// NewMonitor creates a Monitor. Interval defaults to Threshold/4 (min 5s)
// when zero.
func NewMonitor(members func() []membership.NodeMeta, threshold time.Duration, log *slog.Logger) *Monitor {
	interval := threshold / 4
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	return &Monitor{
		Members:   members,
		Threshold: threshold,
		Interval:  interval,
		Log:       log,
		lastSeen:  make(map[string]time.Time),
		warned:    make(map[string]bool),
	}
}

// Run blocks, polling membership on Interval until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

func (m *Monitor) tick() {
	now := time.Now()
	present := make(map[string]struct{})
	for _, meta := range m.Members() {
		if meta.Role != membership.RoleCore {
			continue
		}
		present[meta.NodeID] = struct{}{}
		m.lastSeen[meta.NodeID] = now
		delete(m.warned, meta.NodeID)
	}

	for id, seenAt := range m.lastSeen {
		if _, ok := present[id]; ok {
			continue
		}
		if now.Sub(seenAt) > m.Threshold && !m.warned[id] {
			m.Log.Warn("lifecycle: core node heartbeat missing beyond threshold",
				"node_id", id, "last_seen", seenAt, "threshold", m.Threshold)
			m.warned[id] = true

			if m.PurgeNode != nil {
				if err := m.PurgeNode(id); err != nil {
					m.Log.Error("lifecycle: purge routing entries for dead node failed", "node_id", id, "error", err)
				} else {
					m.Log.Info("lifecycle: purged routing entries for dead node", "node_id", id)
				}
			}
		}
	}
}
