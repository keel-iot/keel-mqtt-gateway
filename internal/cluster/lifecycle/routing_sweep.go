package lifecycle

import (
	"context"
	"log/slog"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// RoutingSweep is a low-frequency safety net, not the primary routing-table
// cleanup mechanism — that's OnUnsubscribed/OnDisconnect's
// UnsubscribeBatch and Monitor's PurgeNode. It periodically compares the
// routing table's inverse index against current gossip membership and
// logs (never deletes) any node holding routing entries that gossip
// hasn't reported for longer than Threshold: evidence of an orphaned
// entry left behind by a bug or race in the primary path, worth
// investigating but not worth automatically acting on here.
type RoutingSweep struct {
	// NodesWithRoutes returns every node ID currently holding at least one
	// routing-table entry (see raft.LocalRegistry.NodesWithRoutes /
	// raft.CoreRegistry.NodesWithRoutes).
	NodesWithRoutes func() []string
	// Members returns the current gossip membership snapshot (any role —
	// routing entries can belong to edge nodes too, not just core).
	Members   func() []membership.NodeMeta
	Threshold time.Duration
	Interval  time.Duration
	Log       *slog.Logger

	absentSince map[string]time.Time
	flagged     map[string]bool
}

// NewRoutingSweep creates a RoutingSweep. Interval defaults to
// Threshold/4 (min 30s) when zero — deliberately coarser than Monitor's,
// since this is a background safety net, not a responsiveness-sensitive
// path.
func NewRoutingSweep(nodesWithRoutes func() []string, members func() []membership.NodeMeta, threshold time.Duration, log *slog.Logger) *RoutingSweep {
	interval := threshold / 4
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	return &RoutingSweep{
		NodesWithRoutes: nodesWithRoutes,
		Members:         members,
		Threshold:       threshold,
		Interval:        interval,
		Log:             log,
		absentSince:     make(map[string]time.Time),
		flagged:         make(map[string]bool),
	}
}

// Run blocks, sweeping on Interval until ctx is cancelled.
func (s *RoutingSweep) Run(ctx context.Context) {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

func (s *RoutingSweep) tick() {
	now := time.Now()
	live := make(map[string]struct{})
	for _, meta := range s.Members() {
		live[meta.NodeID] = struct{}{}
	}

	holding := make(map[string]struct{})
	for _, nodeID := range s.NodesWithRoutes() {
		holding[nodeID] = struct{}{}
	}

	for nodeID := range holding {
		if _, ok := live[nodeID]; ok {
			s.clear(nodeID)
			continue
		}
		if _, tracking := s.absentSince[nodeID]; !tracking {
			s.absentSince[nodeID] = now
			continue
		}
		if now.Sub(s.absentSince[nodeID]) > s.Threshold && !s.flagged[nodeID] {
			s.Log.Warn("lifecycle: routing table holds entries for a node absent from gossip beyond threshold",
				"node_id", nodeID, "absent_since", s.absentSince[nodeID], "threshold", s.Threshold)
			telemetry.RoutingOrphanedNodes.WithLabelValues(nodeID).Set(1)
			s.flagged[nodeID] = true
		}
	}

	// A node that no longer holds any routing entries at all (purged, or
	// its last filter was unsubscribed) needs its bookkeeping cleared too,
	// even though the loop above never visits it.
	for nodeID := range s.absentSince {
		if _, stillHolding := holding[nodeID]; !stillHolding {
			s.clear(nodeID)
		}
	}
}

func (s *RoutingSweep) clear(nodeID string) {
	if _, tracked := s.absentSince[nodeID]; !tracked {
		return
	}
	delete(s.absentSince, nodeID)
	if s.flagged[nodeID] {
		delete(s.flagged, nodeID)
		telemetry.RoutingOrphanedNodes.DeleteLabelValues(nodeID)
	}
}
