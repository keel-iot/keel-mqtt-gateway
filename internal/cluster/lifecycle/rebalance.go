package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
)

// LiveEdgeNodes reduces a membership snapshot to the set of nodeIDs
// currently gossip-visible with the edge role — the nodeIsLiveEdge input
// Rebalance expects. Shared by the management API handler and
// RunScheduledRebalance so both filter live edge nodes the same way.
func LiveEdgeNodes(members []membership.NodeMeta) map[string]bool {
	live := make(map[string]bool)
	for _, meta := range members {
		if meta.Role == membership.RoleEdge {
			live[meta.NodeID] = true
		}
	}
	return live
}

// LiveEdgeNodeIDs is LiveEdgeNodes's slice form — the exact shape
// session.Reconciler's LiveEdgeNodeIDs function needs (see
// keel-design-doc.md's Offline Session Placement ADR, phase 6b). Order is
// not meaningful: Owner's rendezvous hash is deliberately independent of
// input order.
func LiveEdgeNodeIDs(members []membership.NodeMeta) []string {
	live := LiveEdgeNodes(members)
	ids := make([]string, 0, len(live))
	for nodeID := range live {
		ids = append(ids, nodeID)
	}
	return ids
}

// Evictor forces a client_id's connection closed on targetNodeID — see
// dataplane.Forwarder.Evict, which this is satisfied by directly. Kept as
// a narrow local interface so this package doesn't need to import
// internal/cluster/dataplane.
type Evictor interface {
	Evict(ctx context.Context, targetNodeID, clientID string) error
}

// RebalanceConfig tunes when a rebalance pass acts and how aggressively.
type RebalanceConfig struct {
	// ImbalanceThreshold: a node is only rebalanced if its connection
	// count exceeds the cluster-wide average by more than this fraction
	// (e.g. 0.20 = 20%). Avoids reacting to noise-level imbalance.
	ImbalanceThreshold float64
	// MaxEvictionsPerNode caps how many clients are evicted from a single
	// node in one pass, regardless of computed excess — bounds the size
	// of the reconnect wave a single rebalance pass can trigger.
	MaxEvictionsPerNode int
	// EvictStagger is the delay between successive Evict calls within a
	// pass. Spreads the reconnects out instead of firing them all at
	// once — see the design doc's bcrypt-storm findings on why a bursty
	// simultaneous-reconnect wave is the actual expensive case, not
	// steady-state connection count.
	EvictStagger time.Duration
	// ExcludeClientIDs are never selected for eviction (e.g. a shared
	// infra account like a bridge consumer, where a brief drop matters
	// more than for an ordinary device) — they still count toward the
	// node's connection total/average, they just aren't eviction
	// candidates. Nil/empty excludes nothing.
	ExcludeClientIDs map[string]bool
}

// DefaultRebalanceConfig mirrors the values discussed and agreed for the
// first cut of edge rebalancing (see design doc).
func DefaultRebalanceConfig() RebalanceConfig {
	return RebalanceConfig{
		ImbalanceThreshold:  0.20,
		MaxEvictionsPerNode: 50,
		EvictStagger:        200 * time.Millisecond,
	}
}

// RebalanceNodeResult reports what was (or, in a dry run, would be) done
// to a single overloaded node.
type RebalanceNodeResult struct {
	NodeID    string   `json:"node_id"`
	Before    int      `json:"before"`
	Target    int      `json:"target"`
	Evicted   []string `json:"evicted_client_ids"`
	EvictErrs []string `json:"evict_errors,omitempty"`
}

// RebalanceResult is the full outcome of one rebalance pass.
type RebalanceResult struct {
	DryRun        bool                  `json:"dry_run"`
	LiveEdgeNodes int                   `json:"live_edge_nodes"`
	TotalSessions int                   `json:"total_sessions"`
	Average       float64               `json:"average"`
	Nodes         []RebalanceNodeResult `json:"nodes"`
}

// Rebalance computes per-edge-node session counts from sessions (as
// returned by raft.Registry.SessionsSnapshot — client_id -> owning
// nodeID), filtered to nodeIsLiveEdge, and evicts a bounded, randomly
// selected subset of clients from any node exceeding the cluster-wide
// average by more than cfg.ImbalanceThreshold, bringing it back down
// toward (not below) that average.
//
// Selection within an overloaded node is random: sessions carry no
// timestamp/idleness signal today (raft FSM's state.sessions is just
// client_id -> nodeID) — see design doc backlog item on extending that
// state to allow informed (age/idleness-based) selection instead.
//
// A forced eviction only disconnects the client — it does not control
// where it reconnects (that's the external load balancer's call, same
// caveat as the design doc's "kill the loaded pod" manual lever this
// replaces). dryRun performs every step except the actual Evict calls.
func Rebalance(ctx context.Context, sessions map[string]string, nodeIsLiveEdge map[string]bool, cfg RebalanceConfig, evictor Evictor, dryRun bool, log *slog.Logger) (*RebalanceResult, error) {
	if evictor == nil && !dryRun {
		return nil, fmt.Errorf("lifecycle: rebalance: no evictor configured")
	}

	byNode := make(map[string][]string, len(nodeIsLiveEdge))
	for id, live := range nodeIsLiveEdge {
		if live {
			byNode[id] = nil
		}
	}
	total := 0
	for clientID, nodeID := range sessions {
		if _, ok := byNode[nodeID]; !ok {
			continue // not a currently-live edge node — not this pass's job
		}
		byNode[nodeID] = append(byNode[nodeID], clientID)
		total++
	}

	result := &RebalanceResult{
		DryRun:        dryRun,
		LiveEdgeNodes: len(byNode),
		TotalSessions: total,
	}
	if len(byNode) == 0 {
		return result, nil
	}

	average := float64(total) / float64(len(byNode))
	result.Average = average
	threshold := average * (1 + cfg.ImbalanceThreshold)
	target := int(math.Round(average))

	for nodeID, clients := range byNode {
		count := len(clients)
		if float64(count) <= threshold {
			continue
		}
		excess := count - target
		if excess > cfg.MaxEvictionsPerNode {
			excess = cfg.MaxEvictionsPerNode
		}
		if excess <= 0 {
			continue
		}

		evictable := clients
		if len(cfg.ExcludeClientIDs) > 0 {
			evictable = make([]string, 0, len(clients))
			for _, c := range clients {
				if !cfg.ExcludeClientIDs[c] {
					evictable = append(evictable, c)
				}
			}
		}
		if excess > len(evictable) {
			excess = len(evictable)
		}
		if excess <= 0 {
			continue
		}

		rand.Shuffle(len(evictable), func(i, j int) { evictable[i], evictable[j] = evictable[j], evictable[i] })
		toEvict := evictable[:excess]

		nodeResult := RebalanceNodeResult{
			NodeID: nodeID,
			Before: count,
			Target: target,
		}

		for i, clientID := range toEvict {
			if dryRun {
				nodeResult.Evicted = append(nodeResult.Evicted, clientID)
				continue
			}
			if err := evictor.Evict(ctx, nodeID, clientID); err != nil {
				log.Warn("lifecycle: rebalance evict failed", "node_id", nodeID, "client_id", clientID, "error", err)
				nodeResult.EvictErrs = append(nodeResult.EvictErrs, fmt.Sprintf("%s: %v", clientID, err))
				continue
			}
			nodeResult.Evicted = append(nodeResult.Evicted, clientID)

			if i < len(toEvict)-1 && cfg.EvictStagger > 0 {
				select {
				case <-ctx.Done():
					result.Nodes = append(result.Nodes, nodeResult)
					return result, ctx.Err()
				case <-time.After(cfg.EvictStagger):
				}
			}
		}

		result.Nodes = append(result.Nodes, nodeResult)
	}

	return result, nil
}

// RunScheduledRebalance blocks, calling Rebalance on every tick of
// interval until ctx is cancelled — the periodic counterpart to the
// manually-triggered POST /api/cluster/rebalance, opt-in via config (see
// cmd/server/main.go's --rebalance-schedule-* flags). sessions and
// liveEdgeNodes are called fresh on every tick, not captured once, since
// both change over the scheduler's lifetime (raft.Registry.SessionsSnapshot
// and a membership.Members()-derived LiveEdgeNodes snapshot respectively).
//
// dryRun applies to every scheduled pass uniformly — there is no
// separate "observe for N passes, then start evicting" ramp-up; running
// with dryRun=true for a while and inspecting logs before flipping it off
// is the intended way to gain confidence in a new deployment.
func RunScheduledRebalance(ctx context.Context, sessions func() map[string]string, liveEdgeNodes func() map[string]bool, cfg RebalanceConfig, evictor Evictor, dryRun bool, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := Rebalance(ctx, sessions(), liveEdgeNodes(), cfg, evictor, dryRun, log)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("lifecycle: scheduled rebalance failed", "error", err)
				continue
			}
			if len(result.Nodes) == 0 {
				log.Debug("lifecycle: scheduled rebalance: no imbalance", "live_edge_nodes", result.LiveEdgeNodes, "total_sessions", result.TotalSessions, "average", result.Average)
				continue
			}
			log.Info("lifecycle: scheduled rebalance acted", "dry_run", dryRun, "nodes", len(result.Nodes))
			for _, nr := range result.Nodes {
				log.Info("lifecycle: scheduled rebalance node", "node_id", nr.NodeID, "before", nr.Before, "target", nr.Target, "evicted", len(nr.Evicted), "evict_errors", len(nr.EvictErrs))
			}
		}
	}
}
