// OfflineSessionReconciler — see this package's doc and
// keel-design-doc.md's "Offline Session Placement" ADR. Phase 4 of 6:
// same shape as internal/cluster/routing.Reconciler and
// internal/cluster/raft.ACLCache (periodic, jittered, idempotent
// re-assert of drifted state) — not wired into anything yet. All
// dependencies are injected functions rather than concrete types (no
// import of internal/cluster/membership, redisrouter, or routing here),
// so this is testable with fakes, no real Redis/Olric/gossip needed —
// same reasoning as ACLCache/routing.Reconciler's own tests.
package session

import (
	"context"
	"log/slog"
	"math/rand"
	"time"
)

// defaultReconcilerInterval mirrors routing.Reconciler's own default
// (20s) — same trade-off: offline-session ownership is low-frequency
// (only membership changes move it), so a poll this wide is a small
// staleness window in exchange for no per-publish cost.
const defaultReconcilerInterval = 20 * time.Second

// Reconciler recomputes, on every tick, which edge node should own each
// known offline session's topic filters (Owner, over the current live
// edge list) and calls Place for any filter whose owner has drifted from
// what's currently registered — e.g. because the live edge list changed
// since the last tick. Only the ownership *pointer* moves; the session's
// actual data (inflight, subscriptions) is never read or written here —
// see the design doc's "rebalance quasi gratuito".
//
// Safe to call Place redundantly for a filter whose owner didn't
// actually change (reconcileOnce only calls it when CurrentOwner
// disagrees with the freshly-computed one, but a caller wiring this up
// should still make Place idempotent, same posture as
// routing.Reconciler's Subscribe re-assert).
type Reconciler struct {
	// Inventory returns the current set of offline sessions that need
	// placement — e.g. backed by the same Redis client hash
	// RedisSessionHook.StoredClients/StoredSubscriptions already read
	// today, but consulted only here, on a poll, not loaded eagerly by
	// every edge pod at boot (see design doc phase 2).
	Inventory func() ([]OfflineSession, error)

	// LiveEdgeNodeIDs returns the current live edge node list — the
	// single source of truth for Owner's hash. The caller supplies this
	// (typically membership.Membership.Members(), filtered to the edge
	// role) — this package never imports membership itself.
	LiveEdgeNodeIDs func() []string

	// CurrentOwner returns who is currently registered as responsible
	// for clientID's filter (e.g. routing.Router's existing
	// topic-ownership registration), and whether any registration
	// exists at all.
	CurrentOwner func(clientID, filter string) (nodeID string, ok bool)

	// Place registers newOwner as responsible for clientID's filter —
	// called only when it differs from (or is absent from)
	// CurrentOwner's answer.
	Place func(clientID, filter, newOwner string) error

	// Interval is the base check interval (before jitter). Default 20s.
	Interval time.Duration
	Log      *slog.Logger
}

// Run blocks, reconciling on a jittered interval until ctx is done — same
// thundering-herd rationale as routing.Reconciler.Run.
func (r *Reconciler) Run(ctx context.Context) {
	interval := r.Interval
	if interval <= 0 {
		interval = defaultReconcilerInterval
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(interval)):
			r.ReconcileOnce()
		}
	}
}

// jitter returns a duration randomized within +/-25% of d — mirrors
// routing.Reconciler's own jitter, duplicated rather than imported: five
// lines, not worth a cross-package dependency for it.
func jitter(d time.Duration) time.Duration {
	spread := float64(d) * 0.25
	offset := (rand.Float64()*2 - 1) * spread // uniform in [-spread, +spread]
	return d + time.Duration(offset)
}

// ReconcileOnce runs a single reconciliation pass — exported so callers
// (and tests) can trigger it deterministically instead of waiting for
// Run's ticker.
func (r *Reconciler) ReconcileOnce() {
	liveEdges := r.LiveEdgeNodeIDs()
	if len(liveEdges) == 0 {
		r.logWarn("session: reconciler found no live edge nodes — skipping this pass")
		return
	}

	sessions, err := r.Inventory()
	if err != nil {
		r.logWarn("session: reconciler inventory fetch failed", "error", err)
		return
	}

	moved := 0
	for _, s := range sessions {
		newOwner, ok := Owner(s.ClientID, liveEdges)
		if !ok {
			continue // unreachable given the liveEdges check above, kept for clarity
		}
		for _, sub := range s.Subscriptions {
			current, known := r.CurrentOwner(s.ClientID, sub.Filter)
			if known && current == newOwner {
				continue
			}
			if err := r.Place(s.ClientID, sub.Filter, newOwner); err != nil {
				r.logWarn("session: reconciler place failed", "client_id", s.ClientID, "filter", sub.Filter, "owner", newOwner, "error", err)
				continue
			}
			moved++
		}
	}
	if moved > 0 {
		r.logInfo("session: reconciler moved offline-session ownership", "count", moved)
	}
}

func (r *Reconciler) logWarn(msg string, args ...any) {
	if r.Log != nil {
		r.Log.Warn(msg, args...)
	}
}

func (r *Reconciler) logInfo(msg string, args ...any) {
	if r.Log != nil {
		r.Log.Info(msg, args...)
	}
}
