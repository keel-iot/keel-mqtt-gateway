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
// offline session's topic filters and calls Place wherever that's
// drifted from what's currently registered. Only the ownership pointer
// moves; session data (inflight, subscriptions) is never touched here.
//
// Place should be idempotent — reconcileOnce only calls it on a real
// diff, but callers shouldn't rely on that alone.
type Reconciler struct {
	// Inventory returns the current set of offline sessions needing
	// placement.
	Inventory func() ([]OfflineSession, error)

	// LiveEdgeNodeIDs returns the current live edge nodes, feeding
	// Owner's hash. This package never imports membership itself.
	LiveEdgeNodeIDs func() []string

	// CurrentOwner returns who's currently registered for clientID's
	// filter, if anyone.
	CurrentOwner func(clientID, filter string) (nodeID string, ok bool)

	// Place registers newOwner for clientID's filter.
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
