package routing

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	mochimqtt "github.com/mochi-mqtt/server/v2"

	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// defaultReconcilerInterval is how often Reconciler checks whether this
// node's live local subscriptions are still reflected in the distributed
// routing store.
const defaultReconcilerInterval = 20 * time.Second

// LocalSubscriptions returns the de-duplicated set of topic filters any
// currently-connected client on srv is subscribed to, read directly from
// mochi-mqtt's own per-client state (Client.State.Subscriptions) — this is
// the node's ground truth for "what my own clients actually want to
// receive", entirely independent of (and unaffected by data loss in) the
// distributed routing store Router mirrors locally. Reused here rather than
// duplicated in a second map, per the same "don't track state twice"
// principle Router itself already follows for the store-derived side.
func LocalSubscriptions(srv *mochimqtt.Server) []string {
	seen := make(map[string]struct{})
	for _, cl := range srv.Clients.GetAll() {
		for filter := range cl.State.Subscriptions.GetAll() {
			seen[filter] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	return out
}

// registry is the narrow slice of raft.Registry's method set Reconciler
// needs — defined locally (rather than importing internal/cluster/raft) to
// avoid a raft -> routing -> raft import cycle, since both CoreRegistry and
// EdgeRegistry already live in the raft package and import routing.
type registry interface {
	Subscribe(topic, nodeID string) error
	TopicsForNode(nodeID string) []string
}

// Reconciler is the mechanism described in the package doc's "total data
// loss" scenario: if the routing store (Olric) loses all of its state —
// e.g. restarted without persistence, or an operator wipes its dmaps —
// while this node's MQTT clients stay connected the whole time, nothing
// else would ever re-publish their subscriptions: they never send SUBSCRIBE
// again, so OnSubscribed never fires again. Reconciler periodically compares
// LocalSubscriptions (ground truth) against Registry.TopicsForNode (this
// node's view of the store, refreshed by Router's own periodic Scan
// reconcile) and re-asserts (Registry.Subscribe) any filter that's live
// locally but missing from the store — rebuilding the routing table without
// requiring any client to reconnect or re-subscribe.
//
// Safe to call Subscribe redundantly: Router.Subscribe is a plain Put plus a
// pub/sub event, both idempotent, so a false-positive "missing" read (e.g.
// racing Router's own reconcile loop right after a real SUBSCRIBE) just
// costs a harmless extra write, never a correctness problem.
//
// Jittered per-tick (not a fixed ticker) so that many edges detecting the
// same cluster-wide reset at roughly the same moment don't all hit the
// store's Publish/Put path in the same instant — a thundering herd of
// otherwise-identical re-assert writes right when the store is least likely
// to be fully healthy yet.
type Reconciler struct {
	// Server is this node's local mochi-mqtt server — the source of
	// LocalSubscriptions.
	Server *mochimqtt.Server
	// Registry is this node's routing Registry (CoreRegistry or
	// EdgeRegistry) — both already satisfy this interface.
	Registry registry
	// NodeID is this node's cluster node ID.
	NodeID string
	// Interval is the base check interval (before jitter). Default 20s —
	// deliberately a bit longer than Router's own 10s reconcile interval,
	// so by the time Reconciler checks, a real store reset has already had
	// a chance to show up in TopicsForNode.
	Interval time.Duration
	Log      *slog.Logger
}

// Run blocks, checking on a jittered interval until ctx is done.
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
			r.reconcileOnce()
		}
	}
}

// jitter returns a duration randomized within +/-25% of d, so concurrently
// running Reconcilers don't stay in lockstep — see the Reconciler doc
// comment's thundering-herd rationale.
func jitter(d time.Duration) time.Duration {
	spread := float64(d) * 0.25
	offset := (rand.Float64()*2 - 1) * spread // uniform in [-spread, +spread]
	return d + time.Duration(offset)
}

func (r *Reconciler) reconcileOnce() {
	start := time.Now()
	defer func() {
		telemetry.ReconciliationDuration.WithLabelValues("routing").Observe(time.Since(start).Seconds())
	}()

	local := LocalSubscriptions(r.Server)
	if len(local) == 0 {
		return
	}

	known := r.Registry.TopicsForNode(r.NodeID)
	knownSet := make(map[string]struct{}, len(known))
	for _, t := range known {
		knownSet[t] = struct{}{}
	}

	var missing []string
	for _, t := range local {
		if _, ok := knownSet[t]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return
	}

	r.logWarn("routing: reconciler found locally-live filters missing from the cluster routing table — re-asserting (likely a routing-store reset)", "node_id", r.NodeID, "count", len(missing))
	for _, t := range missing {
		if err := r.Registry.Subscribe(t, r.NodeID); err != nil {
			r.logWarn("routing: reconciler re-assert failed", "topic", t, "error", err)
		}
	}
}

func (r *Reconciler) logWarn(msg string, args ...any) {
	if r.Log != nil {
		r.Log.Warn(msg, args...)
	}
}
