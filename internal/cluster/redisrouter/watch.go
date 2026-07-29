package redisrouter

import (
	"context"
	"log/slog"
	"time"
)

// watchInterval controls how often WatchPrimary re-resolves the current
// primary's address and redirects if it changed. Independent of (and
// typically more frequent than) internal/cluster/membership's own
// redisFailoverInterval — that loop only runs on the raft leader and
// decides/writes the primary; this one runs on EVERY node with a Router
// (core and edge alike) and only reads/redirects, so there's no
// leader-election-safety reason to keep them in lockstep, and reacting
// promptly to an already-decided change is exactly what this loop is for.
const watchInterval = 3 * time.Second

// WatchPrimary polls currentPrimary/resolveAddr every watchInterval and
// redirects r whenever the resolved address differs from where it's
// currently pointed. Blocks until ctx is cancelled — run it in its own
// goroutine.
//
// currentPrimary and resolveAddr are function values (typically
// keelraft.Registry.CurrentRedisPrimary and
// membership.Membership.RedisAddrForNode) rather than concrete types, so
// this package stays free of a dependency on raft/membership — same
// dependency-injection posture as Router itself not knowing about raft.
func WatchPrimary(
	ctx context.Context,
	r *Router,
	currentPrimary func() (nodeID string, ok bool),
	resolveAddr func(nodeID string) (addr string, ok bool),
	log *slog.Logger,
) {
	ticker := time.NewTicker(watchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nodeID, ok := currentPrimary()
			if !ok {
				continue // no primary designated yet (e.g. cluster still bootstrapping)
			}
			addr, ok := resolveAddr(nodeID)
			if !ok {
				continue // primary's address not (yet) known via gossip — retry next tick
			}
			if addr == r.Addr() {
				continue // already pointed there — Redirect would no-op anyway, skip the log-on-error path for the common case
			}
			if err := r.Redirect(ctx, addr); err != nil {
				log.Warn("redisrouter: redirect to new primary failed", "node_id", nodeID, "addr", addr, "error", err)
				continue
			}
			log.Info("redisrouter: redirected to new primary", "node_id", nodeID, "addr", addr)
		}
	}
}
