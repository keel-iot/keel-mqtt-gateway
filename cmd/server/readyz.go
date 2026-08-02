package main

import (
	"context"
	"net/http"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/broker"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
)

// redisReadyzTimeout bounds the Ping below — short enough that a hung/dead
// Redis fails the probe quickly instead of stalling whatever is polling
// /readyz (kubelet, compose healthcheck, ...).
const redisReadyzTimeout = 2 * time.Second

// olricReadyzTimeout bounds the Olric probe below — same reasoning as
// redisReadyzTimeout.
const olricReadyzTimeout = 2 * time.Second

// newReadyzHandler builds the /readyz handler. reloader is a pointer to the
// (possibly not-yet-assigned) *broker.CertReloader variable so the handler
// always reads its current value, even though it's registered on the mux
// before broker.New runs — see the comment at its call site in runServer.
//
// When tlsEnabled, readiness requires a currently valid, unexpired
// certificate: a missing, unparsable, or expired cert reports NotReady
// rather than silently letting the node serve plain TCP only.
//
// When rdb is non-nil (REDIS_ADDR configured), readiness also requires
// Redis to actually be usable for QoS1/2 session persistence (see
// keel-design-doc.md's risk #6) — a node that can accept MQTT connections
// but can't talk to Redis would silently drop QoS1/2 delivery guarantees.
// Two checks, both fail-closed:
//   - currentRedisPrimary (nil outside cluster mode, otherwise
//     keelraft.Registry.CurrentRedisPrimary) must report a designated
//     primary — "don't know who's primary" is itself a not-ready reason
//     even if the client can still Ping whatever stale address it holds
//   - the Router's current client must answer a Ping — covers "primary
//     known but unreachable from this node"
//
// raftLeaderKnown and olricPing are both nil outside core role (pure edge
// nodes run neither raft nor an embedded Olric member, so there is nothing
// to gate on). On a core node, both must pass before Ready: a node that
// answers MQTT/mgmt traffic before it has rejoined raft (knows a leader) or
// before its embedded Olric member can actually route a request would
// silently serve session-ownership/routing-table reads against a node that
// isn't really back in the cluster yet — exactly the "readiness gate
// doesn't cover raft/Olric convergence after restart" gap left open in
// keel-design-doc.md.
//   - raftLeaderKnown fail-closed: no known leader (still electing, or
//     partitioned) reports NotReady rather than silently serving reads
//     against a registry that can't commit anything right now.
//   - olricPing fail-closed: a cheap Get against the local embedded member
//     (see cmd/server/main.go's wiring) — any error other than "key not
//     found" means this node's Olric member can't actually serve a
//     request yet (still joining the ring, or partitioned), even though
//     its own Start() already returned.
func newReadyzHandler(tlsEnabled bool, reloader **broker.CertReloader, rdb *redisrouter.Router, currentRedisPrimary func() (string, bool), raftLeaderKnown func() bool, olricPing func(ctx context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if tlsEnabled {
			r := *reloader
			if r == nil || !r.Ready() {
				http.Error(w, "tls: certificate not ready", http.StatusServiceUnavailable)
				return
			}
		}
		if rdb != nil {
			if currentRedisPrimary != nil {
				if _, ok := currentRedisPrimary(); !ok {
					http.Error(w, "redis: primary not yet designated", http.StatusServiceUnavailable)
					return
				}
			}
			ctx, cancel := context.WithTimeout(req.Context(), redisReadyzTimeout)
			defer cancel()
			if err := rdb.Client().Ping(ctx).Err(); err != nil {
				http.Error(w, "redis: primary unreachable", http.StatusServiceUnavailable)
				return
			}
		}
		if raftLeaderKnown != nil && !raftLeaderKnown() {
			http.Error(w, "raft: no leader known", http.StatusServiceUnavailable)
			return
		}
		if olricPing != nil {
			ctx, cancel := context.WithTimeout(req.Context(), olricReadyzTimeout)
			defer cancel()
			if err := olricPing(ctx); err != nil {
				http.Error(w, "olric: not reachable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}
