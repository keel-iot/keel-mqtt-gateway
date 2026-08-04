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

// newReadyzHandler builds the /readyz handler. reloader is a double
// pointer because it's registered on the mux before broker.New assigns it;
// dereferencing at request time (not build time) picks up the real value.
//
// Each check is fail-closed and independently optional (nil func/rdb skips
// it): TLS cert validity, Redis primary reachability, raft leader known,
// Olric member reachable. A node that answers traffic before any of these
// are actually true would silently serve broken requests instead of
// failing the health check.
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
