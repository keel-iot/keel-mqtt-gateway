package telemetry

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SessionsLive, RoutingEntries and InflightMessages are core-only cluster
// gauges, sampled periodically by RunClusterStatsSampler — same pattern as
// EdgeLoadScore's sampler (loadscore.go), just on the core side.
//
// ClusterMembers is set directly, event-driven, from
// internal/cluster/membership's NotifyJoin/NotifyLeave — membership changes
// are already discrete events there, so a poll would only add staleness
// with no benefit.
var (
	SessionsLive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "sessions_live",
		Help:      "Cluster-wide count of raft-claimed live MQTT sessions (core-only, sampled).",
	})
	// SessionsOffline is set directly by internal/session.Reconciler on
	// every reconciliation pass (it already computes the inventory size),
	// rather than sampled separately here.
	SessionsOffline = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "sessions_offline",
		Help:      "Cluster-wide count of persistent offline sessions awaiting delivery, per the last offline-session reconciliation pass.",
	})
	RoutingEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "cluster_routing_entries",
		Help:      "Number of distinct topic filters currently present in the cluster routing table (core-only, sampled).",
	})
	InflightMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "inflight_messages",
		Help:      "Fleet-wide count of QoS1/2 inflight messages persisted in Redis (live inflight + offline-queued, same underlying store — core-only, sampled).",
	})
	ClusterMembers = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "cluster_members",
		Help:      "Current number of gossip-visible cluster members (core+edge).",
	})

	// MembershipChangesTotal counts gossip join/leave events. type: "join" | "leave".
	MembershipChangesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "cluster_membership_changes_total",
		Help:      "Total gossip membership changes, by type.",
	}, []string{"type"})
)

// RunClusterStatsSampler periodically recomputes SessionsLive,
// RoutingEntries and InflightMessages until ctx is done. Core-only — callers
// should not start this on a pure edge node. inflight may be nil (e.g. no
// Redis session persistence configured), in which case InflightMessages is
// left unset rather than sampled as zero.
func RunClusterStatsSampler(ctx context.Context, sessionsLive func() int, routingEntries func() int, inflight func() (int64, error), interval time.Duration, log *slog.Logger) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	tick := func() {
		if sessionsLive != nil {
			SessionsLive.Set(float64(sessionsLive()))
		}
		if routingEntries != nil {
			RoutingEntries.Set(float64(routingEntries()))
		}
		if inflight != nil {
			n, err := inflight()
			if err != nil {
				if log != nil {
					log.Warn("telemetry: inflight messages sample failed", "error", err)
				}
			} else {
				InflightMessages.Set(float64(n))
			}
		}
	}
	tick()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}
