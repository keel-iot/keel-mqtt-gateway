// Package telemetry provides Prometheus metrics and OpenTelemetry tracing for
// keel-gateway.
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Gauge and counter vectors keyed by tenant_id so operators can identify
// per-tenant resource usage from a single scrape.

var (
	// ActiveConnections is the number of currently active MQTT device connections.
	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "active_connections",
		Help:      "Number of currently active MQTT device connections.",
	}, []string{"tenant_id"})

	// ConnectionsTotal counts all MQTT connection attempts with their outcome.
	// result: "success" | "auth_failed" | "rate_limited"
	ConnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "connections_total",
		Help:      "Total MQTT connection attempts.",
	}, []string{"tenant_id", "result"})

	// MessagesPublished counts messages published by devices.
	// qos: "0" | "1" | "2"
	MessagesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "messages_published_total",
		Help:      "Total messages published by devices to the gateway.",
	}, []string{"tenant_id", "qos"})

	// BytesPublished counts bytes published by devices.
	BytesPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "bytes_published_total",
		Help:      "Total payload bytes published by devices.",
	}, []string{"tenant_id"})

	// CommandsDelivered counts commands sent from the platform to devices.
	// result: "delivered" | "device_offline" | "dropped"
	CommandsDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "commands_delivered_total",
		Help:      "Total platform→device commands.",
	}, []string{"tenant_id", "result"})

	// AuthDuration measures the time spent in OnConnectAuthenticate.
	// method: "password" | "jwt" | "certificate"
	AuthDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "keel_gateway",
		Name:      "auth_duration_seconds",
		Help:      "Time spent authenticating device connections.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
	}, []string{"tenant_id", "method"})

	// AutoProvisioningTotal counts auto-provisioning attempts triggered by
	// first-connect X.509 certificate authentication.
	// result: "started" | "skipped_exists" | "error"
	AutoProvisioningTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "auto_provisioning_total",
		Help:      "Device auto-provisioning attempts via X.509 first-connect.",
	}, []string{"tenant_id", "result"})

	// DataVolumeLimitExceeded counts messages dropped because the tenant has
	// reached their daily data-volume quota (max_bytes_per_day).
	DataVolumeLimitExceeded = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "data_volume_limit_exceeded_total",
		Help:      "Messages rejected because the tenant daily data-volume limit was reached.",
	}, []string{"tenant_id"})

	// RoutingOrphanedNodes is set to 1 for a node_id when the periodic
	// routing-table safety sweep (internal/cluster/lifecycle.RoutingSweep)
	// finds entries for a node absent from gossip membership beyond its
	// threshold. Observability only — the sweep never deletes anything
	// itself; see RoutingSweep's doc.
	RoutingOrphanedNodes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "keel_gateway",
		Name:      "cluster_routing_orphaned_nodes",
		Help:      "Set to 1 per node_id when the routing-table safety sweep finds entries for a node absent from gossip beyond threshold.",
	}, []string{"node_id"})

	// ForwarderDropped counts messages dropped from the output-connector
	// buffer (see internal/connector.BufferedConnector) — either because the
	// bounded buffer was full (drop-oldest) or because forwarding to the
	// downstream connector kept failing after retries. Never a silent drop.
	ForwarderDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "forwarder_dropped_total",
		Help:      "Messages dropped from the output-connector forwarder buffer, by reason.",
	}, []string{"connector", "reason"})

	// RaftApplyDuration measures the time a single raft.Apply takes on the
	// leader (internal/cluster/raft.LocalRegistry.apply) — the control-plane
	// cost keel-design-doc.md's PoC checklist asks to isolate, distinct from
	// end-to-end connect latency (which also includes auth, gossip lookups,
	// network hops). op: "claim_session" | "release_session" | "set_redis_primary"
	// | "acl.*". result: "success" | "error" — a raft.Apply that times out
	// waiting for quorum still completes (it returns an error), so duration
	// is recorded either way, not just on success.
	//
	// Buckets extend to 30s, well past applyTimeout (registry.go, 2s):
	// found under a real 1500-device reconnect storm that applyTimeout only
	// bounds how long raft.Apply blocks trying to ENQUEUE the command (per
	// hashicorp/raft's own semantics) — once enqueued, waiting for actual
	// commit+FSM-apply completion is unbounded, so a "success" result can
	// legitimately take much longer than applyTimeout under heavy
	// concurrent load. Narrower buckets silently clipped that tail into a
	// single +Inf bucket, hiding exactly the number this metric exists to
	// surface.
	RaftApplyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "keel_gateway",
		Name:      "raft_apply_duration_seconds",
		Help:      "Time spent in a single raft.Apply call on the leader, by command op and outcome.",
		Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0},
	}, []string{"op", "result"})
)
