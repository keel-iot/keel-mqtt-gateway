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

	// QosDropped counts real QoS1/2 message loss: mochi-mqtt calls
	// OnQosDropped (see internal/broker/redis_session.go) when an inflight
	// message's QoS flow expires or is abandoned before completion — the
	// message is gone, not just delayed. Previously unobserved: that hook
	// only cleaned up Redis state, with no metric or log anywhere.
	QosDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "qos_dropped_total",
		Help:      "QoS1/2 messages whose inflight delivery expired or was abandoned — real message loss, by QoS level.",
	}, []string{"qos"})

	// RaftApplyDuration measures a single raft.Apply call on the leader
	// (internal/cluster/raft.LocalRegistry.apply), separate from
	// end-to-end connect latency (auth, gossip, network hops). result is
	// recorded even on a quorum timeout, not just on success.
	//
	// Buckets extend well past applyTimeout (2s): that timeout only bounds
	// enqueueing the command, not waiting for commit+FSM-apply, so a
	// "success" can legitimately take much longer under load — narrower
	// buckets clipped that tail into +Inf and hid it.
	RaftApplyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "keel_gateway",
		Name:      "raft_apply_duration_seconds",
		Help:      "Time spent in a single raft.Apply call on the leader, by command op and outcome.",
		Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0},
	}, []string{"op", "result"})

	// DisconnectsTotal counts MQTT disconnects by reason, so a spike in
	// evictions/expiries — not just raw disconnect volume — is visible
	// without cross-referencing logs. reason: "normal" | "expired" | "evicted".
	DisconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "disconnects_total",
		Help:      "Total MQTT disconnects, by tenant and reason.",
	}, []string{"tenant_id", "reason"})

	// MessagesForwarded counts messages this node handed off to another
	// cluster node or enqueued for an owned offline session — NOT every
	// MQTT delivery (same-node live delivery is mochi-mqtt's own in-process
	// dispatch, invisible to this hook layer). path: "cluster" | "offline".
	MessagesForwarded = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "messages_forwarded_total",
		Help:      "Messages forwarded to another cluster node or enqueued for an owned offline session, by QoS and path. Excludes same-node live delivery (mochi-mqtt's own in-process dispatch).",
	}, []string{"qos", "path"})

	// RedeliveriesTotal counts QoS1/2 resends (mochi-mqtt's OnQosPublish
	// fires with resends>0 when it reissues an unacknowledged inflight
	// message) — a proxy for flaky client connectivity or slow ACKs.
	RedeliveriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "redeliveries_total",
		Help:      "QoS1/2 inflight message resends, by QoS level.",
	}, []string{"qos"})

	// DeduplicationsTotal counts offline deliveries suppressed by
	// RedisSessionHook.MarkDelivered's PublishID dedup (see
	// internal/broker/offline_delivery_dispatch.go) — expected to be
	// nonzero mainly during an ownership handoff window (fase 6f-1's
	// deliberate brief-double-owner), not an error signal by itself.
	DeduplicationsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "deduplications_total",
		Help:      "Offline deliveries suppressed by PublishID dedup (duplicate enqueue attempt for the same publish event).",
	})

	// ReconciliationDuration measures a single reconciliation pass for
	// either periodic self-heal loop: routing.Reconciler ("routing") or
	// session.Reconciler ("offline_session"). Both already run periodically
	// regardless of this metric; this only makes their cost observable.
	ReconciliationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "keel_gateway",
		Name:      "reconciliation_duration_seconds",
		Help:      "Time spent in a single reconciliation pass, by reconciler.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
	}, []string{"reconciler"})

	// ForwardLatency and ForwardFailuresTotal measure the inter-node gRPC
	// Forward call (internal/cluster/dataplane.GRPCForwarder.Forward) —
	// the design doc's gap #3 cost (a real network hop per cross-node
	// publish), made observable rather than only ever inferred from CPU.
	ForwardLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "keel_gateway",
		Name:      "cluster_forward_latency_seconds",
		Help:      "Latency of a single inter-node gRPC Forward call.",
		Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0},
	})
	ForwardFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "cluster_forward_failures_total",
		Help:      "Inter-node gRPC Forward calls that returned an error (unreachable peer, unknown node, timeout).",
	})

	// StorageFailoversTotal counts completed Redis primary promotions
	// (internal/cluster/membership's failoverRedisPrimary) — expected to be
	// zero in steady state; any increment means a primary was missing
	// beyond redisPrimaryDeadThreshold and a replica was promoted.
	StorageFailoversTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "keel_gateway",
		Name:      "storage_failovers_total",
		Help:      "Completed Redis primary→replica failover promotions.",
	})
)
