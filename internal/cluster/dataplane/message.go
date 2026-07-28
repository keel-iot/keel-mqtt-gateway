// Package dataplane carries MQTT message payloads directly between nodes,
// kept off the raft log (which only replicates routing/session control
// state — see internal/cluster/raft). Phase 1 implements Forwarder with
// point-to-point gRPC; the interface is designed so an embedded-NATS
// implementation can be dropped in later without touching callers.
package dataplane

// Message is a single MQTT publish being routed to a remote node because
// that node owns a client subscribed to the topic (per
// raft.Registry.NodesFor).
type Message struct {
	SourceNodeID string
	TenantID     string
	Topic        string
	Payload      []byte
	QoS          byte
}
