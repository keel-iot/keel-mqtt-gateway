package membership

import "encoding/json"

// Role distinguishes core (raft quorum) nodes from edge (stateless MQTT
// terminator) nodes in gossip metadata.
type Role string

const (
	RoleCore Role = "core"
	RoleEdge Role = "edge"
)

// NodeMeta is gossiped by every member (core and edge) via memberlist's
// per-node metadata so peers can discover each other's role and RPC
// addresses without a separate directory service.
type NodeMeta struct {
	NodeID    string `json:"node_id"`
	Role      Role   `json:"role"`
	RaftAddr  string `json:"raft_addr,omitempty"`  // core only
	GRPCAddr  string `json:"grpc_addr"`            // registry + dataplane RPC
	OlricAddr string `json:"olric_addr,omitempty"` // core only — embedded Olric member's own internal gossip address, used to seed its peer list (see internal/cluster/store)
	// OlricClientAddr is core only — the embedded Olric member's main
	// protocol address (distinct from OlricAddr's gossip port), used by
	// edge nodes to build a thin store.NewRemoteOlricStore client (see
	// internal/cluster/raft.EdgeRegistry).
	OlricClientAddr string `json:"olric_client_addr,omitempty"`

	// RedisAddr is core only — the host:port of the Redis instance
	// co-located with this core node (primary+replica pair for QoS1/2 and
	// session persistence, see internal/broker/redis_session.go). Pure
	// configuration data (the "where"), gossiped like OlricAddr/GRPCAddr —
	// deliberately NOT accompanied by a role field: "which node is primary
	// right now" (the "who") is a single authoritative fact decided via
	// raft.Apply (see internal/cluster/raft's OpSetRedisPrimary), not
	// something gossip's eventually-consistent, best-effort propagation is
	// safe to be the source of truth for.
	RedisAddr string `json:"redis_addr,omitempty"`

	// HTTPAddr is edge-only (also set on a "combined" node, which gossips
	// as core but still runs a local broker — see cmd/server/main.go's
	// brokerRuntimeEnabled) — the metrics-server address (/api/live/stats,
	// /api/live/clients, alongside the existing /healthz/readyz/metrics)
	// this node's local broker state is queryable on. Used by the core
	// management API to aggregate live connection/message stats across
	// every edge into GET /api/metrics — see internal/cluster/management.
	HTTPAddr string `json:"http_addr,omitempty"`
}

func (m NodeMeta) encode() []byte {
	b, _ := json.Marshal(m) // NodeMeta fields are all plain strings; Marshal cannot fail
	return b
}

func decodeMeta(b []byte) (NodeMeta, error) {
	var m NodeMeta
	err := json.Unmarshal(b, &m)
	return m, err
}
