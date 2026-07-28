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
