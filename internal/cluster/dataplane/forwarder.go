package dataplane

import "context"

// Forwarder moves MQTT message payloads between nodes on the data plane,
// separate from the raft control plane. Forward is called by the
// publishing node once it knows (via raft.Registry.NodesFor) which other
// nodes own a subscriber for the topic; Subscribe registers the local
// callback invoked whenever another node forwards a message to this node.
//
// Kept as an interface so a future NATS-backed implementation (embedded
// nats-server, no external dependency) can replace GRPCForwarder without
// changing broker/hooks.go or any other caller.
type Forwarder interface {
	Forward(ctx context.Context, targetNodeID string, msg *Message) error
	Subscribe(handler func(*Message)) error

	// Evict tells targetNodeID to locally disconnect clientID — sent by
	// the node that just won a raft.Registry.ClaimSession takeover to the
	// node that previously owned the session (see
	// internal/broker/hooks.go's OnConnectAuthenticate). Best-effort: a
	// failed Evict does not block or fail the new connection: the old
	// connection's own MQTT keepalive is the backstop.
	Evict(ctx context.Context, targetNodeID, clientID string) error
	// SubscribeEvict registers the local handler invoked whenever another
	// node calls Evict against this one. Only one handler is supported,
	// same as Subscribe.
	SubscribeEvict(handler func(clientID string)) error
}
