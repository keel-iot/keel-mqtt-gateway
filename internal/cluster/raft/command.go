// Package raft implements the keel MQTT cluster's control-plane consensus:
// a minimal FSM (session ownership and ACL state — see
// internal/cluster/routing for topic-filter routing, which moved off raft
// to an AP store) and cluster/quorum configuration, replicated via
// hashicorp/raft and exposed to the rest of the gateway through the
// Registry interface.
package raft

import "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"

// Op identifies the mutation encoded in a Command.
type Op string

const (
	OpClaimSession   Op = "claim_session"
	OpReleaseSession Op = "release_session"

	// ACL ops — see internal/cluster/acl for the data model and
	// evaluation engine these mutate/consult. Kept as a distinct group of
	// ops (not a new Command type) so ACL state stays in the same raft
	// group/log as session ownership, per design: both are low-frequency,
	// authoritative writes, so splitting them into a second raft group
	// would add real operational complexity (separate leader election,
	// separate log/snapshot, separate quorum) without a corresponding
	// benefit — see fsm.go's state docstring for the full rationale.
	OpCreateRole     Op = "acl.create_role"
	OpDeleteRole     Op = "acl.delete_role"
	OpCreateBinding  Op = "acl.create_binding"
	OpDeleteBinding  Op = "acl.delete_binding"
	OpEnableRuleset  Op = "acl.enable_ruleset"
	OpDisableRuleset Op = "acl.disable_ruleset"

	// OpSetRedisPrimary designates NodeID as the current primary for the
	// co-located Redis primary+replica pair on core nodes (QoS1/2 and
	// session persistence — see internal/broker/redis_session.go). Kept on
	// this same FSM/log for the same reason session ownership is: "which
	// node is primary" is a single, authoritative fact, not state a node
	// can independently derive or reconstruct — unlike the routing table
	// (Olric/AP) or the Redis address itself (gossip/NodeMeta, pure
	// configuration data, not a decision). Written only by
	// internal/cluster/membership's Redis failover loop, gated on
	// IsLeader() exactly like reconcileVotersLoop — one arbiter, no second
	// consensus mechanism.
	OpSetRedisPrimary Op = "redis.set_primary"
)

// Command is the unit of replication applied to the FSM through raft.Apply.
// Encoded as JSON on the raft log for readability during PoC debugging;
// swap for a binary encoding (e.g. msgpack) if log throughput becomes a
// bottleneck.
//
// Fields below the session-ownership pair are ACL-op payloads. A single
// flat struct (rather than a json.RawMessage sub-payload) was chosen
// because the full set of ACL ops is small, fixed, and each op only needs
// one or two extra fields — a raw-payload indirection would add a decode
// step for no real flexibility gain at this scale. Revisit if the op set
// grows much larger or payloads become polymorphic.
type Command struct {
	Op       Op     `json:"op"`
	NodeID   string `json:"node_id,omitempty"`
	ClientID string `json:"client_id,omitempty"`

	// ACL payload fields.
	RoleName    string        `json:"role_name,omitempty"`    // create_role, delete_role, create_binding, delete_binding
	Rules       []acl.ACLRule `json:"rules,omitempty"`        // create_role
	Principal   string        `json:"principal,omitempty"`    // create_binding, delete_binding
	RulesetName string        `json:"ruleset_name,omitempty"` // enable_ruleset, disable_ruleset
}
