package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	hraft "github.com/hashicorp/raft"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// applyTimeout bounds how long a single raft.Apply is allowed to take
// before the calling MQTT hook gives up.
const applyTimeout = 2 * time.Second

// Registry is the interface the MQTT broker hooks depend on. Core nodes
// satisfy it with CoreRegistry (routing methods delegate to
// internal/cluster/routing.Router, an Olric-backed AP store; session
// methods delegate to raft via LocalRegistry). Edge nodes satisfy it with
// a gRPC client (see remote_registry.go) that forwards calls to a core
// node — neither the hooks nor the rest of the gateway need to know which
// backend is behind either half.
//
// EvaluateACL is part of the base interface (not an optional capability
// like BatchUnsubscriber/NodePurger below) because, unlike those, it must
// be callable from *every* node: OnACLCheck runs wherever a client
// connects, and both core and edge nodes run the full broker/hooks stack.
// ACL mutation commands (CreateRole, etc.) stay off this interface —
// they're only needed by the management API/CLI, which per design targets
// a core node directly rather than routing through the broker hook path,
// so they're exposed as an optional capability (see ACLAdmin below)
// instead of bloating the interface every Registry implementation must
// satisfy.
type Registry interface {
	Subscribe(topic, nodeID string) error
	Unsubscribe(topic, nodeID string) error
	// NodesFor returns the node IDs that must receive a message published
	// on topic. localNodeID is the caller's own node ID: for a
	// shared-subscription group whose only member matching topic is on
	// localNodeID, that node is excluded from the result (its own
	// mochi-mqtt instance already delivers to that local client) and,
	// when the group has other members, exactly one of those is included
	// instead — preserving MQTT5's exactly-once-per-group delivery
	// across the cluster. Non-shared subscriptions are unaffected: the
	// full matching set is always returned.
	NodesFor(topic, localNodeID string) []string
	// OfflineNodesFor returns the node IDs owning at least one offline
	// session matching topic — the Offline Routing Index, kept distinct
	// from NodesFor's live-subscriber result (see routing.Router's doc).
	OfflineNodesFor(topic string) []string
	// OwnedClientIDs returns every clientID this node currently owns at
	// least one offline-session filter for — the Edge Ownership Index, a
	// filtered read over the same entries Subscribe/Unsubscribe above
	// already maintain precisely, not a separate write.
	OwnedClientIDs(nodeID string) []string
	// ClaimSession records nodeID as clientID's owner (new connection
	// always wins — see fsm.go's OpClaimSession). evictedFrom is the
	// previous owner's node ID when it differs from nodeID, empty
	// otherwise; the caller must tell that node to locally disconnect
	// the client (see dataplane.Forwarder.Evict).
	ClaimSession(clientID, nodeID string) (evictedFrom string, err error)
	// ReleaseSession clears clientID's ownership, but only if nodeID is
	// still the current owner — a release from a node that has already
	// been superseded by a newer ClaimSession is a safe no-op.
	ReleaseSession(clientID, nodeID string) error
	EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision
	// IsRevoked reports whether identity ("<deviceID>@<tenantID>", matches
	// a device cert's CN) has been revoked — part of the base interface
	// for the same reason EvaluateACL is: every node that terminates
	// mTLS connections (core and edge, wherever CertAuthEnabled) needs
	// this on every connect, not just core.
	IsRevoked(identity string) bool
	// CurrentRedisPrimary returns the nodeID currently designated primary
	// for the co-located Redis pair (see fsm.go's OpSetRedisPrimary), if
	// one has ever been set. Part of the base interface (like
	// EvaluateACL, not an optional capability) because every node running
	// an internal/cluster/redisrouter.Router — core or edge — needs it to
	// redirect after a failover, the same way every node needs
	// EvaluateACL on every connect/publish.
	CurrentRedisPrimary() (nodeID string, ok bool)
}

// ACLAdmin is an optional Registry extension for ACL mutations (role/
// binding/ruleset management) and their corresponding reads — used by
// internal/cluster/management's REST API and the `keel-gateway acl` CLI
// subcommand. Deliberately not part of Registry: unlike EvaluateACL
// (a read every OnACLCheck needs, from every node), mutations are rare
// administrative operations that the management API already requires
// targeting a core node for (see internal/cluster/management's package
// doc — mounted only on core nodes), so there's no need for edge nodes to
// implement this. Implemented by CoreRegistry (delegates to
// LocalRegistry, with the same leader-forwarding fallback used by
// ClaimSession/ReleaseSession).
type ACLAdmin interface {
	CreateRole(name string, rules []acl.ACLRule) error
	DeleteRole(name string) error
	CreateBinding(principal, roleName string) error
	DeleteBinding(principal, roleName string) error
	EnableRuleset(name string) error
	DisableRuleset(name string) error
	RolesSnapshot() map[string]acl.Role
	BindingsSnapshot() map[string][]string
	EnabledRulesetsSnapshot() []string
}

// RevocationAdmin is an optional Registry extension for recording a
// device cert revocation — used by internal/cluster/management's
// revocation webhook handler. Deliberately not part of Registry, same
// rationale as ACLAdmin: a rare administrative write (fed by an external
// custodian's webhook, which per design targets a core node), not
// something every node needs to implement. Implemented by CoreRegistry
// (delegates to LocalRegistry, with the same leader-forwarding fallback
// used by the ACL mutations above).
type RevocationAdmin interface {
	RevokeCertificate(identity, serial string) error
	RevokedSnapshot() map[string]int64
}

// BatchUnsubscriber is an optional Registry extension for removing many
// filters for one node in a single call, instead of one call per filter.
// Deliberately not part of Registry: no gRPC RPC backs it, so
// RemoteRegistry (edge nodes) does not implement it — routing-table
// mutations always originate from core nodes in this design. Callers type
// -assert for it and fall back to a per-filter Unsubscribe loop when it's
// absent (see internal/broker/hooks.go's OnDisconnect). Implemented by
// CoreRegistry (delegates to routing.Router).
type BatchUnsubscriber interface {
	UnsubscribeBatch(topics []string, nodeID string) error
}

// NodePurger is an optional Registry extension that removes every
// routing-table entry for a node in one call — used when a core node is
// confirmed dead for good (see internal/cluster/lifecycle.Monitor). Same
// rationale as BatchUnsubscriber for staying out of Registry proper.
// Implemented by CoreRegistry (delegates to routing.Router).
type NodePurger interface {
	PurgeNode(nodeID string) error
}

// NodesWithRoutesProvider exposes a read-only view of which nodes
// currently hold at least one routing-table entry. Used by
// internal/cluster/lifecycle.RoutingSweep. Implemented by CoreRegistry
// (delegates to routing.Router); not part of Registry for the same reason
// as BatchUnsubscriber/NodePurger.
type NodesWithRoutesProvider interface {
	NodesWithRoutes() []string
}

// LocalRegistry wraps an already-started raft.Raft instance and its FSM
// to manage session ownership (clientID -> owning node) — the only state
// that still lives on raft; topic-filter routing moved to
// internal/cluster/routing (see CoreRegistry, which composes both).
// Non-leader core nodes transparently forward the Apply through raft's
// own leader-forwarding (hashicorp/raft returns ErrNotLeader instead of
// forwarding — callers on a follower must retry against the leader; see
// Leader()).
type LocalRegistry struct {
	raft *hraft.Raft
	fsm  *FSM
}

// NewLocalRegistry wraps an already-started raft.Raft instance and its FSM.
func NewLocalRegistry(r *hraft.Raft, fsm *FSM) *LocalRegistry {
	return &LocalRegistry{raft: r, fsm: fsm}
}

func (l *LocalRegistry) apply(cmd Command) (applyResult, error) {
	_, span := telemetry.Tracer().Start(context.Background(), "keel-gateway.raft_apply",
		oteltrace.WithAttributes(attribute.String("raft.op", string(cmd.Op))),
	)
	defer span.End()

	start := time.Now()
	result := "success"
	defer func() {
		telemetry.RaftApplyDuration.WithLabelValues(string(cmd.Op), result).Observe(time.Since(start).Seconds())
	}()

	b, err := json.Marshal(cmd)
	if err != nil {
		result = "error"
		span.SetStatus(codes.Error, err.Error())
		return applyResult{}, fmt.Errorf("registry: marshal command: %w", err)
	}
	future := l.raft.Apply(b, applyTimeout)
	if err := future.Error(); err != nil {
		result = "error"
		span.SetStatus(codes.Error, err.Error())
		return applyResult{}, fmt.Errorf("registry: apply: %w", err)
	}
	res, ok := future.Response().(applyResult)
	if !ok {
		result = "error"
		span.SetStatus(codes.Error, "unexpected apply response type")
		return applyResult{}, fmt.Errorf("registry: unexpected apply response type %T", future.Response())
	}
	if res.err != nil {
		result = "error"
		span.SetStatus(codes.Error, res.err.Error())
	}
	return res, res.err
}

func (l *LocalRegistry) ClaimSession(clientID, nodeID string) (string, error) {
	res, err := l.apply(Command{Op: OpClaimSession, ClientID: clientID, NodeID: nodeID})
	if err != nil {
		return "", err
	}
	return res.evictedNode, nil
}

func (l *LocalRegistry) ReleaseSession(clientID, nodeID string) error {
	_, err := l.apply(Command{Op: OpReleaseSession, ClientID: clientID, NodeID: nodeID})
	return err
}

// SessionOwner returns the node currently owning clientID, if any.
func (l *LocalRegistry) SessionOwner(clientID string) (string, bool) {
	return l.fsm.sessionOwner(clientID)
}

// SessionsSnapshot exposes session ownership for the management API.
func (l *LocalRegistry) SessionsSnapshot() map[string]string {
	return l.fsm.sessionsSnapshot()
}

// ── ACL writes/reads ─────────────────────────────────────────────────────
// These mutate/read the ACL section of the same FSM state used for
// sessions above — see fsm.go's state docstring for why ACL rules stay on
// raft (not Olric) and in the same raft group (not a second one).

// CreateRole defines or replaces a custom role's rules.
func (l *LocalRegistry) CreateRole(name string, rules []acl.ACLRule) error {
	_, err := l.apply(Command{Op: OpCreateRole, RoleName: name, Rules: rules})
	return err
}

// DeleteRole removes a custom role and any bindings pointing at it.
func (l *LocalRegistry) DeleteRole(name string) error {
	_, err := l.apply(Command{Op: OpDeleteRole, RoleName: name})
	return err
}

// CreateBinding attaches roleName to principal.
func (l *LocalRegistry) CreateBinding(principal, roleName string) error {
	_, err := l.apply(Command{Op: OpCreateBinding, Principal: principal, RoleName: roleName})
	return err
}

// DeleteBinding detaches roleName from principal.
func (l *LocalRegistry) DeleteBinding(principal, roleName string) error {
	_, err := l.apply(Command{Op: OpDeleteBinding, Principal: principal, RoleName: roleName})
	return err
}

// EnableRuleset activates a standard ruleset (see acl.StandardRulesets)
// cluster-wide. Activation is itself a raft command, so it lands in the
// log with the same audit trail as any other ACL mutation — never an
// implicit effect of a binary upgrade.
func (l *LocalRegistry) EnableRuleset(name string) error {
	_, err := l.apply(Command{Op: OpEnableRuleset, RulesetName: name})
	return err
}

// DisableRuleset deactivates a standard ruleset cluster-wide.
func (l *LocalRegistry) DisableRuleset(name string) error {
	_, err := l.apply(Command{Op: OpDisableRuleset, RulesetName: name})
	return err
}

// EvaluateACL decides whether principal (clientID/username) may perform
// action on topic, per the currently-enabled standard rulesets and the
// principal's own bindings. Read-only: does not go through raft.Apply,
// consistent with NodesFor's "best-effort, any node's state is fine"
// posture for reads — but unlike NodesFor, a stale/lagging read here is
// safe specifically because acl.Evaluate is fail-closed: at worst a
// not-yet-replicated grant is denied a beat longer than necessary, never
// the reverse.
func (l *LocalRegistry) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	return l.fsm.evaluateACL(clientID, username, topic, action)
}

// IsRevoked is a pure FSM read, same posture as EvaluateACL.
func (l *LocalRegistry) IsRevoked(identity string) bool {
	return l.fsm.isRevoked(identity)
}

// RolesSnapshot exposes custom roles for the management API.
func (l *LocalRegistry) RolesSnapshot() map[string]acl.Role {
	return l.fsm.rolesSnapshot()
}

// BindingsSnapshot exposes principal -> role-name bindings for the
// management API.
func (l *LocalRegistry) BindingsSnapshot() map[string][]string {
	return l.fsm.bindingsSnapshot()
}

// EnabledRulesetsSnapshot exposes the set of currently-enabled standard
// ruleset names for the management API.
func (l *LocalRegistry) EnabledRulesetsSnapshot() []string {
	return l.fsm.enabledRulesetsSnapshot()
}

// ── Redis primary designation ────────────────────────────────────────────
// See fsm.go's OpSetRedisPrimary doc for why this is a raft command rather
// than gossip state.

// SetRedisPrimary designates nodeID as the current primary for the
// co-located Redis pair. Called only by internal/cluster/membership's
// failover loop, itself gated on IsLeader() — see that package.
func (l *LocalRegistry) SetRedisPrimary(nodeID string) error {
	_, err := l.apply(Command{Op: OpSetRedisPrimary, NodeID: nodeID})
	return err
}

// CurrentRedisPrimary returns the nodeID currently designated primary, if
// any has ever been set.
func (l *LocalRegistry) CurrentRedisPrimary() (string, bool) {
	return l.fsm.currentRedisPrimary()
}

// ── Device PKI revocation ─────────────────────────────────────────────────

// RevokeCertificate records identity as revoked.
func (l *LocalRegistry) RevokeCertificate(identity, serial string) error {
	_, err := l.apply(Command{Op: OpRevokeCertificate, Identity: identity, Serial: serial})
	return err
}

// RevokedSnapshot exposes the full revoked-identity set for the
// management API and RevocationCache's periodic refresh.
func (l *LocalRegistry) RevokedSnapshot() map[string]int64 {
	return l.fsm.revokedSnapshot()
}

// IsLeader reports whether the local raft node currently holds leadership.
func (l *LocalRegistry) IsLeader() bool {
	return l.raft.State() == hraft.Leader
}

// Leader returns the current leader's raft address, if known.
//
// Not reliable for identifying "is this node X the leader" — hashicorp/raft
// reports the address a node advertised at AddVoter/Bootstrap time only for
// remote peers; for itself it may report the transport's resolved local
// address (e.g. a DNS name resolved to an IP), which won't string-match
// the hostname form used elsewhere. Use LeaderID for identity comparisons.
func (l *LocalRegistry) Leader() string {
	addr, _ := l.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the raft ServerID of the current leader, if known. IDs
// are caller-assigned (NodeConfig.NodeID) and never resolved/rewritten by
// raft, so — unlike Leader — they're safe to compare against NodeMeta.NodeID.
func (l *LocalRegistry) LeaderID() string {
	_, id := l.raft.LeaderWithID()
	return string(id)
}
