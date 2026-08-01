package raft

import (
	"errors"
	"log/slog"

	hraft "github.com/hashicorp/raft"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
)

// IsNotLeader reports whether err originates from raft.Apply failing
// because the local node isn't the current leader.
func IsNotLeader(err error) bool {
	return errors.Is(err, hraft.ErrNotLeader) || errors.Is(err, hraft.ErrLeadershipLost)
}

// CoreRegistry is the core-node Registry implementation. It composes two
// independent backends behind the single Registry interface:
//
//   - routing (Subscribe/Unsubscribe/NodesFor/UnsubscribeBatch/PurgeNode/
//     NodesWithRoutes) delegates to a routing.Router, backed by an AP
//     store (Olric) — no leader-forwarding needed, any core node can
//     serve these directly.
//   - session ownership (ClaimSession/ReleaseSession) delegates to
//     LocalRegistry (raft), with leader-forwarding over gRPC via fallback
//     when this node isn't the current raft leader (hashicorp/raft
//     doesn't forward Apply calls itself).
//
// Callers (hooks.go, RPCServer) don't need to know about this split.
type CoreRegistry struct {
	local    *LocalRegistry
	router   *routing.Router
	fallback *RemoteRegistry
}

// NewCoreRegistry wraps local (session ownership) and router (routing).
// leaderGRPCAddr must resolve the current raft leader's gRPC address
// (typically membership.Membership.NodeGRPCAddr applied to
// local.LeaderID()); it may return ok=false while leadership is
// unsettled, in which case a session write simply fails and the caller
// retries (mirroring how a real MQTT client would retry a rejected
// operation).
func NewCoreRegistry(local *LocalRegistry, router *routing.Router, leaderGRPCAddr func() (string, bool), log *slog.Logger) *CoreRegistry {
	fallback := NewRemoteRegistry(func() []string {
		addr, ok := leaderGRPCAddr()
		if !ok {
			return nil
		}
		return []string{addr}
	}, log)
	return &CoreRegistry{local: local, router: router, fallback: fallback}
}

func (c *CoreRegistry) Subscribe(topic, nodeID string) error {
	return c.router.Subscribe(topic, nodeID)
}

func (c *CoreRegistry) Unsubscribe(topic, nodeID string) error {
	return c.router.Unsubscribe(topic, nodeID)
}

func (c *CoreRegistry) NodesFor(topic, localNodeID string) []string {
	return c.router.NodesFor(topic, localNodeID)
}

// UnsubscribeBatch removes multiple filters for nodeID in one routing
// write — see routing.Router.UnsubscribeBatch.
func (c *CoreRegistry) UnsubscribeBatch(topics []string, nodeID string) error {
	return c.router.UnsubscribeBatch(topics, nodeID)
}

// PurgeNode removes every routing-table entry for nodeID — see
// routing.Router.PurgeNode.
func (c *CoreRegistry) PurgeNode(nodeID string) error {
	return c.router.PurgeNode(nodeID)
}

// TopicsForNode is a pure local-cache read — see routing.Router.TopicsForNode.
func (c *CoreRegistry) TopicsForNode(nodeID string) []string {
	return c.router.TopicsForNode(nodeID)
}

// NodesWithRoutes is a pure local-cache read — see routing.Router.NodesWithRoutes.
func (c *CoreRegistry) NodesWithRoutes() []string {
	return c.router.NodesWithRoutes()
}

// RoutesSnapshot exposes the routing table for the management API — see
// routing.Router.Snapshot.
func (c *CoreRegistry) RoutesSnapshot() map[string][]string {
	return c.router.Snapshot()
}

func (c *CoreRegistry) ClaimSession(clientID, nodeID string) (string, error) {
	evicted, err := c.local.ClaimSession(clientID, nodeID)
	if err != nil && IsNotLeader(err) {
		return c.fallback.ClaimSession(clientID, nodeID)
	}
	return evicted, err
}

func (c *CoreRegistry) ReleaseSession(clientID, nodeID string) error {
	if err := c.local.ReleaseSession(clientID, nodeID); err != nil {
		if IsNotLeader(err) {
			return c.fallback.ReleaseSession(clientID, nodeID)
		}
		return err
	}
	return nil
}

// EvaluateACL is a pure FSM read — no leader-forwarding needed, since a
// possibly-lagging follower read is safe by construction (fail-closed;
// see LocalRegistry.EvaluateACL).
func (c *CoreRegistry) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	return c.local.EvaluateACL(clientID, username, topic, action)
}

// CurrentRedisPrimary is a pure FSM read, same no-leader-forwarding-needed
// rationale as EvaluateACL.
func (c *CoreRegistry) CurrentRedisPrimary() (string, bool) {
	return c.local.CurrentRedisPrimary()
}

// CreateRole, DeleteRole, CreateBinding, DeleteBinding, EnableRuleset and
// DisableRuleset are ACL *writes* and therefore need the same
// leader-forwarding fallback as ClaimSession/ReleaseSession above — only
// the raft leader can Apply.

func (c *CoreRegistry) CreateRole(name string, rules []acl.ACLRule) error {
	if err := c.local.CreateRole(name, rules); err != nil {
		if IsNotLeader(err) {
			return c.fallback.CreateRole(name, rules)
		}
		return err
	}
	return nil
}

func (c *CoreRegistry) DeleteRole(name string) error {
	if err := c.local.DeleteRole(name); err != nil {
		if IsNotLeader(err) {
			return c.fallback.DeleteRole(name)
		}
		return err
	}
	return nil
}

func (c *CoreRegistry) CreateBinding(principal, roleName string) error {
	if err := c.local.CreateBinding(principal, roleName); err != nil {
		if IsNotLeader(err) {
			return c.fallback.CreateBinding(principal, roleName)
		}
		return err
	}
	return nil
}

func (c *CoreRegistry) DeleteBinding(principal, roleName string) error {
	if err := c.local.DeleteBinding(principal, roleName); err != nil {
		if IsNotLeader(err) {
			return c.fallback.DeleteBinding(principal, roleName)
		}
		return err
	}
	return nil
}

func (c *CoreRegistry) EnableRuleset(name string) error {
	if err := c.local.EnableRuleset(name); err != nil {
		if IsNotLeader(err) {
			return c.fallback.EnableRuleset(name)
		}
		return err
	}
	return nil
}

func (c *CoreRegistry) DisableRuleset(name string) error {
	if err := c.local.DisableRuleset(name); err != nil {
		if IsNotLeader(err) {
			return c.fallback.DisableRuleset(name)
		}
		return err
	}
	return nil
}

// RolesSnapshot, BindingsSnapshot and EnabledRulesetsSnapshot are pure FSM
// reads for the management API, same fail-closed-safe-staleness rationale
// as EvaluateACL.

func (c *CoreRegistry) RolesSnapshot() map[string]acl.Role {
	return c.local.RolesSnapshot()
}

func (c *CoreRegistry) BindingsSnapshot() map[string][]string {
	return c.local.BindingsSnapshot()
}

func (c *CoreRegistry) EnabledRulesetsSnapshot() []string {
	return c.local.EnabledRulesetsSnapshot()
}
