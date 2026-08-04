package raft

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	pb "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/proto/clusterpb"
)

const remoteCallTimeout = 3 * time.Second

// RemoteRegistry is the edge-node Registry implementation: it has no local
// raft instance and forwards every call over gRPC to a core node. Writes
// (Subscribe/Unsubscribe/ClaimSession/ReleaseSession) must land on the
// raft leader; since hashicorp/raft does not forward Apply calls, this
// client retries across every known core node address until one accepts
// the write (a node that isn't leader replies with a FailedPrecondition
// status — see notLeaderStatus in rpc_server.go).
//
// CorePeers is a live callback rather than a static list so it can be fed
// by the membership package as core nodes join/leave the gossip cluster.
type RemoteRegistry struct {
	CorePeers func() []string // returns current core gRPC addresses (host:port)

	log *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewRemoteRegistry creates an edge-side Registry backed by gRPC calls to
// core nodes. corePeers must return a non-empty slice for calls to
// succeed; it is typically membership.Membership.CoreGRPCAddrs.
func NewRemoteRegistry(corePeers func() []string, log *slog.Logger) *RemoteRegistry {
	return &RemoteRegistry{
		CorePeers: corePeers,
		log:       log,
		conns:     make(map[string]*grpc.ClientConn),
	}
}

func (r *RemoteRegistry) clientFor(addr string) (pb.RegistryClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cc, ok := r.conns[addr]; ok {
		return pb.NewRegistryClient(cc), nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("remote registry: dial %s: %w", addr, err)
	}
	r.conns[addr] = cc
	return pb.NewRegistryClient(cc), nil
}

// forEachPeer calls fn against every known core address in turn, stopping
// at the first call that doesn't fail with "not leader". Returns the last
// error if every peer was unreachable or non-leader.
func (r *RemoteRegistry) forEachPeer(fn func(pb.RegistryClient, context.Context) error) error {
	peers := r.CorePeers()
	if len(peers) == 0 {
		return fmt.Errorf("remote registry: no known core peers")
	}
	var lastErr error
	for _, addr := range peers {
		client, err := r.clientFor(addr)
		if err != nil {
			lastErr = err
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), remoteCallTimeout)
		err = fn(client, ctx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
			// Not the leader — try the next core node.
			continue
		}
		r.log.Warn("remote registry: rpc failed", "addr", addr, "error", err)
	}
	return lastErr
}

func (r *RemoteRegistry) Subscribe(topic, nodeID string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.Subscribe(ctx, &pb.SubscribeRequest{Topic: topic, NodeId: nodeID})
		return err
	})
}

func (r *RemoteRegistry) Unsubscribe(topic, nodeID string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.Unsubscribe(ctx, &pb.UnsubscribeRequest{Topic: topic, NodeId: nodeID})
		return err
	})
}

// NodesFor is best-effort: any core node's FSM state is accurate enough
// for routing decisions (reads never require the leader), so it queries
// the first reachable peer and returns nil on total failure rather than
// blocking the MQTT publish path.
func (r *RemoteRegistry) NodesFor(topic, localNodeID string) []string {
	var out []string
	_ = r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, err := c.NodesFor(ctx, &pb.NodesForRequest{Topic: topic, LocalNodeId: localNodeID})
		if err != nil {
			return err
		}
		out = resp.GetNodeIds()
		return nil
	})
	return out
}

func (r *RemoteRegistry) ClaimSession(clientID, nodeID string) (string, error) {
	var evicted string
	err := r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, err := c.ClaimSession(ctx, &pb.ClaimSessionRequest{ClientId: clientID, NodeId: nodeID})
		if err != nil {
			return err
		}
		evicted = resp.GetEvictedNodeId()
		return nil
	})
	return evicted, err
}

func (r *RemoteRegistry) ReleaseSession(clientID, nodeID string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.ReleaseSession(ctx, &pb.ReleaseSessionRequest{ClientId: clientID, NodeId: nodeID})
		return err
	})
}

// EvaluateACL is best-effort/any-peer, same rationale as NodesFor: a
// fail-closed-safe read never needs the leader specifically.
func (r *RemoteRegistry) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	var decision acl.Decision
	err := r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, err := c.EvaluateACL(ctx, &pb.EvaluateACLRequest{
			ClientId: clientID,
			Username: username,
			Topic:    topic,
			Action:   string(action),
		})
		if err != nil {
			return err
		}
		effect := acl.EffectDeny
		if resp.GetAllowed() {
			effect = acl.EffectAllow
		}
		decision = acl.Decision{Effect: effect}
		if mf := resp.GetMatchedFilter(); mf != "" {
			decision.Rule = &acl.ACLRule{TopicFilter: mf}
		}
		return nil
	})
	if err != nil {
		// Unreachable cluster: fail closed, exactly as an FSM lookup that
		// finds nothing would — never optimistic-allow on a transport
		// error.
		return acl.Decision{Effect: acl.EffectDeny}
	}
	return decision
}

// CurrentRedisPrimary is best-effort/any-peer, same rationale as
// EvaluateACL/NodesFor: this read never needs the leader specifically.
// Returns ok=false on total failure (unreachable cluster), same posture
// as a fresh FSM with nothing designated yet — the caller
// (internal/cluster/redisrouter's watcher) simply doesn't redirect until
// a real answer comes back, rather than erroring.
func (r *RemoteRegistry) CurrentRedisPrimary() (string, bool) {
	var nodeID string
	var found bool
	err := r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, err := c.CurrentRedisPrimary(ctx, &pb.CurrentRedisPrimaryRequest{})
		if err != nil {
			return err
		}
		nodeID = resp.GetNodeId()
		found = resp.GetOk()
		return nil
	})
	if err != nil {
		return "", false
	}
	return nodeID, found
}

// ACLSnapshot fetches the full ACL state (custom roles, bindings, enabled
// standard rulesets) from any reachable core node — used by ACLCache to
// populate/refresh edge nodes' local ACL cache. Same any-peer rationale
// as EvaluateACL/NodesFor: a slightly lagging follower's state is safe to
// read (fail-closed semantics live in acl.Evaluate, not in freshness).
func (r *RemoteRegistry) ACLSnapshot() (roles map[string]acl.Role, bindings map[string][]string, enabledRulesets []string, err error) {
	err = r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, callErr := c.ACLSnapshot(ctx, &pb.ACLSnapshotRequest{})
		if callErr != nil {
			return callErr
		}
		roles = make(map[string]acl.Role, len(resp.GetRoles()))
		for _, re := range resp.GetRoles() {
			rules := make([]acl.ACLRule, 0, len(re.GetRules()))
			for _, ru := range re.GetRules() {
				rules = append(rules, acl.ACLRule{TopicFilter: ru.GetTopicFilter(), Actions: ru.GetActions(), Effect: acl.Effect(ru.GetEffect())})
			}
			roles[re.GetName()] = acl.Role{Name: re.GetName(), Rules: rules}
		}
		bindings = make(map[string][]string, len(resp.GetBindings()))
		for _, be := range resp.GetBindings() {
			bindings[be.GetPrincipal()] = be.GetRoleNames()
		}
		enabledRulesets = resp.GetEnabledRulesets()
		return nil
	})
	return roles, bindings, enabledRulesets, err
}

func (r *RemoteRegistry) CreateRole(name string, rules []acl.ACLRule) error {
	pbRules := make([]*pb.ACLRule, 0, len(rules))
	for _, ru := range rules {
		pbRules = append(pbRules, &pb.ACLRule{TopicFilter: ru.TopicFilter, Actions: ru.Actions, Effect: string(ru.Effect)})
	}
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.CreateRole(ctx, &pb.CreateRoleRequest{Name: name, Rules: pbRules})
		return err
	})
}

func (r *RemoteRegistry) DeleteRole(name string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.DeleteRole(ctx, &pb.DeleteRoleRequest{Name: name})
		return err
	})
}

func (r *RemoteRegistry) CreateBinding(principal, roleName string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.CreateBinding(ctx, &pb.CreateBindingRequest{Principal: principal, RoleName: roleName})
		return err
	})
}

func (r *RemoteRegistry) DeleteBinding(principal, roleName string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.DeleteBinding(ctx, &pb.DeleteBindingRequest{Principal: principal, RoleName: roleName})
		return err
	})
}

func (r *RemoteRegistry) EnableRuleset(name string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.EnableRuleset(ctx, &pb.EnableRulesetRequest{Name: name})
		return err
	})
}

func (r *RemoteRegistry) DisableRuleset(name string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.DisableRuleset(ctx, &pb.DisableRulesetRequest{Name: name})
		return err
	})
}

// IsRevoked is best-effort/any-peer, same fail-closed-on-transport-error
// rationale as EvaluateACL: an unreachable cluster must never be read as
// "not revoked".
func (r *RemoteRegistry) IsRevoked(identity string) bool {
	var revoked bool
	err := r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, err := c.IsRevoked(ctx, &pb.IsRevokedRequest{Identity: identity})
		if err != nil {
			return err
		}
		revoked = resp.GetRevoked()
		return nil
	})
	if err != nil {
		return true // unreachable cluster: fail closed, same posture as EvaluateACL
	}
	return revoked
}

func (r *RemoteRegistry) RevokeCertificate(identity, serial string) error {
	return r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		_, err := c.RevokeCertificate(ctx, &pb.RevokeCertificateRequest{Identity: identity, Serial: serial})
		return err
	})
}

// RevokedSnapshot fetches the full revoked-identity set from any
// reachable core node — used by RevocationCache to populate/refresh
// every node's local cache. Same any-peer rationale as ACLSnapshot.
func (r *RemoteRegistry) RevokedSnapshot() (map[string]int64, error) {
	var out map[string]int64
	err := r.forEachPeer(func(c pb.RegistryClient, ctx context.Context) error {
		resp, callErr := c.RevocationSnapshot(ctx, &pb.RevocationSnapshotRequest{})
		if callErr != nil {
			return callErr
		}
		out = resp.GetRevokedIdentities()
		return nil
	})
	return out, err
}
