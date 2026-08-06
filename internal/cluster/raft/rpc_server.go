package raft

import (
	"context"
	"errors"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	pb "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/proto/clusterpb"
)

// RPCServer exposes a Registry over gRPC so edge nodes (which have no
// local raft instance and, since routing moved to Olric, no reason to
// join that ring either) can reach the core quorum for both routing and
// session-ownership calls. Mount with
// pb.RegisterRegistryServer(grpcServer, NewRPCServer(registry)) on every
// core node's gRPC listener — registry should be that node's
// *CoreRegistry, since RemoteRegistry's leader-retry logic for session
// calls (ClaimSession/ReleaseSession) depends on a plain *LocalRegistry
// correctly reporting ErrNotLeader on a follower, which CoreRegistry
// already handles; passing a bare *LocalRegistry here would leave
// routing calls unimplemented (LocalRegistry no longer has them).
type RPCServer struct {
	pb.UnimplementedRegistryServer
	registry Registry
}

// NewRPCServer wraps a Registry for gRPC exposure.
func NewRPCServer(registry Registry) *RPCServer {
	return &RPCServer{registry: registry}
}

// notLeaderStatus surfaces raft.ErrNotLeader as a distinct gRPC status code
// so RemoteRegistry knows to retry a different core node instead of
// treating the write as failed.
func notLeaderStatus(err error) error {
	if errors.Is(err, hraft.ErrNotLeader) || errors.Is(err, hraft.ErrLeadershipLost) {
		return status.Error(codes.FailedPrecondition, "not leader")
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *RPCServer) Subscribe(_ context.Context, req *pb.SubscribeRequest) (*pb.SubscribeResponse, error) {
	if err := s.registry.Subscribe(req.GetTopic(), req.GetNodeId()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.SubscribeResponse{}, nil
}

func (s *RPCServer) Unsubscribe(_ context.Context, req *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	if err := s.registry.Unsubscribe(req.GetTopic(), req.GetNodeId()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.UnsubscribeResponse{}, nil
}

func (s *RPCServer) NodesFor(_ context.Context, req *pb.NodesForRequest) (*pb.NodesForResponse, error) {
	return &pb.NodesForResponse{NodeIds: s.registry.NodesFor(req.GetTopic(), req.GetLocalNodeId())}, nil
}

func (s *RPCServer) ClaimSession(_ context.Context, req *pb.ClaimSessionRequest) (*pb.ClaimSessionResponse, error) {
	evicted, err := s.registry.ClaimSession(req.GetClientId(), req.GetNodeId(), req.GetIdentity())
	if err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.ClaimSessionResponse{Ok: true, EvictedNodeId: evicted}, nil
}

func (s *RPCServer) ReleaseSession(_ context.Context, req *pb.ReleaseSessionRequest) (*pb.ReleaseSessionResponse, error) {
	if err := s.registry.ReleaseSession(req.GetClientId(), req.GetNodeId()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.ReleaseSessionResponse{}, nil
}

func (s *RPCServer) EvaluateACL(_ context.Context, req *pb.EvaluateACLRequest) (*pb.EvaluateACLResponse, error) {
	decision := s.registry.EvaluateACL(req.GetClientId(), req.GetUsername(), req.GetTopic(), acl.Action(req.GetAction()))
	resp := &pb.EvaluateACLResponse{Allowed: decision.Allowed()}
	if decision.Rule != nil {
		resp.MatchedFilter = decision.Rule.TopicFilter
	}
	return resp, nil
}

func (s *RPCServer) CurrentRedisPrimary(_ context.Context, _ *pb.CurrentRedisPrimaryRequest) (*pb.CurrentRedisPrimaryResponse, error) {
	nodeID, ok := s.registry.CurrentRedisPrimary()
	return &pb.CurrentRedisPrimaryResponse{NodeId: nodeID, Ok: ok}, nil
}

func (s *RPCServer) ACLSnapshot(_ context.Context, _ *pb.ACLSnapshotRequest) (*pb.ACLSnapshotResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	resp := &pb.ACLSnapshotResponse{}
	for name, role := range admin.RolesSnapshot() {
		pbRules := make([]*pb.ACLRule, 0, len(role.Rules))
		for _, ru := range role.Rules {
			pbRules = append(pbRules, &pb.ACLRule{TopicFilter: ru.TopicFilter, Actions: ru.Actions, Effect: string(ru.Effect)})
		}
		resp.Roles = append(resp.Roles, &pb.RoleEntry{Name: name, Rules: pbRules})
	}
	for principal, roleNames := range admin.BindingsSnapshot() {
		resp.Bindings = append(resp.Bindings, &pb.BindingEntry{Principal: principal, RoleNames: roleNames})
	}
	resp.EnabledRulesets = admin.EnabledRulesetsSnapshot()
	return resp, nil
}

// aclAdmin type-asserts the wrapped registry for the ACL mutation RPCs
// below, mirroring how internal/broker/hooks.go type-asserts for
// BatchUnsubscriber — these RPCs only make sense when registry is a
// *CoreRegistry (forwarding a leader-bound write received from a peer
// core node); an edge RPCServer is never started, so this should always
// succeed in practice, but we fail cleanly instead of panicking if it
// doesn't.
func (s *RPCServer) aclAdmin() (ACLAdmin, error) {
	admin, ok := s.registry.(ACLAdmin)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "acl admin not supported by this registry")
	}
	return admin, nil
}

func (s *RPCServer) CreateRole(_ context.Context, req *pb.CreateRoleRequest) (*pb.CreateRoleResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	rules := make([]acl.ACLRule, 0, len(req.GetRules()))
	for _, r := range req.GetRules() {
		rules = append(rules, acl.ACLRule{TopicFilter: r.GetTopicFilter(), Actions: r.GetActions(), Effect: acl.Effect(r.GetEffect())})
	}
	if err := admin.CreateRole(req.GetName(), rules); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.CreateRoleResponse{}, nil
}

func (s *RPCServer) DeleteRole(_ context.Context, req *pb.DeleteRoleRequest) (*pb.DeleteRoleResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	if err := admin.DeleteRole(req.GetName()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.DeleteRoleResponse{}, nil
}

func (s *RPCServer) CreateBinding(_ context.Context, req *pb.CreateBindingRequest) (*pb.CreateBindingResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	if err := admin.CreateBinding(req.GetPrincipal(), req.GetRoleName()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.CreateBindingResponse{}, nil
}

func (s *RPCServer) DeleteBinding(_ context.Context, req *pb.DeleteBindingRequest) (*pb.DeleteBindingResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	if err := admin.DeleteBinding(req.GetPrincipal(), req.GetRoleName()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.DeleteBindingResponse{}, nil
}

func (s *RPCServer) EnableRuleset(_ context.Context, req *pb.EnableRulesetRequest) (*pb.EnableRulesetResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	if err := admin.EnableRuleset(req.GetName()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.EnableRulesetResponse{}, nil
}

func (s *RPCServer) DisableRuleset(_ context.Context, req *pb.DisableRulesetRequest) (*pb.DisableRulesetResponse, error) {
	admin, err := s.aclAdmin()
	if err != nil {
		return nil, err
	}
	if err := admin.DisableRuleset(req.GetName()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.DisableRulesetResponse{}, nil
}

// IsRevoked is part of the base Registry interface (every node needs it,
// same as EvaluateACL) — no type-assertion needed.
func (s *RPCServer) IsRevoked(_ context.Context, req *pb.IsRevokedRequest) (*pb.IsRevokedResponse, error) {
	return &pb.IsRevokedResponse{Revoked: s.registry.IsRevoked(req.GetIdentity())}, nil
}

// revocationAdmin type-asserts the wrapped registry for the revocation
// mutation RPC below, same rationale/fallback as aclAdmin.
func (s *RPCServer) revocationAdmin() (RevocationAdmin, error) {
	admin, ok := s.registry.(RevocationAdmin)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "revocation admin not supported by this registry")
	}
	return admin, nil
}

func (s *RPCServer) RevokeCertificate(_ context.Context, req *pb.RevokeCertificateRequest) (*pb.RevokeCertificateResponse, error) {
	admin, err := s.revocationAdmin()
	if err != nil {
		return nil, err
	}
	if err := admin.RevokeCertificate(req.GetIdentity(), req.GetSerial()); err != nil {
		return nil, notLeaderStatus(err)
	}
	return &pb.RevokeCertificateResponse{}, nil
}

func (s *RPCServer) RevocationSnapshot(_ context.Context, _ *pb.RevocationSnapshotRequest) (*pb.RevocationSnapshotResponse, error) {
	admin, err := s.revocationAdmin()
	if err != nil {
		return nil, err
	}
	return &pb.RevocationSnapshotResponse{RevokedIdentities: admin.RevokedSnapshot()}, nil
}

// Register mounts the Registry gRPC service on an existing *grpc.Server.
func Register(s *grpc.Server, registry Registry) {
	pb.RegisterRegistryServer(s, NewRPCServer(registry))
}
