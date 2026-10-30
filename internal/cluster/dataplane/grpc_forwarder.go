package dataplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/proto/clusterpb"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

const forwardTimeout = 3 * time.Second

// Resolve only before the RPC: retrying after an ambiguous response can duplicate delivery.
const membershipResolveInterval = 25 * time.Millisecond

var errUnknownNode = errors.New("dataplane: unknown node")

// GRPCForwarder is the phase-1 Forwarder implementation: direct
// point-to-point gRPC between nodes, one call per forwarded message. No
// batching, no persistence, no delivery guarantee beyond the gRPC call's
// own success/failure — acceptable for QoS 0/1 telemetry fan-out in the
// PoC; QoS session-critical delivery still goes through the owning node's
// own MQTT session (Redis-persisted), not through this path.
type GRPCForwarder struct {
	// resolve maps a node ID (as tracked by raft.Registry) to the gRPC
	// dial address of that node's dataplane listener. Supplied by the
	// membership package, which learns addresses via gossip.
	resolve func(nodeID string) (addr string, ok bool)
	log     *slog.Logger

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn

	handlerMu      sync.RWMutex
	handler        func(*Message)
	contextHandler func(context.Context, *Message)

	evictHandlerMu sync.RWMutex
	evictHandler   func(clientID string)
}

// NewGRPCForwarder creates a Forwarder that resolves target node IDs to
// addresses via resolve. Mount its inbound side on a *grpc.Server with
// pb.RegisterDataplaneServer(s, fwd.Server()) to accept forwards from
// other nodes.
func NewGRPCForwarder(resolve func(nodeID string) (string, bool), log *slog.Logger) *GRPCForwarder {
	return &GRPCForwarder{
		resolve: resolve,
		log:     log,
		conns:   make(map[string]*grpc.ClientConn),
	}
}

// Server returns the pb.DataplaneServer to mount on a *grpc.Server. Kept
// separate from GRPCForwarder itself because the RPC method name
// (Forward) collides with Forwarder.Forward's different signature.
func (f *GRPCForwarder) Server() pb.DataplaneServer {
	return &dataplaneServer{fwd: f}
}

// RegisterServer mounts f's inbound gRPC service on s.
func RegisterServer(s *grpc.Server, f *GRPCForwarder) {
	pb.RegisterDataplaneServer(s, f.Server())
}

type dataplaneServer struct {
	pb.UnimplementedDataplaneServer
	fwd *GRPCForwarder
}

// Forward implements pb.DataplaneServer — the inbound side, invoked by
// gRPC when a remote node calls Forward against this node.
func (s *dataplaneServer) Forward(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	f := s.fwd
	f.handlerMu.RLock()
	h := f.handler
	contextHandler := f.contextHandler
	f.handlerMu.RUnlock()
	if contextHandler != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		contextHandler(ctx, messageFromRequest(req))
	} else if h != nil {
		h(messageFromRequest(req))
	}
	return &pb.ForwardResponse{}, nil
}

func messageFromRequest(req *pb.ForwardRequest) *Message {
	// Zero UUID on parse failure (e.g. empty from an old peer mid
	// rolling-upgrade) — offline delivery dedup treats a zero
	// PublishID as "unknown, skip dedup" rather than dropping the
	// message.
	publishID, _ := uuid.Parse(req.GetPublishId())
	return &Message{
		SourceNodeID: req.GetSourceNodeId(),
		TenantID:     req.GetTenantId(),
		Topic:        req.GetTopic(),
		Payload:      req.GetPayload(),
		QoS:          byte(req.GetQos()),
		PublishID:    publishID,
	}
}

// Evict implements pb.DataplaneServer — the inbound side, invoked by gRPC
// when a remote node calls Evict against this node.
func (s *dataplaneServer) Evict(_ context.Context, req *pb.EvictRequest) (*pb.EvictResponse, error) {
	f := s.fwd
	f.evictHandlerMu.RLock()
	h := f.evictHandler
	f.evictHandlerMu.RUnlock()
	if h != nil {
		h(req.GetClientId())
	}
	return &pb.EvictResponse{}, nil
}

func (f *GRPCForwarder) clientFor(addr string) (pb.DataplaneClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cc, ok := f.conns[addr]; ok {
		return pb.NewDataplaneClient(cc), nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dataplane: dial %s: %w", addr, err)
	}
	f.conns[addr] = cc
	return pb.NewDataplaneClient(cc), nil
}

// Forward sends msg to targetNodeID over gRPC.
func (f *GRPCForwarder) Forward(ctx context.Context, targetNodeID string, msg *Message) (err error) {
	start := time.Now()
	defer func() {
		telemetry.ForwardLatency.Observe(time.Since(start).Seconds())
		if err != nil {
			telemetry.ForwardFailuresTotal.WithLabelValues(forwardFailureReason(err)).Inc()
		}
	}()

	// Resolution and RPC share the original end-to-end budget (or the
	// caller's shorter fan-out budget). Known nodes incur no polling delay.
	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()
	addr, err := f.resolveForwardTarget(ctx, targetNodeID)
	if err != nil {
		return err
	}
	client, err := f.clientFor(addr)
	if err != nil {
		return err
	}
	_, err = client.Forward(ctx, &pb.ForwardRequest{
		SourceNodeId: msg.SourceNodeID,
		TenantId:     msg.TenantID,
		Topic:        msg.Topic,
		Payload:      msg.Payload,
		Qos:          uint32(msg.QoS),
		PublishId:    msg.PublishID.String(),
	})
	if err != nil {
		return fmt.Errorf("dataplane: forward to %s (%s): %w", targetNodeID, addr, err)
	}
	return nil
}

// A routing update can arrive through Olric before gossip has installed the
// target address locally. Wait only while no RPC has been issued. A genuinely
// absent node remains a bounded best-effort failure; other fan-out targets run
// concurrently and do not wait on this resolver.
func (f *GRPCForwarder) resolveForwardTarget(ctx context.Context, nodeID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if addr, ok := f.resolve(nodeID); ok && addr != "" {
		return addr, nil
	}
	ticker := time.NewTicker(membershipResolveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("%w %q: waiting for membership: %w", errUnknownNode, nodeID, ctx.Err())
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return "", fmt.Errorf("%w %q: waiting for membership: %w", errUnknownNode, nodeID, err)
			}
			if addr, ok := f.resolve(nodeID); ok && addr != "" {
				return addr, nil
			}
		}
	}
}

// Subscribe registers the local handler invoked for every message another
// node forwards to this one. Only one handler is supported — call once
// during startup wiring (see broker hooks integration).
func (f *GRPCForwarder) Subscribe(handler func(*Message)) error {
	f.handlerMu.Lock()
	defer f.handlerMu.Unlock()
	f.handler = handler
	f.contextHandler = nil
	return nil
}

// SubscribeContext registers a handler that receives the inbound RPC context.
// It is intentionally separate from Subscribe so existing dataplane
// implementations remain source-compatible while the gRPC path can cancel
// context-aware work when the caller deadline expires.
func (f *GRPCForwarder) SubscribeContext(handler func(context.Context, *Message)) error {
	f.handlerMu.Lock()
	defer f.handlerMu.Unlock()
	f.contextHandler = handler
	f.handler = nil
	return nil
}

func forwardFailureReason(err error) string {
	if errors.Is(err, errUnknownNode) {
		return "unknown_node"
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return "deadline"
	case codes.Unavailable:
		return "unavailable"
	default:
		return "other"
	}
}

// Evict sends a best-effort request to targetNodeID to locally
// disconnect clientID.
func (f *GRPCForwarder) Evict(ctx context.Context, targetNodeID, clientID string) error {
	addr, ok := f.resolve(targetNodeID)
	if !ok {
		return fmt.Errorf("dataplane: unknown node %q", targetNodeID)
	}
	client, err := f.clientFor(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()
	if _, err := client.Evict(ctx, &pb.EvictRequest{ClientId: clientID}); err != nil {
		return fmt.Errorf("dataplane: evict on %s (%s): %w", targetNodeID, addr, err)
	}
	return nil
}

// SubscribeEvict registers the local handler invoked whenever another
// node calls Evict against this one. Only one handler is supported.
func (f *GRPCForwarder) SubscribeEvict(handler func(clientID string)) error {
	f.evictHandlerMu.Lock()
	defer f.evictHandlerMu.Unlock()
	f.evictHandler = handler
	return nil
}
