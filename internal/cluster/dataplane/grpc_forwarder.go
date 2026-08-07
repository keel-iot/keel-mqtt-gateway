package dataplane

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/proto/clusterpb"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

const forwardTimeout = 3 * time.Second

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

	handlerMu sync.RWMutex
	handler   func(*Message)

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
func (s *dataplaneServer) Forward(_ context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	f := s.fwd
	f.handlerMu.RLock()
	h := f.handler
	f.handlerMu.RUnlock()
	if h != nil {
		// Zero UUID on parse failure (e.g. empty from an old peer mid
		// rolling-upgrade) — offline delivery dedup treats a zero
		// PublishID as "unknown, skip dedup" rather than dropping the
		// message.
		publishID, _ := uuid.Parse(req.GetPublishId())
		h(&Message{
			SourceNodeID: req.GetSourceNodeId(),
			TenantID:     req.GetTenantId(),
			Topic:        req.GetTopic(),
			Payload:      req.GetPayload(),
			QoS:          byte(req.GetQos()),
			PublishID:    publishID,
		})
	}
	return &pb.ForwardResponse{}, nil
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
			telemetry.ForwardFailuresTotal.Inc()
		}
	}()

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

// Subscribe registers the local handler invoked for every message another
// node forwards to this one. Only one handler is supported — call once
// during startup wiring (see broker hooks integration).
func (f *GRPCForwarder) Subscribe(handler func(*Message)) error {
	f.handlerMu.Lock()
	defer f.handlerMu.Unlock()
	f.handler = handler
	return nil
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
