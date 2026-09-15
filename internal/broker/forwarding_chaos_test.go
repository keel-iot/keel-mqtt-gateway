package broker

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestKnownButUnreachableForwarding_ConcurrentLiveTarget is the deterministic
// causal K1 test. A's resolver entry remains valid and its TCP listener
// accepts the gRPC connection, but the listener never answers the RPC. B is a
// real gRPC dataplane server. This preserves resolve(A)==true while making
// only the dataplane path unusable, without coupling the test to memberlist's
// failure-detection timing.
func TestKnownButUnreachableForwarding_ConcurrentLiveTarget(t *testing.T) {
	blackhole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blackhole listen: %v", err)
	}
	accepted := make(chan struct{})
	stopBlackhole := make(chan struct{})
	go func() {
		conn, acceptErr := blackhole.Accept()
		if acceptErr == nil {
			close(accepted)
			select {
			case <-stopBlackhole:
			case <-time.After(5 * time.Second):
			}
			_ = conn.Close()
		}
	}()

	liveListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("live listen: %v", err)
	}
	liveServer := grpc.NewServer()
	liveForwarder := dataplane.NewGRPCForwarder(nil, nil)
	liveReached := make(chan struct{})
	var liveOnce sync.Once
	if err := liveForwarder.Subscribe(func(*dataplane.Message) { liveOnce.Do(func() { close(liveReached) }) }); err != nil {
		t.Fatalf("subscribe live handler: %v", err)
	}
	dataplane.RegisterServer(liveServer, liveForwarder)
	go func() { _ = liveServer.Serve(liveListener) }()
	defer liveServer.Stop()
	defer liveListener.Close()

	addresses := map[string]string{
		"unreachable-A": blackhole.Addr().String(),
		"live-B":        liveListener.Addr().String(),
	}
	resolver := func(nodeID string) (string, bool) {
		addr, ok := addresses[nodeID]
		return addr, ok
	}
	forwarder := dataplane.NewGRPCForwarder(resolver, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Explicitly prove the resolver still knows A before the forwarding run.
	if addr, ok := resolver("unreachable-A"); !ok || addr != blackhole.Addr().String() {
		t.Fatalf("expected unreachable-A to remain resolvable, got addr=%q ok=%v", addr, ok)
	}
	faultAt := time.Now()
	t.Logf("fault active: %s; resolver A=%s", faultAt.Format(time.RFC3339Nano), blackhole.Addr())

	start := time.Now()
	t.Logf("publish/fanout started: %s", start.Format(time.RFC3339Nano))
	done := make(chan struct{})
	go func() {
		forwardClusterTargets(context.Background(), forwarder, "publisher", []string{"unreachable-A", "live-B"}, &dataplane.Message{Topic: "telemetry/test"}, "telemetry/test", slog.New(slog.NewTextHandler(io.Discard, nil)), 1)
		close(done)
	}()

	select {
	case <-accepted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("known unreachable target did not reach the blackhole listener")
	}
	select {
	case <-liveReached:
		t.Logf("live target B observed: %s (delta=%s)", time.Now().Format(time.RFC3339Nano), time.Since(start))
		if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
			t.Fatalf("live target was not attempted promptly: %s", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("live target was blocked behind the unreachable target")
	}

	select {
	case <-done:
		elapsed := time.Since(start)
		t.Logf("fanout completed: %s (delta=%s)", time.Now().Format(time.RFC3339Nano), elapsed)
		if elapsed > 3500*time.Millisecond {
			t.Fatalf("fanout exceeded global budget: %s", elapsed)
		}
	case <-time.After(3500 * time.Millisecond):
		t.Fatal("fanout did not terminate at the global deadline")
	}

	if got := testutil.ToFloat64(telemetry.ForwardFailuresTotal.WithLabelValues("deadline")); got < 1 {
		t.Fatalf("expected bounded deadline failure metric, got %v", got)
	}

	// Confirm the direct known-but-unreachable RPC reports a gRPC deadline,
	// rather than the unrelated unknown-node classification.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = forwarder.Forward(ctx, "unreachable-A", &dataplane.Message{Topic: "telemetry/test"})
	t.Logf("known unreachable A failure: %s (delta from fanout start=%s)", time.Now().Format(time.RFC3339Nano), time.Since(start))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded from known blackhole, got %v", err)
	}

	// Restore the same address and verify that the cached ClientConn can
	// recover without recreating the forwarder or restarting the broker.
	close(stopBlackhole)
	blackholeAddr := blackhole.Addr().String()
	_ = blackhole.Close()
	restoreAt := time.Now()
	t.Logf("fault removed: %s (delta from fault=%s)", restoreAt.Format(time.RFC3339Nano), restoreAt.Sub(faultAt))
	time.Sleep(100 * time.Millisecond)
	recoveredListener, err := net.Listen("tcp", blackholeAddr)
	if err != nil {
		t.Fatalf("restore listener: %v", err)
	}
	recoveredServer := grpc.NewServer()
	recoveredForwarder := dataplane.NewGRPCForwarder(nil, nil)
	recovered := make(chan struct{})
	var recoveredOnce sync.Once
	_ = recoveredForwarder.Subscribe(func(*dataplane.Message) { recoveredOnce.Do(func() { close(recovered) }) })
	dataplane.RegisterServer(recoveredServer, recoveredForwarder)
	go func() { _ = recoveredServer.Serve(recoveredListener) }()
	defer recoveredServer.Stop()
	defer recoveredListener.Close()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
		lastErr = forwarder.Forward(ctx, "unreachable-A", &dataplane.Message{Topic: "telemetry/recovered"})
		cancel()
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("cached gRPC connection did not recover after dataplane restoration: %v", lastErr)
	}
	t.Logf("forwarding recovered: %s (delta from restore=%s)", time.Now().Format(time.RFC3339Nano), time.Since(restoreAt))
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("restored dataplane did not receive the forwarded message")
	}
}
