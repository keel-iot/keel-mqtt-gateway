package dataplane

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestGRPCForwarder_Forward_UnknownNodeWaitIsBounded(t *testing.T) {
	fwd := NewGRPCForwarder(func(string) (string, bool) { return "", false }, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := fwd.Forward(ctx, "unknown", &Message{Topic: "t"})
	if !errors.Is(err, errUnknownNode) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected unknown-node and deadline error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond || elapsed > time.Second {
		t.Fatalf("resolution did not respect caller budget: %s", elapsed)
	}
}

func closeTestForwarder(t *testing.T, f *GRPCForwarder) {
	t.Helper()
	t.Cleanup(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, cc := range f.conns {
			_ = cc.Close()
		}
	})
}

func TestGRPCForwarder_Forward_RouteBeforeMembershipDeliversOnce(t *testing.T) {
	addr, received := startTestServer(t)
	var known atomic.Bool
	lookedUp := make(chan struct{}, 1)
	fwd := NewGRPCForwarder(func(id string) (string, bool) {
		if id != "joining-edge" {
			t.Errorf("wrong node: %s", id)
		}
		if known.Load() {
			return addr, true
		}
		select {
		case lookedUp <- struct{}{}:
		default:
		}
		return "", false
	}, nil)
	closeTestForwarder(t, fwd)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := &Message{Topic: "joining/topic", Payload: []byte("payload"), QoS: 2, PublishID: uuid.New()}
	done := make(chan error, 1)
	go func() { done <- fwd.Forward(ctx, "joining-edge", msg) }()
	select {
	case <-lookedUp:
	case <-ctx.Done():
		t.Fatal("resolver was not called")
	}
	// The route has selected joining-edge, but no address is known yet.
	select {
	case err := <-done:
		t.Fatalf("failed before membership arrived: %v", err)
	default:
	}
	known.Store(true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("forward did not resume")
	}
	select {
	case got := <-received:
		if got.PublishID != msg.PublishID || got.Topic != msg.Topic || string(got.Payload) != "payload" || got.QoS != 2 {
			t.Fatalf("message changed: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("no delivery")
	}
	select {
	case <-received:
		t.Fatal("duplicate delivery")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestGRPCForwarder_Forward_CancelledResolutionSendsNothing(t *testing.T) {
	addr, received := startTestServer(t)
	var known atomic.Bool
	lookedUp := make(chan struct{}, 1)
	fwd := NewGRPCForwarder(func(string) (string, bool) {
		if known.Load() {
			return addr, true
		}
		select {
		case lookedUp <- struct{}{}:
		default:
		}
		return "", false
	}, nil)
	closeTestForwarder(t, fwd)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fwd.Forward(ctx, "joining-edge", &Message{Topic: "t"}) }()
	select {
	case <-lookedUp:
	case <-time.After(time.Second):
		t.Fatal("no resolution")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation ignored")
	}
	known.Store(true)
	select {
	case <-received:
		t.Fatal("sent after cancellation")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestGRPCForwarder_Forward_KnownNodeResolvesOnce(t *testing.T) {
	addr, _ := startTestServer(t)
	var calls atomic.Int32
	fwd := NewGRPCForwarder(func(string) (string, bool) { calls.Add(1); return addr, true }, nil)
	closeTestForwarder(t, fwd)
	if err := fwd.Forward(context.Background(), "known", &Message{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("known target resolution calls: %d", calls.Load())
	}
}

// startTestServer runs a real in-process gRPC server backed by its own
// GRPCForwarder, returning the address to dial and the received
// *Message once handler fires.
func startTestServer(t *testing.T) (addr string, received chan *Message) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	fwd := NewGRPCForwarder(nil, nil)
	received = make(chan *Message, 1)
	_ = fwd.Subscribe(func(msg *Message) { received <- msg })
	RegisterServer(s, fwd)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(s.Stop)
	return ln.Addr().String(), received
}

func TestGRPCForwarder_Forward_PublishIDRoundTrips(t *testing.T) {
	addr, received := startTestServer(t)
	client := NewGRPCForwarder(func(string) (string, bool) { return addr, true }, nil)
	t.Cleanup(func() {
		client.mu.Lock()
		for _, cc := range client.conns {
			_ = cc.Close()
		}
		client.mu.Unlock()
	})

	want := uuid.New()
	if err := client.Forward(context.Background(), "target", &Message{
		Topic:     "telemetry/device-1",
		Payload:   []byte("23.5"),
		QoS:       1,
		PublishID: want,
	}); err != nil {
		t.Fatalf("Forward: %v", err)
	}

	select {
	case msg := <-received:
		if msg.PublishID != want {
			t.Fatalf("expected PublishID %s, got %s", want, msg.PublishID)
		}
		if msg.Topic != "telemetry/device-1" || string(msg.Payload) != "23.5" || msg.QoS != 1 {
			t.Fatalf("unexpected message: %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the forwarded message")
	}
}

func TestGRPCForwarder_Forward_EmptyPublishID_DecodesToZeroUUID(t *testing.T) {
	addr, received := startTestServer(t)
	client := NewGRPCForwarder(func(string) (string, bool) { return addr, true }, nil)
	t.Cleanup(func() {
		client.mu.Lock()
		for _, cc := range client.conns {
			_ = cc.Close()
		}
		client.mu.Unlock()
	})

	// Zero-value Message.PublishID mirrors an old peer mid rolling-upgrade
	// that never set it — must decode to uuid.Nil, not fail the forward.
	if err := client.Forward(context.Background(), "target", &Message{Topic: "t", Payload: []byte("x")}); err != nil {
		t.Fatalf("Forward: %v", err)
	}

	select {
	case msg := <-received:
		if msg.PublishID != uuid.Nil {
			t.Fatalf("expected zero UUID, got %s", msg.PublishID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the forwarded message")
	}
}

func TestGRPCForwarder_Forward_ContextReachesHandler(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	fwd := NewGRPCForwarder(nil, nil)
	ctxDone := make(chan struct{})
	_ = fwd.SubscribeContext(func(ctx context.Context, _ *Message) {
		<-ctx.Done()
		close(ctxDone)
	})
	RegisterServer(s, fwd)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(s.Stop)

	client := NewGRPCForwarder(func(string) (string, bool) { return ln.Addr().String(), true }, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := client.Forward(ctx, "target", &Message{Topic: "t"}); err == nil {
		t.Fatal("expected forwarding deadline")
	}
	select {
	case <-ctxDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not receive cancelled RPC context")
	}
}

func TestGRPCForwarder_Forward_AcceptedButTimedOutDoesNotRetry(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	receiver := NewGRPCForwarder(nil, nil)
	var accepted atomic.Int32
	_ = receiver.SubscribeContext(func(ctx context.Context, _ *Message) { accepted.Add(1); <-ctx.Done() })
	RegisterServer(server, receiver)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(server.Stop)
	sender := NewGRPCForwarder(func(string) (string, bool) { return ln.Addr().String(), true }, nil)
	closeTestForwarder(t, sender)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := sender.Forward(ctx, "known", &Message{Topic: "t"}); err == nil {
		t.Fatal("expected ambiguous timeout")
	}
	if accepted.Load() != 1 {
		t.Fatalf("RPC accepted %d times, want exactly once", accepted.Load())
	}
}

func TestGRPCForwarder_Forward_UnknownNodeDefaultBudget(t *testing.T) {
	fwd := NewGRPCForwarder(func(string) (string, bool) { return "", false }, nil)
	start := time.Now()
	err := fwd.Forward(context.Background(), "absent", &Message{Topic: "t"})
	if !errors.Is(err, errUnknownNode) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded unknown-node failure: %v", err)
	}
	if elapsed := time.Since(start); elapsed < forwardTimeout-100*time.Millisecond || elapsed > forwardTimeout+time.Second {
		t.Fatalf("default budget not respected: %v", elapsed)
	}
}

func TestGRPCForwarder_Forward_EmptyAddressWaitsForUsableAddress(t *testing.T) {
	addr, received := startTestServer(t)
	var calls atomic.Int32
	sender := NewGRPCForwarder(func(string) (string, bool) {
		if calls.Add(1) == 1 {
			return "", true
		}
		return addr, true
	}, nil)
	closeTestForwarder(t, sender)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sender.Forward(ctx, "joining", &Message{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("no delivery")
	}
}
