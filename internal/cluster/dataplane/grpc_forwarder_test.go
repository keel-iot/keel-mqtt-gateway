package dataplane

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

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
