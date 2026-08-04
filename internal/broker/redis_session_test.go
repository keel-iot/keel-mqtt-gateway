package broker

import (
	"context"
	"os"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

// newTestRedisSessionHook requires a live Redis (TEST_REDIS_ADDR) — skipped
// otherwise, same convention as newTestRetainedStore.
func newTestRedisSessionHook(t *testing.T) (*RedisSessionHook, context.Context) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set, skipping live-Redis session hook test")
	}
	ctx := context.Background()
	router, err := redisrouter.New(ctx, addr, "")
	if err != nil {
		t.Fatalf("redisrouter.New: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Client().FlushDB(ctx).Err()
	})
	return NewRedisSessionHook(router, nil), ctx
}

func TestNextPacketID_MonotonicPerClient(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)

	first, err := h.NextPacketID(ctx, "device-1")
	if err != nil {
		t.Fatalf("NextPacketID: %v", err)
	}
	second, err := h.NextPacketID(ctx, "device-1")
	if err != nil {
		t.Fatalf("NextPacketID: %v", err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("expected 1 then 2, got %d then %d", first, second)
	}
}

func TestNextPacketID_IndependentPerClient(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)

	a, err := h.NextPacketID(ctx, "device-a")
	if err != nil {
		t.Fatalf("NextPacketID(device-a): %v", err)
	}
	b, err := h.NextPacketID(ctx, "device-b")
	if err != nil {
		t.Fatalf("NextPacketID(device-b): %v", err)
	}
	if a != 1 || b != 1 {
		t.Fatalf("expected each client to start at 1, got device-a=%d device-b=%d", a, b)
	}
}

func TestNextPacketID_NeverZeroWrapsPast65535(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)

	// Fast-forward the counter to just before the wrap boundary instead of
	// calling NextPacketID 65535 times.
	if err := h.router.Client().Set(ctx, redisPacketIDKeyPrefix+"device-1", 65535, 0).Err(); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	id, err := h.NextPacketID(ctx, "device-1")
	if err != nil {
		t.Fatalf("NextPacketID: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected wraparound to 1 (never 0), got %d", id)
	}
}

func TestEnqueueOfflineInflight_ReadableViaInflightForClient(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)

	packetID, err := h.NextPacketID(ctx, "device-1")
	if err != nil {
		t.Fatalf("NextPacketID: %v", err)
	}
	msg := session.InflightMessage{Topic: "telemetry/temp", Payload: []byte("23.5"), QoS: 1}
	if err := h.EnqueueOfflineInflight(ctx, "device-1", packetID, msg); err != nil {
		t.Fatalf("EnqueueOfflineInflight: %v", err)
	}

	stored, err := h.InflightForClient("device-1")
	if err != nil {
		t.Fatalf("InflightForClient: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored inflight message, got %d", len(stored))
	}
	got := stored[0]
	if got.TopicName != "telemetry/temp" || string(got.Payload) != "23.5" || got.PacketID != packetID {
		t.Fatalf("stored message mismatch: %+v", got)
	}
	if got.FixedHeader.Qos != 1 {
		t.Fatalf("expected QoS 1, got %d", got.FixedHeader.Qos)
	}
}
