package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

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

func TestOfflineInventory_JoinsClientsWithTheirSubscriptions(t *testing.T) {
	h, _ := newTestRedisSessionHook(t)

	client := &mqtt.Client{ID: "device-1"}
	client.Properties.Clean = false
	h.saveClient(client)
	pk := packets.Packet{Filters: packets.Subscriptions{
		{Filter: "telemetry/#"},
	}}
	h.OnSubscribed(client, pk, []byte{1})

	inv, err := h.OfflineInventory()
	if err != nil {
		t.Fatalf("OfflineInventory: %v", err)
	}
	if len(inv) != 1 {
		t.Fatalf("expected 1 offline session, got %d: %+v", len(inv), inv)
	}
	got := inv[0]
	if got.ClientID != "device-1" {
		t.Fatalf("expected client ID device-1, got %q", got.ClientID)
	}
	if len(got.Subscriptions) != 1 || got.Subscriptions[0].Filter != "telemetry/#" || got.Subscriptions[0].QoS != 1 {
		t.Fatalf("unexpected subscriptions: %+v", got.Subscriptions)
	}
}

func TestOfflineInventory_NoClients_ReturnsEmptyNotError(t *testing.T) {
	h, _ := newTestRedisSessionHook(t)

	inv, err := h.OfflineInventory()
	if err != nil {
		t.Fatalf("OfflineInventory: %v", err)
	}
	if len(inv) != 0 {
		t.Fatalf("expected no offline sessions, got %+v", inv)
	}
}

func TestOwnedOfflineSessions_ReturnsOnlyRequestedClients(t *testing.T) {
	h, _ := newTestRedisSessionHook(t)

	device1 := &mqtt.Client{ID: "device-1"}
	device1.Properties.Clean = false
	h.saveClient(device1)
	h.OnSubscribed(device1, packets.Packet{Filters: packets.Subscriptions{{Filter: "telemetry/#"}}}, []byte{1})

	device2 := &mqtt.Client{ID: "device-2"}
	device2.Properties.Clean = false
	h.saveClient(device2)
	h.OnSubscribed(device2, packets.Packet{Filters: packets.Subscriptions{{Filter: "cmd/device-2"}}}, []byte{2})

	owned, err := h.OwnedOfflineSessions([]string{"device-1"})
	if err != nil {
		t.Fatalf("OwnedOfflineSessions: %v", err)
	}
	if len(owned) != 1 {
		t.Fatalf("expected exactly 1 owned session (device-2 not requested), got %d: %+v", len(owned), owned)
	}
	if owned[0].ClientID != "device-1" || len(owned[0].Subscriptions) != 1 || owned[0].Subscriptions[0].Filter != "telemetry/#" {
		t.Fatalf("unexpected owned session: %+v", owned[0])
	}
}

func TestOwnedOfflineSessions_EmptyInput_ReturnsEmptyNotNil(t *testing.T) {
	h, _ := newTestRedisSessionHook(t)

	owned, err := h.OwnedOfflineSessions(nil)
	if err != nil {
		t.Fatalf("OwnedOfflineSessions: %v", err)
	}
	if len(owned) != 0 {
		t.Fatalf("expected no owned sessions, got %+v", owned)
	}
}

func TestMarkDelivered_FirstTimeTrue_SecondTimeFalse(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	publishID := uuid.New()

	first, err := h.MarkDelivered(ctx, publishID, "device-1", time.Minute)
	if err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if !first {
		t.Fatalf("expected true (first time), got false")
	}

	second, err := h.MarkDelivered(ctx, publishID, "device-1", time.Minute)
	if err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if second {
		t.Fatalf("expected false (already marked), got true")
	}
}

func TestMarkDelivered_DifferentClientsIndependent(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	publishID := uuid.New()

	if _, err := h.MarkDelivered(ctx, publishID, "device-1", time.Minute); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	first, err := h.MarkDelivered(ctx, publishID, "device-2", time.Minute)
	if err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if !first {
		t.Fatalf("expected true for a different clientID with the same publishID, got false")
	}
}

func TestMarkDelivered_ZeroPublishID_AlwaysTrue(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)

	first, err := h.MarkDelivered(ctx, uuid.Nil, "device-1", time.Minute)
	if err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if !first {
		t.Fatalf("expected true (nothing to dedup against), got false")
	}
	second, err := h.MarkDelivered(ctx, uuid.Nil, "device-1", time.Minute)
	if err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if !second {
		t.Fatalf("expected true again — zero PublishID never dedups, got false")
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

// TestProvides_NeverAdvertisesBootTimeRestore verifies the phase 6g
// cutover: mochi-mqtt's own readStore() only calls StoredClients/
// StoredSubscriptions/StoredInflightMessages when Provides() says yes to
// them, so never advertising them here is what stops the fleet-wide
// eager load at every boot.
func TestProvides_NeverAdvertisesBootTimeRestore(t *testing.T) {
	h := &RedisSessionHook{}
	for _, b := range []byte{byte(mqtt.StoredClients), byte(mqtt.StoredSubscriptions), byte(mqtt.StoredInflightMessages)} {
		if h.Provides(b) {
			t.Fatalf("expected Provides(%d) to be false (boot-time restore must stay disabled)", b)
		}
	}
}

// TestProvides_StillAdvertisesLiveHooks guards against accidentally
// dropping one of the capabilities RedisSessionHook still needs while
// trimming the boot-time ones above.
func TestProvides_StillAdvertisesLiveHooks(t *testing.T) {
	h := &RedisSessionHook{}
	for _, b := range []byte{
		byte(mqtt.OnSessionEstablished),
		byte(mqtt.OnDisconnect),
		byte(mqtt.OnSubscribed),
		byte(mqtt.OnUnsubscribed),
		byte(mqtt.OnQosPublish),
		byte(mqtt.OnQosComplete),
		byte(mqtt.OnQosDropped),
	} {
		if !h.Provides(b) {
			t.Fatalf("expected Provides(%d) to still be true", b)
		}
	}
}
