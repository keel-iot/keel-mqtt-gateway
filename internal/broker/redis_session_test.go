package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
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
	if err := h.router.Client().Set(ctx, redisPacketIDKey("device-1"), 65535, 0).Err(); err != nil {
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

func TestQueueOfflineInflight_SkipsPacketIDAlreadyUsedByLiveMessage(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	cl := &mqtt.Client{ID: "device-1"}
	live := packets.Packet{
		PacketID:    1,
		TopicName:   "telemetry/live",
		Payload:     []byte("live-message"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
	}
	h.OnQosPublish(cl, live, time.Now().Unix(), 0)

	packetID, queued, err := h.QueueOfflineInflight(ctx, uuid.New(), cl.ID, time.Minute, session.InflightMessage{
		Topic: "telemetry/offline", Payload: []byte("offline-message"), QoS: 1,
	})
	if err != nil {
		t.Fatalf("QueueOfflineInflight: %v", err)
	}
	if !queued || packetID != 2 {
		t.Fatalf("expected offline message queued with first free packet ID 2, got queued=%v packetID=%d", queued, packetID)
	}

	stored, err := h.InflightForClient(cl.ID)
	if err != nil {
		t.Fatalf("InflightForClient: %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("expected both live and offline messages to survive, got %d: %+v", len(stored), stored)
	}
	seen := make(map[string]bool, len(stored))
	for _, msg := range stored {
		seen[string(msg.Payload)] = true
	}
	if !seen["live-message"] || !seen["offline-message"] {
		t.Fatalf("packet-ID collision overwrote a message: seen=%v", seen)
	}
}

func TestQueueOfflineInflight_DedupAndEnqueueAreAtomic(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	publishID := uuid.New()
	msg := session.InflightMessage{Topic: "telemetry/x", Payload: []byte("x"), QoS: 1}

	firstID, first, err := h.QueueOfflineInflight(ctx, publishID, "device-1", time.Minute, msg)
	if err != nil || !first {
		t.Fatalf("first QueueOfflineInflight: id=%d queued=%v err=%v", firstID, first, err)
	}
	secondID, second, err := h.QueueOfflineInflight(ctx, publishID, "device-1", time.Minute, msg)
	if err != nil {
		t.Fatalf("second QueueOfflineInflight: %v", err)
	}
	if second || secondID != 0 {
		t.Fatalf("expected duplicate to be suppressed, got id=%d queued=%v", secondID, second)
	}
	if got, err := h.router.Client().HLen(ctx, redisInflightKey("device-1")).Result(); err != nil || got != 1 {
		t.Fatalf("expected exactly one inflight entry, got %d err=%v", got, err)
	}
}

func TestPerClientKeys_IsolateHydrationFromOtherDevices(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	for _, clientID := range []string{"device-1", "device-2"} {
		cl := &mqtt.Client{ID: clientID}
		h.saveClient(cl)
		h.OnSubscribed(cl, packets.Packet{Filters: packets.Subscriptions{{Filter: "telemetry/" + clientID}}}, []byte{1})
		h.OnQosPublish(cl, packets.Packet{PacketID: 1, Payload: []byte(clientID), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}}, 0, 0)
	}

	subs, err := h.SubscriptionsForClient("device-1")
	if err != nil || len(subs) != 1 || subs[0].Client != "device-1" {
		t.Fatalf("device-1 subscriptions leaked across clients: %+v err=%v", subs, err)
	}
	inflight, err := h.InflightForClient("device-1")
	if err != nil || len(inflight) != 1 || inflight[0].Client != "device-1" {
		t.Fatalf("device-1 inflight leaked across clients: %+v err=%v", inflight, err)
	}
	if got, err := h.router.Client().SCard(ctx, redisClientIndex).Result(); err != nil || got != 2 {
		t.Fatalf("expected two indexed clients, got %d err=%v", got, err)
	}
}

func TestMigrateLegacySchema_PreservesExistingSessions(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	client := storage.Client{ID: "legacy-device", T: storage.ClientKey}
	clientRaw, err := client.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal legacy client: %v", err)
	}
	sub := storage.Subscription{ID: "legacy-device:telemetry/#", T: storage.SubscriptionKey, Client: "legacy-device", Filter: "telemetry/#", Qos: 1}
	subRaw, err := sub.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal legacy subscription: %v", err)
	}
	msg := storage.Message{ID: "legacy-device:7", T: storage.InflightKey, Client: "legacy-device", PacketID: 7, Payload: []byte("persisted")}
	msgRaw, err := msg.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal legacy inflight: %v", err)
	}

	pipe := h.router.Client().Pipeline()
	pipe.HSet(ctx, legacyClientHash, client.ID, clientRaw)
	pipe.HSet(ctx, legacySubHash, sub.ID, subRaw)
	pipe.HSet(ctx, legacyInflightHash, msg.ID, msgRaw)
	pipe.Set(ctx, legacyPacketIDPrefix+client.ID, 7, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	if err := h.migrateLegacySchema(ctx); err != nil {
		t.Fatalf("migrateLegacySchema: %v", err)
	}
	subs, err := h.SubscriptionsForClient(client.ID)
	if err != nil || len(subs) != 1 || subs[0].Filter != "telemetry/#" {
		t.Fatalf("migrated subscriptions mismatch: %+v err=%v", subs, err)
	}
	inflight, err := h.InflightForClient(client.ID)
	if err != nil || len(inflight) != 1 || string(inflight[0].Payload) != "persisted" {
		t.Fatalf("migrated inflight mismatch: %+v err=%v", inflight, err)
	}
	if counter, err := h.router.Client().Get(ctx, redisPacketIDKey(client.ID)).Int64(); err != nil || counter != 7 {
		t.Fatalf("migrated packet-ID counter = %d, err=%v", counter, err)
	}
	if remaining, err := h.router.Client().Exists(ctx, legacyClientHash, legacySubHash, legacyInflightHash, legacyPacketIDPrefix+client.ID).Result(); err != nil || remaining != 0 {
		t.Fatalf("legacy keys not removed: remaining=%d err=%v", remaining, err)
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
