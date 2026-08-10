package broker

import (
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// seedSessionState writes a subscription and an in-flight message for
// clientID directly through the hooks a real client would trigger, so
// these tests exercise the same write path production traffic does.
func seedSessionState(t *testing.T, h *RedisSessionHook, clientID, filter string) {
	t.Helper()
	cl := &mqtt.Client{ID: clientID}
	h.OnSubscribed(cl, packets.Packet{Filters: packets.Subscriptions{{Filter: filter}}}, []byte{0})
	h.OnQosPublish(cl, packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Qos: 1}}, 0, 0)
}

func assertSessionStateGone(t *testing.T, h *RedisSessionHook, clientID string) {
	t.Helper()
	subs, err := h.SubscriptionsForClient(clientID)
	if err != nil {
		t.Fatalf("SubscriptionsForClient(%s): %v", clientID, err)
	}
	if len(subs) != 0 {
		t.Errorf("expected no subscriptions left for %s after expiry cleanup, got %d", clientID, len(subs))
	}
	inflight, err := h.InflightForClient(clientID)
	if err != nil {
		t.Fatalf("InflightForClient(%s): %v", clientID, err)
	}
	if len(inflight) != 0 {
		t.Errorf("expected no inflight messages left for %s after expiry cleanup, got %d", clientID, len(inflight))
	}
}

func assertSessionStateIntact(t *testing.T, h *RedisSessionHook, clientID string) {
	t.Helper()
	subs, err := h.SubscriptionsForClient(clientID)
	if err != nil {
		t.Fatalf("SubscriptionsForClient(%s): %v", clientID, err)
	}
	if len(subs) == 0 {
		t.Errorf("expected %s's subscriptions to remain intact, got none", clientID)
	}
	inflight, err := h.InflightForClient(clientID)
	if err != nil {
		t.Fatalf("InflightForClient(%s): %v", clientID, err)
	}
	if len(inflight) == 0 {
		t.Errorf("expected %s's inflight messages to remain intact, got none", clientID)
	}
}

// TestOnDisconnectExpire_CleansSubscriptionsAndInflight reproduces a real
// standalone gap: OnDisconnect(expire=true) only HDels the client-record
// hash field (keel:gw:CL), never the subscription (keel:gw:SUB) or
// inflight (keel:gw:IFM) fields for that clientID — reproducible with
// zero clustering, since RedisSessionHook has no ClusterRegistry
// dependency at all (grepped: none exists in this file).
func TestOnDisconnectExpire_CleansSubscriptionsAndInflight(t *testing.T) {
	h, _ := newTestRedisSessionHook(t)
	seedSessionState(t, h, "expiring-client", "telemetry/x")

	h.OnDisconnect(&mqtt.Client{ID: "expiring-client"}, nil, true)

	assertSessionStateGone(t, h, "expiring-client")
}

// TestOnClientExpired_CleansAllSessionState reproduces the other half of
// the same gap: RedisSessionHook.Provides doesn't even list
// mqtt.OnClientExpired, so a genuinely persistent session's real,
// delayed expiry (the periodic sweep, not the immediate clean-session
// OnDisconnect path above) never touches Redis at all — CL, SUB, and
// IFM all stay orphaned indefinitely.
func TestOnClientExpired_CleansAllSessionState(t *testing.T) {
	h, _ := newTestRedisSessionHook(t)
	seedSessionState(t, h, "persistent-expiring-client", "telemetry/y")
	seedSessionState(t, h, "unrelated-client", "telemetry/z")

	h.OnClientExpired(&mqtt.Client{ID: "persistent-expiring-client"})

	assertSessionStateGone(t, h, "persistent-expiring-client")
	assertSessionStateIntact(t, h, "unrelated-client")
}
