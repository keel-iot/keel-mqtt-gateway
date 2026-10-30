//go:build cluster

package cluster

import (
	"context"
	"encoding/base64"
	"strconv"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redis/go-redis/v9"
)

// TestPacketID_CollisionAcrossLiveAndOfflineCounters is the regression for
// RISK-OWN-001. The original implementation let mochi's live allocator and
// Redis's offline counter both select ID 1, then silently overwrote the live
// entry in a fleet-wide IFM hash. QueueOfflineInflight now watches the
// per-client inflight hash and atomically selects the first unused ID.
//
// PRECONDITION: fresh persistent ClientID with no prior packet-ID history
// on either counter.
// ACTION: (1) receive one live QoS1 message without acking it (so its
// Redis IFM entry survives); (2) disconnect (session persists); (3)
// receive one offline-queued QoS1 message for the same filter while
// disconnected — DeliverOffline's counter also allocates ID 1.
// OBSERVABLE TRANSITION: reconnect on a different Edge (OnSessionEstablish
// rehydrates from whatever survived in keel:gw:IFM).
// ASSERT INVARIANT (the one the matrix's §3 explicitly declined to accept
// as guaranteed, tested anyway because a real product-correctness defect
// doesn't need a pre-accepted invariant to be worth catching): both
// messages are eventually delivered, distinguishably, with no silent
// loss.
// RECOVERY: n/a.
// FINAL ASSERTION: both "live-message" and "offline-message" payloads are
// observed after reconnect; the offline entry must use a non-colliding ID.
// LIMITATIONS: this reproduces the specific low-packet-ID-value ordering
// (freshest, most reliably deterministic case). It does not attempt to
// enumerate every possible interleaving of the two counters.
func TestPacketID_CollisionAcrossLiveAndOfflineCounters(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})
	// This regression exercises the post-activation directional IFM format.
	// Schema absence intentionally means legacy compatibility mode, so make
	// the activation barrier explicit for this test.
	rdb := redis.NewClient(&redis.Options{Addr: h.RedisAddr(0)})
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.Set(ctx, "keel:gw:IFM:SCHEMA", "2", 0).Err(); err != nil {
		t.Fatalf("activate directional IFM schema: %v", err)
	}
	// Existing processes cache the schema mode at startup. Restart the test
	// Edges after activation, matching the documented operational procedure.
	for i := range h.Edges {
		replaceRollingEdge(t, h, i, "keel-mqtt-gateway:cluster-test", "edge-directional-"+strconv.Itoa(i+1))
	}

	const clientID = "pkid-collision-subscriber"
	const filter = "telemetry/#"

	received := make(chan mqtt.Message, 8)
	subOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + h.MQTTAddr(0)).
		SetClientID(clientID).
		SetUsername(consumerUsername).
		SetPassword(consumerPassword).
		SetCleanSession(false).
		SetAutoAckDisabled(true). // leave delivered messages inflight — never PUBACK
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false)
	sub := mqtt.NewClient(subOpts)
	if token := sub.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("subscriber connect: %v", token.Error())
	}
	subToken := sub.Subscribe(filter, 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- msg
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("subscribe: %v", subToken.Error())
	}

	publisher := connectClient(t, h.MQTTAddr(1), "pkid-collision-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)
	topic := publishTopicFor(deviceAID)

	// 1. Live message, delivered while subscriber is connected — mochi's
	// own in-memory counter assigns this clientID's very first packet ID
	// (expected: 1). Left unacked (AutoAckDisabled) so its Redis IFM
	// entry is not cleaned up by OnQosComplete.
	livePubToken := publisher.Publish(topic, 1, false, "live-message")
	if !livePubToken.WaitTimeout(10*time.Second) || livePubToken.Error() != nil {
		t.Fatalf("live publish: %v", livePubToken.Error())
	}

	var liveMsg mqtt.Message
	select {
	case liveMsg = <-received:
	case <-time.After(15 * time.Second):
		t.Fatal("live message was never delivered to the subscriber")
	}
	if string(liveMsg.Payload()) != "live-message" {
		t.Fatalf("expected live-message first, got %q", liveMsg.Payload())
	}
	liveID := liveMsg.MessageID()

	if !waitForRedisHashFieldExists(t, rdb, ctx, redisInflightKeyFor(clientID), "out:"+strconv.FormatUint(uint64(liveID), 10), 15*time.Second) {
		t.Fatalf("live message's Redis IFM entry (clientID:%d) was never persisted — cannot test its survival without it existing first", liveID)
	}

	// 2. Disconnect — persistent session, offline ownership placed, live
	// IFM entry above is untouched (only true expiry/ack clears it).
	sub.Disconnect(250)
	if !waitForOfflineOwnership(t, h, 0, clientID, 20*time.Second) {
		t.Fatal("offline-ownership record never became observable")
	}

	// 3. Offline message while disconnected — the allocator sees packet ID 1
	// already persisted by the live path and atomically selects ID 2.
	offlinePubToken := publisher.Publish(topic, 1, false, "offline-message")
	if !offlinePubToken.WaitTimeout(10*time.Second) || offlinePubToken.Error() != nil {
		t.Fatalf("offline publish: %v", offlinePubToken.Error())
	}
	// The offline enqueue's own packet ID isn't directly observable
	// pre-reconnect without a second management endpoint — the reconnect
	// below is the real, black-box observation point.

	// 4. Reconnect on a DIFFERENT edge — OnSessionEstablish rehydrates
	// from whatever the keel:gw:IFM hash actually contains now.
	reconnectedReceived := make(chan mqtt.Message, 8)
	reconnectOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + h.MQTTAddr(1)).
		SetClientID(clientID).
		SetUsername(consumerUsername).
		SetPassword(consumerPassword).
		SetCleanSession(false).
		SetAutoAckDisabled(true).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
			reconnectedReceived <- msg
		})
	reconnected := mqtt.NewClient(reconnectOpts)
	t.Cleanup(func() { reconnected.Disconnect(250) })
	if token := reconnected.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("reconnect on edge-2: %v", token.Error())
	}

	seen := map[string]bool{}
	deadline := time.After(15 * time.Second)
collect:
	for len(seen) < 2 {
		select {
		case msg := <-reconnectedReceived:
			seen[string(msg.Payload())] = true
		case <-deadline:
			break collect
		}
	}

	if !seen["offline-message"] {
		t.Errorf("expected the offline-queued message to survive reconnect, it did not — seen: %v", seen)
	}
	if !seen["live-message"] {
		t.Errorf("packet-ID collision regression: live-message with packet ID %d was lost; seen: %v", liveID, seen)
	}
}

func redisInflightKeyFor(clientID string) string {
	return "keel:gw:IFM:" + base64.RawURLEncoding.EncodeToString([]byte(clientID))
}

func waitForRedisHashFieldExists(t *testing.T, rdb *redis.Client, ctx context.Context, hash, field string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok, err := rdb.HExists(ctx, hash, field).Result(); err == nil && ok {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
