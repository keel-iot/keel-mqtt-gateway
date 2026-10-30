//go:build cluster

package cluster

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestOfflineSession_QueuedQoS1Delivery_ReconnectOnDifferentEdge is Phase 3
// rung 4's first session/QoS-recovery scenario (docs/testing/
// CLUSTER_CORRECTNESS_MATRIX.md §6 "QoS / session durability" table, and
// the previously-deferred offline-ownership behavioral evidence from that
// document's §4 risk #5 / §11 of the governing task).
//
// PRECONDITION: 3-Core/2-Edge cluster, a persistent (non-clean) session
// subscribed QoS1 on edge-1.
// ACTION: the session disconnects (not expired); a publisher on edge-2
// publishes a matching message while it's offline; the session then
// reconnects on edge-2 — a different Edge than the one it was originally
// on, and one that never held its live in-memory Client object.
// OBSERVABLE TRANSITION: an offline-ownership record becomes visible via
// the real management API before the publish is sent (deterministic poll,
// no sleep); the reconnect exercises OnSessionEstablish's cross-node
// rehydrate path (internal/broker/hooks.go's own doc: "seeded from Redis
// for reconnect on a different node").
// ASSERT INVARIANT: the queued message is delivered exactly once to the
// reconnected client (D1 — Redis-persisted inflight survives; INV-OFF-001..
// 003 — the disconnected session has exactly one deterministic owner).
// RECOVERY: n/a — no node is killed in this scenario, only an ordinary
// client disconnect/reconnect.
// FINAL ASSERTION: payload received matches exactly, exactly once.
// LIMITATIONS: does not prove independent per-Core Reconciler agreement
// (RISK-OWN-005 stays a hypothesis per the matrix's own explicit
// boundary — this test observes the MQTT-level consequence only, not each
// Core's private computation).
func TestOfflineSession_QueuedQoS1Delivery_ReconnectOnDifferentEdge(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	const subscriberID = "offline-recovery-subscriber"
	const filter = "telemetry/#"

	// 1. Persistent session on edge-1: CleanSession=false so the broker
	// retains its subscription/inflight state across disconnect (mochi-mqtt/
	// RedisSessionHook's own persistence path — see hooks.go's OnSessionEstablish
	// doc), unlike every other scenario in this package which uses the
	// default clean session (irrelevant to their own invariants).
	sub := connectPersistentClient(t, h.MQTTAddr(0), subscriberID, consumerUsername, consumerPassword)

	subToken := sub.Subscribe(filter, 1, nil)
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("subscribe: %v", subToken.Error())
	}

	// 2. Ordinary disconnect — not a session-destroying event. OnDisconnect's
	// own logic (hooks.go) only tears down cluster state when expire==true;
	// an ordinary DISCONNECT on a persistent session goes through the
	// placeOfflineOwnership path instead.
	sub.Disconnect(250)

	// 3. Deterministic poll for the offline-ownership record to become
	// observable via the real, already-existing management API (GET
	// /api/cluster/routes) — never a fixed sleep. Reimplements the exact
	// production key encoding (internal/cluster/routing.OwnershipKey) locally
	// rather than importing internal/ — same black-box posture as
	// core_leader_death_test.go's sessionOwnerFrom parsing /api/cluster/sessions
	// directly, just for a different endpoint's real JSON shape.
	if !waitForOfflineOwnership(t, h, 0, subscriberID, 20*time.Second) {
		t.Fatal("offline-ownership record for the disconnected session never became observable via GET /api/cluster/routes")
	}

	// 4. Publish from edge-2 while the subscriber is offline — exercises
	// DeliverOffline's forward-to-rendezvous-owner path regardless of
	// whether the computed owner happens to be edge-1 or edge-2 (both are
	// live; either is a valid rendezvous outcome).
	publisher := connectClient(t, h.MQTTAddr(1), "offline-recovery-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)

	payload := fmt.Sprintf("queued-while-offline-%d", time.Now().UnixNano())
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish while subscriber offline: %v", pubToken.Error())
	}

	// 5. Reconnect on edge-2 — deliberately the OTHER Edge from where this
	// ClientID's live Client object ever existed, forcing OnSessionEstablish's
	// cross-node rehydrate-from-Redis branch (never the same-node resume
	// mochi-mqtt handles natively). A brand-new mqtt.Client instance, no
	// re-subscribe: session resumption is server-side (CleanSession=false),
	// and paho's DefaultPublishHandler receives anything with no other
	// registered per-topic route, which is exactly this reconnect's case.
	received := make(chan string, 4)
	reconnectOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + h.MQTTAddr(1)).
		SetClientID(subscriberID).
		SetUsername(consumerUsername).
		SetPassword(consumerPassword).
		SetCleanSession(false).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
			received <- string(msg.Payload())
		})
	reconnected := mqtt.NewClient(reconnectOpts)
	t.Cleanup(func() { reconnected.Disconnect(250) })
	if token := reconnected.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("reconnect on edge-2 (different edge than original): %v", token.Error())
	}

	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("expected queued payload %q, got %q", payload, got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("queued offline publish was never delivered after reconnecting on a different edge — offline session recovery not functional")
	}

	// No second delivery of the same message (D1's "recoverable," not
	// "duplicated" — MQTT's own QoS1 at-least-once bound, not stricter, so
	// this is a sanity margin check, not a strict single-message assertion).
	select {
	case extra := <-received:
		t.Fatalf("unexpected duplicate delivery of queued message: %q", extra)
	case <-time.After(2 * time.Second):
	}
}

// offlineOwnershipKeyPrefix mirrors internal/cluster/routing.ownershipKeyPrefix
// — reimplemented locally rather than imported so this black-box test
// consumes only the real, already-public HTTP contract (GET
// /api/cluster/routes), never an internal package.
const offlineOwnershipKeyPrefix = "$offline/"

// waitForOfflineOwnership polls core index queryFrom's real
// /api/cluster/routes endpoint until an offline-ownership key
// ($offline/<base64(clientID + NUL + filter)>) decodes to clientID, or
// timeout elapses. Deterministic condition poll, never a fixed sleep.
func waitForOfflineOwnership(t *testing.T, h *Harness, queryFrom int, clientID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + h.ManagementAddr(queryFrom) + "/api/cluster/routes")
		if err == nil {
			var routes map[string][]string
			if json.NewDecoder(resp.Body).Decode(&routes) == nil {
				for key := range routes {
					if decodedClientIDMatches(key, clientID) {
						resp.Body.Close()
						return true
					}
				}
			}
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// decodedClientIDMatches decodes a $offline/ ownership key (see
// internal/cluster/routing.OwnershipKey's exact encoding) and reports
// whether it belongs to clientID.
func decodedClientIDMatches(key, clientID string) bool {
	if !strings.HasPrefix(key, offlineOwnershipKeyPrefix) {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, offlineOwnershipKeyPrefix))
	if err != nil {
		return false
	}
	i := strings.IndexByte(string(raw), 0)
	if i < 0 {
		return false
	}
	return string(raw[:i]) == clientID
}
