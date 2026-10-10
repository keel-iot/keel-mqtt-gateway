//go:build cluster

package cluster

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const sharedOfflineFilter = "$share/offline-group/telemetry/#"

// TestOfflineSharedSubscription_QueuesExactlyOneMember verifies the complete
// offline shared-subscription path against real Core/Edge/Redis containers:
// four persistent members go offline, with the deterministic rendezvous view
// placing some on one Edge and some on the other; one publish must be queued
// for exactly one member, not once per offline session or once per owner Edge.
func TestOfflineSharedSubscription_QueuesExactlyOneMember(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})
	clientIDs := []string{
		"shared-offline-a",
		"shared-offline-b",
		"shared-offline-c",
		"shared-offline-d",
	}

	for i, clientID := range clientIDs {
		c := connectPersistentClient(t, h.MQTTAddr(i%2), clientID, consumerUsername, consumerPassword)
		token := c.Subscribe(sharedOfflineFilter, 1, nil)
		if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe %s: %v", clientID, token.Error())
		}
		c.Disconnect(250)
	}

	owners := waitForSharedOfflineOwners(t, h, clientIDs, sharedOfflineFilter, 20*time.Second)
	ownerCounts := make(map[string]int)
	for _, owner := range owners {
		ownerCounts[owner]++
	}
	if len(ownerCounts) < 2 {
		t.Fatalf("expected shared offline members to be placed on both Edges, got owners=%v", owners)
	}
	if !hasCountAtLeast(ownerCounts, 2) {
		t.Fatalf("expected at least two shared offline members on one Edge, got owners=%v", owners)
	}

	publisher := connectClient(t, h.MQTTAddr(1), "shared-offline-publisher", deviceAID, DevicePwd)
	payload := fmt.Sprintf("shared-offline-%d", time.Now().UnixNano())
	if token := publisher.Publish(publishTopicFor(deviceAID), 1, false, payload); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("publish while all shared members are offline: %v", token.Error())
	}

	received := make(chan receivedSharedMessage, len(clientIDs))
	for i, clientID := range clientIDs {
		// Reconnect on the opposite Edge so OnSessionEstablish exercises the
		// Redis rehydrate path rather than reusing mochi's local Client state.
		connectPersistentWithHandler(t, h.MQTTAddr((i+1)%2), clientID, func(id string) func(mqtt.Client, mqtt.Message) {
			return func(_ mqtt.Client, msg mqtt.Message) {
				received <- receivedSharedMessage{clientID: id, payload: string(msg.Payload())}
			}
		}(clientID))
	}

	var deliveries []receivedSharedMessage
	deadline := time.After(20 * time.Second)
	for len(deliveries) == 0 {
		select {
		case msg := <-received:
			deliveries = append(deliveries, msg)
		case <-deadline:
			t.Fatalf("no shared offline member received the queued publish; owners=%v", owners)
		}
	}
	select {
	case msg := <-received:
		deliveries = append(deliveries, msg)
	case <-time.After(3 * time.Second):
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected exactly one shared offline delivery, got %d: %+v (owners=%v)", len(deliveries), deliveries, owners)
	}
	if deliveries[0].payload != payload {
		t.Fatalf("expected payload %q, got %+v", payload, deliveries[0])
	}
}

// TestOfflineSharedSubscription_LiveMemberSuppressesOfflineQueue verifies
// that a live member wins the shared group and the persistent offline member
// does not receive a queued copy of the same publish.
func TestOfflineSharedSubscription_LiveMemberSuppressesOfflineQueue(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	offlineID := "shared-live-offline"
	offline := connectPersistentClient(t, h.MQTTAddr(0), offlineID, consumerUsername, consumerPassword)
	if token := offline.Subscribe(sharedOfflineFilter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("offline member subscribe: %v", token.Error())
	}
	offline.Disconnect(250)
	if _, ok := waitForSharedOfflineOwner(t, h, offlineID, sharedOfflineFilter, 20*time.Second); !ok {
		t.Fatal("offline shared owner never became observable")
	}

	liveReceived := make(chan string, 2)
	live := connectClientWithHandler(t, h.MQTTAddr(1), "shared-live-online", func(_ mqtt.Client, msg mqtt.Message) {
		liveReceived <- string(msg.Payload())
	})
	if token := live.Subscribe(sharedOfflineFilter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("live member subscribe: %v", token.Error())
	}
	if !waitForLiveRoute(t, h, 0, sharedOfflineFilter, h.Edges[1].ID, 15*time.Second) {
		t.Fatalf("live shared route %q -> %s never became observable", sharedOfflineFilter, h.Edges[1].ID)
	}

	publisher := connectClient(t, h.MQTTAddr(0), "shared-live-publisher", deviceAID, DevicePwd)
	payload := fmt.Sprintf("shared-live-%d", time.Now().UnixNano())
	if token := publisher.Publish(publishTopicFor(deviceAID), 1, false, payload); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("publish with live and offline shared members: %v", token.Error())
	}
	select {
	case got := <-liveReceived:
		if got != payload {
			t.Fatalf("live member received %q, want %q", got, payload)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("live shared member did not receive the publish")
	}

	offlineReceived := make(chan string, 1)
	connectPersistentWithHandler(t, h.MQTTAddr(0), offlineID, func(_ mqtt.Client, msg mqtt.Message) {
		offlineReceived <- string(msg.Payload())
	})
	select {
	case got := <-offlineReceived:
		t.Fatalf("offline member received a queued copy despite a live group member: %q", got)
	case <-time.After(5 * time.Second):
	}
}

type receivedSharedMessage struct {
	clientID string
	payload  string
}

func connectClientWithHandler(t *testing.T, broker, clientID string, handler mqtt.MessageHandler) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + broker).
		SetClientID(clientID).
		SetUsername(consumerUsername).
		SetPassword(consumerPassword).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(handler)
	c := mqtt.NewClient(opts)
	if token := c.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("connect %s to %s: %v", clientID, broker, token.Error())
	}
	t.Cleanup(func() { c.Disconnect(250) })
	return c
}

func connectPersistentWithHandler(t *testing.T, broker, clientID string, handler mqtt.MessageHandler) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + broker).
		SetClientID(clientID).
		SetUsername(consumerUsername).
		SetPassword(consumerPassword).
		SetCleanSession(false).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false).
		SetDefaultPublishHandler(handler)
	c := mqtt.NewClient(opts)
	if token := c.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("connect persistent %s to %s: %v", clientID, broker, token.Error())
	}
	t.Cleanup(func() { c.Disconnect(250) })
	return c
}

func waitForSharedOfflineOwners(t *testing.T, h *Harness, clientIDs []string, filter string, timeout time.Duration) map[string]string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		owners := make(map[string]string, len(clientIDs))
		routes := fetchRoutes(t, h, 0)
		for key, nodes := range routes {
			clientID, routeFilter, ok := decodeSharedOwnershipKey(key)
			if !ok || routeFilter != filter || len(nodes) == 0 {
				continue
			}
			for _, wanted := range clientIDs {
				if clientID == wanted {
					owners[clientID] = nodes[0]
				}
			}
		}
		if len(owners) == len(clientIDs) {
			return owners
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("shared offline ownership did not converge for filter %q", filter)
	return nil
}

func waitForSharedOfflineOwner(t *testing.T, h *Harness, clientID, filter string, timeout time.Duration) (string, bool) {
	owners := waitForSharedOfflineOwners(t, h, []string{clientID}, filter, timeout)
	owner, ok := owners[clientID]
	return owner, ok
}

func decodeSharedOwnershipKey(key string) (clientID, filter string, ok bool) {
	const prefix = "$offline/"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, prefix))
	if err != nil {
		return "", "", false
	}
	parts := strings.SplitN(string(raw), "\x00", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func hasCountAtLeast(counts map[string]int, want int) bool {
	for _, count := range counts {
		if count >= want {
			return true
		}
	}
	return false
}
