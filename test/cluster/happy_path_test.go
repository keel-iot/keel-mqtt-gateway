//go:build cluster

package cluster

import (
	"fmt"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// deviceAID is this test's one publishing device — must exist in the
// credentials fixture NewHarness bakes into every Edge.
const deviceAID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

// Hardcoded test-consumer principal (internal/broker/hooks.go's
// TEMPORARY testConsumerUsername/Password) — bypasses AUTH_BACKEND
// entirely, always available, allowed to subscribe "telemetry/#"
// (isAllowedConsumerSubscribe) regardless of tenant/RBAC config. Same
// mechanism test/e2e/cross_node_test.go itself uses.
const (
	consumerUsername = "test-consumer"
	consumerPassword = "consumer-e2e-testpass"
)

// TestMultiNodeHappyPath is the first executed Phase 3 scenario from
// docs/testing/CLUSTER_CORRECTNESS_MATRIX.md §6 ("Multi-node happy
// path" — C1's cross-node routing invariant, exercised end to end
// against a real 3-Core/2-Edge cluster rather than the single-process
// fakes/mocks the rest of this repo's tests use). Subscribe on Edge A,
// publish through Edge B, confirm delivery — the foundation every later
// ownership/node-loss/QoS-recovery scenario in this package builds on.
//
// Requires Docker; run with: go test -tags cluster ./test/cluster/...
func TestMultiNodeHappyPath(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	consumer := connectClient(t, h.MQTTAddr(0), "happy-path-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)

	received := make(chan string, 1)
	token := consumer.Subscribe("telemetry/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}
	// mochi-mqtt's own SUBACK ordering: the raft.Apply behind OnSubscribed
	// commits before SUBACK is sent (same reasoning cross_node_test.go
	// documents), so no extra sleep is needed here beyond WaitTimeout
	// above already blocking for the SUBACK.

	publisher := connectClient(t, h.MQTTAddr(1), "happy-path-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)

	payload := fmt.Sprintf("hello-from-%d", time.Now().UnixNano())
	// Hono-shaped sub-path ("telemetry/<tenant>/<device>"), same
	// convention test/e2e/cross_node_test.go already uses — isHonoTopicOwned
	// allows it for the publishing device, and it's a real path through
	// the cluster router's own filter matching (unlike the bare "telemetry"
	// literal this test originally used, which production's own local
	// mochi-mqtt matching accepts but the Olric-backed cluster router does
	// not — a real, if narrow, matching gap, tracked separately from this
	// scenario since it isn't what C1's happy path is meant to exercise).
	publishTopic := fmt.Sprintf("telemetry/%s/%s", TenantID, deviceAID)
	pubToken := publisher.Publish(publishTopic, 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish: %v", pubToken.Error())
	}

	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("expected payload %q, got %q", payload, got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for cross-node delivery — consumer on edge-1 never received the publish sent to edge-2")
	}
}

func connectClient(t *testing.T, broker, clientID, username, password string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + broker).
		SetClientID(clientID).
		SetUsername(username).
		SetPassword(password).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false)
	c := mqtt.NewClient(opts)
	token := c.Connect()
	if !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("connect %s to %s: %v", clientID, broker, token.Error())
	}
	t.Cleanup(func() { c.Disconnect(250) })
	return c
}
