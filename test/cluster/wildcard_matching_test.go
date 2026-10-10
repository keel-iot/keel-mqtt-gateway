//go:build cluster

package cluster

import (
	"fmt"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestCrossNodeWildcard_BareTopicMatchesHashFilter is Phase 3 rung 5's
// wildcard-matching regression (issue #19, docs/testing/
// CLUSTER_CORRECTNESS_MATRIX.md §7, feedback/FB-006.md, spec.md
// Requirement R5). This is the literal bare-topic shape
// TestMultiNodeHappyPath originally used and had to work around — see
// that test's own comment.
//
// PRECONDITION: 3-Core/2-Edge cluster, consumer subscribed "telemetry/#"
// on edge-1.
// ACTION: publish to the literal bare topic "telemetry" (no sub-path) —
// a real, `isAllowedPublish`-permitted topic shape (hooks.go's own
// `case topic == "telemetry": return true`) — from edge-2.
// OBSERVABLE TRANSITION: per MQTT's own "#" parent-level matching rule
// (the same rule TestRouterHashWildcard already covers for
// "sport/tennis/#" matching "sport/tennis" at the in-process
// fake-store level), "telemetry/#" must also match "telemetry" itself.
// ASSERT INVARIANT: C1 — a live subscription is routable from every node
// that needs to forward to it. Local mochi-mqtt matching already gets
// this right (single-node); this test is specifically about the
// Olric-backed distributed path issue #19 named.
// FINAL ASSERTION: the consumer receives the bare-topic publish.
// LIMITATIONS: reproduces the exact reported shape only — does not
// enumerate every parent/child wildcard boundary case (that's
// internal/cluster/routing/router_test.go's job at the unit level,
// where the fake in-memory store already proves the pure matching logic
// correct — see this test's own root-cause note once filed).
func TestCrossNodeWildcard_BareTopicMatchesHashFilter(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	consumer := connectClient(t, h.MQTTAddr(0), "wildcard-bare-topic-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)

	received := make(chan string, 1)
	subToken := consumer.Subscribe("telemetry/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("subscribe: %v", subToken.Error())
	}

	publisher := connectClient(t, h.MQTTAddr(1), "wildcard-bare-topic-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)

	payload := fmt.Sprintf("bare-topic-%d", time.Now().UnixNano())
	// The literal bare topic, deliberately — not the Hono sub-path
	// happy_path_test.go now uses as its workaround.
	pubToken := publisher.Publish("telemetry", 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish to bare topic %q: %v", "telemetry", pubToken.Error())
	}

	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("expected %q, got %q", payload, got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cross-node delivery of a bare topic against a \"#\" filter did not happen — issue #19's reported matching gap")
	}
}
