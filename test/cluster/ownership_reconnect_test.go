//go:build cluster

package cluster

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestOwnerEdgeDies_ReconnectSucceedsOnAnotherEdge is Phase 3 rung 2's
// second ownership scenario (issue #10, invariants A1/D1). Matches
// docs/testing/CLUSTER_CORRECTNESS_MATRIX.md §6's "Edge / session
// ownership" table row: "Owner Edge dies while session is live, client
// reconnects to a different Edge."
//
// claimClusterSession's "new connection always wins" rule (see
// ownership_test.go's doc) doesn't special-case a dead previous owner —
// the claim isn't gated on successfully reaching/evicting it, only on
// raft.Apply committing. This scenario proves that unconditionally:
// the reconnect must succeed promptly, not block on any dead-node
// detection (gossip failure suspicion, keepalive expiry, etc.).
func TestOwnerEdgeDies_ReconnectSucceedsOnAnotherEdge(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	const clientID = "reconnect-after-owner-death"

	owner := connectClient(t, h.MQTTAddr(0), clientID, deviceAID, DevicePwd)
	if !owner.IsConnected() {
		t.Fatal("owner never reached connected state on edge-1")
	}

	h.KillEdge(0)

	// Reconnect the same ClientID to the surviving Edge. connectClient's
	// own 20s connect timeout already bounds this — no extra sleep for
	// "the dead node to be noticed" is needed or wanted: the point of
	// this scenario is that ownership claim doesn't depend on that at
	// all, so a slow reconnect here would itself be a regression to
	// report, not something to wait out.
	survivor := connectClient(t, h.MQTTAddr(1), clientID, deviceAID, DevicePwd)
	if !survivor.IsConnected() {
		t.Fatal("reconnect to the surviving edge did not reach connected state")
	}

	consumer := connectClient(t, h.MQTTAddr(1), "reconnect-test-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)

	received := make(chan string, 1)
	subToken := consumer.Subscribe("telemetry/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("consumer subscribe: %v", subToken.Error())
	}

	payload := "alive-after-reconnect"
	pubToken := survivor.Publish(publishTopicFor(deviceAID), 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish from reconnected client: %v", pubToken.Error())
	}

	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("expected payload %q, got %q", payload, got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("reconnected client's publish was never delivered — session not actually functional post-reconnect")
	}
}
