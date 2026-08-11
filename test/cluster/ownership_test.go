//go:build cluster

package cluster

import (
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestDuplicateClientID_NewConnectionEvictsOldOwner is the second rung
// of the Phase 3 complexity ladder — Ownership (issue #10, invariants
// A1/A2/A3). Matches docs/testing/CLUSTER_CORRECTNESS_MATRIX.md §6's
// "Edge / session ownership" table row: "Simultaneous duplicate-ClientID
// CONNECT on two Edges, confirm exactly one committed owner and the
// loser gets evicted."
//
// claimClusterSession's own doc (internal/broker/hooks.go) states the
// rule under test: a new connection always wins; the previous owner is
// evicted best-effort over the cluster data plane, backstopped by that
// node's own MQTT keepalive if the Evict RPC is lost. Both CONNECTs
// below succeed locally (the broker doesn't reject a duplicate ClientID
// at CONNACK time) — the invariant is that the FIRST owner is
// subsequently, asynchronously disconnected once the SECOND claim
// commits, never the other way around, and never both left connected.
func TestDuplicateClientID_NewConnectionEvictsOldOwner(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	const sharedClientID = "ownership-duplicate-client"

	firstEvicted := make(chan struct{})
	var firstEvictedOnce atomic.Bool
	firstOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + h.MQTTAddr(0)).
		SetClientID(sharedClientID).
		SetUsername(deviceAID).
		SetPassword(DevicePwd).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(_ mqtt.Client, _ error) {
			if firstEvictedOnce.CompareAndSwap(false, true) {
				close(firstEvicted)
			}
		})
	first := mqtt.NewClient(firstOpts)
	if token := first.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("first connect: %v", token.Error())
	}
	t.Cleanup(func() { first.Disconnect(250) })

	// claimClusterSession's raft.Apply is synchronous with CONNACK (same
	// ordering happy_path_test.go's SUBACK comment relies on) — no sleep
	// needed here before the second CONNECT for the first claim to have
	// committed.

	second := connectClient(t, h.MQTTAddr(1), sharedClientID, deviceAID, DevicePwd)

	select {
	case <-firstEvicted:
		// Expected: the first connection is the one that loses ownership.
	case <-time.After(15 * time.Second):
		t.Fatal("first connection was never evicted after a second CONNECT claimed the same ClientID")
	}

	// The new owner must still be fully functional — evicting the loser
	// is not itself allowed to disturb the winner's own session.
	payload := "still-connected-after-eviction"
	pubToken := second.Publish(publishTopicFor(deviceAID), 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish from surviving owner: %v", pubToken.Error())
	}
	if !second.IsConnected() {
		t.Fatal("second (winning) connection was not still connected after the eviction")
	}
}

func publishTopicFor(deviceID string) string {
	return "telemetry/" + TenantID + "/" + deviceID
}
