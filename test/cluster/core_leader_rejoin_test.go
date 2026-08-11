//go:build cluster

package cluster

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestCoreLeaderRejoin_ConvergesAfterRestart is Phase 3 rung 3's final
// scenario — the leader-side counterpart to
// TestCoreFollowerRejoin_ReconvergesAfterRestart, kept just as atomic:
// this test doesn't kill-and-restart within TestCoreLeaderDeath_*
// (which uses KillCore — the container and its on-disk raft state are
// gone), it starts its own fresh cluster and uses StopCore/StartCore so
// the ex-leader's real BoltDB state survives to be caught up from.
//
// The ex-leader is not required to become leader again — the
// requirement is recovery and convergence, not the final role.
func TestCoreLeaderRejoin_ConvergesAfterRestart(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	exLeaderIdx := currentLeaderIndex(t, h, 0)
	var survivors []int
	for i := range h.Cores {
		if i != exLeaderIdx {
			survivors = append(survivors, i)
		}
	}
	if len(survivors) != 2 {
		t.Fatalf("expected 2 surviving cores, got %d", len(survivors))
	}

	// A session established before the outage — used at the end to
	// confirm no committed decision was lost across the leader's
	// crash-and-rejoin round trip.
	const preExistingID = "leader-rejoin-pre-existing-session"
	preExisting := connectClient(t, h.MQTTAddr(0), preExistingID, deviceAID, DevicePwd)
	if !preExisting.IsConnected() {
		t.Fatal("pre-existing session never reached connected state before stopping the leader")
	}

	t.Logf("diagnostic: stopping leader=core-%d", exLeaderIdx+1)
	h.StopCore(exLeaderIdx)

	newLeaderIdx, _ := waitForNewLeader(h, exLeaderIdx, survivors, 60*time.Second)
	if newLeaderIdx == -1 {
		t.Fatal("no new leader elected among survivors after stopping the leader")
	}
	t.Logf("diagnostic: new leader=core-%d while ex-leader is down", newLeaderIdx+1)

	// Quorum functional with the ex-leader down, before even attempting
	// the restart — same real-arbitration probe used throughout rung 3.
	midOutageConn := connectClient(t, h.MQTTAddr(0), "mid-leader-outage-connect", deviceAID, DevicePwd)
	if !midOutageConn.IsConnected() {
		t.Fatal("ownership arbitration failed with the ex-leader down, before restart was attempted")
	}
	midOutageConn.Disconnect(250)

	h.StartCore(exLeaderIdx)

	waitForManagementAPI(t, h, exLeaderIdx, 60*time.Second)
	waitForLeaderAgreement(t, h, exLeaderIdx, survivors[0], 60*time.Second)

	// Still a raft voter after rejoin — recovery, not silent demotion.
	views, err := getNodeViews(survivors[0], h)
	if err != nil {
		t.Fatalf("query cluster nodes via core index %d: %v", survivors[0], err)
	}
	found := false
	for _, v := range views {
		idx, err := coreIndexFromNodeID(v.NodeID)
		if err != nil || idx != exLeaderIdx {
			continue
		}
		found = true
		if !v.RaftVoter {
			t.Fatalf("ex-leader core-%d is no longer a raft voter after rejoin", exLeaderIdx+1)
		}
	}
	if !found {
		t.Fatalf("ex-leader core-%d not found in core-%d's node view after rejoin", exLeaderIdx+1, survivors[0]+1)
	}

	// Full 3/3 functional check: new decisions and cross-node MQTT
	// traffic both still correct after full convergence.
	postConn := connectClient(t, h.MQTTAddr(1), "post-leader-rejoin-connect", deviceAID, DevicePwd)
	if !postConn.IsConnected() {
		t.Fatal("ownership arbitration failed after full leader rejoin")
	}
	postConn.Disconnect(250)

	consumer := connectClient(t, h.MQTTAddr(0), "leader-rejoin-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)
	received := make(chan string, 1)
	subToken := consumer.Subscribe("telemetry/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("consumer subscribe: %v", subToken.Error())
	}

	publisher := connectClient(t, h.MQTTAddr(1), "leader-rejoin-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)
	payload := "cross-node-traffic-after-leader-rejoin"
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish after leader rejoin: %v", pubToken.Error())
	}
	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("expected payload %q, got %q", payload, got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cross-node delivery did not survive the leader crash-and-rejoin round trip")
	}

	// No committed decision lost across the crash-and-rejoin round
	// trip: the ex-leader's own raft FSM copy, once caught up, must
	// agree with a node that was never stopped on who owns the
	// pre-existing session.
	ownerFromSurvivor := sessionOwnerFrom(t, h, survivors[0], preExistingID)
	ownerFromExLeader := sessionOwnerFrom(t, h, exLeaderIdx, preExistingID)
	if ownerFromSurvivor == "" || ownerFromExLeader == "" {
		t.Fatalf("pre-existing session's committed ownership was lost (survivor core-%d saw %q, ex-leader core-%d saw %q)",
			survivors[0]+1, ownerFromSurvivor, exLeaderIdx+1, ownerFromExLeader)
	}
	if ownerFromSurvivor != ownerFromExLeader {
		t.Fatalf("ex-leader disagrees with survivor on pre-existing session's owner after rejoin: core-%d says %q, core-%d says %q",
			survivors[0]+1, ownerFromSurvivor, exLeaderIdx+1, ownerFromExLeader)
	}

	preExisting.Disconnect(250)
}
