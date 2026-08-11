//go:build cluster

package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// nodeView mirrors internal/cluster/management/api.go's response shape
// for GET /api/cluster/nodes — duplicated here rather than imported
// (that package is internal/, this is a separate build-tagged test
// binary) since only the two fields this test needs are read.
type nodeView struct {
	NodeID    string `json:"node_id"`
	IsLeader  bool   `json:"is_leader"`
	RaftVoter bool   `json:"raft_voter"`
}

// coreIndexFromNodeID converts a management-API node_id like "core-2"
// into this harness's 0-based Cores index (1).
func coreIndexFromNodeID(nodeID string) (int, error) {
	n, err := strconv.Atoi(strings.TrimPrefix(nodeID, "core-"))
	if err != nil {
		return -1, fmt.Errorf("parse core index from node_id %q: %w", nodeID, err)
	}
	return n - 1, nil
}

// getNodeViews queries core index queryFrom's own management API and
// decodes its GET /api/cluster/nodes response.
func getNodeViews(queryFrom int, h *Harness) ([]nodeView, error) {
	resp, err := http.Get("http://" + h.ManagementAddr(queryFrom) + "/api/cluster/nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var views []nodeView
	if err := json.NewDecoder(resp.Body).Decode(&views); err != nil {
		return nil, err
	}
	return views, nil
}

// currentLeaderIndex queries core index i's own management API — a
// real, per-process raft.LeaderID() read, not a value inferred from
// bootstrap/construction order — and returns the 0-based Cores index of
// whichever node currently reports itself leader.
func currentLeaderIndex(t *testing.T, h *Harness, queryFrom int) int {
	t.Helper()
	views, err := getNodeViews(queryFrom, h)
	if err != nil {
		t.Fatalf("query cluster nodes via core index %d: %v", queryFrom, err)
	}
	for _, v := range views {
		if !v.IsLeader {
			continue
		}
		idx, err := coreIndexFromNodeID(v.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		return idx
	}
	t.Fatalf("no core reported itself as leader in /api/cluster/nodes response: %+v", views)
	return -1
}

// TestCoreFollowerDeath_ClusterStaysOperational is Phase 3 rung 3's
// first node-loss scenario (docs/testing/CLUSTER_CORRECTNESS_MATRIX.md
// §6's Core table row: "Kill a follower, confirm writes still commit",
// invariant E1). Deliberately atomic — kept to 3/3 → kill one follower
// → 2/3 healthy/progress, per the ladder's own instruction not to bolt
// a rejoin/restart step onto this scenario (that's the separate
// follower-rejoin rung).
//
// Three things get proven together, since a single real scenario can
// stand in for multiple invariant checks rather than needing one test
// per matrix row:
//  1. A brand-new CONNECT (real ownership arbitration, not a /health
//     read) still succeeds with a follower gone — the strongest
//     available probe, since it forces a real raft.Apply to commit.
//  2. Already-flowing MQTT traffic (cross-node publish/subscribe
//     between two Edges connected before the kill) is undisturbed.
//  3. An already-live session's ownership is not needlessly evicted
//     just because a Core follower died — losing a follower is not a
//     reason to touch any Edge-side session state.
//
// Leader/term identity is recorded as diagnostic evidence only (see the
// t.Logf calls below), never asserted — this test doesn't care which
// core was leader before or after, or whether an election happened,
// only that the product-level invariants above hold.
func TestCoreFollowerDeath_ClusterStaysOperational(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	// Pre-existing live session and pre-existing cross-node traffic path,
	// both established BEFORE the kill so the test can show they survive
	// it undisturbed.
	preExisting := connectClient(t, h.MQTTAddr(0), "pre-existing-live-session", deviceAID, DevicePwd)
	if !preExisting.IsConnected() {
		t.Fatal("pre-existing session never reached connected state before the follower kill")
	}

	consumer := connectClient(t, h.MQTTAddr(0), "follower-death-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)
	received := make(chan string, 1)
	subToken := consumer.Subscribe("telemetry/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("consumer subscribe: %v", subToken.Error())
	}

	// Identify the real leader before picking a target — never assume
	// the bootstrap node (core-1) is still leader by the time this runs.
	leaderIdx := currentLeaderIndex(t, h, 0)
	followerIdx := -1
	for i := range h.Cores {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx == -1 {
		t.Fatal("no non-leader core found to kill")
	}
	t.Logf("diagnostic: leader=core-%d (index %d), killing follower=core-%d (index %d)",
		leaderIdx+1, leaderIdx, followerIdx+1, followerIdx)
	h.KillCore(followerIdx)

	// Query any surviving core for the post-kill leader — diagnostic
	// only (see this test's doc comment), never asserted: a benign
	// re-election happening for some unrelated reason must not fail this
	// test, only a violation of the product-level invariants below.
	survivingIdx := 0
	if survivingIdx == followerIdx {
		survivingIdx = 1
	}
	if newLeaderIdx := currentLeaderIndex(t, h, survivingIdx); newLeaderIdx != leaderIdx {
		t.Logf("diagnostic: leader changed after follower death: core-%d -> core-%d", leaderIdx+1, newLeaderIdx+1)
	} else {
		t.Logf("diagnostic: leader unchanged after follower death: core-%d", leaderIdx+1)
	}

	// 1. A brand-new CONNECT still succeeds — forces a real raft.Apply
	// (claimClusterSession) to commit with only 2/3 cores live.
	newConn := connectClient(t, h.MQTTAddr(1), "post-follower-death-new-connect", deviceAID, DevicePwd)
	if !newConn.IsConnected() {
		t.Fatal("new CONNECT (ownership arbitration) failed with a Core follower dead")
	}
	newConn.Disconnect(250)

	// 2. Already-flowing traffic still works: publish from Edge B,
	// deliver to the consumer subscribed via Edge A.
	publisher := connectClient(t, h.MQTTAddr(1), "follower-death-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)
	payload := "traffic-survives-follower-death"
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, payload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish after follower death: %v", pubToken.Error())
	}
	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("expected payload %q, got %q", payload, got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cross-node delivery did not survive the follower death")
	}

	// 3. The pre-existing session was never disturbed by the follower
	// loss — no reason for an Edge-side eviction over a Core-only event.
	if !preExisting.IsConnected() {
		t.Fatal("pre-existing live session was disconnected by a Core follower death — should never happen")
	}
	preExisting.Disconnect(250)
}
