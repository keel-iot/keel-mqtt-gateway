//go:build cluster

package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestCoreLeaderDeath_NewLeaderElectedAndProgressRestored is Phase 3
// rung 3's leader-loss scenario (docs/testing/CLUSTER_CORRECTNESS_MATRIX.md
// §6 Core table, "Kill the leader, confirm a new leader is elected and
// writes resume"). Kept atomic like the follower scenarios: no restart
// of the dead leader here — that's the separate leader-rejoin test.
//
// Unlike follower death, a new election is exactly what's under test
// here (not incidental), so — per the ladder's own instruction — this
// records election timing/old-vs-new leader/term as diagnostic evidence
// but never asserts a specific duration or that a particular core wins.
func TestCoreLeaderDeath_NewLeaderElectedAndProgressRestored(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	leaderIdx := currentLeaderIndex(t, h, 0)
	var survivors []int
	for i := range h.Cores {
		if i != leaderIdx {
			survivors = append(survivors, i)
		}
	}
	if len(survivors) != 2 {
		t.Fatalf("expected 2 surviving cores, got %d", len(survivors))
	}

	// Pre-existing live session — must not be evicted or reassigned just
	// because the Core leader died.
	const preExistingID = "leader-death-pre-existing-session"
	preExisting := connectClient(t, h.MQTTAddr(0), preExistingID, deviceAID, DevicePwd)
	if !preExisting.IsConnected() {
		t.Fatal("pre-existing session never reached connected state before the leader kill")
	}

	// Pre-existing cross-node traffic path (subscribe on Edge A, publish
	// from Edge B) — must keep working across the election.
	consumer := connectClient(t, h.MQTTAddr(0), "leader-death-consumer", consumerUsername, consumerPassword)
	defer consumer.Disconnect(250)

	var mu sync.Mutex
	received := map[string]time.Time{}
	heartbeatRecv := make(chan string, 4096)
	subToken := consumer.Subscribe("telemetry/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		payload := string(msg.Payload())
		mu.Lock()
		received[payload] = time.Now()
		mu.Unlock()
		heartbeatRecv <- payload
	})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("consumer subscribe: %v", subToken.Error())
	}

	publisher := connectClient(t, h.MQTTAddr(1), "leader-death-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)

	// Background heartbeat: ordinary PUBLISH from an already-connected,
	// already-owned session needs no new Core arbitration at all — only
	// CONNECT/session-claim does (see claimClusterSession) — so this is
	// exactly the traffic Keel's architecture claims should survive a
	// leaderless window. Started before the kill, stopped after the
	// post-election checks below; every send/receive timestamp is
	// recorded so the leaderless-window overlap (if any) can be reported
	// as evidence without the test's pass/fail depending on catching it.
	sendTimes := map[string]time.Time{}
	var sendMu sync.Mutex
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		seq := 0
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				payload := fmt.Sprintf("heartbeat-%d", seq)
				seq++
				sendMu.Lock()
				sendTimes[payload] = time.Now()
				sendMu.Unlock()
				publisher.Publish(publishTopicFor(deviceAID), 1, false, payload)
			}
		}
	}()

	killTime := time.Now()
	h.KillCore(leaderIdx)
	t.Logf("diagnostic: old leader=core-%d, killed at %s", leaderIdx+1, killTime.Format(time.RFC3339Nano))

	newLeaderIdx, electionDone := waitForNewLeader(h, leaderIdx, survivors, 60*time.Second)
	if newLeaderIdx == -1 {
		close(stopHeartbeat)
		<-heartbeatDone
		t.Fatalf("no new leader elected among survivors within timeout")
	}
	t.Logf("diagnostic: new leader=core-%d, observed at %s (election window %s)",
		newLeaderIdx+1, electionDone.Format(time.RFC3339Nano), electionDone.Sub(killTime))

	// Let the heartbeat run a little past the observed election before
	// stopping, so in-flight sends around that boundary have a chance to
	// be delivered and counted below.
	time.Sleep(1 * time.Second)
	close(stopHeartbeat)
	<-heartbeatDone

	// Evidence only, never asserted: did any heartbeat get delivered
	// while raft had no leader at all? A real observation here is much
	// stronger evidence for "data plane survives a leaderless CP window"
	// than a passing test alone.
	mu.Lock()
	sendMu.Lock()
	deliveredDuringWindow := 0
	for payload, sentAt := range sendTimes {
		if _, ok := received[payload]; ok && sentAt.After(killTime) && sentAt.Before(electionDone) {
			deliveredDuringWindow++
		}
	}
	totalSent, totalReceived := len(sendTimes), len(received)
	sendMu.Unlock()
	mu.Unlock()
	t.Logf("diagnostic: heartbeat traffic — sent=%d received=%d delivered-during-leaderless-window=%d",
		totalSent, totalReceived, deliveredDuringWindow)

	// 1. New leader elected among the surviving cores (never the dead one).
	if newLeaderIdx == leaderIdx {
		t.Fatal("reported new leader is the same core that was killed")
	}

	// 2. Quorum (2/3) still allows new decisions: a brand-new CONNECT's
	// ownership arbitration commits.
	newConn := connectClient(t, h.MQTTAddr(1), "post-leader-death-new-connect", deviceAID, DevicePwd)
	if !newConn.IsConnected() {
		t.Fatal("new CONNECT (ownership arbitration) failed after leader death")
	}
	newConn.Disconnect(250)

	// 3. Pre-existing live session undisturbed.
	if !preExisting.IsConnected() {
		t.Fatal("pre-existing live session was disconnected by the leader death")
	}

	// 4. Cross-node publish still works post-election — a fresh,
	// unambiguous check distinct from the heartbeat noise above.
	finalPayload := "post-election-cross-node-check"
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, finalPayload)
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish after leader death: %v", pubToken.Error())
	}
	deadline := time.After(15 * time.Second)
	found := false
	for !found {
		select {
		case got := <-heartbeatRecv:
			if got == finalPayload {
				found = true
			}
		case <-deadline:
			t.Fatal("cross-node delivery did not survive the leader death")
		}
	}

	// 5/6. No double owner, no lost committed decision: both survivors'
	// own raft FSM copies (SessionsSnapshot — genuinely per-node
	// replicated state, unlike the Olric-backed offline-ownership case
	// investigated earlier) agree on who owns the pre-existing session,
	// and it's still there at all (not silently dropped by the
	// leadership change).
	owner0 := sessionOwnerFrom(t, h, survivors[0], preExistingID)
	owner1 := sessionOwnerFrom(t, h, survivors[1], preExistingID)
	if owner0 == "" || owner1 == "" {
		t.Fatalf("pre-existing session's committed ownership was lost after leader death (core-%d saw %q, core-%d saw %q)",
			survivors[0]+1, owner0, survivors[1]+1, owner1)
	}
	if owner0 != owner1 {
		t.Fatalf("survivors disagree on pre-existing session's owner: core-%d says %q, core-%d says %q",
			survivors[0]+1, owner0, survivors[1]+1, owner1)
	}

	preExisting.Disconnect(250)
}

// waitForNewLeader polls survivors' management APIs until one of them
// reports a leader that is not oldLeaderIdx, returning that leader's
// index and the moment it was first observed. Returns (-1, zero) on
// timeout.
func waitForNewLeader(h *Harness, oldLeaderIdx int, survivors []int, timeout time.Duration) (int, time.Time) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, idx := range survivors {
			if leaderIdx, ok := tryLeaderIndex(h, idx); ok && leaderIdx != oldLeaderIdx {
				return leaderIdx, time.Now()
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return -1, time.Time{}
}

// sessionOwnerFrom queries core index queryFrom's own management API
// (GET /api/cluster/sessions, backed by its own local raft FSM copy)
// for clientID's current owning node.
func sessionOwnerFrom(t *testing.T, h *Harness, queryFrom int, clientID string) string {
	t.Helper()
	resp, err := http.Get("http://" + h.ManagementAddr(queryFrom) + "/api/cluster/sessions")
	if err != nil {
		t.Fatalf("query sessions via core index %d: %v", queryFrom, err)
	}
	defer resp.Body.Close()
	var sessions map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&sessions); err != nil {
		t.Fatalf("decode /api/cluster/sessions response: %v", err)
	}
	return sessions[clientID]
}
