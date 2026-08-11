//go:build cluster

package cluster

import (
	"net/http"
	"testing"
	"time"
)

// TestCoreFollowerRejoin_ReconvergesAfterRestart is Phase 3 rung 3's
// follower-rejoin scenario (docs/testing/CLUSTER_CORRECTNESS_MATRIX.md
// §6's Core table row: "Join/rejoin: kill all 3 Cores, restart, confirm
// gossip rejoinIfIsolated + reconcileVoters reconverge" — narrowed here
// to one follower, matching the ladder's own "kept atomic" instruction:
// this is a separate scenario from follower death, not a continuation
// bolted onto it.
//
// Deliberately uses StopCore/StartCore (container survives, real
// BoltDB raft state on disk intact), not KillCore — a rejoin test that
// silently destroyed and recreated the container would only prove a
// fresh node can join, not that a real restart resumes from existing
// state. internal/cluster/membership/rejoin_test.go's
// TestRejoinIfIsolated_RepopulatesAfterRealDeathAndRestart already
// proves the gossip logic in-process; this proves the full
// separate-container restart path atop it.
func TestCoreFollowerRejoin_ReconvergesAfterRestart(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	leaderIdx := currentLeaderIndex(t, h, 0)
	followerIdx := -1
	for i := range h.Cores {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	if followerIdx == -1 {
		t.Fatal("no non-leader core found to stop")
	}
	// With 3 cores and leaderIdx/followerIdx distinct, exactly one index
	// remains — the third, never-stopped node to compare the restarted
	// follower's view against.
	survivorIdx := -1
	for i := range h.Cores {
		if i != leaderIdx && i != followerIdx {
			survivorIdx = i
			break
		}
	}
	if survivorIdx == -1 {
		t.Fatal("no third core found — expected exactly 3 cores with distinct leader/follower indices")
	}

	t.Logf("diagnostic: leader=core-%d, stopping follower=core-%d", leaderIdx+1, followerIdx+1)
	h.StopCore(followerIdx)

	// 2/3 still healthy — reuses the same real-arbitration probe
	// core_follower_death_test.go uses, not a /health read.
	midConn := connectClient(t, h.MQTTAddr(0), "mid-outage-connect", deviceAID, DevicePwd)
	if !midConn.IsConnected() {
		t.Fatal("ownership arbitration failed with the follower stopped, before restart was even attempted")
	}
	midConn.Disconnect(250)

	h.StartCore(followerIdx)

	// Wait for the restarted process to actually come back up (its own
	// management API answering at all) — a real observable condition,
	// not an arbitrary sleep.
	waitForManagementAPI(t, h, followerIdx, 60*time.Second)

	// Wait for real raft reconvergence: the restarted core's own
	// LeaderID() read agreeing with a node that was never stopped. This
	// is the actual "resumed from existing state, not a fresh empty
	// node" proof — a node that had to re-bootstrap from scratch as a
	// brand-new single-node cluster would never converge on the
	// original leader.
	waitForLeaderAgreement(t, h, followerIdx, survivorIdx, 60*time.Second)

	// The restarted core must still be a raft voter — rejoin, not
	// silent demotion to a non-voting observer.
	views, err := getNodeViews(survivorIdx, h)
	if err != nil {
		t.Fatalf("query cluster nodes via core index %d: %v", survivorIdx, err)
	}
	found := false
	for _, v := range views {
		idx, err := coreIndexFromNodeID(v.NodeID)
		if err != nil || idx != followerIdx {
			continue
		}
		found = true
		if !v.RaftVoter {
			t.Fatalf("core-%d is no longer a raft voter after rejoin", followerIdx+1)
		}
	}
	if !found {
		t.Fatalf("core-%d not found in core-%d's node view after rejoin", followerIdx+1, survivorIdx+1)
	}

	// Full 3/3 functional check: a new CONNECT's arbitration still
	// succeeds post-rejoin (same probe as mid-outage, now with all 3
	// cores back).
	postConn := connectClient(t, h.MQTTAddr(1), "post-rejoin-connect", deviceAID, DevicePwd)
	if !postConn.IsConnected() {
		t.Fatal("ownership arbitration failed after full rejoin")
	}
	postConn.Disconnect(250)
}

func waitForManagementAPI(t *testing.T, h *Harness, coreIdx int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	addr := h.ManagementAddr(coreIdx)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/api/cluster/nodes")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("core-%d's management API never came back up within %s", coreIdx+1, timeout)
}

// waitForLeaderAgreement polls until restartedIdx and survivorIdx's own
// management APIs report the same leader — a rejoining node legitimately
// answers with no leader known yet for a while, which must be a retry
// condition here, not treated as a failure on the first attempt.
func waitForLeaderAgreement(t *testing.T, h *Harness, restartedIdx, survivorIdx int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		restarted, restartedOK := tryLeaderIndex(h, restartedIdx)
		survivor, survivorOK := tryLeaderIndex(h, survivorIdx)
		// Both sides must report an actual known leader (not just "no
		// leader known yet" on both, which would otherwise wrongly look
		// like agreement) and agree on which one.
		if restartedOK && survivorOK && restarted == survivor {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("core-%d never agreed with core-%d on the current leader within %s",
		restartedIdx+1, survivorIdx+1, timeout)
}

// tryLeaderIndex is currentLeaderIndex's poll-friendly sibling: a
// rejoining node legitimately has no leader known yet for a while
// (unreachable, or caught up but not yet observed a heartbeat), which
// must be a retry condition here, never a fatal error.
func tryLeaderIndex(h *Harness, queryFrom int) (int, bool) {
	views, err := getNodeViews(queryFrom, h)
	if err != nil {
		return -1, false
	}
	for _, v := range views {
		if v.IsLeader {
			idx, err := coreIndexFromNodeID(v.NodeID)
			if err != nil {
				return -1, false
			}
			return idx, true
		}
	}
	return -1, false
}
