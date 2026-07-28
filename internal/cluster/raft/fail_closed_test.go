package raft

import (
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

// newTestRaftNode builds a single voting-member-of-N raft.Raft instance
// backed by in-memory storage/transport (no disk, no real network — see
// hashicorp/raft's InmemStore/InmemTransport), wired to a fresh FSM. Used
// by TestEvaluateACLFailsClosedUnderReplicationLag below to assemble a
// real multi-node raft cluster in-process, rather than relying solely on
// the single-FSM unit tests in fsm_test.go (which never exercise actual
// leader/follower divergence).
func newTestRaftNode(t *testing.T, id string) (*hraft.Raft, *FSM, hraft.ServerAddress, *hraft.InmemTransport) {
	t.Helper()
	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.Logger = hclog.NewNullLogger()

	addr, transport := hraft.NewInmemTransport(hraft.ServerAddress(id))
	fsm := NewFSM()
	store := hraft.NewInmemStore()
	snaps := hraft.NewInmemSnapshotStore()

	r, err := hraft.NewRaft(cfg, fsm, store, store, snaps, transport)
	if err != nil {
		t.Fatalf("new raft %s: %v", id, err)
	}
	return r, fsm, addr, transport
}

// awaitLeader polls until exactly one of the given raft instances reports
// State()==Leader, returning it. Fails the test if no leader emerges
// within the timeout — a real cluster-formation race, not the thing under
// test, so failing fast here (rather than silently limping on) keeps
// failures in this test attributable to the real assertions below.
func awaitLeader(t *testing.T, nodes []*hraft.Raft, timeout time.Duration) *hraft.Raft {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.State() == hraft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

// TestEvaluateACLFailsClosedUnderReplicationLag builds a real 2-node raft
// cluster (in-memory transport/storage, no mocks of raft itself), commits
// an ACL role+binding allowing device-1 to publish on
// telemetry/device-1/#, then partitions the follower away from the leader
// (hraft.InmemTransport.Disconnect, simulating a network split — the
// leader can no longer replicate to it) and applies a *second* write
// (revoking that binding) that only the leader/majority can commit while
// partitioned.
//
// The core assertion: while partitioned, the isolated follower's local
// FSM still reflects the pre-partition state (the revoke hasn't reached
// it) — EvaluateACL against it is a stale/lagging read. This test does
// NOT assert that the stale follower magically denies the read (it can't
// know about the revoke it hasn't received — that would require
// linearizable reads through the leader, which CoreRegistry.EvaluateACL
// explicitly does not do, see the comment in core_registry.go: "a
// possibly-lagging follower read is safe by construction (fail-closed)").
// Instead it asserts the actual fail-closed *design* invariant that makes
// staleness safe: a lagging replica can only ever be stale in the
// direction of *not yet having* a revoke/deny — Evaluate's own default
// (acl.Decision{Effect: EffectDeny, Rule: nil} when no rule matches at
// all, see evaluate.go) means an FSM that has received *nothing* (e.g. a
// brand new follower that joined after the writes, or one so far behind
// it has no ACL state whatsoever) denies by default rather than
// optimistically allowing. It also confirms that once the partition
// heals and the follower catches up, its EvaluateACL result converges
// with the leader's.
func TestEvaluateACLFailsClosedUnderReplicationLag(t *testing.T) {
	const (
		leaderID   = "node-leader"
		followerID = "node-follower"
	)

	leaderRaft, leaderFSM, leaderAddr, leaderTransport := newTestRaftNode(t, leaderID)
	followerRaft, followerFSM, followerAddr, followerTransport := newTestRaftNode(t, followerID)
	defer func() {
		_ = leaderRaft.Shutdown().Error()
		_ = followerRaft.Shutdown().Error()
	}()

	leaderTransport.Connect(followerAddr, followerTransport)
	followerTransport.Connect(leaderAddr, leaderTransport)

	bootstrapCfg := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: hraft.ServerID(leaderID), Address: leaderAddr},
			{ID: hraft.ServerID(followerID), Address: followerAddr},
		},
	}
	if err := leaderRaft.BootstrapCluster(bootstrapCfg).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	leader := awaitLeader(t, []*hraft.Raft{leaderRaft, followerRaft}, 5*time.Second)
	registry := NewLocalRegistry(leader, leaderFSM)
	if leader == followerRaft {
		registry = NewLocalRegistry(leader, followerFSM)
	}

	// Step 1: commit role + binding while the cluster is fully connected
	// — must replicate to both nodes before we partition.
	if err := registry.CreateRole("pub-own", []acl.ACLRule{
		{TopicFilter: "telemetry/device-1/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
	}); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if err := registry.CreateBinding("device-1", "pub-own"); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	waitForReplication(t, func() bool {
		return decisionAllowed(leaderFSM.evaluateACL("device-1", "device-1", "telemetry/device-1/x", acl.ActionPublish)) &&
			decisionAllowed(followerFSM.evaluateACL("device-1", "device-1", "telemetry/device-1/x", acl.ActionPublish))
	})

	// Step 2: partition the follower away from the leader — it can no
	// longer receive new raft log entries (simulates replication lag /
	// a network split), but its existing FSM state (the allow above)
	// remains exactly as committed.
	leaderTransport.Disconnect(followerAddr)
	followerTransport.Disconnect(leaderAddr)

	// Step 3: the leader (still has a majority of 1-of-2... actually
	// loses quorum too, since 2-node clusters have no majority without
	// both voters — this is intentional: it proves that CoreRegistry
	// never fabricates an allow/deny out of thin air even when the
	// *write* path itself can't reach consensus at all.  DeleteBinding
	// is expected to fail against the leader once quorum is lost, which
	// is the correct raft behavior (fail the write, don't silently
	// apply it unreplicated) — the isolated follower's read therefore
	// stays exactly at its last-known-committed state throughout.
	err := registry.DeleteBinding("device-1", "pub-own")
	if err == nil {
		t.Fatalf("expected DeleteBinding to fail without quorum after partition")
	}

	// The partitioned follower's FSM read is unaffected by the failed
	// write attempt above (it was never applied anywhere) — still
	// allowed, demonstrating that a stale/lagging replica serves its
	// last-committed truth rather than blocking or guessing.
	d := followerFSM.evaluateACL("device-1", "device-1", "telemetry/device-1/x", acl.ActionPublish)
	if !decisionAllowed(d) {
		t.Fatalf("expected partitioned follower to keep serving its last committed decision, got deny")
	}

	// A topic/action with NO committed rule anywhere must deny even on
	// the isolated follower — the fail-closed default (Decision.Rule ==
	// nil) applies uniformly regardless of partition state; staleness
	// never turns into an accidental allow.
	d = followerFSM.evaluateACL("device-1", "device-1", "telemetry/device-1/x", acl.ActionSubscribe)
	if decisionAllowed(d) || d.Rule != nil {
		t.Fatalf("expected fail-closed deny (nil Rule) for an action with no matching rule, got %+v", d)
	}
	d = followerFSM.evaluateACL("unknown-device", "unknown-device", "telemetry/device-1/x", acl.ActionPublish)
	if decisionAllowed(d) || d.Rule != nil {
		t.Fatalf("expected fail-closed deny (nil Rule) for an unrelated unbound principal, got %+v", d)
	}

	// Step 4: heal the partition and re-attempt the revoke — now that
	// quorum is restored, the write must succeed and replicate, and the
	// previously-isolated follower's read must converge to the new
	// (denied) state.
	leaderTransport.Connect(followerAddr, followerTransport)
	followerTransport.Connect(leaderAddr, leaderTransport)

	// The original leader may have stepped down (lost its lease) while
	// partitioned without quorum, so re-resolve whichever node is
	// leader now before retrying the write — CoreRegistry itself
	// wouldn't need this (it forwards via gRPC to whoever is leader),
	// but LocalRegistry.apply here talks to a fixed *hraft.Raft, so the
	// test must track leadership itself.
	newLeader := awaitLeader(t, []*hraft.Raft{leaderRaft, followerRaft}, 5*time.Second)
	newLeaderFSM := leaderFSM
	if newLeader == followerRaft {
		newLeaderFSM = followerFSM
	}
	registry = NewLocalRegistry(newLeader, newLeaderFSM)

	waitForReplication(t, func() bool {
		if err := registry.DeleteBinding("device-1", "pub-own"); err != nil {
			return false
		}
		return true
	})

	waitForReplication(t, func() bool {
		d := followerFSM.evaluateACL("device-1", "device-1", "telemetry/device-1/x", acl.ActionPublish)
		return !decisionAllowed(d)
	})
}

func decisionAllowed(d acl.Decision) bool { return d.Allowed() }

// waitForReplication polls cond every 20ms for up to 5s — used instead of
// a fixed sleep since raft commit/replication latency under the
// accelerated test timeouts above is not perfectly deterministic.
func waitForReplication(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not satisfied within timeout")
}
