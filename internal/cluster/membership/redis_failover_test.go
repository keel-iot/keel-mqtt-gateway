package membership

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
)

// fakeRedisAdmin records replicaOf/replicaOfNoOne calls instead of talking
// to a real Redis — same pattern as internal/broker/hooks_test.go's
// fakeRegistry/fakeForwarder.
type fakeRedisAdmin struct {
	mu             sync.Mutex
	replicaOfCalls []replicaOfCall
	promoteCalls   []string // addrs promoted via replicaOfNoOne
	failAddr       string   // if set, any call targeting this addr returns an error
}

type replicaOfCall struct {
	addr, primaryHost, primaryPort string
}

func (f *fakeRedisAdmin) replicaOf(_ context.Context, addr, primaryHost, primaryPort string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if addr == f.failAddr {
		return errFakeAdmin
	}
	f.replicaOfCalls = append(f.replicaOfCalls, replicaOfCall{addr, primaryHost, primaryPort})
	return nil
}

func (f *fakeRedisAdmin) replicaOfNoOne(_ context.Context, addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if addr == f.failAddr {
		return errFakeAdmin
	}
	f.promoteCalls = append(f.promoteCalls, addr)
	return nil
}

func (f *fakeRedisAdmin) snapshot() (replicaOf []replicaOfCall, promote []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	replicaOf = make([]replicaOfCall, len(f.replicaOfCalls))
	copy(replicaOf, f.replicaOfCalls)
	promote = make([]string, len(f.promoteCalls))
	copy(promote, f.promoteCalls)
	return
}

type fakeAdminErr struct{ s string }

func (e fakeAdminErr) Error() string { return e.s }

var errFakeAdmin = fakeAdminErr{"fake redis admin: simulated failure"}

// freeAddr returns an available loopback TCP address, released immediately
// — same helper pattern as internal/cluster/raft/backup_restore_test.go's
// freePort, needed here for a real raft TCP transport.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// newTestLeaderMembership builds a *Membership backed by a real,
// single-voter raft node (bootstrapped and awaited into leadership) —
// real enough to exercise raft.Registry.SetRedisPrimary/
// CurrentRedisPrimary genuinely, without a multi-process cluster.
// voterCount overrides coreVoterCount() directly (see that method's doc
// for why: actually AddVoter-ing unreachable placeholder addresses causes
// the sole reachable node to lose leadership once it can't contact a
// quorum of the inflated voter set — a real raft liveness property, not
// something to work around with exotic test topology). members is
// installed directly into the members map (bypassing actual memberlist
// gossip, which these tests don't need — they exercise the
// reconcile/bootstrap/failover logic given a known membership snapshot,
// not gossip convergence itself).
func newTestLeaderMembership(t *testing.T, selfNodeID string, members []NodeMeta, admin redisAdminClient, voterCount int) *Membership {
	t.Helper()

	node, err := keelraft.NewNode(keelraft.NodeConfig{
		NodeID:       selfNodeID,
		RaftBindAddr: freeAddr(t),
		DataDir:      t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown() })

	didBootstrap, err := node.Bootstrap()
	require.NoError(t, err)
	require.True(t, didBootstrap)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, node.IsLeader(), "raft node never became leader")

	m := &Membership{
		testVoterCount:            func() (int, error) { return voterCount, nil },
		raft:                      node,
		self:                      NodeMeta{NodeID: selfNodeID, Role: RoleCore},
		log:                       slog.Default(),
		members:                   make(map[string]NodeMeta),
		redisPrimaryDeadThreshold: 200 * time.Millisecond, // short, deliberate — these tests want the threshold to matter within test timeouts
		redisAdmin:                admin,
		redisMissingSince:         make(map[string]time.Time),
	}
	for _, meta := range members {
		m.members[meta.NodeID] = meta
	}
	return m
}

func TestReconcileRedisPrimary_BootstrapsWhenNoneDesignated(t *testing.T) {
	admin := &fakeRedisAdmin{}
	members := []NodeMeta{
		{NodeID: "core-1", Role: RoleCore, RedisAddr: "127.0.0.1:7001"},
		{NodeID: "core-2", Role: RoleCore, RedisAddr: "127.0.0.1:7002"},
	}
	m := newTestLeaderMembership(t, "core-1", members, admin, 2)

	m.reconcileRedisPrimary()

	primary, ok := m.raft.Registry.CurrentRedisPrimary()
	require.True(t, ok)
	require.Equal(t, "core-1", primary, "bootstrap should prefer self as the initial primary")

	replicaOfCalls, _ := admin.snapshot()
	require.Len(t, replicaOfCalls, 1)
	require.Equal(t, "127.0.0.1:7002", replicaOfCalls[0].addr)
	require.Equal(t, "127.0.0.1", replicaOfCalls[0].primaryHost)
	require.Equal(t, "7001", replicaOfCalls[0].primaryPort)
}

func TestReconcileRedisPrimary_SingleCoreGuard(t *testing.T) {
	admin := &fakeRedisAdmin{}
	members := []NodeMeta{
		{NodeID: "core-1", Role: RoleCore, RedisAddr: "127.0.0.1:7001"},
	}
	m := newTestLeaderMembership(t, "core-1", members, admin, 1)

	m.reconcileRedisPrimary()

	primary, ok := m.raft.Registry.CurrentRedisPrimary()
	require.True(t, ok)
	require.Equal(t, "core-1", primary)

	replicaOfCalls, promoteCalls := admin.snapshot()
	require.Empty(t, replicaOfCalls, "single core has no replica to configure")
	// bootstrapRedisPrimary's single-core branch does call replicaOfNoOne
	// once, defensively — see that method's doc.
	require.Equal(t, []string{"127.0.0.1:7001"}, promoteCalls)
}

func TestReconcileRedisPrimary_HealthyPrimaryReconfiguresReplicas(t *testing.T) {
	admin := &fakeRedisAdmin{}
	members := []NodeMeta{
		{NodeID: "core-1", Role: RoleCore, RedisAddr: "127.0.0.1:7001"},
		{NodeID: "core-2", Role: RoleCore, RedisAddr: "127.0.0.1:7002"},
		{NodeID: "core-3", Role: RoleCore, RedisAddr: "127.0.0.1:7003"},
	}
	m := newTestLeaderMembership(t, "core-1", members, admin, 3)

	err := m.raft.Registry.SetRedisPrimary("core-1")
	require.NoError(t, err)

	m.reconcileRedisPrimary()

	primary, ok := m.raft.Registry.CurrentRedisPrimary()
	require.True(t, ok)
	require.Equal(t, "core-1", primary, "a healthy, already-designated primary must not be replaced")

	replicaOfCalls, _ := admin.snapshot()
	require.Len(t, replicaOfCalls, 2, "both non-primary cores should be (re)configured as replicas")
}

func TestReconcileRedisPrimary_FailoverOnPrimaryMissingBeyondThreshold(t *testing.T) {
	admin := &fakeRedisAdmin{}
	members := []NodeMeta{
		{NodeID: "core-1", Role: RoleCore, RedisAddr: "127.0.0.1:7001"},
		{NodeID: "core-2", Role: RoleCore, RedisAddr: "127.0.0.1:7002"},
	}
	// core-1 is designated but NOT included in the members snapshot below
	// — simulating it having vanished from gossip; core-2 is the sole
	// survivor to promote.
	m := newTestLeaderMembership(t, "core-2", []NodeMeta{members[1]}, admin, 2)

	err := m.raft.Registry.SetRedisPrimary("core-1")
	require.NoError(t, err)

	// First tick: core-1 is absent from the members snapshot (never
	// registered at all, not even briefly) — this is the very case the
	// "missing since" tracking exists for: the clock must start HERE, on
	// this first observed-absent tick, not depend on ever having seen
	// core-1 present in this process's lifetime. Must not fail over yet.
	m.reconcileRedisPrimary()
	primary, _ := m.raft.Registry.CurrentRedisPrimary()
	require.Equal(t, "core-1", primary, "must not fail over on the very first tick a primary is observed missing")

	time.Sleep(m.redisPrimaryDeadThreshold + 50*time.Millisecond)
	m.reconcileRedisPrimary()

	primary, ok := m.raft.Registry.CurrentRedisPrimary()
	require.True(t, ok)
	require.Equal(t, "core-2", primary, "surviving replica should be promoted after the primary is missing beyond threshold")

	_, promoteCalls := admin.snapshot()
	require.Contains(t, promoteCalls, "127.0.0.1:7002")
}

func TestReconcileRedisPrimary_FailoverSkipsWhenPromoteFails(t *testing.T) {
	admin := &fakeRedisAdmin{failAddr: "127.0.0.1:7002"}
	members := []NodeMeta{
		{NodeID: "core-2", Role: RoleCore, RedisAddr: "127.0.0.1:7002"},
	}
	m := newTestLeaderMembership(t, "core-2", members, admin, 2)

	err := m.raft.Registry.SetRedisPrimary("core-1")
	require.NoError(t, err)

	m.reconcileRedisPrimary() // first tick: records core-2 as seen, core-1 as never-seen (not in members)
	time.Sleep(m.redisPrimaryDeadThreshold + 50*time.Millisecond)
	m.reconcileRedisPrimary() // second tick: attempts failover, promotion fails

	primary, ok := m.raft.Registry.CurrentRedisPrimary()
	require.True(t, ok)
	require.Equal(t, "core-1", primary, "raft designation must not advance when the promotion RPC itself failed")
}
