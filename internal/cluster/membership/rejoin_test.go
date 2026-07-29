package membership

import (
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// freePort returns an available loopback TCP port, released immediately —
// same convention as freeAddr (redis_failover_test.go), but memberlist's
// Config.BindPort wants a bare int, not a "host:port" string.
func freePort(t *testing.T) int {
	t.Helper()
	addr := freeAddr(t)
	_, portStr, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return port
}

func waitForMemberCount(t *testing.T, m *Membership, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(m.Members()) >= n {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("member count never reached %d, got %d", n, len(m.Members()))
}

// TestIsIsolated covers the predicate directly (no real gossip needed —
// members are injected straight into the map, same pattern
// newTestLeaderMembership uses), specifically the self-exclusion case a
// GRPCAddr-based comparison got wrong when GRPCAddr is left unset (as it
// is in this package's other Membership tests): comparing by NodeID
// instead avoids that.
func TestIsIsolated(t *testing.T) {
	tests := []struct {
		name    string
		self    NodeMeta
		members []NodeMeta
		want    bool
	}{
		{
			name:    "core node seeing only itself is isolated",
			self:    NodeMeta{NodeID: "core-1", Role: RoleCore},
			members: []NodeMeta{{NodeID: "core-1", Role: RoleCore}},
			want:    true,
		},
		{
			name: "core node seeing another core is not isolated",
			self: NodeMeta{NodeID: "core-1", Role: RoleCore},
			members: []NodeMeta{
				{NodeID: "core-1", Role: RoleCore},
				{NodeID: "core-2", Role: RoleCore},
			},
			want: false,
		},
		{
			name: "edge node seeing only sibling edges is isolated (the real bug: edges survive a total core outage)",
			self: NodeMeta{NodeID: "edge-2", Role: RoleEdge},
			members: []NodeMeta{
				{NodeID: "edge-2", Role: RoleEdge},
				{NodeID: "edge-1", Role: RoleEdge},
				{NodeID: "edge-3", Role: RoleEdge},
			},
			want: true,
		},
		{
			name: "edge node seeing a core is not isolated",
			self: NodeMeta{NodeID: "edge-2", Role: RoleEdge},
			members: []NodeMeta{
				{NodeID: "edge-2", Role: RoleEdge},
				{NodeID: "core-1", Role: RoleCore},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Membership{self: tt.self, members: make(map[string]NodeMeta)}
			for _, meta := range tt.members {
				m.members[meta.NodeID] = meta
			}
			require.Equal(t, tt.want, m.isIsolated())
		})
	}
}

// TestRejoinIfIsolated_RepopulatesAfterRealDeathAndRestart reproduces the
// bug found via test/e2e/olric-quorum-loss.sh: killing and restarting
// every peer a node knows about left that node permanently isolated,
// because memberlist.Join was only ever called once, at New(). node-a is
// genuinely shut down (not just cleared from node-b's own cache — an
// earlier version of this test tried that and it was a no-op, because
// memberlist's OWN internal alive-node state never changed, so re-Join
// found nothing new to tell node-b about) so node-b's real SWIM failure
// detector fires a real NotifyLeave, then node-a comes back on the same
// address (simulating a container restart with a stable node identity)
// and rejoinIfIsolated must bring node-b back to seeing it.
func TestRejoinIfIsolated_RepopulatesAfterRealDeathAndRestart(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	portA := freePort(t)
	addrA := "127.0.0.1:" + strconv.Itoa(portA)

	mA, err := New(Config{
		NodeID:   "node-a",
		Role:     RoleCore,
		BindAddr: "127.0.0.1",
		BindPort: portA,
	}, log)
	require.NoError(t, err)

	portB := freePort(t)
	mB, err := New(Config{
		NodeID:   "node-b",
		Role:     RoleCore,
		BindAddr: "127.0.0.1",
		BindPort: portB,
		Peers:    []string{addrA},
	}, log)
	require.NoError(t, err)
	defer mB.Leave(time.Second)

	waitForMemberCount(t, mB, 2, 5*time.Second)

	// Genuinely kill node-a (Shutdown, not the graceful Leave — closer to
	// the e2e script's docker kill) and wait for node-b's real SWIM
	// failure detector to actually mark it gone.
	require.NoError(t, mA.ml.Shutdown())
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(mB.Members()) > 1 {
		time.Sleep(50 * time.Millisecond)
	}
	require.Len(t, mB.Members(), 1, "test setup: node-b should have detected node-a's real death")

	// node-a "restarts" — a fresh memberlist instance reusing the same
	// bind address, same as a container coming back with the same
	// identity/address after a kill.
	mA2, err := New(Config{
		NodeID:   "node-a",
		Role:     RoleCore,
		BindAddr: "127.0.0.1",
		BindPort: portA,
	}, log)
	require.NoError(t, err)
	defer mA2.Leave(time.Second)

	mB.rejoinIfIsolated()

	waitForMemberCount(t, mB, 2, 5*time.Second)
}

// TestRejoinIfIsolated_NoopWhenNotIsolated confirms the guard actually
// guards — rejoinIfIsolated must not attempt a Join (and must not disturb
// existing membership) when the node already sees at least one peer.
func TestRejoinIfIsolated_NoopWhenNotIsolated(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	portA := freePort(t)
	mA, err := New(Config{
		NodeID:   "node-a2",
		Role:     RoleCore,
		BindAddr: "127.0.0.1",
		BindPort: portA,
	}, log)
	require.NoError(t, err)
	defer mA.Leave(time.Second)

	portB := freePort(t)
	mB, err := New(Config{
		NodeID:   "node-b2",
		Role:     RoleCore,
		BindAddr: "127.0.0.1",
		BindPort: portB,
		Peers:    []string{"127.0.0.1:" + strconv.Itoa(portA)},
	}, log)
	require.NoError(t, err)
	defer mB.Leave(time.Second)

	waitForMemberCount(t, mB, 2, 5*time.Second)

	// Should be a pure no-op: already sees node-a, so it must return
	// before ever calling ml.Join.
	mB.rejoinIfIsolated()
	require.Len(t, mB.Members(), 2)
}

