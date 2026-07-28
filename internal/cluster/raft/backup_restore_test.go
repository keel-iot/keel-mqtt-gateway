package raft

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// freePort finds an available loopback TCP port for a real (non-inmem)
// raft transport, since Snapshot/RecoverCluster exercise the same
// TCPTransport + FileSnapshotStore + BoltStore stack cmd/server's `backup
// raft`/`restore raft` CLI commands drive in production — the in-memory
// transport/store helpers in fail_closed_test.go don't exercise the actual
// disk layout stageSnapshot depends on.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// awaitNodeLeader polls until n reports leadership, or fails the test.
func awaitNodeLeader(t *testing.T, n *Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s never became leader within %s", n.cfg.NodeID, timeout)
}

// TestBackupAndRestoreRoundTrip exercises the real on-disk path behind
// `keel-gateway backup raft` and `keel-gateway restore raft`: bootstrap a
// single-voter node, commit a session-ownership write, force a snapshot
// (Node.Snapshot — what `backup raft` copies out), then simulate total data
// loss (fresh DataDir) and recover into a brand-new node via RecoverCluster
// using only the copied-out snapshot directory — confirming the pre-loss
// state (the session) survives and the recovered node can serve traffic
// (elects itself leader) afterward.
func TestBackupAndRestoreRoundTrip(t *testing.T) {
	origDir := t.TempDir()
	backupDir := t.TempDir()
	recoveredDir := t.TempDir()

	nodeID := "recover-test-node"
	bindAddr := freePort(t)

	orig, err := NewNode(NodeConfig{NodeID: nodeID, RaftBindAddr: bindAddr, DataDir: origDir})
	require.NoError(t, err)

	didBootstrap, err := orig.Bootstrap()
	require.NoError(t, err)
	require.True(t, didBootstrap)
	awaitNodeLeader(t, orig, 5*time.Second)

	_, err = orig.Registry.ClaimSession("device-under-backup", nodeID)
	require.NoError(t, err)

	id, dir, err := orig.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.DirExists(t, dir)

	// "backup raft --output backupDir": copy the snapshot dir out — same
	// operation the CLI command performs against a running node's local
	// filesystem.
	copyDir(t, dir, backupDir)

	require.NoError(t, orig.Shutdown())

	// Simulate total loss: recoveredDir is a fresh, empty DataDir — nothing
	// carried over except the externally-held backup copy.
	err = RecoverCluster(RecoverConfig{
		NodeID:       nodeID,
		RaftBindAddr: bindAddr,
		DataDir:      recoveredDir,
		Voters:       map[string]string{nodeID: bindAddr},
	}, backupDir)
	require.NoError(t, err)

	recovered, err := NewNode(NodeConfig{NodeID: nodeID, RaftBindAddr: bindAddr, DataDir: recoveredDir})
	require.NoError(t, err)
	defer recovered.Shutdown()

	// RecoverCluster already forced the configuration (this node as the
	// sole voter) into the log, so unlike orig this node must NOT call
	// Bootstrap — starting it as-is should be enough for it to notice it's
	// the only voter and elect itself.
	awaitNodeLeader(t, recovered, 5*time.Second)

	sessions := recovered.Registry.SessionsSnapshot()
	require.Equal(t, nodeID, sessions["device-under-backup"], "session-ownership state must survive backup+restore")
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, e.IsDir(), "snapshot dir is expected to be flat")
		in, err := os.Open(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		out, err := os.Create(filepath.Join(dst, e.Name()))
		require.NoError(t, err)
		_, err = io.Copy(out, in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		require.NoError(t, out.Close())
	}
}

// TestRecoverCluster_RequiresVoters guards against a `restore raft` call
// with an empty/malformed --voters list silently forcing an empty
// configuration.
func TestRecoverCluster_RequiresVoters(t *testing.T) {
	err := RecoverCluster(RecoverConfig{
		NodeID:       "n1",
		RaftBindAddr: freePort(t),
		DataDir:      t.TempDir(),
	}, t.TempDir())
	require.Error(t, err)
}
