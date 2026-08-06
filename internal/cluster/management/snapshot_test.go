package management

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/stretchr/testify/require"
)

func freeTestPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestHandleSnapshot_NoRaftNode(t *testing.T) {
	api := &API{Log: slog.Default()}
	rec := httptest.NewRecorder()
	api.Router().ServeHTTP(rec, httptest.NewRequest("POST", "/api/cluster/snapshot", nil))
	require.Equal(t, 503, rec.Code)
}

func TestHandleSnapshot_ReturnsSnapshotDir(t *testing.T) {
	node, err := keelraft.NewNode(keelraft.NodeConfig{
		NodeID:       "mgmt-snapshot-test",
		RaftBindAddr: freeTestPort(t),
		DataDir:      t.TempDir(),
	})
	require.NoError(t, err)
	defer node.Shutdown()

	_, err = node.Bootstrap()
	require.NoError(t, err)
	require.Eventually(t, node.IsLeader, 5*time.Second, 10*time.Millisecond)

	_, err = node.Registry.ClaimSession("device-1", "mgmt-snapshot-test", "")
	require.NoError(t, err)

	api := &API{RaftNode: node, Log: slog.Default()}
	rec := httptest.NewRecorder()
	api.Router().ServeHTTP(rec, httptest.NewRequest("POST", "/api/cluster/snapshot", nil))
	require.Equal(t, 200, rec.Code)

	var resp snapshotResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.ID)
	require.DirExists(t, resp.Dir)
}
