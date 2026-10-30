//go:build integration && linux

package membership

import (
	"context"
	"log/slog"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/stretchr/testify/require"
)

// This Linux integration test uses Docker host networking so Redis replication
// and the in-process controller can reach the same loopback addresses. Only
// test-owned containers are stopped; no application/deployment is required.
func startFailoverTestRedis(t *testing.T, addr, password string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	out, err := exec.Command("docker", "run", "--pull=never", "--detach", "--rm", "--network", "host",
		"redis:7.4-alpine", "redis-server", "--bind", "127.0.0.1", "--port", port,
		"--appendonly", "no", "--save", "", "--requirepass", password, "--masterauth", password).CombinedOutput()
	require.NoError(t, err, "%s", out)
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "--force", id).Run() })
	client := (realRedisAdmin{password: password}).dial(addr)
	t.Cleanup(func() { _ = client.Close() })
	require.Eventually(t, func() bool {
		return client.Ping(context.Background()).Err() == nil
	}, 10*time.Second, 100*time.Millisecond, "Redis did not start")
	return id
}

func TestRedisSidecarFailure_FailoverAndRejoinWithLiveCore(t *testing.T) {
	const password = "redis-failover-test-password"
	primaryAddr, replicaAddr := freeAddr(t), freeAddr(t)
	primaryContainer := startFailoverTestRedis(t, primaryAddr, password)
	startFailoverTestRedis(t, replicaAddr, password)
	admin := realRedisAdmin{password: password}
	primaryClient, replicaClient := admin.dial(primaryAddr), admin.dial(replicaAddr)
	t.Cleanup(func() { _ = primaryClient.Close(); _ = replicaClient.Close() })
	members := []NodeMeta{
		{NodeID: "core-1", Role: RoleCore, RedisAddr: primaryAddr},
		{NodeID: "core-2", Role: RoleCore, RedisAddr: replicaAddr},
	}
	// A real Raft registry with its voter count overridden to exercise the
	// multi-core policy, as in the unit tests. Gossip membership stays intact
	// for the entire test, including while the primary Redis is stopped.
	m := newTestLeaderMembership(t, "core-1", members, admin, 2)
	require.NoError(t, m.raft.Registry.SetRedisPrimary("core-1"))
	m.reconcileRedisPrimary()
	require.NoError(t, primaryClient.Set(context.Background(), "before-crash", "replicated", 0).Err())
	require.Eventually(t, func() bool {
		value, err := replicaClient.Get(context.Background(), "before-crash").Result()
		return err == nil && value == "replicated"
	}, 20*time.Second, 100*time.Millisecond, "initial replication did not catch up")

	router, err := redisrouter.New(context.Background(), primaryAddr, password)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	watchDone, loopDone := make(chan struct{}), make(chan struct{})
	m.stopRedisFailover = make(chan struct{})
	go func() {
		defer close(watchDone)
		redisrouter.WatchPrimary(ctx, router, m.raft.Registry.CurrentRedisPrimary,
			func(nodeID string) (string, bool) {
				for _, member := range members {
					if member.NodeID == nodeID {
						return member.RedisAddr, true
					}
				}
				return "", false
			}, slog.Default())
	}()
	go func() { defer close(loopDone); m.redisFailoverLoop() }()
	t.Cleanup(func() {
		cancel()
		close(m.stopRedisFailover)
		<-watchDone
		<-loopDone
		_ = router.Close()
	})

	out, err := exec.Command("docker", "kill", primaryContainer).CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Eventually(t, func() bool {
		primary, ok := m.raft.Registry.CurrentRedisPrimary()
		return ok && primary == "core-2" && router.Addr() == replicaAddr
	}, 15*time.Second, 100*time.Millisecond, "Redis-only failure did not promote and redirect")
	require.Len(t, m.Members(), 2)
	value, err := router.Client().Get(context.Background(), "before-crash").Result()
	require.NoError(t, err)
	require.Equal(t, "replicated", value)
	require.NoError(t, router.Client().Set(context.Background(), "after-failover", "writable", 0).Err())

	// Recreate only the old Redis, on its original address, like recovery
	// of the PVC in the incident. It starts as master with an empty DB and
	// must be converted into a replica of the new designated primary.
	startFailoverTestRedis(t, primaryAddr, password)
	require.Eventually(t, func() bool {
		info, err := primaryClient.Info(context.Background(), "replication").Result()
		if err != nil || !strings.Contains(info, "role:slave") || !strings.Contains(info, "master_link_status:up") {
			return false
		}
		value, err := primaryClient.Get(context.Background(), "after-failover").Result()
		return err == nil && value == "writable"
	}, 20*time.Second, 100*time.Millisecond, "recovered Redis did not rejoin as a synchronized replica")
	primary, _ := m.raft.Registry.CurrentRedisPrimary()
	require.Equal(t, "core-2", primary)
}
