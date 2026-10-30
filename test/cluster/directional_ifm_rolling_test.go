//go:build cluster

package cluster

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDirectionalIFMRealRollingLifecycle exercises the actual old image and
// the compatibility image against the same Redis-backed cluster. It is kept
// separate from the normal cluster suite because it deliberately spans image
// versions and takes several minutes to start the real infrastructure.
func TestDirectionalIFMRealRollingLifecycle(t *testing.T) {
	oldImage := os.Getenv("IFM_OLD_IMAGE")
	if oldImage == "" {
		oldImage = "ghcr.io/keel-iot/keel-mqtt-gateway:0.2.30"
	}
	compatImage := os.Getenv("IFM_COMPAT_IMAGE")
	if compatImage == "" {
		compatImage = "keel-mqtt-gateway:compat-local"
	}
	h := NewHarnessWithEdgeImages(t, 3, []string{oldImage, oldImage, oldImage}, []string{deviceAID})
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: h.RedisAddr(0)})
	t.Cleanup(func() { _ = rdb.Close() })
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		diagnosticCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, clientID := range []string{"ifm-rolling-session", "ifm-compat-session"} {
			fields, err := rdb.HKeys(diagnosticCtx, redisInflightKeyFor(clientID)).Result()
			t.Logf("rolling diagnostic %s IFM fields=%v error=%v", clientID, fields, err)
		}
		info, err := rdb.Info(diagnosticCtx, "replication").Result()
		t.Logf("rolling diagnostic Redis replication: %s error=%v", info, err)
		client := &http.Client{Timeout: 2 * time.Second}
		for _, path := range []string{"/api/cluster/routes", "/api/cluster/nodes", "/api/cluster/sessions"} {
			resp, err := client.Get("http://" + h.ManagementAddr(0) + path)
			if err != nil {
				t.Logf("rolling diagnostic %s: %v", path, err)
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
			resp.Body.Close()
			t.Logf("rolling diagnostic %s: %s", path, body)
		}
		for _, node := range h.Edges {
			stream, err := node.Container.Logs(diagnosticCtx)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(stream, 512*1024))
			stream.Close()
			lines := strings.Split(string(body), "\n")
			if len(lines) > 70 {
				lines = lines[len(lines)-70:]
			}
			t.Logf("rolling diagnostic edge %s logs:\n%s", node.ID, strings.Join(lines, "\n"))
		}
	})

	if err := rdb.Set(ctx, "keel:gw:IFM:SCHEMA", "1", 0).Err(); err != nil {
		t.Fatalf("set schema 1: %v", err)
	}

	clientID := "ifm-rolling-session"
	filter := "telemetry/#"
	first := connectPersistentClient(t, h.MQTTAddr(0), clientID, consumerUsername, consumerPassword)
	if token := first.Subscribe(filter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("old baseline subscribe: %v", token.Error())
	}
	first.Disconnect(250)
	if !waitForOfflineOwnership(t, h, 0, clientID, 20*time.Second) {
		t.Fatal("old session offline ownership did not converge before schema baseline publish")
	}

	// A second old client creates an incomplete persisted delivery while the
	// first session is offline. This proves the old-only baseline has actual
	// legacy state, rather than relying on a hand-written Redis fixture.
	publisher := connectClient(t, h.MQTTAddr(1), "ifm-rolling-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)
	if token := publisher.Publish(publishTopicFor(deviceAID), 1, false, "legacy-baseline"); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("old baseline publish: %v", token.Error())
	}
	if !waitForRedisAnyNumericIFMField(t, rdb, ctx, clientID, 15*time.Second) {
		t.Fatal("old-only baseline did not create a numeric IFM field")
	}
	assertNoDirectionalFields(t, rdb, ctx, clientID)

	// 2 OLD + 1 COMPAT. The compatibility Edge must still write numeric
	// fields while schema 1 is active.
	replaceRollingEdge(t, h, 0, compatImage, "edge-compat-1")
	assertNoDirectionalFields(t, rdb, ctx, clientID)
	compatClient := connectPersistentClient(t, h.MQTTAddr(0), "ifm-compat-session", consumerUsername, consumerPassword)
	if token := compatClient.Subscribe(filter, 1, nil); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("compat schema-1 subscribe: %v", token.Error())
	}
	compatClient.Disconnect(250)
	if !waitForOfflineOwnership(t, h, 0, "ifm-compat-session", 20*time.Second) {
		t.Fatal("compatibility session offline ownership did not converge before schema publish")
	}
	if token := publisher.Publish(publishTopicFor(deviceAID), 1, false, "compat-legacy"); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		t.Fatalf("compat schema-1 publish: %v", token.Error())
	}
	if !waitForRedisAnyNumericIFMField(t, rdb, ctx, "ifm-compat-session", 15*time.Second) {
		t.Fatal("compatibility Edge wrote no legacy IFM field under schema 1")
	}
	assertNoDirectionalFields(t, rdb, ctx, "ifm-compat-session")

	// COMPAT-created schema-1 state is readable by an old Edge.
	oldReconnect := connectPersistentClient(t, h.MQTTAddr(1), "ifm-compat-session", consumerUsername, consumerPassword)
	oldReconnect.Disconnect(250)

	// 1 OLD + 2 COMPAT, then COMPAT-only. Keep the marker at 1 throughout.
	replaceRollingEdge(t, h, 1, compatImage, "edge-compat-2")
	replaceRollingEdge(t, h, 2, compatImage, "edge-compat-3")
	assertNoDirectionalFields(t, rdb, ctx, "ifm-rolling-session")
	assertNoDirectionalFields(t, rdb, ctx, "ifm-compat-session")

	// Pre-activation rollback: replace a compatibility Edge with the old
	// image while the marker is still 1 and verify the legacy state remains.
	replaceRollingEdge(t, h, 0, oldImage, "edge-old-rollback")
	assertNoDirectionalFields(t, rdb, ctx, "ifm-compat-session")
	replaceRollingEdge(t, h, 0, compatImage, "edge-compat-restored")

	// Activation precondition: all three running Edge containers are now the
	// compatibility image. Only then perform the explicit marker transition.
	if err := rdb.Set(ctx, "keel:gw:IFM:SCHEMA", "2", 0).Err(); err != nil {
		t.Fatalf("activate schema 2: %v", err)
	}
	if got, err := rdb.Get(ctx, "keel:gw:IFM:SCHEMA").Result(); err != nil || got != "2" {
		t.Fatalf("schema marker after activation=%q err=%v", got, err)
	}

	// Restart all compatibility processes after activation so they load the
	// persisted marker. Existing legacy state is then rehydrated and migrated.
	for i := range h.Edges {
		replaceRollingEdge(t, h, i, compatImage, "edge-post-activation-"+strconv.Itoa(i+1))
	}
	for i, edge := range h.Edges {
		info, err := edge.Container.Inspect(ctx)
		if err != nil {
			t.Fatalf("inspect post-activation Edge %d: %v", i, err)
		}
		actualImage := "<unknown>"
		if info.Config != nil {
			actualImage = info.Config.Image
		}
		if actualImage != compatImage {
			t.Fatalf("post-activation Edge %d is running image %q, expected %q", i, actualImage, compatImage)
		}
	}
	// Reconnect the untouched old-session state now that the replacement Edge
	// has loaded schema 2; OnSessionEstablish performs the deterministic
	// legacy-to-directional conversion.
	rehydrated := connectPersistentClient(t, h.MQTTAddr(0), clientID, consumerUsername, consumerPassword)
	defer rehydrated.Disconnect(250)
	if !waitForRedisDirectionalIFMField(t, rdb, ctx, clientID, 15*time.Second) {
		t.Fatal("schema-2 rehydrate did not migrate legacy IFM state")
	}
	assertNoNumericIFMFields(t, rdb, ctx, clientID)
}

func replaceRollingEdge(t *testing.T, h *Harness, index int, image, nodeID string) {
	t.Helper()
	if err := h.Edges[index].Container.Terminate(context.Background()); err != nil {
		t.Fatalf("terminate old Edge %d: %v", index, err)
	}
	h.Edges[index] = h.startEdge(context.Background(), nodeID, "core-1:7946", "redis-core-1:6379", credentialsYAML([]string{deviceAID}), nil, image)
	// Listener startup precedes gossip convergence. In particular, old binaries
	// may have a route before NodeGRPCAddr knows the replacement: publishing in
	// that window tests unknown-node forwarding, not IFM format compatibility.
	// NotifyJoin installs the address before emitting this log event.
	for i, peer := range h.Edges {
		if i == index {
			continue
		}
		joined := wait.ForLog(`"msg":"membership: node joined","node_id":"` + nodeID + `"`).WithStartupTimeout(20 * time.Second)
		if err := joined.WaitUntilReady(context.Background(), peer.Container); err != nil {
			t.Fatalf("edge %s did not discover replacement %s: %v", peer.ID, nodeID, err)
		}
	}

}

func ifmFields(t *testing.T, rdb *redis.Client, ctx context.Context, clientID string) []string {
	t.Helper()
	fields, err := rdb.HKeys(ctx, redisInflightKeyFor(clientID)).Result()
	if err != nil {
		t.Fatalf("HKeys(%s): %v", clientID, err)
	}
	return fields
}

func assertNoDirectionalFields(t *testing.T, rdb *redis.Client, ctx context.Context, clientID string) {
	t.Helper()
	for _, field := range ifmFields(t, rdb, ctx, clientID) {
		if len(field) > 3 && (field[:3] == "in:" || field[:4] == "out:") {
			t.Fatalf("schema 1 emitted directional field %q for %s", field, clientID)
		}
	}
}

func assertNoNumericIFMFields(t *testing.T, rdb *redis.Client, ctx context.Context, clientID string) {
	t.Helper()
	for _, field := range ifmFields(t, rdb, ctx, clientID) {
		if _, err := strconv.ParseUint(field, 10, 16); err == nil {
			t.Fatalf("schema 2 left numeric IFM field %q for %s", field, clientID)
		}
	}
}

func waitForRedisAnyNumericIFMField(t *testing.T, rdb *redis.Client, ctx context.Context, clientID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, field := range ifmFields(t, rdb, ctx, clientID) {
			if _, err := strconv.ParseUint(field, 10, 16); err == nil {
				return true
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func waitForRedisDirectionalIFMField(t *testing.T, rdb *redis.Client, ctx context.Context, clientID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, field := range ifmFields(t, rdb, ctx, clientID) {
			if len(field) > 3 && (field[:3] == "in:" || (len(field) > 4 && field[:4] == "out:")) {
				return true
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
