package routing

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/store"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// newOlricCluster starts n real, independent embedded Olric members on
// loopback, each wrapped in its own Router — a genuine multi-node AP
// cluster, not a fake, so TestConvergence exercises real pub/sub fanout
// and real Scan-based reconciliation across process-independent state.
func newOlricCluster(t *testing.T, n int) []*Router {
	t.Helper()

	var gossipAddrs []string
	var routers []*Router
	for i := 0; i < n; i++ {
		port := freePort(t)
		gossipPort := freePort(t)
		gossipAddr := fmt.Sprintf("127.0.0.1:%d", gossipPort)

		var peers []string
		if len(gossipAddrs) > 0 {
			peers = []string{gossipAddrs[0]}
		}
		gossipAddrs = append(gossipAddrs, gossipAddr)

		st, err := store.NewEmbeddedOlricStore(store.OlricConfig{
			BindAddr:      "127.0.0.1",
			BindPort:      port,
			GossipPort:    gossipPort,
			AdvertiseAddr: "127.0.0.1",
			Peers:         peers,
			DMapName:      "keel.routes.test",
			StartTimeout:  10 * time.Second,
		})
		if err != nil {
			t.Fatalf("start embedded olric store %d: %v", i, err)
		}
		t.Cleanup(func() { _ = st.Close(context.Background()) })

		r, err := New(Config{Store: st, Channel: "keel.routes.events.test", ReconcileInterval: 2 * time.Second})
		if err != nil {
			t.Fatalf("new router %d: %v", i, err)
		}
		t.Cleanup(func() { _ = r.Close() })

		routers = append(routers, r)
	}
	return routers
}

// TestConvergence validates the constraint from the routing-store
// migration: routing state moved off raft (strong consensus) to Olric
// (AP) specifically because it doesn't need every node to agree before a
// write is visible — but every node must still converge to the same view
// given a bit of quiet time. Concurrent, overlapping-topic Subscribes
// from independent Router/Olric-store instances (standing in for
// independent core nodes) must all end up with an identical routing
// table.
func TestConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real embedded Olric members; skipped in -short")
	}

	const nodes = 3
	routers := newOlricCluster(t, nodes)

	// Concurrent writes from every node, including overlapping filters on
	// the same topic (core-0 and core-1 both subscribe telemetry/shared/#
	// via different literal filters) to exercise the union/no-merge-needed
	// property, not just disjoint keys.
	errCh := make(chan error, nodes*2)
	for i, r := range routers {
		nodeID := fmt.Sprintf("core-%d", i)
		go func(r *Router, nodeID string) {
			errCh <- r.Subscribe(fmt.Sprintf("telemetry/node-%s/metrics", nodeID), nodeID)
		}(r, nodeID)
		go func(r *Router, nodeID string) {
			errCh <- r.Subscribe("telemetry/shared/#", nodeID)
		}(r, nodeID)
	}
	for i := 0; i < nodes*2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent subscribe: %v", err)
		}
	}

	wantShared := map[string]bool{"core-0": true, "core-1": true, "core-2": true}

	deadline := time.Now().Add(20 * time.Second)
	var lastSnaps []map[string][]string
	for time.Now().Before(deadline) {
		snaps := make([]map[string][]string, nodes)
		for i, r := range routers {
			snaps[i] = r.Snapshot()
		}
		lastSnaps = snaps

		if allConverged(snaps) && sharedMatches(routers[0], wantShared) {
			// Every node agrees, and specifically the overlapping filter
			// resolves to the full union on every node's own NodesFor.
			for i, r := range routers {
				got := r.NodesFor("telemetry/shared/anything", "")
				if !sharedMatches(r, wantShared) {
					t.Fatalf("node %d: NodesFor(telemetry/shared/anything) = %v, want union %v", i, got, wantShared)
				}
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("routing tables did not converge within timeout; last snapshots: %#v", lastSnaps)
}

func allConverged(snaps []map[string][]string) bool {
	if len(snaps) == 0 {
		return true
	}
	first := normalize(snaps[0])
	for _, s := range snaps[1:] {
		if !reflect.DeepEqual(first, normalize(s)) {
			return false
		}
	}
	return true
}

// normalize sorts each topic's node-ID slice so map/slice comparisons
// aren't sensitive to insertion order.
func normalize(snap map[string][]string) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(snap))
	for topic, nodes := range snap {
		set := make(map[string]bool, len(nodes))
		for _, n := range nodes {
			set[n] = true
		}
		out[topic] = set
	}
	return out
}

func sharedMatches(r *Router, want map[string]bool) bool {
	got := r.NodesFor("telemetry/shared/anything", "")
	if len(got) != len(want) {
		return false
	}
	for _, n := range got {
		if !want[n] {
			return false
		}
	}
	return true
}
