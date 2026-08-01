package routing

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/store"
)

// countingStore wraps a ClusterStore and counts Delete calls plus the total
// keys handed to each — proves the Router issues a single batched Delete
// (not N per-key calls) for UnsubscribeBatch/PurgeNode.
type countingStore struct {
	inner       store.ClusterStore
	deleteCalls int32
	deleteKeys  int32
	perCallKeys []int
}

func newCountingStore(inner store.ClusterStore) *countingStore {
	return &countingStore{inner: inner}
}

func (c *countingStore) Put(ctx context.Context, key string, value []byte) error {
	return c.inner.Put(ctx, key, value)
}
func (c *countingStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.inner.Get(ctx, key)
}
func (c *countingStore) Delete(ctx context.Context, keys ...string) error {
	atomic.AddInt32(&c.deleteCalls, 1)
	atomic.AddInt32(&c.deleteKeys, int32(len(keys)))
	c.perCallKeys = append(c.perCallKeys, len(keys))
	return c.inner.Delete(ctx, keys...)
}
func (c *countingStore) Scan(ctx context.Context) (store.KeyIterator, error) {
	return c.inner.Scan(ctx)
}
func (c *countingStore) Publish(ctx context.Context, channel string, message []byte) error {
	return c.inner.Publish(ctx, channel, message)
}
func (c *countingStore) Subscribe(ctx context.Context, channel string) (store.Subscription, error) {
	return c.inner.Subscribe(ctx, channel)
}
func (c *countingStore) Close(ctx context.Context) error { return c.inner.Close(ctx) }

// TestRouterPurgeNodeSingleStoreCall proves PurgeNode (and
// UnsubscribeBatch) hit the store with exactly ONE Delete carrying every
// key, rather than one Delete per filter. The store-side fan-out to
// per-key Olric calls is an internal OlricStore concern (see that type's
// Delete doc) and invisible here — the Router's batch boundary is what
// matters for avoiding N sequential network round-trips.
func TestRouterPurgeNodeSingleStoreCall(t *testing.T) {
	cs := newCountingStore(newMemStore())
	r, err := New(Config{Store: cs, ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	const n = 5
	for i := 0; i < n; i++ {
		if err := r.Subscribe(("t/" + string(rune('a'+i))), "core-1"); err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}
	// Also subscribe another node to one of the same filters, to confirm
	// purge only removes the target node's entries.
	if err := r.Subscribe("t/a", "core-2"); err != nil {
		t.Fatalf("subscribe core-2: %v", err)
	}
	waitForNodes(t, r, "t/a", []string{"core-1", "core-2"})

	// Reset counters after the subscribe writes.
	atomic.StoreInt32(&cs.deleteCalls, 0)
	atomic.StoreInt32(&cs.deleteKeys, 0)

	if err := r.PurgeNode("core-1"); err != nil {
		t.Fatalf("purge: %v", err)
	}

	if got := atomic.LoadInt32(&cs.deleteCalls); got != 1 {
		t.Fatalf("expected exactly 1 store.Delete call for PurgeNode, got %d", got)
	}
	if got := atomic.LoadInt32(&cs.deleteKeys); got != int32(n) {
		t.Fatalf("expected the single Delete to carry %d keys, got %d", n, got)
	}

	// core-1 fully purged, core-2's own entry on t/a survives.
	waitForNodes(t, r, "t/a", []string{"core-2"})
	for _, f := range []string{"t/a", "t/b", "t/c", "t/d", "t/e"} {
		if nodes := r.NodesFor(f, ""); len(nodes) != 0 && !(f == "t/a" && len(nodes) == 1 && nodes[0] == "core-2") {
			t.Fatalf("filter %q after purge: %v", f, nodes)
		}
	}
	if got := len(r.TopicsForNode("core-1")); got != 0 {
		t.Fatalf("expected core-1 inverse index empty after purge, got %d filters", got)
	}
}

// TestRouterPurgeNodeConvergesAcrossNodes subscribes a node to several
// filters cluster-wide, then PurgeNode from one Router and confirms every
// node's local cache (routes snapshot, NodesFor trie, inverse index)
// converges to empty for that node — i.e. the purge's pub/sub event +
// the underlying Olric store deletion both propagate and stay coherent.
// Uses real embedded Olric members, not a fake, so it exercises the
// per-key fan-out workaround in OlricStore.Delete against a real cluster.
func TestRouterPurgeNodeConvergesAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real embedded Olric members; skipped in -short")
	}
	routers := newOlricCluster(t, 3)
	rA, rB, rC := routers[0], routers[1], routers[2]

	filters := []string{"t/alpha", "t/beta", "t/gamma", "t/delta/#", "t/+/x"}
	for _, f := range filters {
		if err := rA.Subscribe(f, "core-1"); err != nil {
			t.Fatalf("subscribe %s: %v", f, err)
		}
	}
	// Another node subscribes one overlapping filter — must survive the purge.
	if err := rB.Subscribe("t/alpha", "core-2"); err != nil {
		t.Fatalf("subscribe core-2: %v", err)
	}

	// Wait for all nodes to see core-1 on every filter (convergence of writes).
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, f := range filters {
			if !containsNode(rA.NodesFor(matchTopic(f), ""), "core-1") {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Purge core-1 from any node — here rC, to confirm the event fans out
	// regardless of which member issues it.
	if err := rC.PurgeNode("core-1"); err != nil {
		t.Fatalf("purge: %v", err)
	}

	// Every node must converge to: core-1 gone from all filters, core-2 still
	// on t/alpha. Poll until stable (pub/sub propagation + reconcile).
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		allClean := true
		for _, r := range routers {
			if len(r.TopicsForNode("core-1")) != 0 {
				allClean = false
			}
			for _, f := range filters {
				nodes := r.NodesFor(matchTopic(f), "")
				if containsNode(nodes, "core-1") {
					allClean = false
				}
			}
			alphaNodes := r.NodesFor("t/alpha", "")
			if !containsNode(alphaNodes, "core-2") || containsNode(alphaNodes, "core-1") {
				allClean = false
			}
		}
		if allClean {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("routing tables did not converge after purge:\n A=%v\n B=%v\n C=%v",
		rA.Snapshot(), rB.Snapshot(), rC.Snapshot())
}

func containsNode(nodes []string, want string) bool {
	for _, n := range nodes {
		if n == want {
			return true
		}
	}
	return false
}

// matchTopic returns a concrete publish topic that a given filter matches,
// for NodesFor lookups. "#" and "+" expand to plausible literals.
func matchTopic(filter string) string {
	switch {
	case filter == "t/delta/#":
		return "t/delta/sub/deep"
	case filter == "t/+/x":
		return "t/anything/x"
	default:
		return filter
	}
}
