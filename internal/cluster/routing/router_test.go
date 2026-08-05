package routing

import (
	"strings"
	"testing"
	"time"
)

func newTestRouter(t *testing.T) *Router {
	t.Helper()
	r, err := New(Config{Store: newMemStore(), ReconcileInterval: time.Hour}) // reconcile never fires; tests exercise the pub/sub fast path
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// waitForNodes polls NodesFor(topic) until it matches want (as a set) or
// the timeout elapses. Subscribe/Unsubscribe apply to the local cache
// asynchronously (via the store's pub/sub echo), so tests can't assert
// immediately after a call returns.
func waitForNodes(t *testing.T, r *Router, topic string, want []string) []string {
	t.Helper()
	wantSet := make(map[string]bool, len(want))
	for _, n := range want {
		wantSet[n] = true
	}

	deadline := time.Now().Add(2 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = r.NodesFor(topic, "")
		if len(got) == len(wantSet) {
			gotSet := make(map[string]bool, len(got))
			for _, n := range got {
				gotSet[n] = true
			}
			match := true
			for n := range wantSet {
				if !gotSet[n] {
					match = false
					break
				}
			}
			if match {
				return got
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("NodesFor(%q): timed out waiting for %v, last got %v", topic, want, got)
	return nil
}

func TestRouterExactMatch(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe("telemetry/poc/device-1", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	waitForNodes(t, r, "telemetry/poc/device-1", []string{"core-1"})
	if nodes := r.NodesFor("telemetry/poc/device-2", ""); len(nodes) != 0 {
		t.Fatalf("expected no match for a different literal topic, got %v", nodes)
	}
}

func TestRouterHashWildcard(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe("sport/tennis/#", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForNodes(t, r, "sport/tennis", []string{"core-1"}) // "#" also matches its parent level; syncs the cache before the table below

	cases := []struct {
		topic string
		want  bool
	}{
		{"sport/tennis", true},
		{"sport/tennis/player1", true},
		{"sport/tennis/player1/ranking", true},
		{"sport/badminton/player1", false},
		{"sport", false},
	}
	for _, tc := range cases {
		nodes := r.NodesFor(tc.topic, "")
		got := len(nodes) == 1 && nodes[0] == "core-1"
		if got != tc.want {
			t.Errorf("topic %q: got match=%v (%v), want match=%v", tc.topic, got, nodes, tc.want)
		}
	}
}

func TestRouterPlusWildcard(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe("sport/+/player1", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForNodes(t, r, "sport/tennis/player1", []string{"core-1"})
	if nodes := r.NodesFor("sport/tennis/indoor/player1", ""); len(nodes) != 0 {
		t.Fatalf("single-level '+' must not span multiple segments, got %v", nodes)
	}

	r2 := newTestRouter(t)
	if err := r2.Subscribe("sport/+/+/scores", "core-2"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForNodes(t, r2, "sport/tennis/indoor/scores", []string{"core-2"})
	if nodes := r2.NodesFor("sport/tennis/scores", ""); len(nodes) != 0 {
		t.Fatalf("two '+' wildcards must not collapse to fewer segments, got %v", nodes)
	}
}

func TestRouterSysTopicExcludedFromWildcard(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe("#", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := r.Subscribe("+/uptime", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForNodes(t, r, "anything/else", []string{"core-1"}) // sync point for the '#' subscribe

	if nodes := r.NodesFor("$SYS/uptime", ""); len(nodes) != 0 {
		t.Fatalf("top-level '#'/'+' must not match a $SYS topic, got %v", nodes)
	}

	if err := r.Subscribe("$SYS/uptime", "core-2"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForNodes(t, r, "$SYS/uptime", []string{"core-2"})
}

func TestRouterUnionOfOverlappingFilters(t *testing.T) {
	r := newTestRouter(t)
	// A more specific (literal) filter and a more general (wildcard)
	// filter, from different nodes, both active on the same published
	// topic: NodesFor must return the union, unlike the ACL precedence
	// rules used elsewhere.
	if err := r.Subscribe("telemetry/poc/device-1", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := r.Subscribe("telemetry/poc/#", "core-2"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := r.Subscribe("telemetry/+/device-1", "core-3"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	waitForNodes(t, r, "telemetry/poc/device-1", []string{"core-1", "core-2", "core-3"})

	// A node subscribed via two distinct matching filters must still
	// appear only once (dedup), not once per matching filter.
	if err := r.Subscribe("telemetry/poc/+", "core-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		nodes := r.NodesFor("telemetry/poc/device-1", "")
		count := 0
		for _, n := range nodes {
			if n == "core-1" {
				count++
			}
		}
		if len(nodes) == 3 && count == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected core-1 to appear exactly once despite matching two filters, got %v", r.NodesFor("telemetry/poc/device-1", ""))
}

func TestRouterUnsubscribe(t *testing.T) {
	r := newTestRouter(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(r.Subscribe("t/1", "core-1"))
	must(r.Subscribe("t/1", "core-2"))
	waitForNodes(t, r, "t/1", []string{"core-1", "core-2"})

	must(r.Unsubscribe("t/1", "core-1"))
	waitForNodes(t, r, "t/1", []string{"core-2"})

	must(r.Unsubscribe("t/1", "core-2"))
	waitForNodes(t, r, "t/1", nil)
}

func TestRouterUnsubscribeBatchAndPurgeNode(t *testing.T) {
	r := newTestRouter(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(r.Subscribe("t/1", "core-1"))
	must(r.Subscribe("t/2", "core-1"))
	must(r.Subscribe("t/3", "core-1"))
	must(r.Subscribe("t/1", "core-2"))
	waitForNodes(t, r, "t/3", []string{"core-1"})

	must(r.UnsubscribeBatch([]string{"t/1", "t/2"}, "core-1"))
	waitForNodes(t, r, "t/1", []string{"core-2"})
	waitForNodes(t, r, "t/2", nil)
	waitForNodes(t, r, "t/3", []string{"core-1"}) // untouched — not in the batch

	must(r.PurgeNode("core-1"))
	waitForNodes(t, r, "t/3", nil)
	waitForNodes(t, r, "t/1", []string{"core-2"}) // core-2's own route survives
}

// waitForOneOf polls NodesFor(topic, localNodeID) until it returns exactly
// one node from among want, or the timeout elapses — used for shared
// subscription cases where the selected member is arbitrary.
func waitForOneOf(t *testing.T, r *Router, topic, localNodeID string, want []string) string {
	t.Helper()
	wantSet := make(map[string]bool, len(want))
	for _, n := range want {
		wantSet[n] = true
	}

	deadline := time.Now().Add(2 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = r.NodesFor(topic, localNodeID)
		if len(got) == 1 && wantSet[got[0]] {
			return got[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("NodesFor(%q, %q): timed out waiting for exactly one of %v, last got %v", topic, localNodeID, want, got)
	return ""
}

// TestRouterSharedSubscriptionLocalMemberSkipsForward covers the case
// where localNodeID is itself a member of the matching shared group: no
// node should be returned, since that node's own mochi-mqtt instance
// already delivers to its local group member, and a cluster forward would
// double-deliver.
func TestRouterSharedSubscriptionLocalMemberSkipsForward(t *testing.T) {
	r := newTestRouter(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(r.Subscribe("$share/g1/telemetry/device-1", "core-1"))
	must(r.Subscribe("$share/g1/telemetry/device-1", "core-2"))
	waitForOneOf(t, r, "telemetry/device-1", "zzz-unrelated", []string{"core-1", "core-2"}) // wait for propagation

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if nodes := r.NodesFor("telemetry/device-1", "core-1"); len(nodes) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected no forward target when localNodeID is itself a shared-group member, got %v", r.NodesFor("telemetry/device-1", "core-1"))
}

// TestRouterSharedSubscriptionRemoteMemberSelectsExactlyOne covers the
// case where localNodeID is not a member of the matching shared group:
// exactly one other member must be selected, never the full set — that
// preserves exactly-once delivery per group across the cluster.
func TestRouterSharedSubscriptionRemoteMemberSelectsExactlyOne(t *testing.T) {
	r := newTestRouter(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(r.Subscribe("$share/g1/telemetry/device-1", "core-1"))
	must(r.Subscribe("$share/g1/telemetry/device-1", "core-2"))

	waitForOneOf(t, r, "telemetry/device-1", "core-3", []string{"core-1", "core-2"})
}

// TestRouterSharedSubscriptionMixedWithRegular covers a topic matching
// both a regular subscription and a shared group: the regular subscriber
// must always be included, and the shared group contributes at most one
// extra node.
func TestRouterSharedSubscriptionMixedWithRegular(t *testing.T) {
	r := newTestRouter(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(r.Subscribe("telemetry/device-1", "core-1"))
	must(r.Subscribe("$share/g1/telemetry/device-1", "core-2"))
	must(r.Subscribe("$share/g1/telemetry/device-1", "core-3"))

	deadline := time.Now().Add(2 * time.Second)
	var nodes []string
	for time.Now().Before(deadline) {
		nodes = r.NodesFor("telemetry/device-1", "core-4")
		if len(nodes) == 2 && containsNode(nodes, "core-1") &&
			(containsNode(nodes, "core-2") || containsNode(nodes, "core-3")) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected core-1 plus exactly one of core-2/core-3, got %v", nodes)
}

// waitForOfflineNodes mirrors waitForNodes for OfflineNodesFor.
func waitForOfflineNodes(t *testing.T, r *Router, topic string, want []string) []string {
	t.Helper()
	wantSet := make(map[string]bool, len(want))
	for _, n := range want {
		wantSet[n] = true
	}

	deadline := time.Now().Add(2 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = r.OfflineNodesFor(topic)
		if len(got) == len(wantSet) {
			gotSet := make(map[string]bool, len(got))
			for _, n := range got {
				gotSet[n] = true
			}
			match := true
			for n := range wantSet {
				if !gotSet[n] {
					match = false
					break
				}
			}
			if match {
				return got
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("OfflineNodesFor(%q): timed out waiting for %v, last got %v", topic, want, got)
	return nil
}

func TestRouterOfflineNodesFor_MatchesWildcard(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe(OfflineRouteKey("telemetry/#"), "edge-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitForOfflineNodes(t, r, "telemetry/device-1", []string{"edge-1"})
}

// TestRouterOfflineNodesFor_NeverMatchesLiveRoutingIndex verifies the two
// indices stay distinct: a live subscription to a real filter must never
// show up in OfflineNodesFor's result for the same topic, and vice versa.
func TestRouterOfflineNodesFor_NeverMatchesLiveRoutingIndex(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe("telemetry/device-1", "edge-1"); err != nil {
		t.Fatalf("subscribe (live): %v", err)
	}
	if err := r.Subscribe(OfflineRouteKey("telemetry/device-1"), "edge-2"); err != nil {
		t.Fatalf("subscribe (offline route): %v", err)
	}
	waitForNodes(t, r, "telemetry/device-1", []string{"edge-1"})
	waitForOfflineNodes(t, r, "telemetry/device-1", []string{"edge-2"})
}

func TestOwnershipKey_DeterministicAndUnique(t *testing.T) {
	k1 := OwnershipKey("device-1", "telemetry/#")
	k2 := OwnershipKey("device-1", "telemetry/#")
	if k1 != k2 {
		t.Fatalf("expected deterministic key, got %q != %q", k1, k2)
	}
	k3 := OwnershipKey("device-2", "telemetry/#")
	if k1 == k3 {
		t.Fatalf("expected different clientIDs to produce different keys")
	}
	if !strings.HasPrefix(k1, "$offline/") {
		t.Fatalf("expected $offline/ prefix, got %q", k1)
	}
	for _, seg := range strings.Split(k1, "/") {
		if seg == "+" || seg == "#" {
			t.Fatalf("key must never contain a bare wildcard level, got %q in %q", seg, k1)
		}
	}
}

func TestRouterOwnedClientIDs_DedupesAcrossFilters(t *testing.T) {
	r := newTestRouter(t)
	if err := r.Subscribe(OwnershipKey("device-1", "telemetry/#"), "edge-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := r.Subscribe(OwnershipKey("device-1", "cmd/device-1"), "edge-1"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := r.Subscribe(OwnershipKey("device-2", "telemetry/#"), "edge-2"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var owned []string
	for time.Now().Before(deadline) {
		owned = r.OwnedClientIDs("edge-1")
		if len(owned) == 1 && owned[0] == "device-1" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected exactly [device-1] (deduplicated across its two filters), got %v", owned)
}

func TestRouterOwnedClientIDs_NoEntries_ReturnsEmptyNotNil(t *testing.T) {
	r := newTestRouter(t)
	if owned := r.OwnedClientIDs("edge-1"); len(owned) != 0 {
		t.Fatalf("expected no owned clientIDs, got %v", owned)
	}
}
