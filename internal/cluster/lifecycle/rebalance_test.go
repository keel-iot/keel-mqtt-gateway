package lifecycle

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
)

type fakeEvictor struct {
	mu      sync.Mutex
	calls   []string // "nodeID:clientID"
	failFor map[string]bool
}

func (f *fakeEvictor) Evict(_ context.Context, targetNodeID, clientID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := targetNodeID + ":" + clientID
	f.calls = append(f.calls, key)
	if f.failFor[clientID] {
		return fmt.Errorf("simulated failure for %s", clientID)
	}
	return nil
}

func sessionsWithCounts(counts map[string]int) map[string]string {
	sessions := make(map[string]string)
	for nodeID, n := range counts {
		for i := 0; i < n; i++ {
			sessions[fmt.Sprintf("%s-client-%d", nodeID, i)] = nodeID
		}
	}
	return sessions
}

func TestRebalance_BelowThreshold_NoEviction(t *testing.T) {
	// average = (10+11+9)/3 = 10, threshold at 20% = 12 — nobody exceeds it.
	sessions := sessionsWithCounts(map[string]int{"edge-1": 10, "edge-2": 11, "edge-3": 9})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}

	result, err := Rebalance(context.Background(), sessions, live, DefaultRebalanceConfig(), evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if len(result.Nodes) != 0 {
		t.Fatalf("expected no nodes rebalanced, got %d", len(result.Nodes))
	}
	if len(evictor.calls) != 0 {
		t.Fatalf("expected no Evict calls, got %d", len(evictor.calls))
	}
}

func TestRebalance_AboveThreshold_EvictsDownToTarget(t *testing.T) {
	// average = (5+5+30)/3 = ~13.33, target = round(13.33) = 13.
	// edge-3 (30) exceeds threshold (13.33*1.2 ≈ 16) — excess = 30-13 = 17.
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, EvictStagger: 0}

	result, err := Rebalance(context.Background(), sessions, live, cfg, evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if len(result.Nodes) != 1 {
		t.Fatalf("expected exactly 1 node rebalanced, got %d", len(result.Nodes))
	}
	nr := result.Nodes[0]
	if nr.NodeID != "edge-3" {
		t.Fatalf("expected edge-3 to be rebalanced, got %s", nr.NodeID)
	}
	if nr.Before != 30 {
		t.Fatalf("Before = %d, want 30", nr.Before)
	}
	if nr.Target != 13 {
		t.Fatalf("Target = %d, want 13", nr.Target)
	}
	if len(nr.Evicted) != 17 {
		t.Fatalf("evicted %d clients, want 17", len(nr.Evicted))
	}
	if len(evictor.calls) != 17 {
		t.Fatalf("expected 17 real Evict calls, got %d", len(evictor.calls))
	}
}

func TestRebalance_CapsAtMaxEvictionsPerNode(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 3, EvictStagger: 0}

	result, err := Rebalance(context.Background(), sessions, live, cfg, evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if len(result.Nodes) != 1 || len(result.Nodes[0].Evicted) != 3 {
		t.Fatalf("expected exactly 3 evictions (cap), got %+v", result.Nodes)
	}
}

func TestRebalance_DryRun_ReportsWithoutEvicting(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, EvictStagger: 0}

	result, err := Rebalance(context.Background(), sessions, live, cfg, evictor, true, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if !result.DryRun {
		t.Fatal("expected DryRun=true in result")
	}
	if len(result.Nodes) != 1 || len(result.Nodes[0].Evicted) != 17 {
		t.Fatalf("expected dry-run to still report 17 would-evict clients, got %+v", result.Nodes)
	}
	if len(evictor.calls) != 0 {
		t.Fatalf("dry run must not call Evict, got %d calls", len(evictor.calls))
	}
}

func TestRebalance_DryRun_NilEvictor_Ok(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 30})
	live := map[string]bool{"edge-1": true}

	_, err := Rebalance(context.Background(), sessions, live, DefaultRebalanceConfig(), nil, true, silentLogger())
	if err != nil {
		t.Fatalf("dry run with nil evictor should not error: %v", err)
	}
}

func TestRebalance_RealRun_NilEvictor_Errors(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 30})
	live := map[string]bool{"edge-1": true}

	_, err := Rebalance(context.Background(), sessions, live, DefaultRebalanceConfig(), nil, false, silentLogger())
	if err == nil {
		t.Fatal("expected error for real run with nil evictor")
	}
}

func TestRebalance_IgnoresSessionsOnNonLiveNodes(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "ghost-node": 100})
	live := map[string]bool{"edge-1": true} // ghost-node not currently live
	evictor := &fakeEvictor{}

	result, err := Rebalance(context.Background(), sessions, live, DefaultRebalanceConfig(), evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if result.TotalSessions != 5 {
		t.Fatalf("TotalSessions = %d, want 5 (ghost-node excluded)", result.TotalSessions)
	}
	if len(result.Nodes) != 0 {
		t.Fatalf("expected no rebalancing (single live node, no imbalance possible), got %+v", result.Nodes)
	}
}

func TestRebalance_EvictErrorsAreRecordedNotFatal(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	failFor := make(map[string]bool)
	for i := 0; i < 30; i++ {
		if i%2 == 0 {
			failFor[fmt.Sprintf("edge-3-client-%d", i)] = true
		}
	}
	evictor := &fakeEvictor{failFor: failFor}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, EvictStagger: 0}

	result, err := Rebalance(context.Background(), sessions, live, cfg, evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if len(result.Nodes) != 1 {
		t.Fatalf("expected 1 node result, got %d", len(result.Nodes))
	}
	nr := result.Nodes[0]
	if len(nr.Evicted)+len(nr.EvictErrs) != 17 {
		t.Fatalf("evicted(%d)+errs(%d) != 17", len(nr.Evicted), len(nr.EvictErrs))
	}
	if len(nr.EvictErrs) == 0 {
		t.Fatal("expected at least one recorded evict error")
	}
}

func TestRebalance_ContextCancelledMidStagger(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, EvictStagger: 50 * time.Millisecond}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	result, err := Rebalance(ctx, sessions, live, cfg, evictor, false, silentLogger())
	if err == nil {
		t.Fatal("expected context deadline error")
	}
	if result == nil || len(result.Nodes) != 1 {
		t.Fatalf("expected partial result with 1 node entry even on cancellation, got %+v", result)
	}
	// Some evictions should have happened before the deadline, but not all 17.
	if len(result.Nodes[0].Evicted) == 0 || len(result.Nodes[0].Evicted) >= 17 {
		t.Fatalf("expected a partial eviction count, got %d", len(result.Nodes[0].Evicted))
	}
}

func TestRunScheduledRebalance_TicksAndEvicts(t *testing.T) {
	evictor := &fakeEvictor{}
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, EvictStagger: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
	defer cancel()

	RunScheduledRebalance(ctx,
		func() map[string]string { return sessions },
		func() map[string]bool { return live },
		cfg, evictor, false, 20*time.Millisecond, silentLogger())

	evictor.mu.Lock()
	calls := len(evictor.calls)
	evictor.mu.Unlock()
	if calls == 0 {
		t.Fatal("expected at least one Evict call from the scheduled ticker")
	}
}

func TestRunScheduledRebalance_DryRunNeverEvicts(t *testing.T) {
	evictor := &fakeEvictor{}
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, EvictStagger: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
	defer cancel()

	RunScheduledRebalance(ctx,
		func() map[string]string { return sessions },
		func() map[string]bool { return live },
		cfg, evictor, true, 20*time.Millisecond, silentLogger())

	evictor.mu.Lock()
	calls := len(evictor.calls)
	evictor.mu.Unlock()
	if calls != 0 {
		t.Fatalf("dry-run scheduled rebalance must never call Evict, got %d calls", calls)
	}
}

func TestRunScheduledRebalance_StopsOnContextCancel(t *testing.T) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		RunScheduledRebalance(ctx,
			func() map[string]string { return nil },
			func() map[string]bool { return nil },
			DefaultRebalanceConfig(), &fakeEvictor{}, true, 5*time.Millisecond, silentLogger())
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunScheduledRebalance did not return after context cancellation")
	}
}

func TestLiveEdgeNodes_FiltersByRole(t *testing.T) {
	members := []membership.NodeMeta{
		{NodeID: "core-1", Role: membership.RoleCore},
		{NodeID: "edge-1", Role: membership.RoleEdge},
		{NodeID: "edge-2", Role: membership.RoleEdge},
	}
	live := LiveEdgeNodes(members)
	if len(live) != 2 || !live["edge-1"] || !live["edge-2"] || live["core-1"] {
		t.Fatalf("unexpected live set: %+v", live)
	}
}

func TestRebalance_ExcludedClientsNeverEvicted(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}
	// Exclude most of edge-3's clients, leaving only 5 evictable — fewer
	// than the computed excess (17), so eviction should cap at what's
	// actually evictable instead of erroring or picking excluded ones.
	exclude := make(map[string]bool)
	for i := 5; i < 30; i++ {
		exclude[fmt.Sprintf("edge-3-client-%d", i)] = true
	}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, ExcludeClientIDs: exclude}

	result, err := Rebalance(context.Background(), sessions, live, cfg, evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if len(result.Nodes) != 1 {
		t.Fatalf("expected 1 node result, got %d", len(result.Nodes))
	}
	nr := result.Nodes[0]
	if len(nr.Evicted) != 5 {
		t.Fatalf("evicted %d, want 5 (only non-excluded clients available)", len(nr.Evicted))
	}
	for _, id := range nr.Evicted {
		if exclude[id] {
			t.Fatalf("excluded client %q was evicted", id)
		}
	}
}

func TestRebalance_AllCandidatesExcluded_NoEviction(t *testing.T) {
	sessions := sessionsWithCounts(map[string]int{"edge-1": 5, "edge-2": 5, "edge-3": 30})
	live := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	evictor := &fakeEvictor{}
	exclude := make(map[string]bool)
	for i := 0; i < 30; i++ {
		exclude[fmt.Sprintf("edge-3-client-%d", i)] = true
	}
	cfg := RebalanceConfig{ImbalanceThreshold: 0.20, MaxEvictionsPerNode: 100, ExcludeClientIDs: exclude}

	result, err := Rebalance(context.Background(), sessions, live, cfg, evictor, false, silentLogger())
	if err != nil {
		t.Fatalf("Rebalance: %v", err)
	}
	if len(result.Nodes) != 0 {
		t.Fatalf("expected no node results (nothing evictable), got %+v", result.Nodes)
	}
	if len(evictor.calls) != 0 {
		t.Fatalf("expected no Evict calls, got %d", len(evictor.calls))
	}
}
