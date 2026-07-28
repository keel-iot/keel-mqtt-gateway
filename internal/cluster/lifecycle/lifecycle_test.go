package lifecycle

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ── Monitor.PurgeNode wiring ─────────────────────────────────────────────

func TestMonitorCallsPurgeNodeOnceBeyondThreshold(t *testing.T) {
	var purged []string
	m := NewMonitor(func() []membership.NodeMeta { return nil }, time.Minute, silentLogger())
	m.PurgeNode = func(nodeID string) error {
		purged = append(purged, nodeID)
		return nil
	}
	// Simulate: core-1 was seen long ago and is now long gone from gossip.
	m.lastSeen["core-1"] = time.Now().Add(-time.Hour)

	m.tick()
	m.tick() // already warned — must not purge (or log) a second time

	if len(purged) != 1 || purged[0] != "core-1" {
		t.Fatalf("expected PurgeNode called exactly once with core-1, got %v", purged)
	}
}

func TestMonitorPurgeNodeErrorDoesNotPanic(t *testing.T) {
	m := NewMonitor(func() []membership.NodeMeta { return nil }, time.Minute, silentLogger())
	called := false
	m.PurgeNode = func(nodeID string) error {
		called = true
		return errors.New("boom")
	}
	m.lastSeen["core-1"] = time.Now().Add(-time.Hour)

	m.tick()

	if !called {
		t.Fatalf("expected PurgeNode to be invoked despite it returning an error")
	}
}

func TestMonitorNilPurgeNodePreservesLogOnlyBehaviour(t *testing.T) {
	m := NewMonitor(func() []membership.NodeMeta { return nil }, time.Minute, silentLogger())
	m.lastSeen["core-1"] = time.Now().Add(-time.Hour)

	m.tick() // PurgeNode left nil (zero value) — must not panic
}

// ── RoutingSweep ──────────────────────────────────────────────────────────

func TestRoutingSweepFlagsNodeAbsentBeyondThreshold(t *testing.T) {
	holding := []string{"core-1", "core-dead"}
	live := []membership.NodeMeta{{NodeID: "core-1", Role: membership.RoleCore}}

	s := NewRoutingSweep(
		func() []string { return holding },
		func() []membership.NodeMeta { return live },
		time.Minute,
		silentLogger(),
	)

	s.tick() // first observation of core-dead's absence — must not flag yet
	if s.flagged["core-dead"] {
		t.Fatalf("must not flag on first observation, before threshold has elapsed")
	}
	if s.flagged["core-1"] {
		t.Fatalf("core-1 is live and holding routes — must never be flagged")
	}

	// Simulate the threshold having elapsed since first observation.
	s.absentSince["core-dead"] = time.Now().Add(-2 * time.Hour)
	s.tick()

	if !s.flagged["core-dead"] {
		t.Fatalf("expected core-dead to be flagged after exceeding threshold")
	}
	if s.flagged["core-1"] {
		t.Fatalf("core-1 is live, must never be flagged")
	}
	// Never deletes anything — NodesWithRoutes (holding) is untouched by
	// the sweep, only its own internal bookkeeping mutates.
	if len(holding) != 2 {
		t.Fatalf("sweep must never mutate the routing table itself, got holding=%v", holding)
	}
}

func TestRoutingSweepClearsBookkeepingWhenNodeHasNoRoutesLeft(t *testing.T) {
	holding := []string{"core-dead"}
	s := NewRoutingSweep(
		func() []string { return holding },
		func() []membership.NodeMeta { return nil },
		time.Minute,
		silentLogger(),
	)
	s.absentSince["core-dead"] = time.Now().Add(-2 * time.Hour)
	s.tick()
	if !s.flagged["core-dead"] {
		t.Fatalf("expected core-dead flagged before its routes are removed")
	}

	// Node no longer holds any routing entries (e.g. purged elsewhere) —
	// bookkeeping must clear even though the loop keyed on `holding`
	// no longer visits it.
	holding = nil
	s.tick()

	if s.flagged["core-dead"] {
		t.Fatalf("expected flag cleared once the node holds no routes")
	}
	if len(s.absentSince) != 0 {
		t.Fatalf("expected absence tracking cleared, got %v", s.absentSince)
	}
}

func TestRoutingSweepClearsFlagWhenNodeReappearsInGossip(t *testing.T) {
	holding := []string{"core-1"}
	live := []membership.NodeMeta{}
	s := NewRoutingSweep(
		func() []string { return holding },
		func() []membership.NodeMeta { return live },
		time.Minute,
		silentLogger(),
	)
	s.absentSince["core-1"] = time.Now().Add(-2 * time.Hour)
	s.tick()
	if !s.flagged["core-1"] {
		t.Fatalf("expected core-1 flagged while absent from gossip")
	}

	live = []membership.NodeMeta{{NodeID: "core-1", Role: membership.RoleCore}}
	s.tick()

	if s.flagged["core-1"] {
		t.Fatalf("expected flag cleared once the node is visible in gossip again")
	}
}
