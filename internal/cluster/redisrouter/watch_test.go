package redisrouter

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakePrimaryLookup lets a test change what WatchPrimary sees on each poll,
// safely under concurrent access (the watcher goroutine reads it while the
// test goroutine mutates it).
type fakePrimaryLookup struct {
	mu     sync.Mutex
	nodeID string
	ok     bool
	addrs  map[string]string
}

func (f *fakePrimaryLookup) set(nodeID string, addrs map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodeID, f.ok, f.addrs = nodeID, true, addrs
}

func (f *fakePrimaryLookup) currentPrimary() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nodeID, f.ok
}

func (f *fakePrimaryLookup) resolveAddr(nodeID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	addr, ok := f.addrs[nodeID]
	return addr, ok
}

func TestWatchPrimary_RedirectsOnChange(t *testing.T) {
	srv1 := startFakeRedisServer(t)
	srv2 := startFakeRedisServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := New(ctx, srv1.addr(), "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	lookup := &fakePrimaryLookup{}
	lookup.set("core-1", map[string]string{"core-1": srv1.addr()})

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go WatchPrimary(watchCtx, r, lookup.currentPrimary, lookup.resolveAddr, slog.Default())

	// Give the watcher a couple of ticks against the unchanged primary —
	// should stay a no-op (still pointed at srv1).
	time.Sleep(50 * time.Millisecond)
	if r.Addr() != srv1.addr() {
		t.Fatalf("expected router to remain at %s while primary is unchanged, got %s", srv1.addr(), r.Addr())
	}

	// Now the primary changes to core-2 (srv2) — override watchInterval by
	// waiting long enough for at least one real tick.
	lookup.set("core-2", map[string]string{"core-1": srv1.addr(), "core-2": srv2.addr()})

	deadline := time.Now().Add(watchInterval + 2*time.Second)
	for time.Now().Before(deadline) {
		if r.Addr() == srv2.addr() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r.Addr() != srv2.addr() {
		t.Fatalf("expected router to redirect to %s after primary change, got %s", srv2.addr(), r.Addr())
	}
}

func TestWatchPrimary_NoopWhenPrimaryUnknown(t *testing.T) {
	srv1 := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	r, err := New(ctx, srv1.addr(), "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	lookup := &fakePrimaryLookup{} // ok=false: no primary ever designated

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go WatchPrimary(watchCtx, r, lookup.currentPrimary, lookup.resolveAddr, slog.Default())

	time.Sleep(watchInterval + 200*time.Millisecond)
	if r.Addr() != srv1.addr() {
		t.Fatalf("expected router to remain at %s when no primary is designated, got %s", srv1.addr(), r.Addr())
	}
}
