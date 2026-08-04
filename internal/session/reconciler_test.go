package session_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

// fakeOwnership is an in-memory client_id:filter -> nodeID map, standing
// in for whatever registry (routing.Router in a later phase) actually
// tracks topic-filter ownership.
type fakeOwnership struct {
	mu         sync.Mutex
	owner      map[string]string // "clientID:filter" -> nodeID
	placeCalls []placeCall
	placeErr   error
}

type placeCall struct {
	clientID, filter, newOwner string
}

func newFakeOwnership() *fakeOwnership {
	return &fakeOwnership{owner: make(map[string]string)}
}

func (f *fakeOwnership) key(clientID, filter string) string { return clientID + ":" + filter }

func (f *fakeOwnership) currentOwner(clientID, filter string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	nodeID, ok := f.owner[f.key(clientID, filter)]
	return nodeID, ok
}

func (f *fakeOwnership) place(clientID, filter, newOwner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.placeErr != nil {
		return f.placeErr
	}
	f.owner[f.key(clientID, filter)] = newOwner
	f.placeCalls = append(f.placeCalls, placeCall{clientID, filter, newOwner})
	return nil
}

func (f *fakeOwnership) calls() []placeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]placeCall, len(f.placeCalls))
	copy(out, f.placeCalls)
	return out
}

func testSession(clientID string, filters ...string) session.OfflineSession {
	s := session.OfflineSession{ClientID: clientID}
	for _, f := range filters {
		s.Subscriptions = append(s.Subscriptions, session.OfflineSubscription{Filter: f, QoS: 1})
	}
	return s
}

func TestReconciler_NoLiveEdges_SkipsPass(t *testing.T) {
	own := newFakeOwnership()
	r := &session.Reconciler{
		Inventory: func() ([]session.OfflineSession, error) {
			return []session.OfflineSession{testSession("device-1", "a/#")}, nil
		},
		LiveEdgeNodeIDs: func() []string { return nil },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
	}
	r.ReconcileOnce()
	if len(own.calls()) != 0 {
		t.Fatalf("expected no Place calls with zero live edges, got %v", own.calls())
	}
}

func TestReconciler_InventoryError_SkipsPass(t *testing.T) {
	own := newFakeOwnership()
	r := &session.Reconciler{
		Inventory:       func() ([]session.OfflineSession, error) { return nil, fmt.Errorf("redis unreachable") },
		LiveEdgeNodeIDs: func() []string { return []string{"edge-1"} },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
	}
	r.ReconcileOnce()
	if len(own.calls()) != 0 {
		t.Fatalf("expected no Place calls when inventory fetch fails, got %v", own.calls())
	}
}

func TestReconciler_NewSession_PlacesOwnerOnce(t *testing.T) {
	own := newFakeOwnership()
	edges := []string{"edge-1", "edge-2", "edge-3"}
	r := &session.Reconciler{
		Inventory: func() ([]session.OfflineSession, error) {
			return []session.OfflineSession{testSession("device-1", "a/#", "b/#")}, nil
		},
		LiveEdgeNodeIDs: func() []string { return edges },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
	}
	r.ReconcileOnce()

	calls := own.calls()
	if len(calls) != 2 {
		t.Fatalf("expected exactly 2 Place calls (one per filter), got %d: %+v", len(calls), calls)
	}
	wantOwnerA, _ := session.Owner("device-1", edges)
	for _, c := range calls {
		if c.clientID != "device-1" {
			t.Fatalf("unexpected clientID in call %+v", c)
		}
		if c.newOwner != wantOwnerA {
			t.Fatalf("Place owner = %q, want %q (must match session.Owner's own computation)", c.newOwner, wantOwnerA)
		}
	}
}

func TestReconciler_UnchangedOwner_DoesNotCallPlace(t *testing.T) {
	own := newFakeOwnership()
	edges := []string{"edge-1", "edge-2", "edge-3"}
	owner, _ := session.Owner("device-1", edges)
	own.owner[own.key("device-1", "a/#")] = owner // pre-seed as already correct

	r := &session.Reconciler{
		Inventory: func() ([]session.OfflineSession, error) {
			return []session.OfflineSession{testSession("device-1", "a/#")}, nil
		},
		LiveEdgeNodeIDs: func() []string { return edges },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
	}
	r.ReconcileOnce()

	if len(own.calls()) != 0 {
		t.Fatalf("expected no Place calls when owner is already correct, got %v", own.calls())
	}
}

func TestReconciler_MembershipChange_MovesOnlyAffectedSessions(t *testing.T) {
	own := newFakeOwnership()
	fullEdges := []string{"edge-1", "edge-2", "edge-3", "edge-4", "edge-5"}

	sessions := make([]session.OfflineSession, 0, 50)
	for i := 0; i < 50; i++ {
		clientID := fmt.Sprintf("device-%d", i)
		sessions = append(sessions, testSession(clientID, "topic/"+clientID))
		owner, _ := session.Owner(clientID, fullEdges)
		own.owner[own.key(clientID, "topic/"+clientID)] = owner
	}

	// edge-5 leaves — only sessions it owned should move.
	remainingEdges := []string{"edge-1", "edge-2", "edge-3", "edge-4"}
	r := &session.Reconciler{
		Inventory:       func() ([]session.OfflineSession, error) { return sessions, nil },
		LiveEdgeNodeIDs: func() []string { return remainingEdges },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
	}
	r.ReconcileOnce()

	movedCount := 0
	for _, s := range sessions {
		before, _ := session.Owner(s.ClientID, fullEdges)
		if before == "edge-5" {
			movedCount++
		}
	}

	calls := own.calls()
	if len(calls) != movedCount {
		t.Fatalf("expected exactly %d Place calls (sessions previously owned by the removed node), got %d", movedCount, len(calls))
	}
	for _, c := range calls {
		if c.newOwner == "edge-5" {
			t.Fatalf("edge-5 was removed but got assigned as new owner: %+v", c)
		}
	}
}

func TestReconciler_PlaceError_ContinuesWithOtherSessions(t *testing.T) {
	own := newFakeOwnership()
	own.placeErr = fmt.Errorf("simulated write failure")
	edges := []string{"edge-1", "edge-2"}

	r := &session.Reconciler{
		Inventory: func() ([]session.OfflineSession, error) {
			return []session.OfflineSession{testSession("device-1", "a/#")}, nil
		},
		LiveEdgeNodeIDs: func() []string { return edges },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
	}
	r.ReconcileOnce() // must not panic despite Place always failing
	if len(own.calls()) != 0 {
		t.Fatalf("expected no successful Place calls recorded, got %v", own.calls())
	}
}

func TestReconciler_Run_StopsOnContextCancel(t *testing.T) {
	own := newFakeOwnership()
	r := &session.Reconciler{
		Inventory:       func() ([]session.OfflineSession, error) { return nil, nil },
		LiveEdgeNodeIDs: func() []string { return []string{"edge-1"} },
		CurrentOwner:    own.currentOwner,
		Place:           own.place,
		Interval:        5 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Reconciler.Run did not return after context cancellation")
	}
}
