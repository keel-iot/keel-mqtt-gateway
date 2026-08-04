package raft

import (
	"fmt"
	"strings"
	"testing"
)

// fakeOfflineOwnerRegistry is an in-memory stand-in for routing.Router,
// exercised through the exact same Subscribe/Unsubscribe/NodesFor calls
// OfflineOwnership makes — no MQTT wildcard matching, since every key this
// package generates is a single opaque, non-wildcard level.
type fakeOfflineOwnerRegistry struct {
	byTopic  map[string][]string
	subErr   error
	unsubErr error
}

func newFakeOfflineOwnerRegistry() *fakeOfflineOwnerRegistry {
	return &fakeOfflineOwnerRegistry{byTopic: make(map[string][]string)}
}

func (f *fakeOfflineOwnerRegistry) Subscribe(topic, nodeID string) error {
	if f.subErr != nil {
		return f.subErr
	}
	for _, n := range f.byTopic[topic] {
		if n == nodeID {
			return nil
		}
	}
	f.byTopic[topic] = append(f.byTopic[topic], nodeID)
	return nil
}

func (f *fakeOfflineOwnerRegistry) Unsubscribe(topic, nodeID string) error {
	if f.unsubErr != nil {
		return f.unsubErr
	}
	nodes := f.byTopic[topic]
	out := nodes[:0]
	for _, n := range nodes {
		if n != nodeID {
			out = append(out, n)
		}
	}
	f.byTopic[topic] = out
	return nil
}

func (f *fakeOfflineOwnerRegistry) NodesFor(topic, _ string) []string {
	return f.byTopic[topic]
}

func TestOfflineOwnership_CurrentOwner_NoneYet(t *testing.T) {
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}
	if _, ok := o.CurrentOwner("device-1", "telemetry/#"); ok {
		t.Fatalf("expected no current owner before any Place")
	}
}

func TestOfflineOwnership_PlaceThenCurrentOwner(t *testing.T) {
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}

	if err := o.Place("device-1", "telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}
	owner, ok := o.CurrentOwner("device-1", "telemetry/#")
	if !ok || owner != "edge-1" {
		t.Fatalf("expected edge-1, got %q ok=%v", owner, ok)
	}
}

func TestOfflineOwnership_PlaceMovesOwnership_RemovesOldRegistration(t *testing.T) {
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}

	if err := o.Place("device-1", "telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if err := o.Place("device-1", "telemetry/#", "edge-2"); err != nil {
		t.Fatalf("Place: %v", err)
	}

	owner, ok := o.CurrentOwner("device-1", "telemetry/#")
	if !ok || owner != "edge-2" {
		t.Fatalf("expected edge-2 after move, got %q ok=%v", owner, ok)
	}
}

func TestOfflineOwnership_PlaceSameOwner_NoOp(t *testing.T) {
	reg := newFakeOfflineOwnerRegistry()
	o := &OfflineOwnership{Registry: reg}

	if err := o.Place("device-1", "telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if err := o.Place("device-1", "telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}

	owner, ok := o.CurrentOwner("device-1", "telemetry/#")
	if !ok || owner != "edge-1" {
		t.Fatalf("expected edge-1, got %q ok=%v", owner, ok)
	}
}

func TestOfflineOwnership_DifferentClientsSameFilter_NeverCollide(t *testing.T) {
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}

	if err := o.Place("device-1", "telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if err := o.Place("device-2", "telemetry/#", "edge-2"); err != nil {
		t.Fatalf("Place: %v", err)
	}

	owner1, _ := o.CurrentOwner("device-1", "telemetry/#")
	owner2, _ := o.CurrentOwner("device-2", "telemetry/#")
	if owner1 != "edge-1" || owner2 != "edge-2" {
		t.Fatalf("expected independent ownership, got device-1=%q device-2=%q", owner1, owner2)
	}
}

func TestOfflineOwnership_WildcardFilterSegments_NeverCrossMatch(t *testing.T) {
	// Same clientID, two filters where one's literal segment ("b") would
	// collide with the other's wildcard segment ("+") under real MQTT
	// topic matching — the whole reason the key is a single opaque base64
	// level rather than clientID+"/"+filter.
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}

	if err := o.Place("device-1", "a/b", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if err := o.Place("device-1", "a/+", "edge-2"); err != nil {
		t.Fatalf("Place: %v", err)
	}

	ownerB, _ := o.CurrentOwner("device-1", "a/b")
	ownerWildcard, _ := o.CurrentOwner("device-1", "a/+")
	if ownerB != "edge-1" || ownerWildcard != "edge-2" {
		t.Fatalf("expected no cross-match, got a/b=%q a/+=%q", ownerB, ownerWildcard)
	}
}

func TestOfflineOwnership_SubscribeFailure_PropagatesError(t *testing.T) {
	reg := newFakeOfflineOwnerRegistry()
	reg.subErr = fmt.Errorf("store unreachable")
	o := &OfflineOwnership{Registry: reg}

	if err := o.Place("device-1", "telemetry/#", "edge-1"); err == nil {
		t.Fatalf("expected Subscribe failure to propagate")
	}
}

func TestOfflineOwnership_Clear_RemovesRegistration(t *testing.T) {
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}

	if err := o.Place("device-1", "telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Place: %v", err)
	}
	if err := o.Clear("device-1", "telemetry/#"); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	if _, ok := o.CurrentOwner("device-1", "telemetry/#"); ok {
		t.Fatalf("expected no owner after Clear")
	}
}

func TestOfflineOwnership_Clear_NothingRegistered_NoOp(t *testing.T) {
	o := &OfflineOwnership{Registry: newFakeOfflineOwnerRegistry()}

	if err := o.Clear("device-1", "telemetry/#"); err != nil {
		t.Fatalf("expected no error clearing an unregistered (clientID, filter), got %v", err)
	}
}

func TestOfflineOwnerKey_DeterministicAndUnique(t *testing.T) {
	k1 := offlineOwnerKey("device-1", "telemetry/#")
	k2 := offlineOwnerKey("device-1", "telemetry/#")
	if k1 != k2 {
		t.Fatalf("expected deterministic key, got %q != %q", k1, k2)
	}
	k3 := offlineOwnerKey("device-2", "telemetry/#")
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
