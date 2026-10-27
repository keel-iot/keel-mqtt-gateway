package routing

import (
	"reflect"
	"sort"
	"testing"
)

func TestOfflineDeliveryCandidates_MatchesExactOwnership(t *testing.T) {
	r := newTestRouter(t)
	r.mu.Lock()
	for _, entry := range []struct{ client, filter, node string }{
		{"exact", "devices/a/state", "edge-a"},
		{"wild", "devices/+/state", "edge-a"},
		{"wild", "devices/#", "edge-a"}, // overlapping filters fetch once
		{"other-topic", "commands/#", "edge-a"},
		{"other-node", "devices/#", "edge-b"},
		{"dollar", "$SYS/#", "edge-a"},
		{"all", "#", "edge-a"},
		{"unselected", "$share/group/devices/#", "edge-b"},
		{"selected", "$share/group/devices/#", "edge-a"},
		{"live-suppressed", "$share/live/devices/#", "edge-a"},
	} {
		r.addLocalLocked(OwnershipKey(entry.client, entry.filter), entry.node)
	}
	r.addLocalLocked("$share/live/devices/#", "edge-live")
	// Add-only route hints alone cannot make a client a delivery candidate.
	r.addLocalLocked(OfflineRouteKey("devices/#"), "edge-a")
	r.mu.Unlock()

	got, shared := r.OfflineDeliveryCandidates("devices/a/state", "edge-a")
	sort.Strings(got)
	want := []string{"all", "exact", "selected", "wild"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clients=%v, want %v", got, want)
	}
	if !reflect.DeepEqual(shared, map[string]string{"$share/group/devices/#": "selected"}) {
		t.Fatalf("shared selection=%v", shared)
	}
	got, _ = r.OfflineDeliveryCandidates("$SYS/load", "edge-a")
	if !reflect.DeepEqual(got, []string{"dollar"}) {
		t.Fatalf("$SYS matched ordinary wildcard: %v", got)
	}
	got, _ = r.OfflineDeliveryCandidates("devices/a/state", "missing-node")
	if len(got) != 0 {
		t.Fatalf("missing node returned %v", got)
	}

	// Exact ownership removal/handoff takes effect without refreshing Redis.
	r.mu.Lock()
	r.removeLocalLocked(OwnershipKey("selected", "$share/group/devices/#"), "edge-a")
	r.removeLocalLocked(OwnershipKey("exact", "devices/a/state"), "edge-a")
	r.mu.Unlock()
	got, shared = r.OfflineDeliveryCandidates("devices/a/state", "edge-a")
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"all", "wild"}) || len(shared) != 0 {
		t.Fatalf("stale ownership selected clients=%v shared=%v", got, shared)
	}
	got, shared = r.OfflineDeliveryCandidates("devices/a/state", "edge-b")
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"other-node", "unselected"}) || shared["$share/group/devices/#"] != "unselected" {
		t.Fatalf("new shared owner: clients=%v shared=%v", got, shared)
	}
}
