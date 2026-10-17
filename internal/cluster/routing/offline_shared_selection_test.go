package routing

import (
	"reflect"
	"testing"
)

func TestOfflineSharedClientsFor_ExactClusterSelection(t *testing.T) {
	r := newTestRouter(t)
	const topic = "kimera/devices/device-1/metrics"
	const liveFilter = "$share/live/kimera/devices/#"
	const offlineFilter = "$share/offline/kimera/devices/#"
	r.mu.Lock()
	r.addLocalLocked(liveFilter, "edge-live")
	r.addLocalLocked(OwnershipKey("stale-live-member", liveFilter), "edge-a")
	r.addLocalLocked(OwnershipKey("offline-z", offlineFilter), "edge-a")
	r.addLocalLocked(OwnershipKey("offline-b", offlineFilter), "edge-a")
	r.addLocalLocked(OwnershipKey("offline-a", offlineFilter), "edge-b")
	// A regular offline route on edge-b must not authorize that edge to
	// deliver a second copy to its offline shared member.
	r.addLocalLocked(OfflineRouteKey("kimera/devices/#"), "edge-b")
	r.mu.Unlock()

	for _, nodeID := range []string{"edge-a", "edge-b", "edge-live"} {
		want := map[string]string{}
		if nodeID == "edge-a" {
			want[offlineFilter] = "offline-b"
		}
		if got := r.OfflineSharedClientsFor(topic, nodeID); !reflect.DeepEqual(got, want) {
			t.Fatalf("node %s: selection=%v, want %v", nodeID, got, want)
		}
	}
	if got := r.OfflineSharedClientsFor("unrelated/topic", "edge-a"); len(got) != 0 {
		t.Fatalf("non-matching topic selected %v", got)
	}
	// Ownership handoff must move the exact selection, not leave the old
	// node/client eligible through an add-only offline route hint.
	r.mu.Lock()
	r.removeLocalLocked(OwnershipKey("offline-b", offlineFilter), "edge-a")
	r.removeLocalLocked(OwnershipKey("offline-z", offlineFilter), "edge-a")
	r.mu.Unlock()
	if got := r.OfflineSharedClientsFor(topic, "edge-a"); len(got) != 0 {
		t.Fatalf("old owner still selected: %v", got)
	}
	want := map[string]string{offlineFilter: "offline-a"}
	if got := r.OfflineSharedClientsFor(topic, "edge-b"); !reflect.DeepEqual(got, want) {
		t.Fatalf("new owner selection=%v, want %v", got, want)
	}
}
