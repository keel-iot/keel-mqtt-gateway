package session

import "testing"

func TestInventoryCacheSnapshotIsDefensive(t *testing.T) {
	cache := NewInventoryCache()
	sessions := []OfflineSession{{
		ClientID:      "device-1",
		Subscriptions: []OfflineSubscription{{Filter: "cmd/device-1", QoS: 1}},
	}}
	cache.Set(sessions)
	sessions[0].Subscriptions[0].Filter = "mutated"

	got, updatedAt, ready := cache.Snapshot()
	if !ready {
		t.Fatal("expected cache to be ready after Set")
	}
	if updatedAt.IsZero() {
		t.Fatal("expected cache update time")
	}
	if got[0].Subscriptions[0].Filter != "cmd/device-1" {
		t.Fatalf("cache retained caller mutation: %+v", got)
	}

	got[0].Subscriptions[0].Filter = "mutated again"
	again, _, _ := cache.Snapshot()
	if again[0].Subscriptions[0].Filter != "cmd/device-1" {
		t.Fatalf("snapshot exposed cache storage: %+v", again)
	}
}
