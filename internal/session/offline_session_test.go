package session_test

import (
	"testing"

	"github.com/mochi-mqtt/server/v2/hooks/storage"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

func TestFromStorage_FiltersToOneClient(t *testing.T) {
	subs := []storage.Subscription{
		{Client: "device-1", Filter: "telemetry/#", Qos: 1},
		{Client: "device-1", Filter: "cmd/device-1", Qos: 2},
		{Client: "device-2", Filter: "telemetry/#", Qos: 0},
	}

	got := session.FromStorage("device-1", subs)

	if got.ClientID != "device-1" {
		t.Fatalf("ClientID = %q, want device-1", got.ClientID)
	}
	if len(got.Subscriptions) != 2 {
		t.Fatalf("expected 2 subscriptions for device-1, got %d: %+v", len(got.Subscriptions), got.Subscriptions)
	}
	want := map[string]byte{"telemetry/#": 1, "cmd/device-1": 2}
	for _, s := range got.Subscriptions {
		if qos, ok := want[s.Filter]; !ok || qos != s.QoS {
			t.Fatalf("unexpected subscription %+v, want one of %v", s, want)
		}
	}
}

func TestFromStorage_NoMatchingSubscriptions(t *testing.T) {
	subs := []storage.Subscription{
		{Client: "device-2", Filter: "telemetry/#", Qos: 0},
	}

	got := session.FromStorage("device-1", subs)

	if got.ClientID != "device-1" {
		t.Fatalf("ClientID = %q, want device-1", got.ClientID)
	}
	if len(got.Subscriptions) != 0 {
		t.Fatalf("expected no subscriptions, got %+v", got.Subscriptions)
	}
}

func TestAllFromStorage_OnePerClient(t *testing.T) {
	clients := []storage.Client{
		{ID: "device-1"},
		{ID: "device-2"},
		{ID: "device-3"}, // no subscriptions at all — must still appear, empty
	}
	subs := []storage.Subscription{
		{Client: "device-1", Filter: "telemetry/#", Qos: 1},
		{Client: "device-1", Filter: "cmd/device-1", Qos: 2},
		{Client: "device-2", Filter: "telemetry/#", Qos: 0},
	}

	got := session.AllFromStorage(clients, subs)

	if len(got) != 3 {
		t.Fatalf("expected 3 offline sessions (one per client), got %d", len(got))
	}

	byID := make(map[string]session.OfflineSession, len(got))
	for _, s := range got {
		byID[s.ClientID] = s
	}

	if len(byID["device-1"].Subscriptions) != 2 {
		t.Fatalf("device-1: expected 2 subscriptions, got %+v", byID["device-1"].Subscriptions)
	}
	if len(byID["device-2"].Subscriptions) != 1 {
		t.Fatalf("device-2: expected 1 subscription, got %+v", byID["device-2"].Subscriptions)
	}
	if len(byID["device-3"].Subscriptions) != 0 {
		t.Fatalf("device-3: expected 0 subscriptions, got %+v", byID["device-3"].Subscriptions)
	}
}

func TestAllFromStorage_EmptyInputs(t *testing.T) {
	got := session.AllFromStorage(nil, nil)
	if len(got) != 0 {
		t.Fatalf("expected no offline sessions from empty inputs, got %+v", got)
	}
}

func TestAllFromStorage_SubscriptionsForUnknownClientAreIgnored(t *testing.T) {
	clients := []storage.Client{{ID: "device-1"}}
	subs := []storage.Subscription{
		{Client: "device-1", Filter: "telemetry/#", Qos: 1},
		{Client: "device-ghost", Filter: "cmd/#", Qos: 1}, // no matching storage.Client row
	}

	got := session.AllFromStorage(clients, subs)

	if len(got) != 1 {
		t.Fatalf("expected 1 offline session (only device-1 has a Client row), got %d", len(got))
	}
	if len(got[0].Subscriptions) != 1 {
		t.Fatalf("expected device-1 to have exactly its own subscription, got %+v", got[0].Subscriptions)
	}
}

func TestFilterOffline_DropsLiveClaimedClients(t *testing.T) {
	sessions := []session.OfflineSession{
		{ClientID: "device-1"},
		{ClientID: "device-2"},
		{ClientID: "device-3"},
	}
	liveClaimed := map[string]string{"device-2": "edge-1"}

	got := session.FilterOffline(sessions, liveClaimed)

	if len(got) != 2 {
		t.Fatalf("expected 2 offline sessions (device-2 excluded), got %d: %+v", len(got), got)
	}
	for _, s := range got {
		if s.ClientID == "device-2" {
			t.Fatalf("device-2 is live-claimed, must not appear in the filtered result")
		}
	}
}

func TestFilterOffline_NoLiveClaims_ReturnsAllUnchanged(t *testing.T) {
	sessions := []session.OfflineSession{{ClientID: "device-1"}, {ClientID: "device-2"}}

	got := session.FilterOffline(sessions, nil)

	if len(got) != 2 {
		t.Fatalf("expected both sessions unchanged, got %+v", got)
	}
}

func TestFilterOffline_EveryClientLive_ReturnsEmptyNotNil(t *testing.T) {
	sessions := []session.OfflineSession{{ClientID: "device-1"}}
	liveClaimed := map[string]string{"device-1": "edge-1"}

	got := session.FilterOffline(sessions, liveClaimed)

	if len(got) != 0 {
		t.Fatalf("expected no offline sessions, got %+v", got)
	}
}
