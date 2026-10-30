package broker

import (
	"fmt"
	"net"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// Exercise the actual Mochi publish path: the selection hook must run even
// when the topic has ordinary subscriptions only.
type ordinaryOfflineSelectionHook struct {
	mqtt.HookBase
	keel *keelHook
}

func (*ordinaryOfflineSelectionHook) ID() string { return "ordinary-offline-selection-test" }
func (*ordinaryOfflineSelectionHook) Provides(b byte) bool {
	return b == mqtt.OnSelectSubscribers || b == mqtt.OnACLCheck
}
func (*ordinaryOfflineSelectionHook) OnACLCheck(*mqtt.Client, string, bool) bool { return true }
func (h *ordinaryOfflineSelectionHook) OnSelectSubscribers(subs *mqtt.Subscribers, pk packets.Packet) *mqtt.Subscribers {
	return h.keel.OnSelectSubscribers(subs, pk)
}

func TestOrdinaryOfflineClusterHasOneEnqueueOwner(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, externalStore := range []bool{false, true} {
			for _, qos := range []byte{1, 2} {
				t.Run(fmt.Sprintf("cluster=%t/store=%t/qos=%d", cluster, externalStore, qos), func(t *testing.T) {
					server := mqtt.New(&mqtt.Options{InlineClient: true})
					h := &keelHook{server: server}
					if cluster {
						h.clusterRegistry = &fakeRegistry{}
					}
					if externalStore {
						h.offlineDeliveryStore = newFakeOfflineDeliveryStore()
					}
					if err := server.AddHook(&ordinaryOfflineSelectionHook{keel: h}, nil); err != nil {
						t.Fatal(err)
					}
					offline := server.NewClient(nil, "tcp", "offline", false)
					offline.Properties.Clean = false
					offline.Stop(nil)
					server.Clients.Add(offline)
					server.Topics.Subscribe(offline.ID, packets.Subscription{Filter: "ordinary/#", Qos: qos})
					if err := server.Publish("ordinary/topic", []byte("one-message"), false, qos); err != nil {
						t.Fatal(err)
					}
					want := 1
					if cluster && externalStore {
						want = 0
					}
					if got := offline.State.Inflight.LenOutbound(); got != want {
						t.Fatalf("local offline copies=%d, want %d", got, want)
					}
				})
			}
		}
	}
}

func TestOrdinaryOfflineSelectionKeepsLiveAndInlineClients(t *testing.T) {
	server := mqtt.New(nil)
	h := &keelHook{server: server, clusterRegistry: &fakeRegistry{}, offlineDeliveryStore: newFakeOfflineDeliveryStore()}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	live := server.NewClient(conn, "tcp", "live", false)
	inline := server.NewClient(nil, "inline", "inline", true)
	offline := server.NewClient(nil, "tcp", "offline", false)
	offline.Stop(nil)
	for _, cl := range []*mqtt.Client{live, inline, offline} {
		server.Clients.Add(cl)
	}
	subs := &mqtt.Subscribers{Subscriptions: map[string]packets.Subscription{
		"live": {Filter: "a/#"}, "inline": {Filter: "a/#"}, "offline": {Filter: "a/#"}, "missing": {Filter: "a/#"},
	}}
	got := h.OnSelectSubscribers(subs, packets.Packet{})
	if len(got.Subscriptions) != 2 {
		t.Fatalf("unexpected selections: %v", got.Subscriptions)
	}
	for _, id := range []string{"live", "inline"} {
		if _, ok := got.Subscriptions[id]; !ok {
			t.Fatalf("live/inline client %s removed", id)
		}
	}
}
