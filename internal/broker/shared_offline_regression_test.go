package broker

import (
	"context"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

func TestSharedOfflineRegression_LiveGroupDoesNotAccumulateOfflineCopies(t *testing.T) {
	const topic = "kimera/devices/device-1/metrics"
	reg := &fakeRegistry{
		nodesFor:       map[string][]string{topic: {"edge-live"}},
		ownedClientIDs: map[string][]string{"edge-publisher": {"mqtt-bridge-old"}},
	}
	fwd := &fakeForwarder{}
	h := newClusterTestHook(reg, fwd, "edge-publisher")
	store := newFakeOfflineDeliveryStore()
	store.sessions["mqtt-bridge-old"] = session.OfflineSession{
		ClientID:      "mqtt-bridge-old",
		Subscriptions: []session.OfflineSubscription{{Filter: "$share/mqtt-bridge/kimera/devices/#", QoS: 1}},
	}
	h.offlineDeliveryStore = store
	for i := 0; i < 100; i++ {
		h.forwardToClusterSubscribers(context.Background(), &auth.DeviceInfo{}, packets.Packet{
			TopicName: topic, Payload: []byte("measurement"), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2},
		})
	}
	if len(fwd.forwardCalls) != 100 || len(store.enqueued) != 0 {
		t.Fatalf("100 publishes: live forwards=%d, extra offline copies=%d", len(fwd.forwardCalls), len(store.enqueued))
	}
	// The receiving node invokes the same dispatcher after live delivery.
	DeliverOffline(context.Background(), reg, store, "edge-publisher", uuid.New(), topic, []byte("measurement"), 2, time.Minute, nil)
	if len(store.enqueued) != 0 {
		t.Fatalf("inbound delivery accumulated offline copies: %v", store.enqueued)
	}
}

type selectedOfflineRegistry struct {
	*fakeRegistry
	selected map[string]map[string]string // node -> exact filter -> client
}

func (r *selectedOfflineRegistry) OfflineSharedClientsFor(_ string, nodeID string) map[string]string {
	return r.selected[nodeID]
}

func TestSharedOfflineRegression_OneOfflineMemberAcrossAllDispatchers(t *testing.T) {
	const liveFilter = "$share/live/kimera/devices/#"
	const offlineFilter = "$share/offline/kimera/devices/#"
	reg := &selectedOfflineRegistry{
		fakeRegistry: &fakeRegistry{ownedClientIDs: map[string][]string{
			"edge-a": {"old-live", "offline-a"},
			"edge-b": {"offline-b", "ordinary"},
		}},
		selected: map[string]map[string]string{"edge-a": {offlineFilter: "offline-a"}},
	}
	store := newFakeOfflineDeliveryStore()
	for clientID, filter := range map[string]string{
		"old-live": liveFilter, "offline-a": offlineFilter,
		"offline-b": offlineFilter, "ordinary": "kimera/devices/#",
	} {
		store.sessions[clientID] = session.OfflineSession{ClientID: clientID, Subscriptions: []session.OfflineSubscription{{Filter: filter, QoS: 1}}}
	}
	publishID := uuid.New()
	for _, nodeID := range []string{"edge-a", "edge-b"} {
		// Both nodes receive the event: one owns the selected shared member,
		// the other is a real target for an ordinary offline subscription.
		DeliverOffline(context.Background(), reg, store, nodeID, publishID, "kimera/devices/a", []byte("x"), 2, time.Minute, nil)
	}
	if want := []string{"offline-a", "ordinary"}; !reflect.DeepEqual(store.enqueued, want) {
		t.Fatalf("enqueued=%v, want %v", store.enqueued, want)
	}
	// A repeated forwarding attempt must remain deduplicated.
	DeliverOffline(context.Background(), reg, store, "edge-a", publishID, "kimera/devices/a", []byte("x"), 2, time.Minute, nil)
	if len(store.enqueued) != 2 {
		t.Fatalf("repeated forward enqueued a duplicate: %v", store.enqueued)
	}
}

func TestSharedOfflineRegression_LiveElectionExcludesOfflineSessions(t *testing.T) {
	server := mqtt.New(nil)
	h := &keelHook{server: server, clusterRegistry: &fakeRegistry{}}
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
	online := server.NewClient(conn, "tcp", "online", false)
	offline := server.NewClient(nil, "tcp", "offline", false)
	offline.Stop(nil)
	ghost := server.NewClient(nil, "tcp", "ghost", false)
	for _, cl := range []*mqtt.Client{online, offline, ghost} {
		server.Clients.Add(cl)
	}
	const filter = "$share/g/kimera/devices/#"
	subs := &mqtt.Subscribers{
		Shared: map[string]map[string]packets.Subscription{filter: {
			"online": {Filter: filter, Qos: 1}, "offline": {Filter: filter, Qos: 1},
			"ghost": {Filter: filter, Qos: 1}, "missing": {Filter: filter, Qos: 1},
		}},
		Subscriptions: map[string]packets.Subscription{"ordinary": {Filter: "kimera/devices/#", Qos: 1}},
	}
	got := h.OnSelectSubscribers(subs, packets.Packet{})
	got.SelectShared()
	if len(got.SharedSelected) != 1 {
		t.Fatalf("selected=%v, want only online", got.SharedSelected)
	}
	if _, ok := got.SharedSelected["online"]; !ok {
		t.Fatalf("selected offline member: %v", got.SharedSelected)
	}
	if len(got.Subscriptions) != 1 {
		t.Fatalf("ordinary subscription was altered: %v", got.Subscriptions)
	}
}

func TestSharedOfflineRegression_SameEdgeResumeLoadsRedisQueue(t *testing.T) {
	server := mqtt.New(nil)
	existing := server.NewClient(nil, "tcp", "bridge", false)
	existing.Stop(nil)
	existing.State.Inflight.Set(packets.Packet{PacketID: 7, TopicName: "existing", FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}})
	server.Clients.Add(existing)
	h := &keelHook{server: server, log: slog.Default(), sessionStore: &fakeSessionStore{inflight: []storage.Message{{
		Client: "bridge", PacketID: 42, TopicName: "kimera/devices/a", Payload: []byte("offline"),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
	}}}}
	reconnected := server.NewClient(nil, "tcp", "bridge", false)
	h.OnSessionEstablish(reconnected, packets.Packet{Connect: packets.ConnectParams{Clean: false}})
	if got, ok := existing.State.Inflight.Get(42); !ok || string(got.Payload) != "offline" {
		t.Fatalf("same-edge resume missed the Redis offline queue: packet=%+v found=%v", got, ok)
	}
	if _, ok := existing.State.Inflight.Get(7); !ok {
		t.Fatal("same-edge resume discarded existing inflight state")
	}
}
