package broker

import (
	"context"
	"net"
	"reflect"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
)

type plannedRegistry struct {
	*fakeRegistry
	plan routing.LivePlan
}

func (r *plannedRegistry) LiveDeliveryPlan(_, _ string) routing.LivePlan { return r.plan }

func TestLiveSharedRoutingCarriesOneDecisionToEachTarget(t *testing.T) {
	const a = "$share/a/telemetry/#"
	const b = "$share/b/telemetry/#"
	reg := &plannedRegistry{fakeRegistry: &fakeRegistry{}, plan: routing.LivePlan{
		Nodes:       []string{"self", "edge-b", "ordinary-only"},
		SharedNodes: map[string]string{a: "self", b: "edge-b"},
	}}
	fwd := &fakeForwarder{}
	h := newClusterTestHook(nil, fwd, "self")
	h.clusterRegistry = reg
	local := h.forwardToClusterSubscribers(context.Background(), &auth.DeviceInfo{}, packets.Packet{
		TopicName: "telemetry/device", Payload: []byte("x"), FixedHeader: packets.FixedHeader{Qos: 2},
	})
	if !reflect.DeepEqual(local, []string{a}) {
		t.Fatalf("local groups=%#v", local)
	}
	if len(fwd.forwardCalls) != 2 {
		t.Fatalf("forwards=%v", fwd.forwardCalls)
	}
	for _, call := range fwd.forwardCalls {
		want := []string{}
		if call.targetNodeID == "edge-b" {
			want = []string{b}
		}
		if !reflect.DeepEqual(call.msg.SharedFilters, want) {
			t.Fatalf("target %s: groups=%#v, want %#v", call.targetNodeID, call.msg.SharedFilters, want)
		}
	}
}

func TestLiveSharedRoutingFiltersOnlyTheAuthorizedGroups(t *testing.T) {
	const a = "$share/a/telemetry/#"
	const b = "$share/b/telemetry/#"
	server := mqtt.New(nil)
	h := &keelHook{server: server, clusterRegistry: &fakeRegistry{}}
	for _, id := range []string{"client-a", "client-b"} {
		conn, peer := net.Pipe()
		t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
		server.Clients.Add(server.NewClient(conn, "tcp", id, false))
	}
	for _, tc := range []struct {
		name    string
		filters []string
		want    int
	}{
		{"legacy", nil, 2},
		{"ordinary-only", []string{}, 0},
		{"group-a", []string{a}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subs := &mqtt.Subscribers{
				Shared: map[string]map[string]packets.Subscription{
					a: {"client-a": {Filter: a}}, b: {"client-b": {Filter: b}},
				},
				Subscriptions: map[string]packets.Subscription{"ordinary": {Filter: "telemetry/#"}},
			}
			got := h.OnSelectSubscribers(subs, packets.Packet{SharedFilters: tc.filters})
			if len(got.Shared) != tc.want || len(got.Subscriptions) != 1 {
				t.Fatalf("shared=%v ordinary=%v", got.Shared, got.Subscriptions)
			}
			if tc.name == "group-a" {
				if _, ok := got.Shared[a]; !ok {
					t.Fatal("authorized group removed")
				}
			}
		})
	}
}
