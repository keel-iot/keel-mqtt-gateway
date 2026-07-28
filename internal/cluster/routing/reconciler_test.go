package routing

import (
	"net"
	"sync"
	"testing"

	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

// newTestServerWithSubscriber builds a real mochi-mqtt Server with one
// client directly injected as subscribed to each of filters — bypassing the
// real CONNECT/SUBSCRIBE protocol entirely (net.Pipe, no listener), so this
// only exercises LocalSubscriptions/Reconciler's own logic, not mochi-mqtt's
// subscribe handling.
func newTestServerWithSubscriber(t *testing.T, clientID string, filters ...string) *mochimqtt.Server {
	t.Helper()
	srv := mochimqtt.New(nil)

	conn, _ := net.Pipe()
	t.Cleanup(func() { _ = conn.Close() })

	cl := srv.NewClient(conn, "test", clientID, false)
	for _, f := range filters {
		cl.State.Subscriptions.Add(f, packets.Subscription{Filter: f})
	}
	srv.Clients.Add(cl)
	return srv
}

// fakeRegistry is a minimal in-memory registry stand-in satisfying the
// package-private `registry` interface, for testing Reconciler without a
// real store.ClusterStore/Olric.
type fakeRegistry struct {
	mu             sync.Mutex
	subscribeCalls []string
	topicsForNode  map[string][]string
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{topicsForNode: make(map[string][]string)}
}

func (f *fakeRegistry) Subscribe(topic, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribeCalls = append(f.subscribeCalls, topic)
	f.topicsForNode[nodeID] = append(f.topicsForNode[nodeID], topic)
	return nil
}

func (f *fakeRegistry) TopicsForNode(nodeID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.topicsForNode[nodeID]...)
}

func TestReconciler_NoOpWhenStoreAlreadyMatches(t *testing.T) {
	srv := newTestServerWithSubscriber(t, "client-1", "telemetry/device-1/#")
	reg := newFakeRegistry()
	reg.topicsForNode["node-a"] = []string{"telemetry/device-1/#"}

	r := &Reconciler{Server: srv, Registry: reg, NodeID: "node-a"}
	r.reconcileOnce()

	require.Empty(t, reg.subscribeCalls, "already-consistent state must not trigger any re-assert")
}

func TestReconciler_ReassertsAfterStoreReset(t *testing.T) {
	srv := newTestServerWithSubscriber(t, "client-1", "telemetry/device-1/#", "command/device-1")
	reg := newFakeRegistry() // empty TopicsForNode — simulates a fully-wiped store

	r := &Reconciler{Server: srv, Registry: reg, NodeID: "node-a"}
	r.reconcileOnce()

	require.ElementsMatch(t, []string{"telemetry/device-1/#", "command/device-1"}, reg.subscribeCalls,
		"every locally-live filter missing from the store must be re-asserted")
}

func TestReconciler_NoOpWhenNoLocalClients(t *testing.T) {
	srv := mochimqtt.New(nil)
	reg := newFakeRegistry()

	r := &Reconciler{Server: srv, Registry: reg, NodeID: "node-a"}
	r.reconcileOnce()

	require.Empty(t, reg.subscribeCalls, "nothing to reconcile when no client is locally connected")
}

func TestReconciler_PartialMismatchOnlyReassertsMissing(t *testing.T) {
	srv := newTestServerWithSubscriber(t, "client-1", "t/1", "t/2")
	reg := newFakeRegistry()
	reg.topicsForNode["node-a"] = []string{"t/1"} // t/2 missing

	r := &Reconciler{Server: srv, Registry: reg, NodeID: "node-a"}
	r.reconcileOnce()

	require.Equal(t, []string{"t/2"}, reg.subscribeCalls)
}

func TestLocalSubscriptions_UnionsAcrossClients(t *testing.T) {
	srv := mochimqtt.New(nil)
	for i, clientID := range []string{"c1", "c2"} {
		conn, _ := net.Pipe()
		t.Cleanup(func() { _ = conn.Close() })
		cl := srv.NewClient(conn, "test", clientID, false)
		cl.State.Subscriptions.Add("shared/topic", packets.Subscription{Filter: "shared/topic"})
		cl.State.Subscriptions.Add("only/"+clientID, packets.Subscription{Filter: "only/" + clientID})
		srv.Clients.Add(cl)
		_ = i
	}

	got := LocalSubscriptions(srv)
	require.ElementsMatch(t, []string{"shared/topic", "only/c1", "only/c2"}, got)
}
