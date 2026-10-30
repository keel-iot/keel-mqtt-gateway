package broker

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

type candidateRegistry struct {
	clients []string
	shared  map[string]string
	queries int
}

func (r *candidateRegistry) OwnedClientIDs(string) []string {
	panic("optimized delivery must not enumerate all owned clients")
}
func (r *candidateRegistry) OfflineDeliveryCandidates(string, string) ([]string, map[string]string) {
	r.queries++
	return r.clients, r.shared
}

type candidateStore struct {
	*fakeOfflineDeliveryStoreImpl
	fetched []string
	queries int
}

func (s *candidateStore) OwnedOfflineSessions(ids []string) ([]session.OfflineSession, error) {
	s.queries++
	s.fetched = append(s.fetched, ids...)
	return s.fakeOfflineDeliveryStoreImpl.OwnedOfflineSessions(ids)
}

func TestDeliverOffline_ReadsOnlyCandidates(t *testing.T) {
	reg := &candidateRegistry{clients: []string{"target"}}
	store := &candidateStore{fakeOfflineDeliveryStoreImpl: newFakeOfflineDeliveryStore()}
	store.sessions["target"] = session.OfflineSession{ClientID: "target", Subscriptions: []session.OfflineSubscription{
		{Filter: "devices/#", QoS: 1}, {Filter: "devices/+/state", QoS: 2},
	}}
	store.sessions["unrelated"] = session.OfflineSession{ClientID: "unrelated", Subscriptions: []session.OfflineSubscription{{Filter: "commands/#", QoS: 1}}}
	DeliverOffline(context.Background(), reg, store, "edge", uuid.New(), "devices/a/state", []byte("x"), 2, time.Minute, nil)
	if !reflect.DeepEqual(store.fetched, []string{"target"}) || !reflect.DeepEqual(store.enqueued, []string{"target"}) {
		t.Fatalf("fetched=%v enqueued=%v", store.fetched, store.enqueued)
	}
}

func TestDeliverOffline_NoCandidatesOrQoSZero_NoStoreReads(t *testing.T) {
	for _, qos := range []byte{0, 1} {
		reg := &candidateRegistry{}
		store := &candidateStore{fakeOfflineDeliveryStoreImpl: newFakeOfflineDeliveryStore()}
		DeliverOffline(context.Background(), reg, store, "edge", uuid.New(), "devices/a/state", nil, qos, time.Minute, nil)
		if store.queries != 0 || (qos == 0 && reg.queries != 0) {
			t.Fatalf("qos=%d: store queries=%d routing queries=%d", qos, store.queries, reg.queries)
		}
	}
}

func TestDeliverOffline_CandidatesPreserveSharedDecisions(t *testing.T) {
	const filter = "$share/group/devices/#"
	reg := &candidateRegistry{clients: []string{"chosen", "ordinary"}, shared: map[string]string{filter: "chosen"}}
	store := &candidateStore{fakeOfflineDeliveryStoreImpl: newFakeOfflineDeliveryStore()}
	store.sessions["chosen"] = session.OfflineSession{ClientID: "chosen", Subscriptions: []session.OfflineSubscription{{Filter: filter, QoS: 1}}}
	// This client also matches an ordinary subscription. Its shared membership
	// must not cause a duplicate when the exact cluster decision chose another.
	store.sessions["ordinary"] = session.OfflineSession{ClientID: "ordinary", Subscriptions: []session.OfflineSubscription{
		{Filter: filter, QoS: 1}, {Filter: "devices/#", QoS: 1},
	}}
	publishID := uuid.New()
	for i := 0; i < 2; i++ {
		DeliverOffline(context.Background(), reg, store, "edge", publishID, "devices/a/state", nil, 1, time.Minute, nil)
	}
	if !reflect.DeepEqual(store.enqueued, []string{"chosen", "ordinary"}) {
		t.Fatalf("shared/ordinary overlap or dedup changed: %v", store.enqueued)
	}
}

func TestDeliverOffline_RechecksPersistedSubscriptions(t *testing.T) {
	reg := &candidateRegistry{clients: []string{"changed"}}
	store := &candidateStore{fakeOfflineDeliveryStoreImpl: newFakeOfflineDeliveryStore()}
	store.sessions["changed"] = session.OfflineSession{ClientID: "changed", Subscriptions: []session.OfflineSubscription{{Filter: "commands/#", QoS: 1}}}
	DeliverOffline(context.Background(), reg, store, "edge", uuid.New(), "devices/a/state", nil, 1, time.Minute, nil)
	if store.queries != 1 || len(store.enqueued) != 0 {
		t.Fatalf("stale routing bypassed persisted subscription check: reads=%d enqueued=%v", store.queries, store.enqueued)
	}
}

func TestDeliverOffline_EmptySharedDecisionDoesNotSelectLocally(t *testing.T) {
	reg := &candidateRegistry{clients: []string{"client"}}
	store := &candidateStore{fakeOfflineDeliveryStoreImpl: newFakeOfflineDeliveryStore()}
	store.sessions["client"] = session.OfflineSession{ClientID: "client", Subscriptions: []session.OfflineSubscription{{Filter: "$share/group/devices/#", QoS: 1}}}
	DeliverOffline(context.Background(), reg, store, "edge", uuid.New(), "devices/a/state", nil, 1, time.Minute, nil)
	if len(store.enqueued) != 0 {
		t.Fatalf("empty cluster decision caused local shared delivery: %v", store.enqueued)
	}
}
