package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

type fakeOfflineDeliveryRegistry struct {
	owned map[string][]string // nodeID -> clientIDs
}

func (f *fakeOfflineDeliveryRegistry) OwnedClientIDs(nodeID string) []string {
	return f.owned[nodeID]
}

type fakeOfflineDeliveryStoreImpl struct {
	sessions   map[string]session.OfflineSession // clientID -> session
	delivered  map[string]bool                   // "publishID:clientID" -> marked
	nextPacket uint16
	enqueued   []string // clientIDs actually enqueued
	nextIDErr  error
	enqueueErr error
}

func newFakeOfflineDeliveryStore() *fakeOfflineDeliveryStoreImpl {
	return &fakeOfflineDeliveryStoreImpl{
		sessions:  make(map[string]session.OfflineSession),
		delivered: make(map[string]bool),
	}
}

func (f *fakeOfflineDeliveryStoreImpl) OwnedOfflineSessions(ownedClientIDs []string) ([]session.OfflineSession, error) {
	out := make([]session.OfflineSession, 0, len(ownedClientIDs))
	for _, id := range ownedClientIDs {
		if s, ok := f.sessions[id]; ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeOfflineDeliveryStoreImpl) NextPacketID(_ context.Context, _ string) (uint16, error) {
	if f.nextIDErr != nil {
		return 0, f.nextIDErr
	}
	f.nextPacket++
	return f.nextPacket, nil
}

func (f *fakeOfflineDeliveryStoreImpl) EnqueueOfflineInflight(_ context.Context, clientID string, _ uint16, _ session.InflightMessage) error {
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.enqueued = append(f.enqueued, clientID)
	return nil
}

func (f *fakeOfflineDeliveryStoreImpl) MarkDelivered(_ context.Context, publishID uuid.UUID, clientID string, _ time.Duration) (bool, error) {
	key := publishID.String() + ":" + clientID
	if f.delivered[key] {
		return false, nil
	}
	f.delivered[key] = true
	return true, nil
}

func TestDeliverOffline_NoOwnedClients_NoOp(t *testing.T) {
	reg := &fakeOfflineDeliveryRegistry{owned: map[string][]string{}}
	store := newFakeOfflineDeliveryStore()

	DeliverOffline(context.Background(), reg, store, "edge-1", uuid.New(), "telemetry/device-1", []byte("x"), 1, time.Minute, nil)

	if len(store.enqueued) != 0 {
		t.Fatalf("expected no enqueue, got %v", store.enqueued)
	}
}

func TestDeliverOffline_MatchingOwnedSession_Enqueues(t *testing.T) {
	reg := &fakeOfflineDeliveryRegistry{owned: map[string][]string{"edge-1": {"device-1"}}}
	store := newFakeOfflineDeliveryStore()
	store.sessions["device-1"] = session.OfflineSession{
		ClientID:      "device-1",
		Subscriptions: []session.OfflineSubscription{{Filter: "telemetry/#", QoS: 1}},
	}

	DeliverOffline(context.Background(), reg, store, "edge-1", uuid.New(), "telemetry/device-1", []byte("23.5"), 1, time.Minute, nil)

	if len(store.enqueued) != 1 || store.enqueued[0] != "device-1" {
		t.Fatalf("expected device-1 enqueued once, got %v", store.enqueued)
	}
}

func TestDeliverOffline_DuplicatePublishID_EnqueuedOnlyOnce(t *testing.T) {
	reg := &fakeOfflineDeliveryRegistry{owned: map[string][]string{"edge-1": {"device-1"}}}
	store := newFakeOfflineDeliveryStore()
	store.sessions["device-1"] = session.OfflineSession{
		ClientID:      "device-1",
		Subscriptions: []session.OfflineSubscription{{Filter: "telemetry/#", QoS: 1}},
	}
	publishID := uuid.New()

	DeliverOffline(context.Background(), reg, store, "edge-1", publishID, "telemetry/device-1", []byte("x"), 1, time.Minute, nil)
	DeliverOffline(context.Background(), reg, store, "edge-1", publishID, "telemetry/device-1", []byte("x"), 1, time.Minute, nil)

	if len(store.enqueued) != 1 {
		t.Fatalf("expected exactly 1 enqueue across both calls (same publishID), got %d: %v", len(store.enqueued), store.enqueued)
	}
}

func TestDeliverOffline_DifferentPublishIDs_BothEnqueued(t *testing.T) {
	reg := &fakeOfflineDeliveryRegistry{owned: map[string][]string{"edge-1": {"device-1"}}}
	store := newFakeOfflineDeliveryStore()
	store.sessions["device-1"] = session.OfflineSession{
		ClientID:      "device-1",
		Subscriptions: []session.OfflineSubscription{{Filter: "telemetry/#", QoS: 1}},
	}

	DeliverOffline(context.Background(), reg, store, "edge-1", uuid.New(), "telemetry/device-1", []byte("x"), 1, time.Minute, nil)
	DeliverOffline(context.Background(), reg, store, "edge-1", uuid.New(), "telemetry/device-1", []byte("y"), 1, time.Minute, nil)

	if len(store.enqueued) != 2 {
		t.Fatalf("expected 2 enqueues (two distinct publish events), got %d: %v", len(store.enqueued), store.enqueued)
	}
}

func TestDeliverOffline_NilRegistryOrStore_NoOpNoPanic(t *testing.T) {
	reg := &fakeOfflineDeliveryRegistry{owned: map[string][]string{"edge-1": {"device-1"}}}
	store := newFakeOfflineDeliveryStore()

	DeliverOffline(context.Background(), nil, store, "edge-1", uuid.New(), "t", nil, 1, time.Minute, nil)
	DeliverOffline(context.Background(), reg, nil, "edge-1", uuid.New(), "t", nil, 1, time.Minute, nil)
}

func TestDeliverOffline_OwnedOfflineSessionsError_NoPanic(t *testing.T) {
	reg := &fakeOfflineDeliveryRegistry{owned: map[string][]string{"edge-1": {"device-1"}}}
	store := &erroringOfflineDeliveryStore{err: fmt.Errorf("redis unreachable")}

	DeliverOffline(context.Background(), reg, store, "edge-1", uuid.New(), "t", nil, 1, time.Minute, nil)
}

type erroringOfflineDeliveryStore struct {
	err error
}

func (e *erroringOfflineDeliveryStore) OwnedOfflineSessions(_ []string) ([]session.OfflineSession, error) {
	return nil, e.err
}
func (e *erroringOfflineDeliveryStore) NextPacketID(_ context.Context, _ string) (uint16, error) {
	return 0, nil
}
func (e *erroringOfflineDeliveryStore) EnqueueOfflineInflight(_ context.Context, _ string, _ uint16, _ session.InflightMessage) error {
	return nil
}
func (e *erroringOfflineDeliveryStore) MarkDelivered(_ context.Context, _ uuid.UUID, _ string, _ time.Duration) (bool, error) {
	return true, nil
}
