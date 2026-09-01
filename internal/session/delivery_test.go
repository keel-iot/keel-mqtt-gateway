package session_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

// fakeInflightStore is an in-memory stand-in for the Redis-backed
// packet-ID allocator + inflight hash a later phase wires this up to.
type fakeInflightStore struct {
	mu       sync.Mutex
	nextID   map[string]uint16
	enqueued []enqueuedMsg
	queueErr error
}

type enqueuedMsg struct {
	clientID string
	packetID uint16
	msg      session.InflightMessage
}

func newFakeInflightStore() *fakeInflightStore {
	return &fakeInflightStore{nextID: make(map[string]uint16)}
}

func (f *fakeInflightStore) queue(clientID string, msg session.InflightMessage) (uint16, bool, error) {
	if f.queueErr != nil {
		return 0, false, f.queueErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID[clientID]++
	packetID := f.nextID[clientID]
	f.enqueued = append(f.enqueued, enqueuedMsg{clientID, packetID, msg})
	return packetID, true, nil
}

func (f *fakeInflightStore) all() []enqueuedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]enqueuedMsg, len(f.enqueued))
	copy(out, f.enqueued)
	return out
}

func testSubSession(clientID string, subs ...session.OfflineSubscription) session.OfflineSession {
	return session.OfflineSession{ClientID: clientID, Subscriptions: subs}
}

func TestDeliver_QoS0Publish_NeverQueued(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 2}),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("23.5"), 0)

	if delivered != 0 || len(store.all()) != 0 {
		t.Fatalf("expected nothing queued for a QoS0 publish, got delivered=%d calls=%v", delivered, store.all())
	}
}

func TestDeliver_QoS1Publish_QoS1Subscription_QueuedAtQoS1(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("23.5"), 1)

	calls := store.all()
	if delivered != 1 || len(calls) != 1 {
		t.Fatalf("expected exactly 1 delivery, got delivered=%d calls=%v", delivered, calls)
	}
	if calls[0].msg.QoS != 1 {
		t.Fatalf("expected QoS 1, got %d", calls[0].msg.QoS)
	}
}

func TestDeliver_QoS2Publish_QoS1Subscription_DowngradedToQoS1(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
	}
	d.Deliver(owned, "telemetry/temp", []byte("x"), 2)

	calls := store.all()
	if len(calls) != 1 || calls[0].msg.QoS != 1 {
		t.Fatalf("expected downgrade to QoS 1 (min(2,1)), got %+v", calls)
	}
}

func TestDeliver_QoS1Publish_QoS2Subscription_DowngradedToQoS1(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 2}),
	}
	d.Deliver(owned, "telemetry/temp", []byte("x"), 1)

	calls := store.all()
	if len(calls) != 1 || calls[0].msg.QoS != 1 {
		t.Fatalf("expected downgrade to QoS 1 (min(1,2)), got %+v", calls)
	}
}

func TestDeliver_OverlappingFilters_QueuedOnceAtHighestEffectiveQoS(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1",
			session.OfflineSubscription{Filter: "telemetry/#", QoS: 1},
			session.OfflineSubscription{Filter: "telemetry/temp", QoS: 2},
		),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("x"), 2)

	calls := store.all()
	if delivered != 1 || len(calls) != 1 {
		t.Fatalf("expected exactly 1 enqueue for overlapping filters on the same session, got delivered=%d calls=%+v", delivered, calls)
	}
	if calls[0].msg.QoS != 2 {
		t.Fatalf("expected the higher effective QoS (2) across matching filters, got %d", calls[0].msg.QoS)
	}
}

func TestDeliver_NoMatchingFilter_NotQueued(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "other/#", QoS: 1}),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("x"), 1)

	if delivered != 0 || len(store.all()) != 0 {
		t.Fatalf("expected no match, got delivered=%d calls=%v", delivered, store.all())
	}
}

func TestDeliver_MultipleSessions_OnlyMatchingOnesQueued(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
		testSubSession("device-2", session.OfflineSubscription{Filter: "other/#", QoS: 1}),
		testSubSession("device-3", session.OfflineSubscription{Filter: "telemetry/temp", QoS: 1}),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("x"), 1)

	calls := store.all()
	if delivered != 2 || len(calls) != 2 {
		t.Fatalf("expected exactly 2 deliveries (device-1, device-3), got delivered=%d calls=%+v", delivered, calls)
	}
	clients := map[string]bool{calls[0].clientID: true, calls[1].clientID: true}
	if !clients["device-1"] || !clients["device-3"] {
		t.Fatalf("expected device-1 and device-3 to receive the message, got %+v", calls)
	}
}

func TestDeliver_QueueFailure_SkipsSessionContinuesOthers(t *testing.T) {
	store := newFakeInflightStore()
	store.queueErr = fmt.Errorf("redis unreachable")
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("x"), 1)

	if delivered != 0 || len(store.all()) != 0 {
		t.Fatalf("expected no successful delivery when Queue fails, got delivered=%d calls=%v", delivered, store.all())
	}
}

func TestDeliver_QueueFailure_ContinuesOthers(t *testing.T) {
	store := newFakeInflightStore()
	store.queueErr = fmt.Errorf("redis write failed")
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
		testSubSession("device-2", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
	}
	delivered := d.Deliver(owned, "telemetry/temp", []byte("x"), 1)

	if delivered != 0 {
		t.Fatalf("expected 0 successful deliveries when Queue always fails, got %d", delivered)
	}
}

func TestDeliver_PayloadAndTopicPreserved(t *testing.T) {
	store := newFakeInflightStore()
	d := &session.OfflineDelivery{Queue: store.queue}

	owned := []session.OfflineSession{
		testSubSession("device-1", session.OfflineSubscription{Filter: "telemetry/#", QoS: 1}),
	}
	d.Deliver(owned, "telemetry/temp", []byte("23.5C"), 1)

	calls := store.all()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %+v", calls)
	}
	if calls[0].msg.Topic != "telemetry/temp" || string(calls[0].msg.Payload) != "23.5C" {
		t.Fatalf("topic/payload not preserved: %+v", calls[0].msg)
	}
	if calls[0].packetID != 1 {
		t.Fatalf("expected first packet ID to be 1, got %d", calls[0].packetID)
	}
}
