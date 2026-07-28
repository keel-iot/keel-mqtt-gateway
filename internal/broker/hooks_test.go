package broker

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
)

// fakeRegistry is a minimal keelraft.Registry stand-in used to exercise
// OnACLCheck's RBAC wiring and claimClusterSession's cluster-session
// wiring without pulling in raft/gRPC machinery.
type fakeRegistry struct {
	decision acl.Decision
	lastCall struct {
		clientID, username, topic string
		action                    acl.Action
	}

	// claimFn overrides ClaimSession's behavior when set; nil means
	// "claim succeeds, no previous owner" (the common case other tests
	// in this file rely on).
	claimFn func(clientID, nodeID string) (string, error)

	mu           sync.Mutex
	releaseCalls []releaseCall
}

type releaseCall struct {
	clientID, nodeID string
}

func (f *fakeRegistry) Subscribe(topic, nodeID string) error   { return nil }
func (f *fakeRegistry) Unsubscribe(topic, nodeID string) error { return nil }
func (f *fakeRegistry) NodesFor(topic string) []string         { return nil }
func (f *fakeRegistry) ClaimSession(clientID, nodeID string) (string, error) {
	if f.claimFn != nil {
		return f.claimFn(clientID, nodeID)
	}
	return "", nil
}
func (f *fakeRegistry) ReleaseSession(clientID, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls = append(f.releaseCalls, releaseCall{clientID, nodeID})
	return nil
}
func (f *fakeRegistry) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	f.lastCall.clientID, f.lastCall.username, f.lastCall.topic, f.lastCall.action = clientID, username, topic, action
	return f.decision
}

// fakeForwarder is a minimal dataplane.Forwarder stand-in that records
// Evict calls — used to verify claimClusterSession tells the previous
// owner to disconnect its local client, instead of silently allowing two
// nodes to both believe they own the same client_id.
type fakeForwarder struct {
	mu         sync.Mutex
	evictCalls []evictCall
	evictErr   error
}

type evictCall struct {
	targetNodeID, clientID string
}

func (f *fakeForwarder) Forward(ctx context.Context, targetNodeID string, msg *dataplane.Message) error {
	return nil
}
func (f *fakeForwarder) Subscribe(handler func(*dataplane.Message)) error { return nil }
func (f *fakeForwarder) Evict(ctx context.Context, targetNodeID, clientID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictCalls = append(f.evictCalls, evictCall{targetNodeID, clientID})
	return f.evictErr
}
func (f *fakeForwarder) SubscribeEvict(handler func(string)) error { return nil }

// calls returns a snapshot of recorded Evict calls.
func (f *fakeForwarder) calls() []evictCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]evictCall, len(f.evictCalls))
	copy(out, f.evictCalls)
	return out
}

// waitForCalls polls until f has recorded at least n Evict calls or
// timeout elapses — claimClusterSession fires Evict from a goroutine by
// design (best-effort, must not block the new connection), so tests
// observing it must poll rather than assert immediately.
func waitForCalls(t *testing.T, f *fakeForwarder, n int, timeout time.Duration) []evictCall {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if calls := f.calls(); len(calls) >= n {
			return calls
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d Evict call(s), got %d", n, len(f.calls()))
	return nil
}

func newTestHook(reg *fakeRegistry) *keelHook {
	h := &keelHook{
		log:     slog.Default(),
		clients: map[string]*clientState{},
	}
	if reg != nil {
		h.clusterRegistry = reg
	}
	return h
}

func deviceState(username string) *clientState {
	return &clientState{
		info: &auth.DeviceInfo{
			ID:         uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
			TenantID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			TenantSlug: "acme",
			FleetIDStr: "nofleet",
		},
		method:   auth.DetectAuthMethod([]byte("token")),
		username: username,
	}
}

// TestOnACLCheckRBACExplicitDenyWins verifies that when RBAC produces an
// explicit decision (Rule != nil), it is authoritative even if the legacy
// hardcoded ACL logic would have allowed the same topic.
func TestOnACLCheckRBACExplicitDenyWins(t *testing.T) {
	reg := &fakeRegistry{decision: acl.Decision{Effect: acl.EffectDeny, Rule: &acl.ACLRule{}}}
	h := newTestHook(reg)
	cl := &mqtt.Client{ID: "device-1"}
	h.clients[cl.ID] = deviceState("device-1@11111111-1111-1111-1111-111111111111")

	// "telemetry" alone is allowed by the legacy hardcoded logic (see
	// isAllowedPublish) — an explicit RBAC deny must override that.
	if h.OnACLCheck(cl, "telemetry", true) {
		t.Fatalf("expected explicit RBAC deny to override legacy allow")
	}
}

// TestOnACLCheckRBACExplicitAllowWins verifies an explicit RBAC allow is
// honored even for a topic the legacy hardcoded logic would reject.
func TestOnACLCheckRBACExplicitAllowWins(t *testing.T) {
	reg := &fakeRegistry{decision: acl.Decision{Effect: acl.EffectAllow, Rule: &acl.ACLRule{}}}
	h := newTestHook(reg)
	cl := &mqtt.Client{ID: "device-1"}
	h.clients[cl.ID] = deviceState("device-1@11111111-1111-1111-1111-111111111111")

	if !h.OnACLCheck(cl, "not-a-legacy-allowed-topic", true) {
		t.Fatalf("expected explicit RBAC allow to override legacy deny")
	}
}

// TestOnACLCheckRBACNoOpinionFallsThrough verifies that when RBAC has no
// matching rule at all (Rule == nil, the fail-closed default), OnACLCheck
// falls through to the existing hardcoded ACL logic unchanged.
func TestOnACLCheckRBACNoOpinionFallsThrough(t *testing.T) {
	reg := &fakeRegistry{decision: acl.Decision{Effect: acl.EffectDeny, Rule: nil}}
	h := newTestHook(reg)
	cl := &mqtt.Client{ID: "device-1"}
	h.clients[cl.ID] = deviceState("device-1@11111111-1111-1111-1111-111111111111")

	// "telemetry" is allowed by legacy isAllowedPublish; RBAC abstains
	// (nil Rule), so the legacy allow should still apply.
	if !h.OnACLCheck(cl, "telemetry", true) {
		t.Fatalf("expected fallthrough to legacy allow when RBAC has no matching rule")
	}

	// A topic the legacy logic denies should still be denied.
	if h.OnACLCheck(cl, "not-a-legacy-allowed-topic", true) {
		t.Fatalf("expected fallthrough to legacy deny when RBAC has no matching rule")
	}
}

// TestOnACLCheckNoClusterRegistryUsesLegacyOnly verifies standalone mode
// (clusterRegistry == nil) never touches RBAC and behaves exactly as
// before.
func TestOnACLCheckNoClusterRegistryUsesLegacyOnly(t *testing.T) {
	h := newTestHook(nil)
	cl := &mqtt.Client{ID: "device-1"}
	h.clients[cl.ID] = deviceState("device-1@11111111-1111-1111-1111-111111111111")

	if !h.OnACLCheck(cl, "telemetry", true) {
		t.Fatalf("expected legacy allow for telemetry publish in standalone mode")
	}
	if h.OnACLCheck(cl, "not-a-legacy-allowed-topic", true) {
		t.Fatalf("expected legacy deny for unknown topic in standalone mode")
	}
}

// TestOnACLCheckTestConsumerRoleUnaffected verifies the TEMPORARY
// test-consumer role still works when RBAC abstains (no roles/rulesets
// configured for it yet — see rbac-migration).
func TestOnACLCheckTestConsumerRoleUnaffected(t *testing.T) {
	reg := &fakeRegistry{decision: acl.Decision{Effect: acl.EffectDeny, Rule: nil}}
	h := newTestHook(reg)
	cl := &mqtt.Client{ID: "consumer-1"}
	h.clients[cl.ID] = &clientState{
		info:     testConsumerDeviceInfo(),
		method:   authMethodTestConsumer,
		username: testConsumerUsername,
	}

	if !h.OnACLCheck(cl, "telemetry/#", false) {
		t.Fatalf("expected test-consumer to still be allowed to subscribe to telemetry/#")
	}
	if h.OnACLCheck(cl, "telemetry/#", true) {
		t.Fatalf("expected test-consumer publish to still be denied")
	}
}

// TestOnACLCheckUnknownClientDenied verifies a client with no clientState
// (not authenticated / already disconnected) is always denied, RBAC or
// not.
func TestOnACLCheckUnknownClientDenied(t *testing.T) {
	h := newTestHook(&fakeRegistry{decision: acl.Decision{Effect: acl.EffectAllow, Rule: &acl.ACLRule{}}})
	cl := &mqtt.Client{ID: "ghost"}

	if h.OnACLCheck(cl, "telemetry", true) {
		t.Fatalf("expected deny for client with no tracked state")
	}
}

// ── claimClusterSession: session-ownership wiring ───────────────────────
//
// These are the tests for the priority question/fix from the core/edge
// isolation follow-up task: nothing previously prevented the same
// client_id from being accepted by two different nodes simultaneously
// (ClaimSession/ReleaseSession were built and unit-tested in isolation
// but never called from the live connect/disconnect path). These verify
// the wiring added to fix that, not just the FSM logic in isolation
// (already covered by raft.TestFSMClaimSessionOverride).

func newClusterTestHook(reg *fakeRegistry, fwd *fakeForwarder, selfNodeID string) *keelHook {
	h := newTestHook(reg)
	h.clusterFwd = fwd
	h.clusterNodeID = selfNodeID
	return h
}

// TestClaimClusterSession_NoPreviousOwner covers the common case: first
// connection for a client_id, nothing to evict.
func TestClaimClusterSession_NoPreviousOwner(t *testing.T) {
	fwd := &fakeForwarder{}
	h := newClusterTestHook(&fakeRegistry{}, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}

	if !h.claimClusterSession("device-1", "tenant-1") {
		t.Fatalf("expected claim to succeed")
	}
	// Give any stray goroutine a moment, then assert nothing was evicted.
	time.Sleep(5 * time.Millisecond)
	if calls := fwd.calls(); len(calls) != 0 {
		t.Fatalf("expected no Evict calls, got %v", calls)
	}
}

// TestClaimClusterSession_EvictsPreviousOwnerOnDifferentNode is the core
// "not silent double acceptance" test: when ClaimSession reports the
// session was taken over from a different node, that node must be told
// to evict its local client — this is what makes "new connection wins"
// an actual takeover instead of two nodes both believing they own the
// client_id.
func TestClaimClusterSession_EvictsPreviousOwnerOnDifferentNode(t *testing.T) {
	fwd := &fakeForwarder{}
	reg := &fakeRegistry{claimFn: func(clientID, nodeID string) (string, error) {
		return "edge-2", nil // "device-1" was previously owned by edge-2
	}}
	h := newClusterTestHook(reg, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}

	if !h.claimClusterSession("device-1", "tenant-1") {
		t.Fatalf("expected claim to succeed")
	}

	calls := waitForCalls(t, fwd, 1, time.Second)
	if calls[0].targetNodeID != "edge-2" || calls[0].clientID != "device-1" {
		t.Fatalf("expected Evict(edge-2, device-1), got %+v", calls[0])
	}
}

// TestClaimClusterSession_NoEvictWhenOwnerIsSelf guards against a
// spurious self-eviction if ClaimSession ever reported the caller's own
// node ID as the "previous" owner (e.g. a reconnect racing its own prior
// disconnect on the same node).
func TestClaimClusterSession_NoEvictWhenOwnerIsSelf(t *testing.T) {
	fwd := &fakeForwarder{}
	reg := &fakeRegistry{claimFn: func(clientID, nodeID string) (string, error) {
		return nodeID, nil
	}}
	h := newClusterTestHook(reg, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}

	if !h.claimClusterSession("device-1", "tenant-1") {
		t.Fatalf("expected claim to succeed")
	}
	time.Sleep(5 * time.Millisecond)
	if calls := fwd.calls(); len(calls) != 0 {
		t.Fatalf("expected no self-eviction, got %v", calls)
	}
}

// TestClaimClusterSession_RejectsConnectionOnError verifies the
// fail-closed choice: a ClaimSession error (cluster unreachable/leader
// unknown) rejects the connection and rolls back the local bookkeeping
// already applied, rather than silently admitting a connection with no
// enforced cross-node exclusivity.
func TestClaimClusterSession_RejectsConnectionOnError(t *testing.T) {
	fwd := &fakeForwarder{}
	reg := &fakeRegistry{claimFn: func(clientID, nodeID string) (string, error) {
		return "", context.DeadlineExceeded
	}}
	h := newClusterTestHook(reg, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}
	h.tenantConns = map[string]int{"tenant-1": 1}

	if h.claimClusterSession("device-1", "tenant-1") {
		t.Fatalf("expected claim to be rejected")
	}
	if _, ok := h.clients["device-1"]; ok {
		t.Fatalf("expected local client bookkeeping to be rolled back")
	}
	if h.tenantConns["tenant-1"] != 0 {
		t.Fatalf("expected tenant connection count rolled back, got %d", h.tenantConns["tenant-1"])
	}
	time.Sleep(5 * time.Millisecond)
	if calls := fwd.calls(); len(calls) != 0 {
		t.Fatalf("expected no Evict call on a rejected claim, got %v", calls)
	}
}

// TestClaimClusterSession_StandaloneNoop verifies standalone mode
// (clusterRegistry nil) never blocks a connection on cluster wiring.
func TestClaimClusterSession_StandaloneNoop(t *testing.T) {
	h := newTestHook(nil)
	if !h.claimClusterSession("device-1", "tenant-1") {
		t.Fatalf("expected standalone claim to be a no-op success")
	}
}

// TestOnDisconnect_ReleasesClusterSession verifies the disconnect side of
// the fix: a clean local disconnect releases this node's cluster session
// ownership so a future ClaimSession elsewhere isn't fighting a stale
// entry — the other half of "not silently leaving both nodes believing
// they own the client_id."
func TestOnDisconnect_ReleasesClusterSession(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	h.OnDisconnect(cl, nil, false)

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.releaseCalls) != 1 {
		t.Fatalf("expected exactly 1 ReleaseSession call, got %d", len(reg.releaseCalls))
	}
	if reg.releaseCalls[0].clientID != "device-1" || reg.releaseCalls[0].nodeID != "edge-1" {
		t.Fatalf("expected ReleaseSession(device-1, edge-1), got %+v", reg.releaseCalls[0])
	}
}

// TestOnDisconnect_StaleGenerationSkipsRelease verifies a disconnect from
// a superseded (lower-generation) connection does not release the
// *newer* local connection's session ownership.
func TestOnDisconnect_StaleGenerationSkipsRelease(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{"device-1": 2} // a newer connection already took over
	state := deviceState("dev-1")
	state.generation = 1 // this disconnect belongs to the older connection
	h.clients["device-1"] = state

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	h.OnDisconnect(cl, nil, false)

	if len(reg.releaseCalls) != 0 {
		t.Fatalf("expected no ReleaseSession call for a stale-generation disconnect, got %+v", reg.releaseCalls)
	}
}
