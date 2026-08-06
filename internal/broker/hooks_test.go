package broker

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
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
	claimFn func(clientID, nodeID, identity string) (string, error)

	mu               sync.Mutex
	releaseCalls     []releaseCall
	unsubscribeCalls []unsubscribeCall

	// revokedIdentities, when non-nil, controls IsRevoked's answer for
	// specific identities; nil means "nothing is revoked" (the common
	// case other tests in this file rely on).
	revokedIdentities map[string]bool

	// nodesFor/offlineNodesFor, when non-nil, control NodesFor/
	// OfflineNodesFor's per-topic answer; a missing topic key returns nil,
	// same as the zero-value fake other tests in this file rely on.
	nodesFor        map[string][]string
	offlineNodesFor map[string][]string
	// ownedClientIDs, when non-nil, controls OwnedClientIDs' per-node
	// answer.
	ownedClientIDs map[string][]string
}

type releaseCall struct {
	clientID, nodeID string
}

type unsubscribeCall struct {
	topic, nodeID string
}

func (f *fakeRegistry) Subscribe(topic, nodeID string) error { return nil }
func (f *fakeRegistry) Unsubscribe(topic, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unsubscribeCalls = append(f.unsubscribeCalls, unsubscribeCall{topic, nodeID})
	return nil
}
func (f *fakeRegistry) NodesFor(topic, localNodeID string) []string { return f.nodesFor[topic] }
func (f *fakeRegistry) OfflineNodesFor(topic string) []string       { return f.offlineNodesFor[topic] }
func (f *fakeRegistry) OwnedClientIDs(nodeID string) []string       { return f.ownedClientIDs[nodeID] }
func (f *fakeRegistry) ClaimSession(clientID, nodeID, identity string) (string, error) {
	if f.claimFn != nil {
		return f.claimFn(clientID, nodeID, identity)
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
func (f *fakeRegistry) CurrentRedisPrimary() (string, bool) { return "", false }
func (f *fakeRegistry) IsRevoked(identity string) bool      { return f.revokedIdentities[identity] }

// fakeForwarder is a minimal dataplane.Forwarder stand-in that records
// Evict calls — used to verify claimClusterSession tells the previous
// owner to disconnect its local client, instead of silently allowing two
// nodes to both believe they own the same client_id.
type fakeForwarder struct {
	mu           sync.Mutex
	evictCalls   []evictCall
	evictErr     error
	forwardCalls []forwardCall
}

type evictCall struct {
	targetNodeID, clientID string
}

type forwardCall struct {
	targetNodeID string
	msg          *dataplane.Message
}

func (f *fakeForwarder) Forward(ctx context.Context, targetNodeID string, msg *dataplane.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forwardCalls = append(f.forwardCalls, forwardCall{targetNodeID, msg})
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

	if !h.claimClusterSession("device-1", "tenant-1", "") {
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
	reg := &fakeRegistry{claimFn: func(clientID, nodeID, identity string) (string, error) {
		return "edge-2", nil // "device-1" was previously owned by edge-2
	}}
	h := newClusterTestHook(reg, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}

	if !h.claimClusterSession("device-1", "tenant-1", "") {
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
	reg := &fakeRegistry{claimFn: func(clientID, nodeID, identity string) (string, error) {
		return nodeID, nil
	}}
	h := newClusterTestHook(reg, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}

	if !h.claimClusterSession("device-1", "tenant-1", "") {
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
	reg := &fakeRegistry{claimFn: func(clientID, nodeID, identity string) (string, error) {
		return "", context.DeadlineExceeded
	}}
	h := newClusterTestHook(reg, fwd, "edge-1")
	h.clients["device-1"] = &clientState{}
	h.tenantConns = map[string]int{"tenant-1": 1}

	if h.claimClusterSession("device-1", "tenant-1", "") {
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
	if !h.claimClusterSession("device-1", "tenant-1", "") {
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

// TestOnDisconnect_PersistentSessionKeepsRouting verifies a persistent
// (non-expiring) session's disconnect releases cluster session ownership
// but leaves its cluster-wide routing entries alone — clearing them broke
// cross-node delivery to a QoS1/2 offline subscriber immediately, before
// any node crash was even involved.
func TestOnDisconnect_PersistentSessionKeepsRouting(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnDisconnect(cl, nil, false) // expire=false: persistent session, not ending

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.unsubscribeCalls) != 0 {
		t.Fatalf("expected no Unsubscribe call for a persistent session disconnect, got %+v", reg.unsubscribeCalls)
	}
	if len(reg.releaseCalls) != 1 {
		t.Fatalf("expected cluster session ownership to still be released, got %d calls", len(reg.releaseCalls))
	}
}

// TestOnDisconnect_ExpiringSessionClearsRouting verifies a clean/expiring
// session's disconnect does clear its cluster-wide routing entries, since
// the session (and therefore the subscription) is genuinely gone.
func TestOnDisconnect_ExpiringSessionClearsRouting(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnDisconnect(cl, nil, true) // expire=true: clean session, truly ending

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.unsubscribeCalls) != 1 || reg.unsubscribeCalls[0].topic != "telemetry/tenant/device-1" {
		t.Fatalf("expected exactly 1 Unsubscribe call for telemetry/tenant/device-1, got %+v", reg.unsubscribeCalls)
	}
}

// TestOnDisconnect_PersistentSessionKeepsACLIdentity verifies a persistent
// session's disconnect leaves h.clients/h.generation alone — OnACLCheck
// must keep succeeding for that client_id while mochi-mqtt still delivers
// queued QoS1/2 messages to it offline. Deleting these unconditionally on
// disconnect made every such delivery fail-closed before ever reaching
// Inflight/Redis.
func TestOnDisconnect_PersistentSessionKeepsACLIdentity(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	// OnConnectAuthenticate always sets h.generation[cl.ID] on connect (see
	// hooks.go); pre-populate it here to match that real invariant instead
	// of relying on Go's zero-value-for-missing-key behavior, which would
	// make the "still present" assertion below pass for the wrong reason.
	h.generation = map[string]uint64{"device-1": 0}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	h.OnDisconnect(cl, nil, false) // expire=false: persistent session, not ending

	h.mu.RLock()
	_, stillPresent := h.clients["device-1"]
	_, genStillPresent := h.generation["device-1"]
	h.mu.RUnlock()
	if !stillPresent {
		t.Fatalf("expected h.clients entry to survive a persistent session's disconnect")
	}
	if !genStillPresent {
		t.Fatalf("expected h.generation entry to survive a persistent session's disconnect")
	}
}

// TestOnClientExpired_CleansUpACLIdentityAndRouting verifies the true
// end-of-life event (mochi-mqtt's own session-expiry sweep) finally tears
// down what OnDisconnect left alone for a persistent session: ACL identity,
// generation counter, and cluster routing entries.
func TestOnClientExpired_CleansUpACLIdentityAndRouting(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{"device-1": 3}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnClientExpired(cl)

	h.mu.RLock()
	_, clientsPresent := h.clients["device-1"]
	_, genPresent := h.generation["device-1"]
	h.mu.RUnlock()
	if clientsPresent {
		t.Fatalf("expected h.clients entry to be removed on true session expiry")
	}
	if genPresent {
		t.Fatalf("expected h.generation entry to be removed on true session expiry")
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.unsubscribeCalls) != 1 || reg.unsubscribeCalls[0].topic != "telemetry/tenant/device-1" {
		t.Fatalf("expected exactly 1 Unsubscribe call for telemetry/tenant/device-1, got %+v", reg.unsubscribeCalls)
	}
}

// TestOnClientExpired_AlreadyReconnectedIsNoop verifies that if the
// client_id was already overwritten by a newer connection's fresh state
// (reconnect before expiry), a stale OnClientExpired call for the old
// Client object does not clear the new connection's identity or routing.
func TestOnClientExpired_AlreadyReconnectedIsNoop(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients = map[string]*clientState{} // no entry: as if never registered / already cleaned up

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnClientExpired(cl)

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.unsubscribeCalls) != 0 {
		t.Fatalf("expected no Unsubscribe call when h.clients has no entry for client_id, got %+v", reg.unsubscribeCalls)
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

// TestOnDisconnect_EvictedPersistentSessionClearsRouting verifies that a
// disconnect caused by our own cluster-level Evict (cl.StopCause() ==
// ErrSessionTakenOver) tears down ACL identity and cluster routing even
// for a persistent session (expire == false) — the session has
// definitively moved to a different node, so there's no "genuinely still
// offline" ambiguity left to preserve, unlike an ordinary disconnect.
// Without this, a live client reconnecting to a different node while the
// old node stays up would leave a stale duplicate route (and a leaked ACL
// identity entry) behind on the old node until OnClientExpired's sweep
// eventually caught it.
func TestOnDisconnect_EvictedPersistentSessionClearsRouting(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{"device-1": 0}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})
	cl.Stop(packets.ErrSessionTakenOver) // mirrors cmd/server/main.go's SubscribeEvict handler

	h.OnDisconnect(cl, nil, false) // expire=false: persistent session per MQTT semantics

	h.mu.RLock()
	_, stillPresent := h.clients["device-1"]
	h.mu.RUnlock()
	if stillPresent {
		t.Fatalf("expected h.clients entry to be cleared on an evicted disconnect, even for a persistent session")
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.unsubscribeCalls) != 1 || reg.unsubscribeCalls[0].topic != "telemetry/tenant/device-1" {
		t.Fatalf("expected exactly 1 Unsubscribe call for telemetry/tenant/device-1, got %+v", reg.unsubscribeCalls)
	}
}

// TestOnDisconnect_PersistentSession_PlacesOfflineOwnership verifies the
// phase 6e eager path: a genuinely-offline persistent session gets its
// rendezvous-computed owner (internal/session.Owner) placed immediately,
// for every filter it was subscribed to, rather than waiting for the
// periodic session.Reconciler's next tick.
func TestOnDisconnect_PersistentSession_PlacesOfflineOwnership(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	ownership := &fakeOfflineOwnership{}
	h.offlineOwnership = ownership
	live := []string{"edge-1", "edge-2", "edge-3"}
	h.liveEdgeNodeIDs = func() []string { return live }

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnDisconnect(cl, nil, false) // expire=false: persistent session, genuinely offline

	wantOwner, ok := session.Owner("device-1", live)
	if !ok {
		t.Fatalf("test setup: expected session.Owner to resolve for a non-empty live list")
	}

	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if len(ownership.placeCalls) != 1 {
		t.Fatalf("expected exactly 1 Place call, got %+v", ownership.placeCalls)
	}
	got := ownership.placeCalls[0]
	if got.clientID != "device-1" || got.filter != "telemetry/tenant/device-1" || got.newOwner != wantOwner {
		t.Fatalf("expected Place(device-1, telemetry/tenant/device-1, %s), got %+v", wantOwner, got)
	}
}

// TestOnDisconnect_ExpiringSession_DoesNotPlaceOfflineOwnership verifies a
// clean/expiring session's disconnect never places offline ownership —
// there is no offline session left to own.
func TestOnDisconnect_ExpiringSession_DoesNotPlaceOfflineOwnership(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	ownership := &fakeOfflineOwnership{}
	h.offlineOwnership = ownership
	h.liveEdgeNodeIDs = func() []string { return []string{"edge-1", "edge-2"} }

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnDisconnect(cl, nil, true) // expire=true: clean session, truly ending

	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if len(ownership.placeCalls) != 0 {
		t.Fatalf("expected no Place calls for an expiring session, got %+v", ownership.placeCalls)
	}
}

// TestOnDisconnect_NoLiveEdgeNodeIDs_SkipsPlacement verifies the standalone
// (or not-yet-ready) case — h.liveEdgeNodeIDs nil — never panics and simply
// skips placement, leaving it to the periodic Reconciler.
func TestOnDisconnect_NoLiveEdgeNodeIDs_SkipsPlacement(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	ownership := &fakeOfflineOwnership{}
	h.offlineOwnership = ownership
	// h.liveEdgeNodeIDs left nil.

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnDisconnect(cl, nil, false)

	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if len(ownership.placeCalls) != 0 {
		t.Fatalf("expected no Place calls when liveEdgeNodeIDs is nil, got %+v", ownership.placeCalls)
	}
}

// TestOnDisconnect_NoOfflineOwnership_SkipsPlacement verifies standalone
// mode (h.offlineOwnership nil, the zero value used by every other
// OnDisconnect test in this file) never panics.
func TestOnDisconnect_NoOfflineOwnership_SkipsPlacement(t *testing.T) {
	reg := &fakeRegistry{}
	h := newClusterTestHook(reg, &fakeForwarder{}, "edge-1")
	h.tenantConns = map[string]int{}
	h.generation = map[string]uint64{}
	h.clients["device-1"] = deviceState("dev-1")

	cl := &mqtt.Client{ID: "device-1", State: mqtt.ClientState{Subscriptions: mqtt.NewSubscriptions()}}
	cl.State.Subscriptions.Add("telemetry/tenant/device-1", packets.Subscription{Filter: "telemetry/tenant/device-1"})

	h.OnDisconnect(cl, nil, false) // must not panic with offlineOwnership/liveEdgeNodeIDs both nil
}

// fakeSessionStore is a minimal sessionStore stand-in for OnSessionEstablish
// tests — see RedisSessionHook.SubscriptionsForClient/InflightForClient.
type fakeSessionStore struct {
	subs     []storage.Subscription
	inflight []storage.Message
}

func (f *fakeSessionStore) SubscriptionsForClient(clientID string) ([]storage.Subscription, error) {
	return f.subs, nil
}

func (f *fakeSessionStore) InflightForClient(clientID string) ([]storage.Message, error) {
	return f.inflight, nil
}

// fakeOfflineOwnership is a minimal offlineOwnershipStore stand-in for
// placeOfflineOwnership/clearOfflineOwnership tests.
type fakeOfflineOwnership struct {
	mu         sync.Mutex
	placeCalls []offlineOwnershipCall
	clearCalls []offlineOwnershipCall
	placeErr   error
	clearErr   error
}

type offlineOwnershipCall struct {
	clientID, filter, newOwner string
}

func (f *fakeOfflineOwnership) Place(clientID, filter, newOwner string) error {
	if f.placeErr != nil {
		return f.placeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.placeCalls = append(f.placeCalls, offlineOwnershipCall{clientID, filter, newOwner})
	return nil
}

func (f *fakeOfflineOwnership) Clear(clientID, filter string) error {
	if f.clearErr != nil {
		return f.clearErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clearCalls = append(f.clearCalls, offlineOwnershipCall{clientID: clientID, filter: filter})
	return nil
}

// TestOnSessionEstablish_CleanSessionNoop verifies a clean-session connect
// never triggers Redis rehydration — there's nothing to resume by
// definition.
func TestOnSessionEstablish_CleanSessionNoop(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{subs: []storage.Subscription{{Filter: "telemetry/x"}}}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: true}})

	if _, ok := server.Clients.Get("device-1"); ok {
		t.Fatalf("expected no ghost client registered for a clean-session connect")
	}
}

// TestOnSessionEstablish_NoSessionStoreNoop verifies standalone/no-Redis
// mode (sessionStore nil) never touches server.Clients.
func TestOnSessionEstablish_NoSessionStoreNoop(t *testing.T) {
	server := mqtt.New(nil)
	h := &keelHook{log: slog.Default(), server: server}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}})

	if _, ok := server.Clients.Get("device-1"); ok {
		t.Fatalf("expected no ghost client registered when sessionStore is nil")
	}
}

// TestOnSessionEstablish_AlreadyLocalNoop verifies that when this node
// already has local state for client_id (in-place resume — the common
// case, e.g. same-node reconnect or a restart that already ran readStore
// at boot), OnSessionEstablish doesn't second-guess it with its own ghost.
func TestOnSessionEstablish_AlreadyLocalNoop(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{subs: []storage.Subscription{{Filter: "telemetry/x"}}}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store}

	existing := server.NewClient(nil, "tcp", "device-1", false)
	server.Clients.Add(existing)

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}})

	got, ok := server.Clients.Get("device-1")
	if !ok {
		t.Fatalf("expected the pre-existing local client entry to remain")
	}
	if got != existing {
		t.Fatalf("expected OnSessionEstablish not to replace the pre-existing local client entry")
	}
}

// TestOnSessionEstablish_NothingPersistedNoop verifies that when the
// session store has no subscriptions or inflight for this client_id, no
// ghost is created — there's nothing to rehydrate.
func TestOnSessionEstablish_NothingPersistedNoop(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}})

	if _, ok := server.Clients.Get("device-1"); ok {
		t.Fatalf("expected no ghost client registered when nothing is persisted for this client_id")
	}
}

// TestOnSessionEstablish_RehydratesFromRedis verifies the core fix: a
// persistent session reconnecting to a node with no local state for
// client_id, but with subscriptions/inflight persisted in Redis, gets a
// ghost client seeded so mochi-mqtt's own inheritClientSession (called
// right after this hook returns) can merge/resend them through its
// already-correct takeover path.
func TestOnSessionEstablish_RehydratesFromRedis(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{
		subs: []storage.Subscription{
			{Filter: "telemetry/tenant/device-1", Qos: 1},
		},
		inflight: []storage.Message{
			{PacketID: 7, TopicName: "telemetry/tenant/device-1", Payload: []byte("seq=1")},
		},
	}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}, ProtocolVersion: 4})

	ghost, ok := server.Clients.Get("device-1")
	if !ok {
		t.Fatalf("expected a ghost client to be registered for rehydration")
	}
	if ghost.Properties.Clean {
		t.Fatalf("expected ghost.Properties.Clean to be false")
	}
	subs := ghost.State.Subscriptions.GetAll()
	if _, ok := subs["telemetry/tenant/device-1"]; !ok {
		t.Fatalf("expected ghost to carry the persisted subscription, got %+v", subs)
	}
	if ghost.State.Inflight.Len() != 1 {
		t.Fatalf("expected ghost to carry 1 persisted inflight message, got %d", ghost.State.Inflight.Len())
	}
}

// TestOnSessionEstablish_ClearsOfflineOwnership_CrossNodeResume verifies the
// phase 6e mirror of placeOfflineOwnership: reconnecting to a node with no
// local state (the ghost-rehydration branch) still clears offline
// ownership for every persisted filter.
func TestOnSessionEstablish_ClearsOfflineOwnership_CrossNodeResume(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{
		subs: []storage.Subscription{{Filter: "telemetry/tenant/device-1", Qos: 1}},
	}
	ownership := &fakeOfflineOwnership{}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store, offlineOwnership: ownership}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}, ProtocolVersion: 4})

	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if len(ownership.clearCalls) != 1 || ownership.clearCalls[0].clientID != "device-1" || ownership.clearCalls[0].filter != "telemetry/tenant/device-1" {
		t.Fatalf("expected exactly 1 Clear(device-1, telemetry/tenant/device-1) call, got %+v", ownership.clearCalls)
	}
}

// TestOnSessionEstablish_ClearsOfflineOwnership_SameNodeResume verifies
// clearing still happens on the same-node fast-resume branch — a disconnect
// eagerly places ownership without knowing in advance whether the
// reconnect will land on the same node or not, so both branches must clear.
func TestOnSessionEstablish_ClearsOfflineOwnership_SameNodeResume(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{
		subs: []storage.Subscription{{Filter: "telemetry/tenant/device-1", Qos: 1}},
	}
	ownership := &fakeOfflineOwnership{}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store, offlineOwnership: ownership}

	existing := server.NewClient(nil, "tcp", "device-1", false)
	server.Clients.Add(existing)

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}})

	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if len(ownership.clearCalls) != 1 || ownership.clearCalls[0].clientID != "device-1" || ownership.clearCalls[0].filter != "telemetry/tenant/device-1" {
		t.Fatalf("expected exactly 1 Clear(device-1, telemetry/tenant/device-1) call, got %+v", ownership.clearCalls)
	}
}

// TestOnSessionEstablish_CleanSession_DoesNotClearOfflineOwnership verifies
// the existing pk.Connect.Clean early return also skips the new clear step
// — nothing to clear for a clean session by definition.
func TestOnSessionEstablish_CleanSession_DoesNotClearOfflineOwnership(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{subs: []storage.Subscription{{Filter: "telemetry/x"}}}
	ownership := &fakeOfflineOwnership{}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store, offlineOwnership: ownership}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: true}})

	ownership.mu.Lock()
	defer ownership.mu.Unlock()
	if len(ownership.clearCalls) != 0 {
		t.Fatalf("expected no Clear calls for a clean-session connect, got %+v", ownership.clearCalls)
	}
}

// TestOnSessionEstablish_NoOfflineOwnership_Noop verifies standalone mode
// (h.offlineOwnership nil) never panics.
func TestOnSessionEstablish_NoOfflineOwnership_Noop(t *testing.T) {
	server := mqtt.New(nil)
	store := &fakeSessionStore{subs: []storage.Subscription{{Filter: "telemetry/x"}}}
	h := &keelHook{log: slog.Default(), server: server, sessionStore: store}

	cl := server.NewClient(nil, "tcp", "device-1", false)
	h.OnSessionEstablish(cl, packets.Packet{Connect: packets.ConnectParams{Clean: false}}) // must not panic
}

func TestFanOutNodes_UnionsLiveAndOfflineWithoutDuplicates(t *testing.T) {
	reg := &fakeRegistry{
		nodesFor:        map[string][]string{"telemetry/device-1": {"edge-1", "edge-2"}},
		offlineNodesFor: map[string][]string{"telemetry/device-1": {"edge-2", "edge-3"}},
	}
	h := newClusterTestHook(reg, &fakeForwarder{}, "self")

	got := h.fanOutNodes("telemetry/device-1")

	want := map[string]bool{"edge-1": true, "edge-2": true, "edge-3": true}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for _, n := range got {
		if !want[n] {
			t.Fatalf("unexpected node %q in %v", n, got)
		}
	}
}

func TestFanOutNodes_NoOfflineOwners_ReturnsLiveOnly(t *testing.T) {
	reg := &fakeRegistry{
		nodesFor: map[string][]string{"telemetry/device-1": {"edge-1"}},
	}
	h := newClusterTestHook(reg, &fakeForwarder{}, "self")

	got := h.fanOutNodes("telemetry/device-1")
	if len(got) != 1 || got[0] != "edge-1" {
		t.Fatalf("expected [edge-1], got %v", got)
	}
}

// TestForwardToClusterSubscribers_DeliversLocallyOwnedOfflineSession
// verifies a session this node owns is delivered directly (no
// self-forward), while a remote live subscriber still gets a real
// Forward call carrying a non-zero PublishID.
func TestForwardToClusterSubscribers_DeliversLocallyOwnedOfflineSession(t *testing.T) {
	reg := &fakeRegistry{
		nodesFor:       map[string][]string{"telemetry/device-1": {"edge-9"}},
		ownedClientIDs: map[string][]string{"self": {"device-2"}},
	}
	fwd := &fakeForwarder{}
	h := newClusterTestHook(reg, fwd, "self")
	store := newFakeOfflineDeliveryStore()
	store.sessions["device-2"] = session.OfflineSession{
		ClientID:      "device-2",
		Subscriptions: []session.OfflineSubscription{{Filter: "telemetry/#", QoS: 1}},
	}
	h.offlineDeliveryStore = store

	info := &auth.DeviceInfo{TenantID: uuid.MustParse("11111111-1111-1111-1111-111111111111")}
	pk := packets.Packet{TopicName: "telemetry/device-1", Payload: []byte("23.5"), FixedHeader: packets.FixedHeader{Qos: 1}}
	h.forwardToClusterSubscribers(context.Background(), info, pk)

	if len(store.enqueued) != 1 || store.enqueued[0] != "device-2" {
		t.Fatalf("expected device-2 delivered locally, got %v", store.enqueued)
	}

	calls := fwd.forwardCalls
	if len(calls) != 1 || calls[0].targetNodeID != "edge-9" {
		t.Fatalf("expected exactly 1 Forward call to edge-9, got %+v", calls)
	}
	if calls[0].msg.PublishID == uuid.Nil {
		t.Fatalf("expected a non-zero PublishID on the forwarded message")
	}
}
