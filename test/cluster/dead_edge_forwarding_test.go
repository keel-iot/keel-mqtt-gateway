//go:build cluster

package cluster

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestDeadEdgeForwarding_DocumentedLossConfirmed is Phase 3 rung 5's first
// "known risky path" scenario (docs/testing/CLUSTER_CORRECTNESS_MATRIX.md
// §6's "Kill destination Edge mid-flight..." row, RISK-OWN-002/003, D2/C4).
// This is a regression test for best-effort forwarding and dead-edge route
// convergence after the targeted forwarding remediation.
//
// PRECONDITION: a live subscriber owns a subscription on edge-1; the
// routing table has a live route entry filter -> edge-1's node ID.
// ACTION: kill edge-1 outright (collateral: the subscriber's own
// connection dies with it — that's expected, not the thing under test);
// publish a matching message from edge-2, the only live Edge left with a
// route to the (now-dead) subscription.
// OBSERVABLE TRANSITION: the forwarder's gRPC call to the dead edge-1
// fails; per source (CLUSTER_CORRECTNESS_MATRIX.md §1's "Verified gap,
// real message loss"), this is logged and dropped, never retried,
// and never surfaces as an error to the original publisher.
// ASSERT INVARIANT: D2 (silent, at-most-once loss on forward failure,
// no retry) and C4 (the stale route to the dead edge is not purged).
// RECOVERY: the rest of the cluster (edge-2, both surviving Cores) stays
// fully healthy — checked via an entirely independent publish/subscribe
// round trip through edge-2 alone, after the failed forward.
// FINAL ASSERTION: publisher's own PUBACK succeeds (no visible error);
// the dead route is eventually removed from GET /api/cluster/routes; the
// independent edge-2-only round trip still works.
// LIMITATIONS: does not force a known-but-unreachable RPC to remain in the
// suspicion window; that requires deterministic network fault injection.
func TestDeadEdgeForwarding_DocumentedLossConfirmed(t *testing.T) {
	h := NewHarness(t, 3, 2, []string{deviceAID})

	const filter = "telemetry/#"
	deadEdgeNodeID := h.Edges[0].ID // "edge-1"

	// A persistent (non-clean) session, deliberately — not incidental.
	// mochi-mqtt's own OnDisconnect only tears down cluster routing when
	// expire==true (internal/broker/hooks.go), which for a *clean* session
	// is unconditional on any disconnect, including the graceful shutdown a
	// container's own SIGTERM handling may run before final termination.
	// C4 is specifically about a *live/persistent* session's stale route
	// surviving an edge's death — using a clean session here would silently
	// test the wrong thing (a documented, correct cleanup path) and could
	// misreport it as the C4 gap.
	victim := connectPersistentClient(t, h.MQTTAddr(0), "dead-edge-victim-subscriber", consumerUsername, consumerPassword)
	subToken := victim.Subscribe(filter, 1, func(_ mqtt.Client, _ mqtt.Message) {})
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("victim subscribe on edge-1: %v", subToken.Error())
	}

	if !waitForLiveRoute(t, h, 0, filter, deadEdgeNodeID, 15*time.Second) {
		t.Fatalf("live route %q -> %s never became observable via GET /api/cluster/routes before the kill", filter, deadEdgeNodeID)
	}

	h.KillEdge(0)

	publisher := connectClient(t, h.MQTTAddr(1), "dead-edge-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)

	// 1. D2: the publish itself must not surface any error to the
	// publisher — the forward-to-a-dead-node failure is invisible at the
	// publish API, exactly as documented (logged-and-dropped, not
	// propagated as a publish failure).
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, "lost-because-destination-edge-is-dead")
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish toward a dead-destination route unexpectedly failed at the publisher: %v — the documented behavior is a silent drop, not a visible publish error", pubToken.Error())
	}

	// 2. Cluster stays healthy: a fully independent round trip entirely
	// through the surviving edge-2 must still work — no cascading failure
	// from the failed forward above.
	independentConsumer := connectClient(t, h.MQTTAddr(1), "dead-edge-independent-consumer", consumerUsername, consumerPassword)
	defer independentConsumer.Disconnect(250)
	received := make(chan string, 1)
	indepSub := independentConsumer.Subscribe(filter, 1, func(_ mqtt.Client, msg mqtt.Message) {
		received <- string(msg.Payload())
	})
	if !indepSub.WaitTimeout(10*time.Second) || indepSub.Error() != nil {
		t.Fatalf("independent post-kill subscribe on edge-2: %v", indepSub.Error())
	}
	indepPayload := "cluster-still-healthy-after-dead-edge-forward"
	indepPub := publisher.Publish(publishTopicFor(deviceAID), 1, false, indepPayload)
	if !indepPub.WaitTimeout(10*time.Second) || indepPub.Error() != nil {
		t.Fatalf("independent post-kill publish: %v", indepPub.Error())
	}
	select {
	case got := <-received:
		if got != indepPayload {
			t.Fatalf("expected %q, got %q", indepPayload, got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cluster did not stay healthy after the dead-edge forward failure — an entirely independent edge-2-only round trip never completed")
	}

	// 3. The confirmed leave must converge the stale route away from the
	// dead edge, independently of MQTT OnDisconnect/graceful shutdown.
	if !waitForRouteAbsent(t, h, 0, filter, deadEdgeNodeID, 15*time.Second) {
		routes := fetchRoutes(t, h, 0)
		t.Fatalf("dead route %q -> %s was not purged — found: %v", filter, deadEdgeNodeID, routes[filter])
	}
}

func fetchRoutes(t *testing.T, h *Harness, queryFrom int) map[string][]string {
	t.Helper()
	resp, err := http.Get("http://" + h.ManagementAddr(queryFrom) + "/api/cluster/routes")
	if err != nil {
		t.Fatalf("query routes via core index %d: %v", queryFrom, err)
	}
	defer resp.Body.Close()
	var routes map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&routes); err != nil {
		t.Fatalf("decode /api/cluster/routes response: %v", err)
	}
	return routes
}

func containsNode(nodes []string, nodeID string) bool {
	for _, n := range nodes {
		if n == nodeID {
			return true
		}
	}
	return false
}

// waitForLiveRoute polls GET /api/cluster/routes until filter's node list
// contains nodeID, or timeout elapses — deterministic condition poll, no
// fixed sleep, same posture as waitForOfflineOwnership.
func waitForLiveRoute(t *testing.T, h *Harness, queryFrom int, filter, nodeID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		routes := fetchRoutes(t, h, queryFrom)
		if containsNode(routes[filter], nodeID) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func waitForRouteAbsent(t *testing.T, h *Harness, queryFrom int, filter, nodeID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		routes := fetchRoutes(t, h, queryFrom)
		if !containsNode(routes[filter], nodeID) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
