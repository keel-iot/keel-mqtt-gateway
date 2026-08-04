package session_test

import (
	"fmt"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

func TestOwner_EmptyListReturnsNotFound(t *testing.T) {
	owner, ok := session.Owner("device-1", nil)
	if ok || owner != "" {
		t.Fatalf("expected (\"\", false) for empty node list, got (%q, %v)", owner, ok)
	}
}

func TestOwner_SingleNodeAlwaysWins(t *testing.T) {
	owner, ok := session.Owner("device-1", []string{"edge-1"})
	if !ok || owner != "edge-1" {
		t.Fatalf("expected (edge-1, true), got (%q, %v)", owner, ok)
	}
}

func TestOwner_Deterministic(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3"}
	first, ok := session.Owner("device-42", nodes)
	if !ok {
		t.Fatal("expected an owner")
	}
	for i := 0; i < 100; i++ {
		got, ok := session.Owner("device-42", nodes)
		if !ok || got != first {
			t.Fatalf("Owner not deterministic: call %d got %q, want %q", i, got, first)
		}
	}
}

func TestOwner_OrderIndependent(t *testing.T) {
	a := []string{"edge-1", "edge-2", "edge-3", "edge-4"}
	b := []string{"edge-4", "edge-2", "edge-1", "edge-3"}

	for _, clientID := range []string{"device-1", "device-2", "device-3", "device-99"} {
		ownerA, _ := session.Owner(clientID, a)
		ownerB, _ := session.Owner(clientID, b)
		if ownerA != ownerB {
			t.Fatalf("%s: owner depends on list order: %q (a) vs %q (b)", clientID, ownerA, ownerB)
		}
	}
}

func TestOwner_AlwaysInCandidateList(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3", "edge-4", "edge-5"}
	valid := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		valid[n] = true
	}
	for i := 0; i < 200; i++ {
		clientID := "device-" + string(rune('a'+i%26)) + string(rune(i))
		owner, ok := session.Owner(clientID, nodes)
		if !ok || !valid[owner] {
			t.Fatalf("owner %q for %q is not one of the candidate nodes %v", owner, clientID, nodes)
		}
	}
}

// TestOwner_MinimalDisruption verifies rendezvous hashing's key property:
// removing a node that is NOT clientID's current owner must never change
// clientID's owner — only sessions actually owned by the removed node
// should be reassigned. This is what makes rebalance on membership change
// cheap (see the design doc): most sessions keep their owner.
func TestOwner_MinimalDisruption(t *testing.T) {
	full := []string{"edge-1", "edge-2", "edge-3", "edge-4", "edge-5"}

	unaffected := 0
	total := 500
	for i := 0; i < total; i++ {
		clientID := randClientID(i)
		before, _ := session.Owner(clientID, full)
		if before == "edge-5" {
			continue // this session's owner is about to be removed — expected to change
		}

		withoutOne := []string{"edge-1", "edge-2", "edge-3", "edge-4"} // edge-5 removed
		after, _ := session.Owner(clientID, withoutOne)
		if after == before {
			unaffected++
		}
	}

	// Every session whose owner wasn't the removed node must be unaffected —
	// removing one candidate must never change another session's owner.
	totalConsidered := 0
	for i := 0; i < total; i++ {
		clientID := randClientID(i)
		before, _ := session.Owner(clientID, full)
		if before != "edge-5" {
			totalConsidered++
		}
	}
	if unaffected != totalConsidered {
		t.Fatalf("expected all %d sessions not owned by the removed node to keep their owner, only %d did", totalConsidered, unaffected)
	}
}

func TestOwner_RoughlyUniformDistribution(t *testing.T) {
	nodes := []string{"edge-1", "edge-2", "edge-3", "edge-4"}
	counts := make(map[string]int)
	const n = 4000
	for i := 0; i < n; i++ {
		owner, _ := session.Owner(randClientID(i), nodes)
		counts[owner]++
	}
	if len(counts) != len(nodes) {
		t.Fatalf("expected all %d nodes to receive at least one session, got distribution %v", len(nodes), counts)
	}
	expected := n / len(nodes)
	for node, c := range counts {
		// Generous tolerance (±30%) — this is a statistical property, not
		// an exact guarantee, and the test must not be flaky.
		if c < expected*7/10 || c > expected*13/10 {
			t.Fatalf("node %s got %d sessions, expected roughly %d (±30%%), distribution: %v", node, c, expected, counts)
		}
	}
}

// randClientID returns a plain, distinct client_id per seed — rendezvous
// hashing (FNV underneath) is what's responsible for spreading these
// evenly across nodes, so the input doesn't need to look random itself.
func randClientID(seed int) string {
	return fmt.Sprintf("device-%d", seed)
}
