package store

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestStaggeredJoinViaPeersFunc simulates the realistic K8s rolling-update
// race: node B's peer resolver already knows node A's address from the
// start (this project's own gossip membership converges fast and
// StatefulSet pods start readiness-gated, so the sibling's *identity* is
// essentially always known already) — but node A's Olric listener isn't
// actually accepting connections yet at that exact moment, becoming
// reachable only partway through B's join budget.
//
// This is deliberately not "B's resolver returns no address, then later
// returns one" — investigation (see internal/cluster/store's PeersFunc
// doc and TestPeersFuncNeverRetriesOnEmptyPeerList below) found that
// Olric's join attempt succeeds immediately, with zero retries, whenever
// the peer list is empty — the retry loop only ever engages when a
// non-empty address is known but connecting to it fails. So the "sibling
// address genuinely unknown yet" case is not the one PeersFunc helps
// with; this test targets the case it does.
func TestStaggeredJoinViaPeersFunc(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real embedded Olric members; skipped in -short")
	}

	portA := freePort(t)
	gossipPortA := freePort(t)
	gossipAddrA := fmt.Sprintf("127.0.0.1:%d", gossipPortA)

	portB := freePort(t)
	gossipPortB := freePort(t)

	// B's resolver reports A's address from t=0 (identity already known),
	// but nothing is listening there yet — A doesn't actually start until
	// 700ms in, well within B's 3s join budget (15 * 200ms) below.
	resolve := func() ([]string, error) {
		return []string{gossipAddrA}, nil
	}

	type result struct {
		store *OlricStore
		err   error
	}
	storeBCh := make(chan result, 1)
	go func() {
		st, err := NewEmbeddedOlricStore(OlricConfig{
			BindAddr:          "127.0.0.1",
			BindPort:          portB,
			GossipPort:        gossipPortB,
			AdvertiseAddr:     "127.0.0.1",
			PeersFunc:         resolve,
			JoinRetryInterval: 200 * time.Millisecond,
			MaxJoinAttempts:   15,
			DMapName:          "keel.routes.staggered.test",
		})
		storeBCh <- result{store: st, err: err}
	}()

	time.Sleep(700 * time.Millisecond)

	storeA, err := NewEmbeddedOlricStore(OlricConfig{
		BindAddr:      "127.0.0.1",
		BindPort:      portA,
		GossipPort:    gossipPortA,
		AdvertiseAddr: "127.0.0.1",
		DMapName:      "keel.routes.staggered.test",
	})
	if err != nil {
		t.Fatalf("start node A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close(context.Background()) })

	res := <-storeBCh
	if res.err != nil {
		t.Fatalf("start node B: %v", res.err)
	}
	storeB := res.store
	t.Cleanup(func() { _ = storeB.Close(context.Background()) })

	// If B had given up before A came up (the old static-Peers behaviour
	// would have, with only one join attempt against an address nothing
	// was listening on yet), A and B would each report exactly 1 member
	// forever. Converging to 2 on both sides confirms the live-retrying
	// join actually bridged them.
	deadline := time.Now().Add(10 * time.Second)
	for {
		membersA, errA := storeA.client.Members(context.Background())
		membersB, errB := storeB.client.Members(context.Background())
		if errA == nil && errB == nil && len(membersA) == 2 && len(membersB) == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes did not converge into one cluster: A=%d members (err=%v), B=%d members (err=%v)",
				len(membersA), errA, len(membersB), errB)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestPeersFuncNeverRetriesOnEmptyPeerList documents the finding above: a
// resolver returning no peers succeeds as an immediate solo bootstrap,
// with no retry — the join budget (JoinRetryInterval * MaxJoinAttempts)
// is never actually spent in this case, unlike when a peer address is
// known but unreachable.
func TestPeersFuncNeverRetriesOnEmptyPeerList(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real embedded Olric member; skipped in -short")
	}

	port := freePort(t)
	gossipPort := freePort(t)

	start := time.Now()
	st, err := NewEmbeddedOlricStore(OlricConfig{
		BindAddr:      "127.0.0.1",
		BindPort:      port,
		GossipPort:    gossipPort,
		AdvertiseAddr: "127.0.0.1",
		PeersFunc: func() ([]string, error) {
			return nil, nil
		},
		JoinRetryInterval: 5 * time.Second, // would dominate elapsed time if a retry ever happened
		MaxJoinAttempts:   3,
		DMapName:          "keel.routes.staggered.test",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected solo bootstrap to succeed, got: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(context.Background()) })

	if elapsed > 2*time.Second {
		t.Fatalf("expected immediate solo bootstrap (no retry) with an empty peer list, took %s", elapsed)
	}
}
