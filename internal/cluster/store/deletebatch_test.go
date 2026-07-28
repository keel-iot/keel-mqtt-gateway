package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// newOlricTriCluster spins up 3 real embedded Olric members joined into one
// cluster, returning stores and the first member's gossip address. Reuses the
// same wiring newOlricCluster-style helpers in this package use.
func newOlricTriCluster(t *testing.T, dmapName string) []*OlricStore {
	t.Helper()
	var gossipAddrs []string
	var stores []*OlricStore
	for i := 0; i < 3; i++ {
		port := freePort(t)
		gossipPort := freePort(t)
		gossipAddr := fmt.Sprintf("127.0.0.1:%d", gossipPort)
		var peers []string
		if len(gossipAddrs) > 0 {
			peers = []string{gossipAddrs[0]}
		}
		gossipAddrs = append(gossipAddrs, gossipAddr)
		st, err := NewEmbeddedOlricStore(OlricConfig{
			BindAddr:      "127.0.0.1",
			BindPort:      port,
			GossipPort:    gossipPort,
			AdvertiseAddr: "127.0.0.1",
			Peers:         peers,
			DMapName:      dmapName,
			StartTimeout:  10 * time.Second,
		})
		if err != nil {
			t.Fatalf("start olric store %d: %v", i, err)
		}
		t.Cleanup(func() { _ = st.Close(context.Background()) })
		stores = append(stores, st)
	}
	// Give the cluster time to settle partition ownership.
	time.Sleep(3 * time.Second)
	return stores
}

// TestMultiKeyDeleteAcrossPartitionOwners confirms whether Olric's variadic
// DMap.Delete(ctx, keys...) actually removes every key when those keys hash to
// multiple distinct partition owners. Code inspection of both olric-data/olric
// and tochemey/olric shows deleteKeys returns unconditionally after the first
// REMOTE owner in its per-owner loop (the `else` branch ends with a bare
// `return 0, cmd.Err()`), which would silently skip remaining owners. This
// test proves the real behavior either way — if it shows survivors, the
// variadic path is lossy and OlricStore.Delete must not rely on it.
func TestMultiKeyDeleteAcrossPartitionOwners(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real embedded Olric members; skipped in -short")
	}
	stores := newOlricTriCluster(t, "keel.routes.delbatch.test")
	st := stores[0]
	ctx := context.Background()

	const n = 60
	for i := 0; i < n; i++ {
		if err := st.Put(ctx, fmt.Sprintf("k-%02d", i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%02d", i)
	}
	if err := st.Delete(ctx, keys...); err != nil {
		t.Fatalf("batch delete: %v", err)
	}

	// Settle any async replication/repair before scanning.
	time.Sleep(time.Second)
	it, err := st.Scan(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	survivors := 0
	for it.Next() {
		survivors++
	}
	it.Close()
	if survivors != 0 {
		t.Fatalf("multi-key Delete left %d/%d keys behind — variadic path is lossy across partition owners", survivors, n)
	}
}
