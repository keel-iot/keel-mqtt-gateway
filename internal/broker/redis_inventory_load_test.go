package broker

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/hooks/storage"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

// Exercise several SSCAN pages against real Redis and verify both inventory
// correctness and the command reduction. A malformed live record must never be
// fetched or decoded; an expired index member must still be ignored.
func TestOfflineInventoryExcept_SkipsLivePayloadReads(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	live := make(map[string]string)
	pipe := h.router.Client().Pipeline()
	for i := 0; i < 8600; i++ {
		id := fmt.Sprintf("live-%05d", i)
		live[id] = "edge-1"
		pipe.SAdd(ctx, redisClientIndex, id)
		pipe.Set(ctx, redisClientKey(id), "invalid-live-record", 0)
	}
	lastSeen := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	for _, id := range []string{"offline-a", "offline-b"} {
		client := storage.Client{ID: id, T: storage.ClientKey}
		raw, err := client.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		sub := storage.Subscription{Client: id, Filter: "devices/+/state", Qos: 1, T: storage.SubscriptionKey}
		subRaw, err := sub.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		pipe.SAdd(ctx, redisClientIndex, id)
		pipe.Set(ctx, redisClientKey(id), raw, 0)
		pipe.Set(ctx, redisLastSeenKey(id), lastSeen.Format(time.RFC3339Nano), 0)
		pipe.HSet(ctx, redisSubKey(id), sub.Filter, subRaw)
	}
	pipe.SAdd(ctx, redisClientIndex, "expired-client")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	counts := func() map[string]int64 {
		t.Helper()
		info, err := h.router.Client().Info(ctx, "commandstats").Result()
		if err != nil {
			t.Fatal(err)
		}
		out := make(map[string]int64)
		for _, line := range strings.Split(info, "\n") {
			name, fields, ok := strings.Cut(strings.TrimSpace(line), ":calls=")
			if !ok {
				continue
			}
			calls, _, _ := strings.Cut(fields, ",")
			out[name], err = strconv.ParseInt(calls, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	before := counts()
	got, err := h.OfflineInventoryExcept(live)
	if err != nil {
		t.Fatal(err)
	}
	after := counts()
	if len(got) != 2 {
		t.Fatalf("expected two offline sessions, got %+v", got)
	}
	for _, item := range got {
		if item.ClientID != "offline-a" && item.ClientID != "offline-b" {
			t.Fatalf("unexpected client %q", item.ClientID)
		}
		want := session.OfflineSession{
			ClientID: item.ClientID, LastSeenAt: lastSeen,
			Subscriptions: []session.OfflineSubscription{{Filter: "devices/+/state", QoS: 1}},
		}
		if !reflect.DeepEqual(item, want) {
			t.Fatalf("got %+v, want %+v", item, want)
		}
	}
	for name, want := range map[string]int64{"cmdstat_get": 6, "cmdstat_hgetall": 3} {
		if delta := after[name] - before[name]; delta != want {
			t.Errorf("%s: got %d reads, want %d (no live-client reads)", name, delta, want)
		}
	}
	if delta := after["cmdstat_sscan"] - before["cmdstat_sscan"]; delta < 2 {
		t.Fatalf("expected multiple SSCAN pages, got %d", delta)
	}
	if len(live) != 8600 {
		t.Fatal("caller snapshot was modified")
	}

	// A client that connected since the previous pass must be excluded.
	live["offline-b"] = "edge-2"
	got, err = h.OfflineInventoryExcept(live)
	if err != nil || len(got) != 1 || got[0].ClientID != "offline-a" {
		t.Fatalf("updated snapshot: got %+v, err %v", got, err)
	}
	// Once it disconnects, the next pass includes it again.
	delete(live, "offline-b")
	got, err = h.OfflineInventoryExcept(live)
	if err != nil || len(got) != 2 {
		t.Fatalf("disconnected client: got %+v, err %v", got, err)
	}
}

func TestOfflineInventoryExcept_AllLiveAndNilSnapshot(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	client := storage.Client{ID: "device", T: storage.ClientKey}
	raw, err := client.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.router.Client().Set(ctx, redisClientKey(client.ID), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := h.router.Client().SAdd(ctx, redisClientIndex, client.ID).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := h.OfflineInventoryExcept(map[string]string{client.ID: "edge"})
	if err != nil || len(got) != 0 {
		t.Fatalf("all live: got %+v, err %v", got, err)
	}
	got, err = h.OfflineInventoryExcept(nil)
	if err != nil || len(got) != 1 || got[0].ClientID != client.ID {
		t.Fatalf("nil snapshot: got %+v, err %v", got, err)
	}
	legacy, err := h.OfflineInventory()
	if err != nil || !reflect.DeepEqual(got, legacy) {
		t.Fatalf("unfiltered inventory changed: got %+v, err %v", legacy, err)
	}
}

func BenchmarkOfflineInventoryOnlineFleet(b *testing.B) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		b.Skip("TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	router, err := redisrouter.New(ctx, addr, "")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = router.Client().FlushDB(ctx).Err()
		_ = router.Close()
	})
	h := NewRedisSessionHook(router, nil)
	live := make(map[string]string)
	pipe := router.Client().Pipeline()
	for i := 0; i < 8600; i++ {
		id := fmt.Sprintf("bench-%05d", i)
		live[id] = "edge"
		client := storage.Client{ID: id, T: storage.ClientKey}
		raw, err := client.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		pipe.Set(ctx, redisClientKey(id), raw, 0)
		pipe.SAdd(ctx, redisClientIndex, id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatal(err)
	}
	b.Run("unfiltered", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := h.OfflineInventory()
			if err != nil || len(got) != len(live) {
				b.Fatalf("inventory len=%d, err=%v", len(got), err)
			}
		}
	})
	b.Run("skip_online", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := h.OfflineInventoryExcept(live)
			if err != nil || len(got) != 0 {
				b.Fatalf("inventory len=%d, err=%v", len(got), err)
			}
		}
	})
}
