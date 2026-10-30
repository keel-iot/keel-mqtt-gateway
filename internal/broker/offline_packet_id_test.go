package broker

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/redis/go-redis/v9"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

func TestOfflinePacketCounter_FullAndAlmostFullQueues(t *testing.T) {
	for _, prefix := range []string{"", "out:"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			key := redisInflightKey("bridge-full")
			fields := make(map[string]any, 65535)
			for i := 1; i < 65535; i++ {
				fields[prefix+strconv.Itoa(i)] = "occupied"
			}
			if err := h.router.Client().HSet(ctx, key, fields).Err(); err != nil {
				t.Fatal(err)
			}
			for _, full := range []bool{false, true} {
				if full {
					if err := h.router.Client().HSet(ctx, key, prefix+"65535", "occupied").Err(); err != nil {
						t.Fatal(err)
					}
				}
				budget, cancel := context.WithTimeout(ctx, 2*time.Second)
				start := time.Now()
				var next int64
				err := h.router.Client().Watch(budget, func(tx *redis.Tx) error {
					var err error
					next, err = nextOfflinePacketCounter(budget, tx, key, 0, prefix)
					return err
				}, key)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				want := int64(65535)
				if full {
					want = 0
				}
				if next != want {
					t.Fatalf("full=%v counter=%d, want %d", full, next, want)
				}
				t.Logf("prefix=%q full=%v elapsed=%v", prefix, full, time.Since(start))
			}
		})
	}
}

func TestOfflinePacketCounter_RespectsDirectionalAndLegacyReservations(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	key := redisInflightKey("bridge-directional")
	if err := h.router.Client().HSet(ctx, key, "in:1", "inbound", "out:2", "outbound", "3", "legacy").Err(); err != nil {
		t.Fatal(err)
	}
	for counter, want := range map[int64]int64{0: 1, 1: 4, 65535: 65536} {
		err := h.router.Client().Watch(ctx, func(tx *redis.Tx) error {
			next, err := nextOfflinePacketCounter(ctx, tx, key, counter, "out:")
			if err == nil && next != want {
				t.Errorf("counter=%d next=%d, want %d", counter, next, want)
			}
			return err
		}, key)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestQueueOfflineInflight_DeadlineWhileWaitingForSameClient(t *testing.T) {
	h := NewRedisSessionHook(nil, nil)
	release, err := h.lockOfflineClient(context.Background(), "busy-bridge")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, queued, err := h.QueueOfflineInflight(ctx, uuid.New(), "busy-bridge", time.Minute, session.InflightMessage{})
	if queued || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued=%v err=%v, want deadline exceeded", queued, err)
	}
	release()
	if len(h.offlineLocks) != 0 {
		t.Fatalf("lock references leaked: %v", h.offlineLocks)
	}
}

func TestQueueOfflineInflight_FullLegacyQueueFailsWithoutNetworkScan(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	// Literal marker keeps this regression usable on the 0.2.45 baseline,
	// as well as with the separately pending directional-schema changes.
	if err := h.router.Client().Set(ctx, redisKeyPrefix+"IFM:SCHEMA", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	key := redisInflightKey("bridge-full")
	fields := make(map[string]any, 65535)
	for i := 1; i <= 65535; i++ {
		fields[strconv.Itoa(i)] = "occupied"
	}
	if err := h.router.Client().HSet(ctx, key, fields).Err(); err != nil {
		t.Fatal(err)
	}
	budget, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	start := time.Now()
	_, queued, err := h.QueueOfflineInflight(budget, uuid.New(), "bridge-full", time.Minute, session.InflightMessage{Topic: "kimera/devices/a", QoS: 1})
	if queued || !errors.Is(err, packets.ErrQuotaExceeded) {
		t.Fatalf("queued=%v err=%v, want quota exceeded", queued, err)
	}
	if budget.Err() != nil {
		t.Fatalf("full queue exhausted the time budget: %v", budget.Err())
	}
	t.Logf("full legacy queue rejected in %v", time.Since(start))
}
