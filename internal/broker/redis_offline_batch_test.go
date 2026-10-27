package broker

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/redis/go-redis/v9"
)

type subscriptionBatchObserver struct{ sizes []int }

func (o *subscriptionBatchObserver) DialHook(next redis.DialHook) redis.DialHook { return next }
func (o *subscriptionBatchObserver) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}
func (o *subscriptionBatchObserver) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() != "hgetall" {
				return fmt.Errorf("unexpected command in subscription read batch: %s", cmd.Name())
			}
		}
		o.sizes = append(o.sizes, len(cmds))
		return next(ctx, cmds)
	}
}

func TestOwnedOfflineSessions_BoundedBatchesPreserveOrder(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	ids := make([]string, 600)
	pipe := h.router.Client().Pipeline()
	for i := range ids {
		ids[i] = fmt.Sprintf("device-%03d", i)
		sub := storage.Subscription{Client: ids[i], Filter: "devices/+/state", Qos: 2, T: storage.SubscriptionKey}
		raw, err := sub.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		pipe.HSet(ctx, redisSubKey(ids[i]), sub.Filter, raw)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	observer := &subscriptionBatchObserver{}
	h.router.Client().AddHook(observer)
	got, err := h.OwnedOfflineSessions(ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("got %d sessions, want %d", len(got), len(ids))
	}
	for i, item := range got {
		if item.ClientID != ids[i] || len(item.Subscriptions) != 1 || item.Subscriptions[0].QoS != 2 {
			t.Fatalf("session %d changed: %+v", i, item)
		}
	}
	if !reflect.DeepEqual(observer.sizes, []int{256, 256, 88}) {
		t.Fatalf("unbounded or unbatched subscription reads: %v", observer.sizes)
	}
	// A later subscription update must be read immediately, without a cache TTL.
	if err := h.router.Client().Del(ctx, redisSubKey(ids[0])).Err(); err != nil {
		t.Fatal(err)
	}
	got, err = h.OwnedOfflineSessions(ids[:1])
	if err != nil || len(got) != 1 || len(got[0].Subscriptions) != 0 {
		t.Fatalf("stale subscription after update: %+v, err=%v", got, err)
	}
}

func TestOwnedOfflineSessions_BatchReadErrorReturnsNoPartialInventory(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	if err := h.router.Client().Set(ctx, redisSubKey("wrong-type"), "invalid", 0).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := h.OwnedOfflineSessions([]string{"missing", "wrong-type"})
	if err == nil || got != nil {
		t.Fatalf("partial success on Redis error: got=%v err=%v", got, err)
	}
}
