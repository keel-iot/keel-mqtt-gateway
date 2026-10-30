package broker

import (
	"context"
	"sync/atomic"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/redis/go-redis/v9"
)

type statelessRedisCommands struct{ calls atomic.Int64 }

func (*statelessRedisCommands) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *statelessRedisCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { h.calls.Add(1); return next(ctx, cmd) }
}
func (h *statelessRedisCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.calls.Add(int64(len(cmds)))
		return next(ctx, cmds)
	}
}

func TestRedisStatelessInboundQoS1CannotDeleteOutboundDelivery(t *testing.T) {
	for _, schema := range []int{redisIFMSchemaLegacy, redisIFMSchemaV2} {
		t.Run(string(rune('0'+schema)), func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			h.setSchemaVersion(schema)
			if err := h.router.Client().Set(ctx, redisIFMSchemaKey, schema, 0).Err(); err != nil {
				t.Fatal(err)
			}
			cl := &mqtt.Client{ID: "stateless-qos1"}
			key := redisInflightKey(cl.ID)
			field := "7"
			if schema == redisIFMSchemaV2 {
				field = "out:7"
			}
			if err := h.router.Client().HSet(ctx, key, field, "outbound-payload").Err(); err != nil {
				t.Fatal(err)
			}
			commands := new(statelessRedisCommands)
			h.router.Client().AddHook(commands)
			ack := packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Puback}, InboundFlow: true}
			if err := h.OnQosPublishResult(cl, ack, 0, 0); err != nil {
				t.Fatal(err)
			}
			if err := h.OnQosCompleteResult(cl, ack); err != nil {
				t.Fatal(err)
			}
			if n := commands.calls.Load(); n != 0 {
				t.Fatalf("stateless ACK performed %d Redis commands", n)
			}
			got, err := h.router.Client().HGet(ctx, key, field).Result()
			if err != nil || got != "outbound-payload" {
				t.Fatalf("outbound overwritten/deleted: got=%q err=%v", got, err)
			}
			if n, err := h.router.Client().HLen(ctx, key).Result(); err != nil || n != 1 {
				t.Fatalf("stateless ACK added state: count=%d err=%v", n, err)
			}
			ack.InboundFlow = false // An actual client PUBACK must still complete the server's delivery.
			if err := h.OnQosCompleteResult(cl, ack); err != nil {
				t.Fatal(err)
			}
			if n, err := h.router.Client().HLen(ctx, key).Result(); err != nil || n != 0 {
				t.Fatalf("outbound completion failed: count=%d err=%v", n, err)
			}
		})
	}
}
