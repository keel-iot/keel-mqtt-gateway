package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/redis/go-redis/v9"
)

// Count client round trips, not GETs executed inside a Lua script.
type inflightWriteCommands struct {
	gets     atomic.Int64
	activate func(context.Context) error
	once     sync.Once
}

func (*inflightWriteCommands) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *inflightWriteCommands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" {
			h.gets.Add(1)
		}
		if h.activate != nil && (cmd.Name() == "evalsha" || cmd.Name() == "eval") {
			var err error
			h.once.Do(func() { err = h.activate(ctx) })
			if err != nil {
				return err
			}
		}
		return next(ctx, cmd)
	}
}
func (*inflightWriteCommands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestInflightWriteStableSchemaDoesNotReadMarkerBeforeFencedWrite(t *testing.T) {
	for _, schema := range []int{redisIFMSchemaLegacy, redisIFMSchemaV2} {
		t.Run(fmt.Sprint(schema), func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			h.setSchemaVersion(schema)
			if err := h.router.Client().Set(ctx, redisIFMSchemaKey, schema, 0).Err(); err != nil {
				t.Fatal(err)
			}
			commands := new(inflightWriteCommands)
			h.router.Client().AddHook(commands)
			cl := &mqtt.Client{ID: "steady-writer"}
			pk := packets.Packet{PacketID: 7, TopicName: "test/topic", Payload: []byte("payload"), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
			if err := h.OnQosPublishResult(cl, pk, 10, 0); err != nil {
				t.Fatal(err)
			}
			if n := commands.gets.Load(); n != 0 {
				t.Fatalf("stable write made %d extra marker GETs", n)
			}
			field := "7"
			if schema == redisIFMSchemaV2 {
				field = "out:7"
			}
			assertStoredInflightMessage(t, ctx, h, cl.ID, field, "payload")
		})
	}
}

func TestInflightWriteActivationBetweenFieldSelectionAndScriptIsFenced(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	h.setSchemaVersion(redisIFMSchemaLegacy)
	if err := h.router.Client().Set(ctx, redisIFMSchemaKey, redisIFMSchemaLegacy, 0).Err(); err != nil {
		t.Fatal(err)
	}
	commands := &inflightWriteCommands{activate: func(ctx context.Context) error {
		return h.router.Client().Set(ctx, redisIFMSchemaKey, redisIFMSchemaV2, 0).Err()
	}}
	h.router.Client().AddHook(commands)
	cl := &mqtt.Client{ID: "activation-race"}
	pk := packets.Packet{PacketID: 7, TopicName: "test/topic", Payload: []byte("payload"), FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
	if err := h.OnQosPublishResult(cl, pk, 10, 0); err != nil {
		t.Fatal(err)
	}
	if h.schemaVersion() != redisIFMSchemaV2 {
		t.Fatal("writer did not refresh after rejected stale write")
	}
	if exists, err := h.router.Client().HExists(ctx, redisInflightKey(cl.ID), "7").Result(); err != nil || exists {
		t.Fatalf("stale numeric field committed: %v, %v", exists, err)
	}
	assertStoredInflightMessage(t, ctx, h, cl.ID, "out:7", "payload")
}

func TestInflightWriteRejectsInvalidSchemaEvenWithValidCachedSchema(t *testing.T) {
	h, ctx := newTestRedisSessionHook(t)
	if err := h.router.Client().Set(ctx, redisIFMSchemaKey, "invalid", 0).Err(); err != nil {
		t.Fatal(err)
	}
	cl := &mqtt.Client{ID: "invalid-schema-writer"}
	pk := packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
	if err := h.OnQosPublishResult(cl, pk, 0, 0); err == nil {
		t.Fatal("invalid schema accepted")
	}
	if n, err := h.router.Client().HLen(ctx, redisInflightKey(cl.ID)).Result(); err != nil || n != 0 {
		t.Fatalf("write bypassed schema validation: %d, %v", n, err)
	}
}

func assertStoredInflightMessage(t *testing.T, ctx context.Context, h *RedisSessionHook, clientID, field, payload string) {
	t.Helper()
	data, err := h.router.Client().HGet(ctx, redisInflightKey(clientID), field).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var message storage.Message
	if err := message.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if message.ID != field || string(message.Payload) != payload {
		t.Fatalf("stored record does not match field/payload: %+v", message)
	}
}

func TestExplicitIFMSchemaStartupValidation(t *testing.T) {
	for _, tc := range []struct {
		mode, marker string
		wantError    bool
	}{
		{"auto", "", false}, {"1", "", false}, {"2", "", true},
		{"1", "1", false}, {"2", "2", false}, {"1", "2", true},
		{"2", "1", true}, {"auto", "invalid", true}, {"3", "2", true},
	} {
		t.Run(tc.mode+"/"+tc.marker, func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			if tc.marker == "" {
				if err := h.router.Client().Del(ctx, redisIFMSchemaKey).Err(); err != nil {
					t.Fatal(err)
				}
			} else if err := h.router.Client().Set(ctx, redisIFMSchemaKey, tc.marker, 0).Err(); err != nil {
				t.Fatal(err)
			}
			err := h.ConfigureIFMSchema(ctx, tc.mode)
			if (err != nil) != tc.wantError {
				t.Fatalf("ConfigureIFMSchema: %v, want error %v", err, tc.wantError)
			}
			raw, err := h.router.Client().Get(ctx, redisIFMSchemaKey).Result()
			if tc.marker == "" {
				if err != redis.Nil {
					t.Fatalf("configuration created a schema marker: %q, %v", raw, err)
				}
			} else if err != nil || raw != tc.marker {
				t.Fatalf("configuration modified marker: %q, %v", raw, err)
			}
		})
	}
}

func TestExplicitIFMSchemaRejectsMutationsAfterMarkerChange(t *testing.T) {
	for _, dropped := range []bool{false, true} {
		t.Run(fmt.Sprint(dropped), func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			rdb := h.router.Client()
			if err := rdb.Set(ctx, redisIFMSchemaKey, "1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := h.ConfigureIFMSchema(ctx, "1"); err != nil {
				t.Fatal(err)
			}
			cl := &mqtt.Client{ID: "pinned-writer"}
			key := redisInflightKey(cl.ID)
			if err := rdb.HSet(ctx, key, "7", "legacy", "in:7", "inbound", "out:7", "outbound").Err(); err != nil {
				t.Fatal(err)
			}
			if err := rdb.Set(ctx, redisIFMSchemaKey, "2", 0).Err(); err != nil {
				t.Fatal(err)
			}
			pk := packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
			if err := h.OnQosPublishResult(cl, pk, 0, 0); err == nil {
				t.Fatal("pinned writer followed activation")
			}
			if _, _, err := h.QueueOfflineInflight(ctx, [16]byte{}, cl.ID, 0, session.InflightMessage{QoS: 1, Payload: []byte("offline")}); err == nil {
				t.Fatal("offline writer followed activation")
			}
			pk.FixedHeader.Type = packets.Pubcomp
			var err error
			if dropped {
				err = h.OnQosDroppedResult(cl, pk)
			} else {
				err = h.OnQosCompleteResult(cl, pk)
			}
			if err == nil {
				t.Fatal("pinned deletion followed activation")
			}
			if n, err := rdb.HLen(ctx, key).Result(); err != nil || n != 3 {
				t.Fatalf("mismatched mutation changed records: %d, %v", n, err)
			}
			if err := h.ActivateDirectionalSchema(ctx); err == nil {
				t.Fatal("explicit mode activated migration")
			}
			if h.schemaVersion() != redisIFMSchemaLegacy {
				t.Fatal("explicit schema switched")
			}
		})
	}
}

func TestInflightDeleteActivationIsAtomicAndPreservesOppositeDirection(t *testing.T) {
	for _, dropped := range []bool{false, true} {
		t.Run(fmt.Sprint(dropped), func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			rdb := h.router.Client()
			if err := rdb.Set(ctx, redisIFMSchemaKey, "1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			h.setSchemaVersion(redisIFMSchemaLegacy)
			cl := &mqtt.Client{ID: "delete-activation"}
			if err := rdb.HSet(ctx, redisInflightKey(cl.ID), "7", "legacy", "in:7", "inbound", "out:7", "outbound").Err(); err != nil {
				t.Fatal(err)
			}
			rdb.AddHook(&inflightWriteCommands{activate: func(ctx context.Context) error {
				return rdb.Set(ctx, redisIFMSchemaKey, "2", 0).Err()
			}})
			pk := packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Pubcomp}}
			var err error
			if dropped {
				pk.FixedHeader.Type = packets.Publish
				err = h.OnQosDroppedResult(cl, pk)
			} else {
				err = h.OnQosCompleteResult(cl, pk)
			}
			if err != nil {
				t.Fatal(err)
			}
			remaining, err := rdb.HGetAll(ctx, redisInflightKey(cl.ID)).Result()
			if err != nil || len(remaining) != 1 || remaining["in:7"] != "inbound" {
				t.Fatalf("opposite direction lost or stale delivery retained: %v, %v", remaining, err)
			}
		})
	}
}

func TestInflightDeleteStableSchemaDoesNotReadMarkerBeforeFencedDelete(t *testing.T) {
	for _, mode := range []string{"auto", "1", "2"} {
		t.Run(mode, func(t *testing.T) {
			h, ctx := newTestRedisSessionHook(t)
			marker := mode
			if mode == "auto" {
				marker = "2"
			}
			if err := h.router.Client().Set(ctx, redisIFMSchemaKey, marker, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := h.ConfigureIFMSchema(ctx, mode); err != nil {
				t.Fatal(err)
			}
			commands := new(inflightWriteCommands)
			h.router.Client().AddHook(commands)
			cl := &mqtt.Client{ID: "steady-delete"}
			pk := packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
			if err := h.OnQosPublishResult(cl, pk, 0, 0); err != nil {
				t.Fatal(err)
			}
			pk.FixedHeader.Type = packets.Pubcomp
			if err := h.OnQosCompleteResult(cl, pk); err != nil {
				t.Fatal(err)
			}
			if n := commands.gets.Load(); n != 0 {
				t.Fatalf("stable operations made %d marker round trips", n)
			}
			if n, err := h.router.Client().HLen(ctx, redisInflightKey(cl.ID)).Result(); err != nil || n != 0 {
				t.Fatalf("delivery retained: %d, %v", n, err)
			}
		})
	}
}
