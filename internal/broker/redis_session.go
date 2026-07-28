// Package broker — RedisSessionHook persists MQTT sessions, subscriptions, and
// in-flight QoS≥1 messages to Redis so that the gateway can be replaced or
// restarted without losing session state (required for horizontal scaling and
// rolling deploys).
//
// This hook is optional: when no Redis client is injected the broker falls back
// to its default in-memory behaviour.
//
// Key schema
//
//	keel:gw:CL   — HSET of storage.Client  (keyed by client-id)
//	keel:gw:SUB  — HSET of storage.Subscription (keyed by clientID:filter)
//	keel:gw:IFM  — HSET of storage.Message (keyed by clientID:packetID)
package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/redis/go-redis/v9"
)

// Redis key prefixes — separate from any other consumers sharing the same
// Redis instance.
const (
	redisKeyPrefix    = "keel:gw:"
	redisClientHash   = redisKeyPrefix + storage.ClientKey
	redisSubHash      = redisKeyPrefix + storage.SubscriptionKey
	redisInflightHash = redisKeyPrefix + storage.InflightKey
)

// RedisSessionHook implements the mochi-mqtt Hook interface, persisting MQTT
// session data to Redis.  It is injected before the main keelHook so that
// session data is available during server startup.
type RedisSessionHook struct {
	mqtt.HookBase
	rdb *redis.Client
	log *slog.Logger
}

// NewRedisSessionHook creates a hook using a pre-initialised Redis client.
func NewRedisSessionHook(rdb *redis.Client, log *slog.Logger) *RedisSessionHook {
	return &RedisSessionHook{rdb: rdb, log: log}
}

func (h *RedisSessionHook) ID() string { return "keel-redis-session" }

func (h *RedisSessionHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		byte(mqtt.OnSessionEstablished),
		byte(mqtt.OnDisconnect),
		byte(mqtt.OnSubscribed),
		byte(mqtt.OnUnsubscribed),
		byte(mqtt.OnQosPublish),
		byte(mqtt.OnQosComplete),
		byte(mqtt.OnQosDropped),
		byte(mqtt.StoredClients),
		byte(mqtt.StoredInflightMessages),
		byte(mqtt.StoredSubscriptions),
	}, []byte{b})
}

// Init is called by mochi-mqtt on hook registration.
func (h *RedisSessionHook) Init(_ any) error {
	// Ping to verify connectivity.
	if err := h.rdb.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("redis session hook: ping failed: %w", err)
	}
	h.log.Info("mqtt-gateway: Redis session hook ready")
	return nil
}

// ── write helpers ─────────────────────────────────────────────────────────────

func inflightFieldKey(cl *mqtt.Client, pk packets.Packet) string {
	return cl.ID + ":" + pk.FormatID()
}

func subFieldKey(cl *mqtt.Client, filter string) string {
	return cl.ID + ":" + filter
}

// ── write hooks ───────────────────────────────────────────────────────────────

// OnSessionEstablished persists the client record when a new session is opened.
func (h *RedisSessionHook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	h.saveClient(cl)
}

func (h *RedisSessionHook) saveClient(cl *mqtt.Client) {
	props := cl.Properties.Props.Copy(false)
	in := &storage.Client{
		ID:              cl.ID,
		T:               storage.ClientKey,
		Remote:          cl.Net.Remote,
		Listener:        cl.Net.Listener,
		Username:        cl.Properties.Username,
		Clean:           cl.Properties.Clean,
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Properties: storage.ClientProperties{
			SessionExpiryInterval: props.SessionExpiryInterval,
			AuthenticationMethod:  props.AuthenticationMethod,
			AuthenticationData:    props.AuthenticationData,
			RequestProblemInfo:    props.RequestProblemInfo,
			RequestResponseInfo:   props.RequestResponseInfo,
			ReceiveMaximum:        props.ReceiveMaximum,
			TopicAliasMaximum:     props.TopicAliasMaximum,
			User:                  props.User,
			MaximumPacketSize:     props.MaximumPacketSize,
		},
		Will: storage.ClientWill(cl.Properties.Will),
	}
	data, err := in.MarshalBinary()
	if err != nil {
		h.log.Error("redis session: marshal client", "error", err, "id", cl.ID)
		return
	}
	if err := h.rdb.HSet(context.Background(), redisClientHash, cl.ID, data).Err(); err != nil {
		h.log.Error("redis session: hset client", "error", err, "id", cl.ID)
	}
}

// OnDisconnect removes a client record when the session has expired (clean=true
// or explicit session expiry).  Non-expiring disconnects keep the record so the
// client can resume after a reconnect.
func (h *RedisSessionHook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	if !expire {
		return
	}
	if cl.StopCause() == packets.ErrSessionTakenOver {
		return
	}
	if err := h.rdb.HDel(context.Background(), redisClientHash, cl.ID).Err(); err != nil {
		h.log.Error("redis session: hdel client", "error", err, "id", cl.ID)
	}
}

// OnSubscribed persists one or more new subscriptions.
func (h *RedisSessionHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	ctx := context.Background()
	for i, f := range pk.Filters {
		in := &storage.Subscription{
			ID:                subFieldKey(cl, f.Filter),
			T:                 storage.SubscriptionKey,
			Client:            cl.ID,
			Filter:            f.Filter,
			Qos:               reasonCodes[i],
			Identifier:        f.Identifier,
			NoLocal:           f.NoLocal,
			RetainHandling:    f.RetainHandling,
			RetainAsPublished: f.RetainAsPublished,
		}
		data, err := in.MarshalBinary()
		if err != nil {
			h.log.Error("redis session: marshal subscription", "error", err)
			continue
		}
		if err := h.rdb.HSet(ctx, redisSubHash, subFieldKey(cl, f.Filter), data).Err(); err != nil {
			h.log.Error("redis session: hset subscription", "error", err)
		}
	}
}

// OnUnsubscribed removes subscriptions from the store.
func (h *RedisSessionHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	ctx := context.Background()
	for _, f := range pk.Filters {
		if err := h.rdb.HDel(ctx, redisSubHash, subFieldKey(cl, f.Filter)).Err(); err != nil {
			h.log.Error("redis session: hdel subscription", "error", err)
		}
	}
}

// OnQosPublish persists an in-flight QoS≥1 message.
func (h *RedisSessionHook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, sent int64, _ int) {
	props := pk.Properties.Copy(false)
	in := &storage.Message{
		ID:          inflightFieldKey(cl, pk),
		T:           storage.InflightKey,
		Client:      cl.ID,
		Origin:      pk.Origin,
		FixedHeader: pk.FixedHeader,
		TopicName:   pk.TopicName,
		Payload:     pk.Payload,
		Sent:        sent,
		Created:     pk.Created,
		Properties: storage.MessageProperties{
			PayloadFormat:          props.PayloadFormat,
			MessageExpiryInterval:  props.MessageExpiryInterval,
			ContentType:            props.ContentType,
			ResponseTopic:          props.ResponseTopic,
			CorrelationData:        props.CorrelationData,
			SubscriptionIdentifier: props.SubscriptionIdentifier,
			TopicAlias:             props.TopicAlias,
			User:                   props.User,
		},
	}
	data, err := in.MarshalBinary()
	if err != nil {
		h.log.Error("redis session: marshal inflight", "error", err)
		return
	}
	if err := h.rdb.HSet(context.Background(), redisInflightHash, inflightFieldKey(cl, pk), data).Err(); err != nil {
		h.log.Error("redis session: hset inflight", "error", err)
	}
}

// OnQosComplete removes a successfully acknowledged in-flight message.
func (h *RedisSessionHook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	if err := h.rdb.HDel(context.Background(), redisInflightHash, inflightFieldKey(cl, pk)).Err(); err != nil {
		h.log.Error("redis session: hdel inflight", "error", err)
	}
}

// OnQosDropped removes an expired in-flight message.
func (h *RedisSessionHook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	h.OnQosComplete(cl, pk)
}

// ── restore on startup ────────────────────────────────────────────────────────

// StoredClients returns all client records persisted in Redis.
// Called by mochi-mqtt during server startup to restore clean and persistent sessions.
func (h *RedisSessionHook) StoredClients() ([]storage.Client, error) {
	rows, err := h.rdb.HGetAll(context.Background(), redisClientHash).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis session: HGetAll clients: %w", err)
	}
	out := make([]storage.Client, 0, len(rows))
	for _, raw := range rows {
		var c storage.Client
		if err := c.UnmarshalBinary([]byte(raw)); err != nil {
			h.log.Error("redis session: unmarshal client", "error", err)
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

// StoredSubscriptions returns all subscription records persisted in Redis.
func (h *RedisSessionHook) StoredSubscriptions() ([]storage.Subscription, error) {
	rows, err := h.rdb.HGetAll(context.Background(), redisSubHash).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis session: HGetAll subscriptions: %w", err)
	}
	out := make([]storage.Subscription, 0, len(rows))
	for _, raw := range rows {
		var s storage.Subscription
		if err := s.UnmarshalBinary([]byte(raw)); err != nil {
			h.log.Error("redis session: unmarshal subscription", "error", err)
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// StoredInflightMessages returns all in-flight messages persisted in Redis.
func (h *RedisSessionHook) StoredInflightMessages() ([]storage.Message, error) {
	rows, err := h.rdb.HGetAll(context.Background(), redisInflightHash).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis session: HGetAll inflight: %w", err)
	}
	out := make([]storage.Message, 0, len(rows))
	for _, raw := range rows {
		var m storage.Message
		if err := m.UnmarshalBinary([]byte(raw)); err != nil {
			h.log.Error("redis session: unmarshal inflight", "error", err)
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// StoredRetainedMessages returns an empty slice — retained messages are not
// persisted by this hook (they are handled by the mochi-mqtt in-memory store).
func (h *RedisSessionHook) StoredRetainedMessages() ([]storage.Message, error) {
	return nil, nil
}
