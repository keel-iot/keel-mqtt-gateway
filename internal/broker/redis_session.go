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
	"strconv"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/redis/go-redis/v9"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
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
	router *redisrouter.Router
	log    *slog.Logger
}

// NewRedisSessionHook creates a hook using a pre-initialised Redis router
// (see internal/cluster/redisrouter — the single indirection point every
// Redis consumer goes through, so a primary failover updates them all at
// once instead of needing a separate swap site here).
func NewRedisSessionHook(router *redisrouter.Router, log *slog.Logger) *RedisSessionHook {
	return &RedisSessionHook{router: router, log: log}
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
	if err := h.router.Client().Ping(context.Background()).Err(); err != nil {
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
	if err := h.router.Client().HSet(context.Background(), redisClientHash, cl.ID, data).Err(); err != nil {
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
	if err := h.router.Client().HDel(context.Background(), redisClientHash, cl.ID).Err(); err != nil {
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
		if err := h.router.Client().HSet(ctx, redisSubHash, subFieldKey(cl, f.Filter), data).Err(); err != nil {
			h.log.Error("redis session: hset subscription", "error", err)
		}
	}
}

// OnUnsubscribed removes subscriptions from the store.
func (h *RedisSessionHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	ctx := context.Background()
	for _, f := range pk.Filters {
		if err := h.router.Client().HDel(ctx, redisSubHash, subFieldKey(cl, f.Filter)).Err(); err != nil {
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
		PacketID:    pk.PacketID,
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
	if err := h.router.Client().HSet(context.Background(), redisInflightHash, inflightFieldKey(cl, pk), data).Err(); err != nil {
		h.log.Error("redis session: hset inflight", "error", err)
	}
}

// OnQosComplete removes a successfully acknowledged in-flight message.
func (h *RedisSessionHook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	if err := h.router.Client().HDel(context.Background(), redisInflightHash, inflightFieldKey(cl, pk)).Err(); err != nil {
		h.log.Error("redis session: hdel inflight", "error", err)
	}
}

// OnQosDropped removes an expired in-flight message — real message loss,
// not just a delay (see telemetry.QosDropped's doc).
func (h *RedisSessionHook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	telemetry.QosDropped.WithLabelValues(strconv.Itoa(int(pk.FixedHeader.Qos))).Inc()
	h.OnQosComplete(cl, pk)
}

// ── restore on startup ────────────────────────────────────────────────────────

// StoredClients returns all client records persisted in Redis.
// Called by mochi-mqtt during server startup to restore clean and persistent sessions.
func (h *RedisSessionHook) StoredClients() ([]storage.Client, error) {
	rows, err := h.router.Client().HGetAll(context.Background(), redisClientHash).Result()
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
	rows, err := h.router.Client().HGetAll(context.Background(), redisSubHash).Result()
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

// OfflineInventory returns every persisted client's OfflineSession view,
// feeding session.Reconciler's Inventory function. Reuses
// StoredClients/StoredSubscriptions rather than a new query — same
// full-fleet view, and the Reconciler already runs on its own poll
// interval.
func (h *RedisSessionHook) OfflineInventory() ([]session.OfflineSession, error) {
	clients, err := h.StoredClients()
	if err != nil {
		return nil, fmt.Errorf("redis session: offline inventory clients: %w", err)
	}
	subs, err := h.StoredSubscriptions()
	if err != nil {
		return nil, fmt.Errorf("redis session: offline inventory subscriptions: %w", err)
	}
	return session.AllFromStorage(clients, subs), nil
}

// OwnedOfflineSessions returns the OfflineSession view for exactly
// ownedClientIDs (see keelraft.Registry.OwnedClientIDs) — one targeted
// SubscriptionsForClient lookup per owned client, never a fleet-wide
// scan. Used on the inbound side of offline delivery, where the set of
// candidates is already bounded to what this node owns.
func (h *RedisSessionHook) OwnedOfflineSessions(ownedClientIDs []string) ([]session.OfflineSession, error) {
	out := make([]session.OfflineSession, 0, len(ownedClientIDs))
	for _, clientID := range ownedClientIDs {
		subs, err := h.SubscriptionsForClient(clientID)
		if err != nil {
			return nil, fmt.Errorf("redis session: owned offline sessions for %s: %w", clientID, err)
		}
		out = append(out, session.FromStorage(clientID, subs))
	}
	return out, nil
}

// redisDedupKeyPrefix namespaces the offline-delivery dedup markers below
// from the CL/SUB/IFM/PKID hashes.
const redisDedupKeyPrefix = redisKeyPrefix + "PUBDEDUP:"

// MarkDelivered records that publishID has been delivered to clientID,
// returning true the first time (the caller should proceed with
// delivery) and false if it was already marked (a duplicate delivery
// attempt for the same MQTT PUBLISH event, e.g. during the brief window
// where two nodes are both registered as offline owner — see
// OfflineOwnership.Place). ttl bounds how long the marker survives; it
// only needs to outlive that handoff window, not the message itself.
//
// A zero publishID (no PublishID was ever set — e.g. a peer mid rolling
// upgrade that predates this field) always returns true: there is
// nothing to dedup against, so the caller proceeds and accepts the
// at-least-once delivery QoS1/2 already tolerate.
func (h *RedisSessionHook) MarkDelivered(ctx context.Context, publishID uuid.UUID, clientID string, ttl time.Duration) (firstTime bool, err error) {
	if publishID == uuid.Nil {
		return true, nil
	}
	key := redisDedupKeyPrefix + publishID.String() + ":" + clientID
	ok, err := h.router.Client().SetNX(ctx, key, 1, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis session: mark delivered: %w", err)
	}
	return ok, nil
}

// StoredInflightMessages returns all in-flight messages persisted in Redis.
func (h *RedisSessionHook) StoredInflightMessages() ([]storage.Message, error) {
	rows, err := h.router.Client().HGetAll(context.Background(), redisInflightHash).Result()
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

// ── per-client lookup (session rehydration on reconnect) ─────────────────────
//
// Unlike Stored*() above (called once, at process boot, over every client),
// these scan only the hash fields for one clientID — used by keelHook when a
// persistent session reconnects to a node that never had it locally (e.g. a
// different node than the one that originally owned it, or the same node
// after MaximumSessionExpiryInterval already expired it locally but Redis
// still has it). Field keys are "clientID:filter" / "clientID:packetID" (see
// subFieldKey/inflightFieldKey), so HScan's MATCH pattern lets Redis do the
// filtering server-side instead of fetching every client's data.

// hscanAll drains every field/value pair matching pattern from key,
// following HScan's cursor until exhausted.
func (h *RedisSessionHook) hscanAll(ctx context.Context, key, pattern string) ([]string, error) {
	var out []string
	var cursor uint64
	for {
		batch, next, err := h.router.Client().HScan(ctx, key, cursor, pattern, 100).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		out = append(out, batch...)
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

// SubscriptionsForClient returns clientID's persisted subscriptions.
func (h *RedisSessionHook) SubscriptionsForClient(clientID string) ([]storage.Subscription, error) {
	fieldVals, err := h.hscanAll(context.Background(), redisSubHash, clientID+":*")
	if err != nil {
		return nil, fmt.Errorf("redis session: HScan subscriptions for %s: %w", clientID, err)
	}
	out := make([]storage.Subscription, 0, len(fieldVals)/2)
	for i := 1; i < len(fieldVals); i += 2 { // odd indices are values, even are field names
		var s storage.Subscription
		if err := s.UnmarshalBinary([]byte(fieldVals[i])); err != nil {
			h.log.Error("redis session: unmarshal subscription", "error", err, "client_id", clientID)
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// InflightForClient returns clientID's persisted in-flight QoS1/2 messages.
func (h *RedisSessionHook) InflightForClient(clientID string) ([]storage.Message, error) {
	fieldVals, err := h.hscanAll(context.Background(), redisInflightHash, clientID+":*")
	if err != nil {
		return nil, fmt.Errorf("redis session: HScan inflight for %s: %w", clientID, err)
	}
	out := make([]storage.Message, 0, len(fieldVals)/2)
	for i := 1; i < len(fieldVals); i += 2 {
		var m storage.Message
		if err := m.UnmarshalBinary([]byte(fieldVals[i])); err != nil {
			h.log.Error("redis session: unmarshal inflight", "error", err, "client_id", clientID)
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// ── offline session delivery ────────────────────────────────────────────────
// Backs OfflineDelivery.NextPacketID/Enqueue for a session with no live
// mqtt.Client anywhere in the cluster.

// redisPacketIDKeyPrefix namespaces the per-client packet-ID counters below
// from the CL/SUB/IFM hashes — a plain counter, not a hash field.
const redisPacketIDKeyPrefix = redisKeyPrefix + "PKID:"

// NextPacketID returns a persisted, monotonically increasing packet ID for
// clientID's offline inflight queue — Redis INCR rather than
// mqtt.Client's own NextPacketID, which needs a live Client object this
// path doesn't have. Wraps at 65535, skipping 0 (reserved per spec).
func (h *RedisSessionHook) NextPacketID(ctx context.Context, clientID string) (uint16, error) {
	n, err := h.router.Client().Incr(ctx, redisPacketIDKeyPrefix+clientID).Result()
	if err != nil {
		return 0, fmt.Errorf("redis session: incr packet id for %s: %w", clientID, err)
	}
	// n starts at 1 on the first call and counts up forever; map it onto
	// the 65535 valid packet IDs (1..65535) so it cycles instead of
	// overflowing uint16.
	return uint16((n-1)%65535) + 1, nil
}

// EnqueueOfflineInflight writes into the same Redis hash (keel:gw:IFM)
// OnQosPublish uses for live clients, so InflightForClient/
// OnSessionEstablish picks it up transparently on reconnect, wherever
// that happens, with no changes needed there.
func (h *RedisSessionHook) EnqueueOfflineInflight(ctx context.Context, clientID string, packetID uint16, msg session.InflightMessage) error {
	in := &storage.Message{
		ID:        clientID + ":" + strconv.FormatUint(uint64(packetID), 10),
		T:         storage.InflightKey,
		Client:    clientID,
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		PacketID:  packetID,
		Created:   time.Now().Unix(),
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  msg.QoS,
		},
	}
	data, err := in.MarshalBinary()
	if err != nil {
		return fmt.Errorf("redis session: marshal offline inflight: %w", err)
	}
	if err := h.router.Client().HSet(ctx, redisInflightHash, in.ID, data).Err(); err != nil {
		return fmt.Errorf("redis session: hset offline inflight: %w", err)
	}
	return nil
}
