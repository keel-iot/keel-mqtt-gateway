// Package broker — RedisSessionHook persists MQTT sessions, subscriptions, and
// in-flight QoS≥1 messages to Redis so that the gateway can be replaced or
// restarted without losing session state (required for horizontal scaling and
// rolling deploys).
//
// This hook is optional: when no Redis client is injected the broker falls back
// to its default in-memory behaviour.
//
// Key schema (v2)
//
//	keel:gw:CLIENTS          — SET of client IDs (inventory only)
//	keel:gw:CL:<client>      — storage.Client value
//	keel:gw:SUB:<client>     — HASH of storage.Subscription (field: filter)
//	keel:gw:IFM:<client>     — HASH of storage.Message (field: packet ID)
//	keel:gw:PKID:<client>    — offline packet-ID counter
//
// <client> is base64url encoded. Keeping each session in its own Redis keys
// makes reconnect hydration O(size of that session), rather than forcing an
// HSCAN over a fleet-wide hash for every reconnect.
package broker

import (
	"bytes"
	"context"
	"encoding/base64"
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
	redisKeyPrefix       = "keel:gw:"
	redisClientIndex     = redisKeyPrefix + "CLIENTS"
	redisMigrationLock   = redisKeyPrefix + "MIGRATE:V2"
	redisScanCount       = int64(256)
	legacyClientHash     = redisKeyPrefix + storage.ClientKey
	legacySubHash        = redisKeyPrefix + storage.SubscriptionKey
	legacyInflightHash   = redisKeyPrefix + storage.InflightKey
	legacyPacketIDPrefix = redisKeyPrefix + "PKID:"
)

func redisClientToken(clientID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(clientID))
}

func redisClientKey(clientID string) string {
	return redisKeyPrefix + storage.ClientKey + ":" + redisClientToken(clientID)
}

func redisSubKey(clientID string) string {
	return redisKeyPrefix + storage.SubscriptionKey + ":" + redisClientToken(clientID)
}

func redisInflightKey(clientID string) string {
	return redisKeyPrefix + storage.InflightKey + ":" + redisClientToken(clientID)
}

func redisPacketIDKey(clientID string) string {
	return redisKeyPrefix + "PKID:" + redisClientToken(clientID)
}

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

// Provides deliberately omits StoredClients/StoredSubscriptions/
// StoredInflightMessages: mochi-mqtt's own readStore() would otherwise
// eagerly load and materialize every persisted client fleet-wide as an
// in-memory *mqtt.Client at every boot, regardless of whether this node
// serves or owns any of them — the root cause of a real OOM incident.
// Rehydration now happens lazily, per client, on actual reconnect (see
// OnSessionEstablish), and offline delivery goes straight to Redis (see
// DeliverOffline), so no in-memory Client is ever needed for a session
// nobody is connected to. The three methods stay defined below (mqtt.Hook
// requires them structurally) but are otherwise unreachable from
// mochi-mqtt now; the first two are still used directly by OfflineInventory.
func (h *RedisSessionHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		byte(mqtt.OnSessionEstablished),
		byte(mqtt.OnDisconnect),
		byte(mqtt.OnSubscribed),
		byte(mqtt.OnUnsubscribed),
		byte(mqtt.OnQosPublish),
		byte(mqtt.OnQosComplete),
		byte(mqtt.OnQosDropped),
		byte(mqtt.OnClientExpired),
	}, []byte{b})
}

// Init is called by mochi-mqtt on hook registration.
func (h *RedisSessionHook) Init(_ any) error {
	ctx := context.Background()
	// Ping to verify connectivity.
	if err := h.router.Client().Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis session hook: ping failed: %w", err)
	}
	if err := h.migrateLegacySchema(ctx); err != nil {
		return fmt.Errorf("redis session hook: migrate v1 schema: %w", err)
	}
	h.log.Info("mqtt-gateway: Redis session hook ready")
	return nil
}

// migrateLegacySchema converts the original three fleet-wide hashes into the
// per-client v2 keys. HSCAN bounds every Redis reply; a distributed lock keeps
// concurrently starting gateway nodes from duplicating the migration.
//
// The migration is intended for a coordinated gateway restart. Mixed binaries
// cannot safely share the two schemas because a v1 node has no knowledge of
// the v2 keys.
func (h *RedisSessionHook) migrateLegacySchema(ctx context.Context) error {
	rdb := h.router.Client()
	legacy, err := rdb.Exists(ctx, legacyClientHash, legacySubHash, legacyInflightHash).Result()
	if err != nil || legacy == 0 {
		return err
	}

	token := uuid.NewString()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		acquired, err := rdb.SetNX(ctx, redisMigrationLock, token, 30*time.Minute).Result()
		if err != nil {
			return err
		}
		if acquired {
			break
		}
		remaining, err := rdb.Exists(ctx, legacyClientHash, legacySubHash, legacyInflightHash).Result()
		if err != nil {
			return err
		}
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for another node to migrate Redis session schema")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer func() {
		const unlock = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`
		_ = rdb.Eval(context.Background(), unlock, []string{redisMigrationLock}, token).Err()
	}()

	migratedClients := make(map[string]struct{})
	if err := h.migrateLegacyHash(ctx, legacyClientHash, func(pipe redis.Pipeliner, _ string, raw string) error {
		var client storage.Client
		if err := client.UnmarshalBinary([]byte(raw)); err != nil {
			return fmt.Errorf("unmarshal legacy client: %w", err)
		}
		pipe.Set(ctx, redisClientKey(client.ID), raw, 0)
		pipe.SAdd(ctx, redisClientIndex, client.ID)
		migratedClients[client.ID] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if err := h.migrateLegacyHash(ctx, legacySubHash, func(pipe redis.Pipeliner, _ string, raw string) error {
		var sub storage.Subscription
		if err := sub.UnmarshalBinary([]byte(raw)); err != nil {
			return fmt.Errorf("unmarshal legacy subscription: %w", err)
		}
		pipe.HSet(ctx, redisSubKey(sub.Client), sub.Filter, raw)
		pipe.SAdd(ctx, redisClientIndex, sub.Client)
		migratedClients[sub.Client] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if err := h.migrateLegacyHash(ctx, legacyInflightHash, func(pipe redis.Pipeliner, _ string, raw string) error {
		var msg storage.Message
		if err := msg.UnmarshalBinary([]byte(raw)); err != nil {
			return fmt.Errorf("unmarshal legacy inflight: %w", err)
		}
		pipe.HSet(ctx, redisInflightKey(msg.Client), strconv.FormatUint(uint64(msg.PacketID), 10), raw)
		pipe.SAdd(ctx, redisClientIndex, msg.Client)
		migratedClients[msg.Client] = struct{}{}
		return nil
	}); err != nil {
		return err
	}

	pipe := rdb.TxPipeline()
	for clientID := range migratedClients {
		oldKey := legacyPacketIDPrefix + clientID
		n, err := rdb.Get(ctx, oldKey).Result()
		if err == nil {
			pipe.Set(ctx, redisPacketIDKey(clientID), n, 0)
			if oldKey != redisPacketIDKey(clientID) {
				pipe.Del(ctx, oldKey)
			}
		} else if !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read legacy packet-ID counter for %s: %w", clientID, err)
		}
	}
	pipe.Del(ctx, legacyClientHash, legacySubHash, legacyInflightHash)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("finalize legacy session migration: %w", err)
	}
	if h.log != nil {
		h.log.Info("redis session: migrated v1 fleet-wide hashes to v2 per-client keys", "clients", len(migratedClients))
	}
	return nil
}

func (h *RedisSessionHook) migrateLegacyHash(ctx context.Context, key string, add func(redis.Pipeliner, string, string) error) error {
	rdb := h.router.Client()
	var cursor uint64
	for {
		rows, next, err := rdb.HScan(ctx, key, cursor, "*", redisScanCount).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("scan legacy hash %s: %w", key, err)
		}
		pipe := rdb.Pipeline()
		for i := 0; i+1 < len(rows); i += 2 {
			if err := add(pipe, rows[i], rows[i+1]); err != nil {
				return fmt.Errorf("migrate legacy hash %s field %s: %w", key, rows[i], err)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("write migrated hash %s page: %w", key, err)
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// ── write helpers ─────────────────────────────────────────────────────────────

func inflightFieldKey(cl *mqtt.Client, pk packets.Packet) string {
	return pk.FormatID()
}

func subFieldKey(cl *mqtt.Client, filter string) string {
	return filter
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
	ctx := context.Background()
	pipe := h.router.Client().TxPipeline()
	pipe.Set(ctx, redisClientKey(cl.ID), data, 0)
	pipe.SAdd(ctx, redisClientIndex, cl.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		h.log.Error("redis session: persist client", "error", err, "id", cl.ID)
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
	h.deleteSessionState(context.Background(), cl.ID)
}

// OnClientExpired is mochi-mqtt's callback for a persistent
// (clean_session=false) session's delayed expiry — the periodic sweep
// in Server.clearExpiredClients, distinct from OnDisconnect's expire
// path above (which only fires immediately for a session that was never
// meant to persist at all: clean session, or MQTT5 SessionExpiryInterval
// 0). Before this existed, RedisSessionHook didn't advertise this hook
// at all (see Provides), so a persistent session's Redis-side state —
// client record, subscriptions, in-flight messages — was never cleaned
// up on real expiry, only on the narrower immediate-expiry path, and
// even that path only cleared the client record, not subscriptions or
// in-flight state. Reproducible with zero clustering: this hook has no
// ClusterRegistry dependency anywhere in this file.
func (h *RedisSessionHook) OnClientExpired(cl *mqtt.Client) {
	h.deleteSessionState(context.Background(), cl.ID)
}

// deleteSessionState removes every Redis key this hook wrote for
// clientID: the client record, every subscription, and every in-flight
// message — the full set an expired session's Redis footprint should
// leave behind is none of it.
func (h *RedisSessionHook) deleteSessionState(ctx context.Context, clientID string) {
	pipe := h.router.Client().TxPipeline()
	pipe.Del(ctx,
		redisClientKey(clientID),
		redisSubKey(clientID),
		redisInflightKey(clientID),
		redisPacketIDKey(clientID),
	)
	pipe.SRem(ctx, redisClientIndex, clientID)
	if _, err := pipe.Exec(ctx); err != nil {
		h.log.Error("redis session: delete session state", "error", err, "id", clientID)
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
		if err := h.router.Client().HSet(ctx, redisSubKey(cl.ID), subFieldKey(cl, f.Filter), data).Err(); err != nil {
			h.log.Error("redis session: hset subscription", "error", err)
		}
	}
}

// OnUnsubscribed removes subscriptions from the store.
func (h *RedisSessionHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	ctx := context.Background()
	for _, f := range pk.Filters {
		if err := h.router.Client().HDel(ctx, redisSubKey(cl.ID), subFieldKey(cl, f.Filter)).Err(); err != nil {
			h.log.Error("redis session: hdel subscription", "error", err)
		}
	}
}

// OnQosPublish persists an in-flight QoS≥1 message.
func (h *RedisSessionHook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, sent int64, resends int) {
	if resends > 0 {
		telemetry.RedeliveriesTotal.WithLabelValues(strconv.Itoa(int(pk.FixedHeader.Qos))).Inc()
	}
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
	if err := h.router.Client().HSet(context.Background(), redisInflightKey(cl.ID), inflightFieldKey(cl, pk), data).Err(); err != nil {
		h.log.Error("redis session: hset inflight", "error", err)
	}
}

// OnQosComplete removes a successfully acknowledged in-flight message.
func (h *RedisSessionHook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	if err := h.router.Client().HDel(context.Background(), redisInflightKey(cl.ID), inflightFieldKey(cl, pk)).Err(); err != nil {
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
	ctx := context.Background()
	ids, err := h.indexedClientIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis session: scan client index: %w", err)
	}
	out := make([]storage.Client, 0, len(ids))
	for _, clientID := range ids {
		raw, err := h.router.Client().Get(ctx, redisClientKey(clientID)).Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("redis session: get client %s: %w", clientID, err)
		}
		var c storage.Client
		if err := c.UnmarshalBinary(raw); err != nil {
			h.log.Error("redis session: unmarshal client", "error", err)
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

// StoredSubscriptions returns all subscription records persisted in Redis.
func (h *RedisSessionHook) StoredSubscriptions() ([]storage.Subscription, error) {
	ctx := context.Background()
	ids, err := h.indexedClientIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis session: scan client index: %w", err)
	}
	var out []storage.Subscription
	for _, clientID := range ids {
		rows, err := h.router.Client().HGetAll(ctx, redisSubKey(clientID)).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("redis session: hgetall subscriptions for %s: %w", clientID, err)
		}
		for _, raw := range rows {
			var s storage.Subscription
			if err := s.UnmarshalBinary([]byte(raw)); err != nil {
				h.log.Error("redis session: unmarshal subscription", "error", err)
				continue
			}
			out = append(out, s)
		}
	}
	return out, nil
}

// indexedClientIDs drains the small client-ID index with SSCAN. Session
// payloads are never stored in this set, so inventory cannot produce one
// unbounded Redis reply even for a large fleet.
func (h *RedisSessionHook) indexedClientIDs(ctx context.Context) ([]string, error) {
	var out []string
	var cursor uint64
	for {
		batch, next, err := h.router.Client().SScan(ctx, redisClientIndex, cursor, "*", redisScanCount).Result()
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

// OfflineInventory returns every persisted client's OfflineSession view,
// feeding session.Reconciler's Inventory function. The fleet index is consumed
// in bounded pages and each page fetches exact per-client keys through a
// pipeline. No command returns or scans the fleet's complete session payload.
func (h *RedisSessionHook) OfflineInventory() ([]session.OfflineSession, error) {
	ctx := context.Background()
	var out []session.OfflineSession
	var cursor uint64
	for {
		ids, next, err := h.router.Client().SScan(ctx, redisClientIndex, cursor, "*", redisScanCount).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("redis session: offline inventory scan: %w", err)
		}

		pipe := h.router.Client().Pipeline()
		clientCmds := make([]*redis.StringCmd, len(ids))
		subCmds := make([]*redis.MapStringStringCmd, len(ids))
		for i, clientID := range ids {
			clientCmds[i] = pipe.Get(ctx, redisClientKey(clientID))
			subCmds[i] = pipe.HGetAll(ctx, redisSubKey(clientID))
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("redis session: offline inventory page: %w", err)
		}
		for i, clientID := range ids {
			raw, err := clientCmds[i].Bytes()
			if errors.Is(err, redis.Nil) {
				continue // stale index member; expiry cleanup will remove it
			}
			if err != nil {
				return nil, fmt.Errorf("redis session: offline inventory client %s: %w", clientID, err)
			}
			var client storage.Client
			if err := client.UnmarshalBinary(raw); err != nil {
				h.log.Error("redis session: offline inventory unmarshal client", "client_id", clientID, "error", err)
				continue
			}
			subs := make([]storage.Subscription, 0, len(subCmds[i].Val()))
			for _, rawSub := range subCmds[i].Val() {
				var sub storage.Subscription
				if err := sub.UnmarshalBinary([]byte(rawSub)); err != nil {
					h.log.Error("redis session: offline inventory unmarshal subscription", "client_id", clientID, "error", err)
					continue
				}
				subs = append(subs, sub)
			}
			out = append(out, session.FromStorage(client.ID, subs))
		}
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
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
	key := redisDedupKeyPrefix + publishID.String() + ":" + redisClientToken(clientID)
	ok, err := h.router.Client().SetNX(ctx, key, 1, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis session: mark delivered: %w", err)
	}
	return ok, nil
}

// InflightCount returns the number of QoS1/2 inflight messages currently
// persisted in Redis. It pipelines one HLEN per indexed client; unlike the v1
// global hash this stays metadata-only and never transfers message payloads.
func (h *RedisSessionHook) InflightCount(ctx context.Context) (int64, error) {
	var total int64
	var cursor uint64
	for {
		ids, next, err := h.router.Client().SScan(ctx, redisClientIndex, cursor, "*", redisScanCount).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return 0, err
		}
		pipe := h.router.Client().Pipeline()
		cmds := make([]*redis.IntCmd, 0, len(ids))
		for _, clientID := range ids {
			cmds = append(cmds, pipe.HLen(ctx, redisInflightKey(clientID)))
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return 0, err
		}
		for _, cmd := range cmds {
			total += cmd.Val()
		}
		if next == 0 {
			return total, nil
		}
		cursor = next
	}
}

// StoredInflightMessages returns all in-flight messages persisted in Redis.
func (h *RedisSessionHook) StoredInflightMessages() ([]storage.Message, error) {
	ctx := context.Background()
	ids, err := h.indexedClientIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis session: scan client index: %w", err)
	}
	var out []storage.Message
	for _, clientID := range ids {
		rows, err := h.router.Client().HGetAll(ctx, redisInflightKey(clientID)).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("redis session: hgetall inflight for %s: %w", clientID, err)
		}
		for _, raw := range rows {
			var m storage.Message
			if err := m.UnmarshalBinary([]byte(raw)); err != nil {
				h.log.Error("redis session: unmarshal inflight", "error", err)
				continue
			}
			out = append(out, m)
		}
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
// these read only the keys for one clientID — used by keelHook when a
// persistent session reconnects to a node that never had it locally (e.g. a
// different node than the one that originally owned it, or the same node
// after MaximumSessionExpiryInterval already expired it locally but Redis
// still has it). The hashes contain only this client's fields, so HGETALL is
// bounded by one session and cannot scan unrelated devices.

// SubscriptionsForClient returns clientID's persisted subscriptions.
func (h *RedisSessionHook) SubscriptionsForClient(clientID string) ([]storage.Subscription, error) {
	rows, err := h.router.Client().HGetAll(context.Background(), redisSubKey(clientID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis session: HGetAll subscriptions for %s: %w", clientID, err)
	}
	out := make([]storage.Subscription, 0, len(rows))
	for _, raw := range rows {
		var s storage.Subscription
		if err := s.UnmarshalBinary([]byte(raw)); err != nil {
			h.log.Error("redis session: unmarshal subscription", "error", err, "client_id", clientID)
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// InflightForClient returns clientID's persisted in-flight QoS1/2 messages.
func (h *RedisSessionHook) InflightForClient(clientID string) ([]storage.Message, error) {
	rows, err := h.router.Client().HGetAll(context.Background(), redisInflightKey(clientID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis session: HGetAll inflight for %s: %w", clientID, err)
	}
	out := make([]storage.Message, 0, len(rows))
	for _, raw := range rows {
		var m storage.Message
		if err := m.UnmarshalBinary([]byte(raw)); err != nil {
			h.log.Error("redis session: unmarshal inflight", "error", err, "client_id", clientID)
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// ── offline session delivery ────────────────────────────────────────────────
// Backs OfflineDelivery.Queue for a session with no live mqtt.Client anywhere
// in the cluster.

// NextPacketID returns a persisted, monotonically increasing packet ID for
// clientID's offline inflight queue — Redis INCR rather than
// mqtt.Client's own NextPacketID, which needs a live Client object this
// path doesn't have. Wraps at 65535, skipping 0 (reserved per spec).
func (h *RedisSessionHook) NextPacketID(ctx context.Context, clientID string) (uint16, error) {
	n, err := h.router.Client().Incr(ctx, redisPacketIDKey(clientID)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis session: incr packet id for %s: %w", clientID, err)
	}
	// n starts at 1 on the first call and counts up forever; map it onto
	// the 65535 valid packet IDs (1..65535) so it cycles instead of
	// overflowing uint16.
	return uint16((n-1)%65535) + 1, nil
}

// EnqueueOfflineInflight is retained for focused storage tests and callers
// which already own a packet ID. HSetNX is deliberate: an allocator bug can
// now fail visibly but can never overwrite an existing live message.
func (h *RedisSessionHook) EnqueueOfflineInflight(ctx context.Context, clientID string, packetID uint16, msg session.InflightMessage) error {
	data, err := marshalOfflineInflight(clientID, packetID, msg)
	if err != nil {
		return err
	}
	field := strconv.FormatUint(uint64(packetID), 10)
	inserted, err := h.router.Client().HSetNX(ctx, redisInflightKey(clientID), field, data).Result()
	if err != nil {
		return fmt.Errorf("redis session: hsetnx offline inflight: %w", err)
	}
	if !inserted {
		return fmt.Errorf("redis session: packet id %d already in use for %s", packetID, clientID)
	}
	return nil
}

func marshalOfflineInflight(clientID string, packetID uint16, msg session.InflightMessage) ([]byte, error) {
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
		return nil, fmt.Errorf("redis session: marshal offline inflight: %w", err)
	}
	return data, nil
}

var errOfflineAlreadyDelivered = errors.New("offline publish already delivered")

// QueueOfflineInflight atomically deduplicates a publish, selects a packet ID
// that is not present in the client's persisted inflight hash, advances the
// counter, and stores the message. WATCH also detects a concurrent live
// OnQosPublish touching the same per-client hash, so the transaction retries
// instead of overwriting that live message.
func (h *RedisSessionHook) QueueOfflineInflight(ctx context.Context, publishID uuid.UUID, clientID string, ttl time.Duration, msg session.InflightMessage) (packetID uint16, queued bool, err error) {
	rdb := h.router.Client()
	inflightKey := redisInflightKey(clientID)
	counterKey := redisPacketIDKey(clientID)
	watchKeys := []string{inflightKey, counterKey}
	var dedupKey string
	if publishID != uuid.Nil {
		dedupKey = redisDedupKeyPrefix + publishID.String() + ":" + redisClientToken(clientID)
		watchKeys = append(watchKeys, dedupKey)
	}

	for attempt := 0; attempt < 32; attempt++ {
		err = rdb.Watch(ctx, func(tx *redis.Tx) error {
			if dedupKey != "" {
				exists, err := tx.Exists(ctx, dedupKey).Result()
				if err != nil {
					return err
				}
				if exists != 0 {
					return errOfflineAlreadyDelivered
				}
			}

			counter, err := tx.Get(ctx, counterKey).Int64()
			if errors.Is(err, redis.Nil) {
				counter = 0
			} else if err != nil {
				return err
			}

			var nextCounter int64
			for offset := int64(0); offset < 65535; offset++ {
				candidate := uint16((counter+offset)%65535) + 1
				used, err := tx.HExists(ctx, inflightKey, strconv.FormatUint(uint64(candidate), 10)).Result()
				if err != nil {
					return err
				}
				if !used {
					packetID = candidate
					nextCounter = counter + offset + 1
					break
				}
			}
			if packetID == 0 {
				return packets.ErrQuotaExceeded
			}

			data, err := marshalOfflineInflight(clientID, packetID, msg)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, counterKey, nextCounter, 0)
				pipe.HSet(ctx, inflightKey, strconv.FormatUint(uint64(packetID), 10), data)
				if dedupKey != "" {
					pipe.Set(ctx, dedupKey, 1, ttl)
				}
				return nil
			})
			return err
		}, watchKeys...)
		switch {
		case errors.Is(err, errOfflineAlreadyDelivered):
			return 0, false, nil
		case errors.Is(err, redis.TxFailedErr):
			packetID = 0
			continue
		case err != nil:
			return 0, false, fmt.Errorf("redis session: atomic offline enqueue for %s: %w", clientID, err)
		default:
			return packetID, true, nil
		}
	}
	return 0, false, fmt.Errorf("redis session: atomic offline enqueue for %s: too much concurrent contention", clientID)
}
