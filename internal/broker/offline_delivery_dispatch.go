package broker

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// offlineDeliveryRegistry is the narrow slice of keelraft.Registry
// DeliverOffline needs — defined locally rather than depending on the
// full Registry interface, same reasoning as sessionStore/
// offlineOwnershipStore above.
type offlineDeliveryRegistry interface {
	OwnedClientIDs(nodeID string) []string
}

// offlineDeliveryStore is satisfied by *RedisSessionHook — narrowed for
// testability, same pattern as sessionStore above.
type offlineDeliveryStore interface {
	OwnedOfflineSessions(ownedClientIDs []string) ([]session.OfflineSession, error)
	NextPacketID(ctx context.Context, clientID string) (uint16, error)
	EnqueueOfflineInflight(ctx context.Context, clientID string, packetID uint16, msg session.InflightMessage) error
	MarkDelivered(ctx context.Context, publishID uuid.UUID, clientID string, ttl time.Duration) (bool, error)
}

// DeliverOffline checks nodeID's own owned offline sessions (per
// registry.OwnedClientIDs) for a match against topic and enqueues each
// match into Redis, deduplicated per publishID via store.MarkDelivered.
// Used both for a publish forwarded from another node (the inbound
// dataplane handler in cmd/server) and for this node's own local
// publishes (keelHook.OnPublish) — a locally-owned session never needs a
// network round trip either way, so both call sites share this one path.
// No-op when registry or store is nil (standalone mode, or Redis not
// configured).
func DeliverOffline(ctx context.Context, registry offlineDeliveryRegistry, store offlineDeliveryStore, nodeID string, publishID uuid.UUID, topic string, payload []byte, qos byte, dedupTTL time.Duration, log *slog.Logger) {
	if registry == nil || store == nil {
		return
	}
	owned := registry.OwnedClientIDs(nodeID)
	if len(owned) == 0 {
		return
	}
	sessions, err := store.OwnedOfflineSessions(owned)
	if err != nil {
		if log != nil {
			log.Error("cluster: offline delivery: owned sessions fetch failed", "error", err)
		}
		return
	}
	delivery := &session.OfflineDelivery{
		NextPacketID: func(clientID string) (uint16, error) {
			return store.NextPacketID(ctx, clientID)
		},
		Enqueue: func(clientID string, packetID uint16, msg session.InflightMessage) error {
			first, err := store.MarkDelivered(ctx, publishID, clientID, dedupTTL)
			if err != nil {
				return err
			}
			if !first {
				telemetry.DeduplicationsTotal.Inc()
				return nil // already delivered for this publish event elsewhere
			}
			if err := store.EnqueueOfflineInflight(ctx, clientID, packetID, msg); err != nil {
				return err
			}
			telemetry.MessagesForwarded.WithLabelValues(strconv.Itoa(int(qos)), "offline").Inc()
			return nil
		},
		Log: log,
	}
	delivery.Deliver(sessions, topic, payload, qos)
}
