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

const offlineDeliveryTimeout = 5 * time.Second

// offlineDeliveryRegistry is the narrow slice of keelraft.Registry
// DeliverOffline needs — defined locally rather than depending on the
// full Registry interface, same reasoning as sessionStore/
// offlineOwnershipStore above.
type offlineDeliveryRegistry interface {
	OwnedClientIDs(nodeID string) []string
}

type offlineCandidateSelector interface {
	OfflineDeliveryCandidates(topic, nodeID string) ([]string, map[string]string)
}

type offlineSharedSelector interface {
	OfflineSharedClientsFor(topic, nodeID string) map[string]string
}

// offlineDeliveryStore is satisfied by *RedisSessionHook — narrowed for
// testability, same pattern as sessionStore above.
type offlineDeliveryStore interface {
	OwnedOfflineSessions(ownedClientIDs []string) ([]session.OfflineSession, error)
	QueueOfflineInflight(ctx context.Context, publishID uuid.UUID, clientID string, ttl time.Duration, msg session.InflightMessage) (packetID uint16, queued bool, err error)
}

// DeliverOffline selects this node's offline candidates using local routing
// when available, then checks persisted subscriptions and enqueues each
// match into Redis, deduplicated per publishID via store.MarkDelivered.
// Used both for a publish forwarded from another node (the inbound
// dataplane handler in cmd/server) and for this node's own local
// publishes (keelHook.OnPublish) — a locally-owned session never needs a
// network round trip either way, so both call sites share this one path.
// No-op when registry or store is nil (standalone mode, or Redis not
// configured).
func DeliverOffline(ctx context.Context, registry offlineDeliveryRegistry, store offlineDeliveryStore, nodeID string, publishID uuid.UUID, topic string, payload []byte, qos byte, dedupTTL time.Duration, log *slog.Logger) {
	if registry == nil || store == nil || qos == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// The inbound gRPC context is bounded by the dataplane forwarding timeout.
	// Redis offline persistence must not be canceled merely because the sender
	// stopped waiting for the RPC response; otherwise a temporary contention
	// burst turns into an unpersisted QoS1/2 delivery. Keep a bounded, detached
	// context so the operation still stops during a real Redis stall.
	queueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), offlineDeliveryTimeout)
	defer cancel()
	// Always provide a decision, including an empty one. Falling back to local
	// selection would enqueue shared copies despite live members on other edges.
	sharedSelected := make(map[string]string)
	var owned []string
	if selector, ok := registry.(offlineCandidateSelector); ok {
		var selected map[string]string
		owned, selected = selector.OfflineDeliveryCandidates(topic, nodeID)
		for filter, clientID := range selected {
			sharedSelected[filter] = clientID
		}
	} else {
		// Compatibility for registry implementations without local selection.
		owned = registry.OwnedClientIDs(nodeID)
		if selector, ok := registry.(offlineSharedSelector); ok {
			for filter, clientID := range selector.OfflineSharedClientsFor(topic, nodeID) {
				sharedSelected[filter] = clientID
			}
		}
	}
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
		SharedSelected: sharedSelected,
		Queue: func(clientID string, msg session.InflightMessage) (uint16, bool, error) {
			packetID, queued, err := store.QueueOfflineInflight(queueCtx, publishID, clientID, dedupTTL, msg)
			if err != nil {
				return 0, false, err
			}
			if !queued {
				telemetry.DeduplicationsTotal.Inc()
				return 0, false, nil
			}
			telemetry.MessagesForwarded.WithLabelValues(strconv.Itoa(int(qos)), "offline").Inc()
			return packetID, true, nil
		},
		Log: log,
	}
	delivery.Deliver(sessions, topic, payload, qos)
}
