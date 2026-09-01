// Publish delivery for offline sessions — see this package's doc.
package session

import (
	"log/slog"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

// InflightMessage is the minimal payload persisted for an offline
// session's queued QoS1/2 delivery — smaller than mochi-mqtt's own
// storage.Message, no MQTT5 properties or retry bookkeeping.
type InflightMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

// OfflineDelivery persists an inbound publish for offline sessions
// directly to Redis, bypassing mqtt.Client entirely.
type OfflineDelivery struct {
	// Queue atomically allocates a non-colliding packet ID and persists msg.
	// Allocation and insertion must not be split: a live connection can write
	// the same packet-ID space between two independent Redis operations.
	Queue func(clientID string, msg InflightMessage) (packetID uint16, queued bool, err error)

	Log *slog.Logger
}

// Deliver matches an inbound publish against every subscription of every
// session in owned — this node's own share, never the whole fleet — and
// enqueues it wherever the effective QoS (min(publish, subscription),
// maxed across matching filters) is 1 or 2. QoS 0 is never queued, same
// as mqtt.Client's own inflight tracking for live clients. A session
// matching multiple filters is enqueued once, at the highest QoS.
//
// Best-effort per session: a Queue failure skips that session, never aborts
// the rest of owned. queued=false means a deduplicated delivery and is not an
// error.
func (d *OfflineDelivery) Deliver(owned []OfflineSession, topic string, payload []byte, publishQoS byte) (delivered int) {
	for _, s := range owned {
		qos, matched := bestMatchQoS(s, topic, publishQoS)
		if !matched || qos == 0 {
			continue
		}

		msg := InflightMessage{Topic: topic, Payload: payload, QoS: qos}
		packetID, queued, err := d.Queue(s.ClientID, msg)
		if err != nil {
			d.logWarn("session: offline delivery enqueue failed", "client_id", s.ClientID, "error", err)
			continue
		}
		if !queued {
			continue
		}
		_ = packetID // retained in the API for logging/diagnostics by callers
		delivered++
	}
	return delivered
}

// bestMatchQoS returns the highest effective QoS (min(publishQoS,
// sub.QoS) per MQTT-3.3.5's downgrade rule, maxed across every
// subscription of s that matches topic) and whether anything matched at
// all.
func bestMatchQoS(s OfflineSession, topic string, publishQoS byte) (qos byte, matched bool) {
	for _, sub := range s.Subscriptions {
		if !acl.MatchTopic(sub.Filter, topic) {
			continue
		}
		matched = true
		effective := sub.QoS
		if publishQoS < effective {
			effective = publishQoS
		}
		if effective > qos {
			qos = effective
		}
	}
	return qos, matched
}

func (d *OfflineDelivery) logWarn(msg string, args ...any) {
	if d.Log != nil {
		d.Log.Warn(msg, args...)
	}
}
