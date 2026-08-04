// Offline publish delivery — see this package's doc and
// keel-design-doc.md's "Offline Session Placement" ADR. Phase 5 of 6:
// the new publish path for offline sessions, still not wired into the
// gateway's real hooks — see OfflineDelivery's doc for what it will
// eventually be called with (this node's own owned share of sessions,
// resolved via Owner/Reconciler in earlier phases).
package session

import (
	"log/slog"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

// InflightMessage is the minimal payload persisted for an offline
// session's queued QoS1/2 delivery — deliberately smaller than
// mochi-mqtt's own storage.Message (no MQTT5 properties, no retry
// bookkeeping): those concerns belong to whatever writes this into
// Redis in a later phase, not to this package.
type InflightMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
}

// OfflineDelivery persists an inbound publish for offline sessions
// directly to Redis, bypassing mqtt.Client entirely — the piece that
// lets Deliver work without a live Client object for offline sessions,
// per this package's whole reason for existing.
type OfflineDelivery struct {
	// NextPacketID returns the next packet ID to use for clientID's
	// queued message. Deliberately NOT mochi-mqtt's own
	// Client.NextPacketID, which scans in-memory Inflight state on a
	// live Client object we don't have here by design — a later phase
	// backs this with a persisted, monotonic-per-client counter (e.g.
	// Redis INCR, wrapping at the 16-bit boundary, skipping 0).
	NextPacketID func(clientID string) (uint16, error)

	// Enqueue persists msg as packetID's inflight entry for clientID —
	// e.g. reusing RedisSessionHook's own keel:gw:IFM hash in a later
	// phase, so the client's *existing* lazy rehydration path
	// (OnSessionEstablish) picks it up transparently on reconnect,
	// wherever in the cluster that happens — no change needed there.
	Enqueue func(clientID string, packetID uint16, msg InflightMessage) error

	Log *slog.Logger
}

// Deliver matches an inbound publish against every subscription of every
// session in owned — the sessions THIS node currently owns (see
// OfflineSessionPlacement/Reconciler from earlier phases), never the
// whole fleet — and enqueues it for each session whose effective QoS
// (the standard MQTT downgrade rule: min(publish QoS, subscription QoS),
// maxed across every matching subscription of that session) is 1 or 2.
//
// QoS 0 is never queued for an offline session, matching the MQTT spec
// (only QoS1/2 survive a disconnect) and what mqtt.Client's own inflight
// tracking already does for live clients — not a Keel-specific choice.
// A session matching more than one filter is enqueued once, at the
// highest effective QoS among its matches, not once per matching filter.
//
// Best-effort per session: a NextPacketID or Enqueue failure for one
// session is logged and skipped, never aborts the rest of owned.
func (d *OfflineDelivery) Deliver(owned []OfflineSession, topic string, payload []byte, publishQoS byte) (delivered int) {
	for _, s := range owned {
		qos, matched := bestMatchQoS(s, topic, publishQoS)
		if !matched || qos == 0 {
			continue
		}

		packetID, err := d.NextPacketID(s.ClientID)
		if err != nil {
			d.logWarn("session: offline delivery packet ID allocation failed", "client_id", s.ClientID, "error", err)
			continue
		}
		msg := InflightMessage{Topic: topic, Payload: payload, QoS: qos}
		if err := d.Enqueue(s.ClientID, packetID, msg); err != nil {
			d.logWarn("session: offline delivery enqueue failed", "client_id", s.ClientID, "packet_id", packetID, "error", err)
			continue
		}
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
