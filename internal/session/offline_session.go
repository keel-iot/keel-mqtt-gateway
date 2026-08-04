// Package session models MQTT state that outlives a live connection —
// distinct from mqtt.Client (github.com/mochi-mqtt/server/v2), which
// represents a live transport (socket, goroutines, keepalive). MQTT
// conflates "connection" and "session" into that one object because
// mochi-mqtt is designed as a standalone broker; Keel is a distributed
// system built on top of it, and needs to represent a persistent session
// that has no live connection anywhere in the cluster right now.
//
// See keel-design-doc.md's "Offline Session Placement — Proposed
// Architecture" for the full design this package implements incrementally
// (phase 1 of 6: this type only, no behavior change yet — nothing in the
// gateway constructs or consumes it outside its own tests until a later
// phase wires it in).
package session

import (
	"github.com/mochi-mqtt/server/v2/hooks/storage"
)

// OfflineSubscription is one topic filter an offline session is
// subscribed to — enough to re-match an incoming publish (see
// internal/cluster/acl.MatchTopic, reused rather than duplicated) without
// needing mochi-mqtt's own in-memory subscription trie, which only knows
// about live clients.
type OfflineSubscription struct {
	Filter string
	QoS    byte
}

// OfflineSession is a persistent MQTT session with no live connection
// anywhere in the cluster right now. Deliberately carries none of
// mqtt.Client's live-connection state — no socket, no goroutine, no
// channel, no keepalive — only what's needed to place it on an owner
// edge (see the design doc's OfflineSessionPlacement) and re-match
// publishes against its subscriptions once it's there.
type OfflineSession struct {
	ClientID      string
	Subscriptions []OfflineSubscription
}

// FromStorage builds the OfflineSession for clientID out of mochi-mqtt's
// own persisted-subscription rows (storage.Subscription, as returned by
// RedisSessionHook.StoredSubscriptions) — a pure, allocation-only
// transform, no I/O. Rows for other client_ids are ignored, so callers
// can pass the full fleet-wide slice without pre-filtering.
func FromStorage(clientID string, subs []storage.Subscription) OfflineSession {
	s := OfflineSession{ClientID: clientID}
	for _, sub := range subs {
		if sub.Client != clientID {
			continue
		}
		s.Subscriptions = append(s.Subscriptions, OfflineSubscription{
			Filter: sub.Filter,
			QoS:    sub.Qos,
		})
	}
	return s
}

// AllFromStorage builds one OfflineSession per distinct client_id present
// in clients, using subs to populate each one's Subscriptions. Mirrors
// what RedisSessionHook.StoredClients/StoredSubscriptions return today
// (the fleet-wide, unfiltered rows) — a later phase changes *what* calls
// this and *when*, not this transform itself.
func AllFromStorage(clients []storage.Client, subs []storage.Subscription) []OfflineSession {
	byClient := make(map[string][]storage.Subscription)
	for _, sub := range subs {
		byClient[sub.Client] = append(byClient[sub.Client], sub)
	}

	out := make([]OfflineSession, 0, len(clients))
	for _, c := range clients {
		out = append(out, FromStorage(c.ID, byClient[c.ID]))
	}
	return out
}

// FilterOffline drops any session whose client_id is currently claimed by
// a live connection somewhere in the cluster (liveClaimed, as returned by
// raft.Registry's SessionsSnapshot — clientID keys, node ID values, only
// presence matters here). Redis persists a client's record for its whole
// life, connected or not (see AllFromStorage's doc), so without this a
// currently-connected client would still show up as "offline" — the
// Reconciler would keep placing/re-placing rendezvous ownership for a
// session that isn't offline at all, fighting keelHook's OnSessionEstablish,
// which clears that same registration the moment the client reconnects.
func FilterOffline(sessions []OfflineSession, liveClaimed map[string]string) []OfflineSession {
	out := make([]OfflineSession, 0, len(sessions))
	for _, s := range sessions {
		if _, live := liveClaimed[s.ClientID]; live {
			continue
		}
		out = append(out, s)
	}
	return out
}
