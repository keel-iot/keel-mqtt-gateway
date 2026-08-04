// Package session models MQTT state that outlives a live connection,
// distinct from mqtt.Client which represents the live transport (socket,
// goroutines, keepalive). mochi-mqtt conflates connection and session
// into one object; Keel needs to represent a persistent session with no
// live connection anywhere in the cluster.
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
// the fleet-wide, unfiltered rows RedisSessionHook.StoredClients/
// StoredSubscriptions return.
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

// FilterOffline drops sessions whose client_id is claimed by a live
// connection (liveClaimed, from raft.Registry's SessionsSnapshot — only
// key presence matters). Redis keeps a client's record whether it's
// connected or not, so without this a live client would keep showing up
// as offline and fighting the reconnect-time ownership clear.
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
