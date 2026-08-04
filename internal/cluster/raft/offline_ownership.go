package raft

import "encoding/base64"

// offlineOwnerRegistry is the narrow slice of Registry's method set
// OfflineOwnership needs — defined locally rather than depending on the
// full Registry interface, same reasoning as routing.Reconciler's own
// local registry interface: a test double only has to satisfy three
// methods, not the whole ACL/session surface.
type offlineOwnerRegistry interface {
	Subscribe(topic, nodeID string) error
	Unsubscribe(topic, nodeID string) error
	NodesFor(topic, localNodeID string) []string
}

// offlineOwnerKey encodes (clientID, filter) into a single opaque MQTT
// topic level under the reserved "$offline/" prefix, reusing the existing
// routing table as the ownership store for offline sessions instead of a
// new, separately-consistent one — see keel-design-doc.md's Offline
// Session Placement ADR, phase 6c.
//
// Deliberately one opaque base64 level, not clientID+"/"+filter:
// Registry.NodesFor performs real MQTT wildcard matching (it's the same
// trie live topic routing uses), so a raw filter segment equal to "+" or
// "#" — or one that happens to align with a different client's filter via
// wildcard matching — would corrupt lookups for an unrelated (clientID,
// filter) pair. base64.RawURLEncoding never emits '+', '/', or '=', so the
// opaque level can never be, or contain, a wildcard level: every lookup is
// an exact, unambiguous match. The "$" prefix additionally keeps it outside
// any real client's '#'/'+' wildcard subscriptions, per MQTT's own
// reserved-topic rule (Topic Filters starting with a wildcard never match
// Topic Names starting with '$').
func offlineOwnerKey(clientID, filter string) string {
	return "$offline/" + base64.RawURLEncoding.EncodeToString([]byte(clientID+"\x00"+filter))
}

// OfflineOwnership backs session.Reconciler's CurrentOwner/Place against
// this node's routing Registry. Deliberately no separate storage: ownership
// is a derived fact (session.Owner's rendezvous hash of clientID + live
// edges), not state kept in sync on the side. What Registry gives it is
// the self-healing the routing table already has for free — PurgeNode
// already wipes a crashed node's entries (including these, since they live
// in the exact same per-node topic index), and every Reconciler tick just
// recomputes and re-asserts the desired owner. Nothing here ever reads a
// "previous owner" as authoritative; CurrentOwner only tells Place what to
// clean up before writing the new registration.
type OfflineOwnership struct {
	Registry offlineOwnerRegistry
}

// CurrentOwner reports the node currently registered as clientID's owner
// for filter, if any.
func (o *OfflineOwnership) CurrentOwner(clientID, filter string) (string, bool) {
	nodes := o.Registry.NodesFor(offlineOwnerKey(clientID, filter), "")
	if len(nodes) == 0 {
		return "", false
	}
	return nodes[0], true
}

// Place registers newOwner as clientID's owner for filter, removing the
// previous registration first if it differs. The Unsubscribe is
// best-effort: a failure there is not fatal — a stale second registration
// only means NodesFor briefly returns two entries, self-corrected on the
// next Place (Reconciler tick) regardless.
func (o *OfflineOwnership) Place(clientID, filter, newOwner string) error {
	key := offlineOwnerKey(clientID, filter)
	if old, ok := o.CurrentOwner(clientID, filter); ok && old != newOwner {
		_ = o.Registry.Unsubscribe(key, old)
	}
	return o.Registry.Subscribe(key, newOwner)
}

// Clear removes clientID's offline-ownership registration for filter, if
// any — called when a session comes back online (see
// internal/broker.keelHook.OnSessionEstablish), so a reconnected client is
// never left with a stale offline-delivery target between now and the
// Reconciler's next tick.
func (o *OfflineOwnership) Clear(clientID, filter string) error {
	old, ok := o.CurrentOwner(clientID, filter)
	if !ok {
		return nil
	}
	return o.Registry.Unsubscribe(offlineOwnerKey(clientID, filter), old)
}
