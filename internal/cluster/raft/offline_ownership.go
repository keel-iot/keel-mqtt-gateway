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

// offlineOwnerKey encodes (clientID, filter) as one opaque MQTT topic
// level under "$offline/", reusing the routing table as the ownership
// store instead of adding a new one. Not clientID+"/"+filter: NodesFor
// does real wildcard matching, so a raw "+" or "#" segment could
// cross-match a different client's filter. base64.RawURLEncoding never
// emits '+', '/', or '=', so the level can't ever be a wildcard, and the
// "$" prefix keeps it outside any real client's wildcard subscriptions.
func offlineOwnerKey(clientID, filter string) string {
	return "$offline/" + base64.RawURLEncoding.EncodeToString([]byte(clientID+"\x00"+filter))
}

// OfflineOwnership backs session.Reconciler's CurrentOwner/Place against
// this node's routing Registry — no separate storage, since ownership is
// a derived fact (session.Owner's hash), not state kept in sync on the
// side. PurgeNode already wipes a crashed node's entries for free, since
// they live in the same per-node topic index as live routing.
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
// any — called when a session comes back online, so it isn't left with a
// stale offline-delivery target until the Reconciler's next tick.
func (o *OfflineOwnership) Clear(clientID, filter string) error {
	old, ok := o.CurrentOwner(clientID, filter)
	if !ok {
		return nil
	}
	return o.Registry.Unsubscribe(offlineOwnerKey(clientID, filter), old)
}
