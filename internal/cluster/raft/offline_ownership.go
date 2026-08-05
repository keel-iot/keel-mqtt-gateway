package raft

import (
	"encoding/base64"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
)

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
// previous registration only after the new one is in place. Subscribing
// before unsubscribing means the handoff window ever risks a brief
// duplicate (both old and new registered at once), never a gap with no
// owner at all — duplicates are tolerable (QoS1 already allows them by
// spec; QoS2 closes the gap via PublishID dedup, see OfflineDelivery),
// but a missed enqueue is not.
//
// Also registers newOwner in the Offline Routing Index (routing.OfflineRouteKey,
// keyed by filter alone, shared across every client with that filter).
// Deliberately add-only there: unlike the exact per-(clientID,filter) key
// above, this one has no reference count, so unsubscribing the old owner
// could remove an entry a different client on the same node still needs.
// A stale entry only costs one harmless extra Forward to a node that
// finds nothing to deliver; PurgeNode clears a dead node's entries on
// crash regardless.
func (o *OfflineOwnership) Place(clientID, filter, newOwner string) error {
	key := offlineOwnerKey(clientID, filter)
	old, hadOwner := o.CurrentOwner(clientID, filter)
	if err := o.Registry.Subscribe(key, newOwner); err != nil {
		return err
	}
	if err := o.Registry.Subscribe(routing.OfflineRouteKey(filter), newOwner); err != nil {
		return err
	}
	if hadOwner && old != newOwner {
		_ = o.Registry.Unsubscribe(key, old)
	}
	return nil
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
