package raft

import (
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
)

// EdgeRegistry is the edge-node Registry implementation, composing three
// independent backends — mirroring CoreRegistry's split, but routing and
// ACL evaluation now come from local caches instead of a per-call RPC:
//
//   - routing delegates to a routing.Router over a thin Olric client
//     (store.NewRemoteOlricStore) instead of RemoteRegistry's old gRPC path
//   - session ownership still delegates to RemoteRegistry/gRPC — raft
//     state only exists on core nodes
//   - ACL and cert-revocation checks go through ACLCache/RevocationCache,
//     periodically-refreshed local reads — see their own docs for the
//     staleness trade-off
type EdgeRegistry struct {
	router          *routing.Router
	remote          *RemoteRegistry
	aclCache        *ACLCache
	revocationCache *RevocationCache
}

// NewEdgeRegistry composes an edge node's Registry. remote is used for
// ClaimSession/ReleaseSession and to feed aclCache/revocationCache's
// periodic refresh.
func NewEdgeRegistry(router *routing.Router, remote *RemoteRegistry, aclCache *ACLCache, revocationCache *RevocationCache) *EdgeRegistry {
	return &EdgeRegistry{router: router, remote: remote, aclCache: aclCache, revocationCache: revocationCache}
}

func (e *EdgeRegistry) Subscribe(topic, nodeID string) error {
	return e.router.Subscribe(topic, nodeID)
}

func (e *EdgeRegistry) Unsubscribe(topic, nodeID string) error {
	return e.router.Unsubscribe(topic, nodeID)
}

func (e *EdgeRegistry) NodesFor(topic, localNodeID string) []string {
	return e.router.NodesFor(topic, localNodeID)
}

func (e *EdgeRegistry) OfflineNodesFor(topic string) []string {
	return e.router.OfflineNodesFor(topic)
}

func (e *EdgeRegistry) OwnedClientIDs(nodeID string) []string {
	return e.router.OwnedClientIDs(nodeID)
}

// UnsubscribeBatch satisfies the optional BatchUnsubscriber capability
// hooks.go's OnDisconnect type-asserts for — previously only CoreRegistry
// implemented it, so edge nodes fell back to a per-filter Unsubscribe
// loop (still over gRPC); now it's a local, single-call batch write
// against the router, same as core.
func (e *EdgeRegistry) UnsubscribeBatch(topics []string, nodeID string) error {
	return e.router.UnsubscribeBatch(topics, nodeID)
}

// TopicsForNode is a pure local-cache read — see routing.Router.TopicsForNode.
// Used by routing.Reconciler to detect when this node's own routing entries
// have gone missing from the store (e.g. a total Olric data-loss event)
// while its MQTT clients are still connected.
func (e *EdgeRegistry) TopicsForNode(nodeID string) []string {
	return e.router.TopicsForNode(nodeID)
}

func (e *EdgeRegistry) ClaimSession(clientID, nodeID string) (string, error) {
	return e.remote.ClaimSession(clientID, nodeID)
}

func (e *EdgeRegistry) ReleaseSession(clientID, nodeID string) error {
	return e.remote.ReleaseSession(clientID, nodeID)
}

func (e *EdgeRegistry) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	return e.aclCache.EvaluateACL(clientID, username, topic, action)
}

func (e *EdgeRegistry) IsRevoked(identity string) bool {
	return e.revocationCache.IsRevoked(identity)
}

// CurrentRedisPrimary forwards to RemoteRegistry — no local cache for
// this the way EvaluateACL has ACLCache, since it's a single small value
// polled directly by internal/cluster/redisrouter's watcher rather than
// consulted per-message; a per-poll gRPC round-trip is cheap enough not
// to need one.
func (e *EdgeRegistry) CurrentRedisPrimary() (string, bool) {
	return e.remote.CurrentRedisPrimary()
}

// Close releases the router and ACL cache's background goroutines/store
// connection. Does not close remote (RemoteRegistry owns no background
// resources — just lazily-dialed gRPC connections).
func (e *EdgeRegistry) Close() error {
	e.aclCache.Close()
	e.revocationCache.Close()
	return e.router.Close()
}
