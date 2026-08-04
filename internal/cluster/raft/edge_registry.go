package raft

import (
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
)

// EdgeRegistry is the edge-node Registry implementation, composing three
// independent backends behind the single Registry interface — mirroring
// CoreRegistry's split, but with routing and ACL evaluation now served
// from local caches instead of an RPC per call:
//
//   - routing (Subscribe/Unsubscribe/NodesFor/UnsubscribeBatch) delegates
//     to a routing.Router backed by a thin Olric client
//     (store.NewRemoteOlricStore) — the same local trie cache + pub/sub +
//     periodic Scan reconciliation core nodes use, just talking to Olric
//     as a non-member client instead of an embedded member. This was
//     already built (store.NewRemoteOlricStore's doc literally says "used
//     by edge nodes") but never wired up here — NodesFor previously fell
//     through to RemoteRegistry's per-call gRPC to a core node instead.
//   - session ownership (ClaimSession/ReleaseSession) still delegates to
//     RemoteRegistry/gRPC: this is genuinely raft-backed, strongly
//     consistent state that only exists on core nodes, so there is no
//     local-cache equivalent to give it — every claim/release must reach
//     the raft leader.
//   - ACL evaluation delegates to an ACLCache, a locally-held,
//     periodically-refreshed read cache — see ACLCache's doc for the
//     explicit staleness trade-off this accepts (no push invalidation,
//     just a poll interval).
//   - Device cert revocation checks delegate to a RevocationCache, same
//     periodic-poll trade-off as ACLCache — see its doc.
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
