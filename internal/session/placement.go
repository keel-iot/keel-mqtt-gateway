// Owner placement for offline sessions — see this package's doc and
// keel-design-doc.md's "Offline Session Placement" ADR. Phase 3 of 6:
// this pure function only, not wired into anything yet.
package session

import "hash/fnv"

// Owner returns which of liveEdgeNodeIDs is responsible for clientID's
// offline session, using rendezvous (highest random weight) hashing: for
// every candidate, score = hash(clientID, nodeID); the candidate with the
// highest score wins.
//
// Deterministic and order-independent — every node computes the same
// answer from the same (clientID, liveEdgeNodeIDs), with no stored owner
// table and no consensus (see the design doc's Ownership Model table:
// this is a pure function, not a raft-arbitrated decision like
// ClaimSession). Recomputing over a changed node list reassigns only the
// sessions whose previous owner is no longer in the list, or for which a
// newly-added node now scores higher — never the whole set, unlike a
// naive hash % len(nodes) that reshuffles almost everything whenever N
// changes.
//
// Returns ("", false) when liveEdgeNodeIDs is empty — callers must treat
// that as "no owner available right now", not "any node is fine".
func Owner(clientID string, liveEdgeNodeIDs []string) (string, bool) {
	if len(liveEdgeNodeIDs) == 0 {
		return "", false
	}

	var best string
	var bestScore uint64
	for i, nodeID := range liveEdgeNodeIDs {
		score := rendezvousScore(clientID, nodeID)
		if i == 0 || score > bestScore {
			best, bestScore = nodeID, score
		}
	}
	return best, true
}

// rendezvousScore combines two independent FNV-1a hashes (of clientID and
// nodeID separately) through a splitmix64-style finalizer.
//
// Deliberately NOT a single incremental hash over the concatenated bytes
// (clientID + separator + nodeID): measured empirically to produce a
// systematic bias here, not just noise — with edge node IDs differing
// only in their last character ("edge-1".."edge-4"), FNV-1a's mixing
// after an identical shared prefix doesn't avalanche enough, and one
// candidate consistently won close to 2x as often as the others across
// thousands of distinct client IDs. Hashing the two inputs independently
// and then mixing removes that correlation — verified against the same
// node-ID pattern before choosing this over a "simpler-looking"
// single-hash approach.
func rendezvousScore(clientID, nodeID string) uint64 {
	x := fnvHash64(clientID) ^ fnvHash64(nodeID)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func fnvHash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
