package membership

import (
	"context"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisFailoverInterval mirrors reconcileInterval's rationale: periodic
// sweep rather than purely event-driven, so a missed transition (e.g. this
// node becoming leader after the previous leader's last tick) can't
// silently stick forever.
const redisFailoverInterval = 2 * time.Second

// redisAdminTimeout bounds a single SLAVEOF/REPLICAOF admin call against
// one core node's co-located Redis.
const redisAdminTimeout = 5 * time.Second

// redisAdminClient issues Redis replication admin commands against a
// specific node's co-located Redis instance. Narrowed interface (rather
// than exposing *redis.Client directly) so tests can substitute a fake
// without a real Redis — same pattern as fakeRegistry/fakeForwarder in
// internal/broker/hooks_test.go.
type redisAdminClient interface {
	// replicaOf points the Redis instance at addr to replicate from
	// primaryHost:primaryPort.
	replicaOf(ctx context.Context, addr, primaryHost, primaryPort string) error
	// replicaOfNoOne promotes the Redis instance at addr to primary
	// (stops replicating from anyone).
	replicaOfNoOne(ctx context.Context, addr string) error
}

// realRedisAdmin is redisAdminClient's production implementation: a
// short-lived *redis.Client per call. These are low-frequency
// administrative operations (failover, a new core joining) — not a
// per-message hot path — so there's no persistent-connection pool to
// manage here, unlike internal/cluster/redisrouter's app-level Router.
type realRedisAdmin struct {
	password string
}

func (a realRedisAdmin) dial(addr string) *redis.Client {
	// Protocol pinned to RESP2 — see internal/cluster/redisrouter's same
	// choice; nothing here needs RESP3-only features either.
	return redis.NewClient(&redis.Options{Addr: addr, Password: a.password, Protocol: 2})
}

func (a realRedisAdmin) replicaOf(ctx context.Context, addr, primaryHost, primaryPort string) error {
	client := a.dial(addr)
	defer client.Close()
	// go-redis v9.18.0 has no ReplicaOf method despite SlaveOf's doc
	// comment recommending one — SLAVEOF is simply the pre-5.0 command
	// name; Redis has treated SLAVEOF/REPLICAOF as synonyms since
	// REPLICAOF was introduced, so this sends the identical command a
	// ReplicaOf method would.
	return client.SlaveOf(ctx, primaryHost, primaryPort).Err()
}

func (a realRedisAdmin) replicaOfNoOne(ctx context.Context, addr string) error {
	client := a.dial(addr)
	defer client.Close()
	return client.SlaveOf(ctx, "NO", "ONE").Err()
}

// redisFailoverLoop periodically reconciles the co-located Redis
// primary+replica topology across core nodes, whenever this node is raft
// leader — same single-arbiter principle as reconcileVotersLoop, so
// there's never a moment where two nodes could both decide to promote
// themselves (the raft.Apply in failoverRedisPrimary/bootstrapRedisPrimary
// is itself gated on this being the leader, and hashicorp/raft guarantees
// at most one leader at a time).
//
// "Who is primary" is a raft-replicated fact (see
// internal/cluster/raft's OpSetRedisPrimary) — not gossip — precisely so a
// leadership change mid-failover doesn't risk two different nodes
// deciding two different things: the new leader reads the same
// CurrentRedisPrimary() the old one would have, from the same log,
// instead of re-deciding from scratch with gossip's eventually-consistent
// view.
func (m *Membership) redisFailoverLoop() {
	ticker := time.NewTicker(redisFailoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopRedisFailover:
			return
		case <-ticker.C:
			m.reconcileRedisPrimary()
		}
	}
}

// coreVoterCount returns the number of servers in the raft voter
// configuration — the authoritative "how many cores does this cluster
// have" count, independent of which of them happen to be visible via
// gossip on this particular tick. Same source reconcileVoters already
// treats as ground truth (m.raft.Raft.GetConfiguration()), for the same
// reason: gossip is eventually consistent and, during a real node
// failure, is exactly the thing that has (correctly) stopped reflecting
// the dead node — a stale-visibility signal, not a topology-size signal.
//
// testVoterCount overrides this for unit tests: actually AddVoter-ing
// unreachable placeholder addresses to inflate a real raft
// configuration's size (the naive way to test a ">1 voter" scenario)
// causes the single reachable node to lose leadership once it can no
// longer contact a quorum of the (now larger, mostly-unreachable) voter
// set for heartbeats — a real hashicorp/raft liveness property, not
// something worth working around with exotic test topology. Overriding
// the count directly tests this method's own logic against a known
// value without that fragility.
func (m *Membership) coreVoterCount() (int, error) {
	if m.testVoterCount != nil {
		return m.testVoterCount()
	}
	future := m.raft.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	return len(future.Configuration().Servers), nil
}

// coreMembersWithRedis returns every currently-known core member that has
// a co-located Redis address — core nodes without one (Redis co-location
// not configured there) are invisible to this reconciliation, same as an
// edge node.
func (m *Membership) coreMembersWithRedis() []NodeMeta {
	all := m.Members()
	out := make([]NodeMeta, 0, len(all))
	for _, meta := range all {
		if meta.Role == RoleCore && meta.RedisAddr != "" {
			out = append(out, meta)
		}
	}
	return out
}

func (m *Membership) reconcileRedisPrimary() {
	if !m.raft.IsLeader() {
		return
	}

	// Ground truth for "is this genuinely a single-core cluster" is the
	// raft voter configuration, NOT how many Redis-bearing cores are
	// currently visible via gossip — during exactly the scenario this
	// loop exists to handle (the primary just died), the dead node is by
	// definition absent from current gossip, so counting visible members
	// would misidentify a real 2-node cluster missing its primary as
	// "single-core" and refuse to fail over. Same reasoning reconcileVoters
	// already applies (raft configuration, not m.Members(), is the
	// authoritative "who is actually in this cluster").
	voterCount, err := m.coreVoterCount()
	if err != nil {
		m.log.Warn("membership: get raft configuration for redis failover", "error", err)
		return
	}

	coreMembers := m.coreMembersWithRedis()

	primary, ok := m.raft.Registry.CurrentRedisPrimary()
	if !ok {
		m.bootstrapRedisPrimary(coreMembers, voterCount)
		return
	}

	// Single-core guard: with only one core in the raft voter
	// configuration, there is no replica to promote and no replica
	// topology to maintain, ever — not just "not right now". Explicit
	// early return rather than letting the loops below silently no-op, so
	// this case is a documented guard, not an accident of empty slices.
	if voterCount <= 1 {
		return
	}

	visible := false
	for _, meta := range coreMembers {
		if meta.NodeID == primary {
			visible = true
			break
		}
	}

	now := time.Now()
	m.muRedis.Lock()
	if visible {
		// Present again (or still) — clear any prior absence tracking so
		// a later disappearance starts its own fresh threshold window,
		// not one left over from an earlier, since-recovered blip.
		delete(m.redisMissingSince, primary)
		m.muRedis.Unlock()
		m.ensureReplicasConfigured(coreMembers, primary)
		return
	}
	missingSince, tracked := m.redisMissingSince[primary]
	if !tracked {
		// First tick this primary is observed absent — start the clock
		// here rather than failing over immediately (a single missed
		// gossip round shouldn't trigger a failover) or, the bug this
		// replaced, never starting the clock at all when the primary was
		// already gone before this process ever got a chance to see it
		// present even once (a real scenario: the leader that will run
		// this reconciliation is often elected only shortly AFTER the old
		// primary died).
		m.redisMissingSince[primary] = now
		m.muRedis.Unlock()
		return
	}
	m.muRedis.Unlock()

	if now.Sub(missingSince) <= m.redisPrimaryDeadThreshold {
		return // within the grace window — do nothing yet, not even replica reconfiguration (primary might still be right)
	}

	m.log.Warn("membership: redis primary missing beyond threshold, initiating failover",
		"node_id", primary, "threshold", m.redisPrimaryDeadThreshold)
	m.failoverRedisPrimary(coreMembers, primary)
}

// bootstrapRedisPrimary designates an initial Redis primary when raft has
// never had one set (fresh cluster, or a fresh raft log after disaster
// recovery — see keel-design-doc.md's raft backup/restore). Prefers self
// when self is a Redis-bearing core node, purely so the very first core to
// reach this code path (typically the bootstrap node) is the common case,
// not because self is special in any way the rest of this loop treats
// differently afterward.
func (m *Membership) bootstrapRedisPrimary(coreMembers []NodeMeta, voterCount int) {
	if len(coreMembers) == 0 {
		return // no Redis-bearing core known yet — nothing to designate
	}

	candidate := coreMembers[0]
	for _, meta := range coreMembers {
		if meta.NodeID == m.self.NodeID {
			candidate = meta
			break
		}
	}

	if err := m.raft.Registry.SetRedisPrimary(candidate.NodeID); err != nil {
		m.log.Warn("membership: bootstrap redis primary designation failed", "node_id", candidate.NodeID, "error", err)
		return
	}
	m.log.Info("membership: designated initial redis primary", "node_id", candidate.NodeID)

	if voterCount > 1 {
		m.ensureReplicasConfigured(coreMembers, candidate.NodeID)
		return
	}
	// Single-core guard (raft voter count, not gossip visibility — see
	// reconcileRedisPrimary): explicitly ensure the sole node isn't left
	// replicating from a stale prior target (e.g. this same node was a
	// replica before a disaster-recovery restore) — REPLICAOF NO ONE is
	// idempotent when already the case, so unconditional is safe.
	ctx, cancel := context.WithTimeout(context.Background(), redisAdminTimeout)
	defer cancel()
	if err := m.redisAdmin.replicaOfNoOne(ctx, candidate.RedisAddr); err != nil {
		m.log.Warn("membership: ensure sole redis node is primary failed", "node_id", candidate.NodeID, "error", err)
	}
}

// ensureReplicasConfigured issues SLAVEOF/REPLICAOF toward primaryNodeID
// for every other known Redis-bearing core — re-issued every tick
// regardless of whether anything changed, which is safe: Redis no-ops a
// REPLICAOF/SLAVEOF call that already points at the given master, so this
// naturally covers a brand-new core joining (it simply becomes visible in
// coreMembersWithRedis on some future tick and gets configured then, no
// separate "on join" handling needed) as well as re-asserting the
// intended topology after a transient network split.
func (m *Membership) ensureReplicasConfigured(coreMembers []NodeMeta, primaryNodeID string) {
	// No explicit single-core early-return here: the caller
	// (reconcileRedisPrimary/bootstrapRedisPrimary) already gates on the
	// raft-voter-count ground truth before calling this. If coreMembers
	// happens to contain only the primary itself (nobody else currently
	// visible via gossip), the loop below naturally does zero iterations
	// — correctly inert without needing a redundant, gossip-count-based
	// guard here (that was the actual source of a previous bug: gossip
	// visibility during a real failover is exactly when the dead node is
	// invisible, which isn't the same thing as "there is truly no other
	// core").
	var primaryAddr string
	for _, meta := range coreMembers {
		if meta.NodeID == primaryNodeID {
			primaryAddr = meta.RedisAddr
			break
		}
	}
	if primaryAddr == "" {
		return // primary's redis addr not (yet) known via gossip — retry next tick
	}
	primaryHost, primaryPort, err := net.SplitHostPort(primaryAddr)
	if err != nil {
		m.log.Warn("membership: invalid redis primary addr", "addr", primaryAddr, "error", err)
		return
	}

	for _, meta := range coreMembers {
		if meta.NodeID == primaryNodeID {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), redisAdminTimeout)
		err := m.redisAdmin.replicaOf(ctx, meta.RedisAddr, primaryHost, primaryPort)
		cancel()
		if err != nil {
			m.log.Warn("membership: configure redis replica failed",
				"node_id", meta.NodeID, "primary", primaryNodeID, "error", err)
		}
	}
}

// failoverRedisPrimary promotes a surviving replica after the previous
// primary has been missing beyond redisPrimaryDeadThreshold. Promotion
// happens BEFORE the raft designation is updated — if replicaOfNoOne
// itself fails, the authoritative "who is primary" fact is never advanced
// to a node that isn't actually writable yet, same fail-safe ordering as
// claimClusterSession rolling back local state on a failed raft.Apply.
func (m *Membership) failoverRedisPrimary(coreMembers []NodeMeta, deadPrimary string) {
	// No explicit single-core early-return here either — see
	// ensureReplicasConfigured's doc. The caller already gated on raft
	// voter count; if coreMembers contains no survivor (deadPrimary is the
	// only entry, or the list is otherwise empty), the loop below simply
	// finds nothing and the "no surviving replica candidate found" log
	// below fires, which is the correct, honest outcome for that case.
	var candidate NodeMeta
	found := false
	for _, meta := range coreMembers {
		if meta.NodeID == deadPrimary {
			continue
		}
		candidate = meta
		found = true
		break // any survivor works — only the leader acts, so no promotion race to break ties for
	}
	if !found {
		m.log.Warn("membership: redis primary dead but no surviving replica candidate found", "dead_primary", deadPrimary)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisAdminTimeout)
	err := m.redisAdmin.replicaOfNoOne(ctx, candidate.RedisAddr)
	cancel()
	if err != nil {
		m.log.Error("membership: promote redis replica failed", "node_id", candidate.NodeID, "error", err)
		return
	}

	if err := m.raft.Registry.SetRedisPrimary(candidate.NodeID); err != nil {
		m.log.Error("membership: raft designation of new redis primary failed", "node_id", candidate.NodeID, "error", err)
		return
	}
	m.muRedis.Lock()
	delete(m.redisMissingSince, deadPrimary) // stale now — the map is only ever keyed by the CURRENT primary's nodeID
	m.muRedis.Unlock()
	m.log.Info("membership: redis primary failover complete", "old_primary", deadPrimary, "new_primary", candidate.NodeID)

	m.ensureReplicasConfigured(coreMembers, candidate.NodeID)
}
