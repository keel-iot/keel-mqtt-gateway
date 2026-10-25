package membership

import (
	"context"
	"net"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// redisFailoverInterval mirrors reconcileInterval's rationale: periodic
// sweep rather than purely event-driven, so a missed transition (e.g. this
// node becoming leader after the previous leader's last tick) can't
// silently stick forever.
const redisFailoverInterval = 2 * time.Second

// redisAdminTimeout bounds a single SLAVEOF/REPLICAOF admin call against
// one core node's co-located Redis.
const redisAdminTimeout = 5 * time.Second

// A stalled Redis must not block failure detection for the default client
// socket timeout (or its retries). LOADING and authentication errors also
// fail this probe: accepting TCP connections alone is not readiness.
const redisHealthTimeout = 2 * time.Second

// redisAdminClient issues Redis replication admin commands against a
// specific node's co-located Redis instance. Narrowed interface (rather
// than exposing *redis.Client directly) so tests can substitute a fake
// without a real Redis — same pattern as fakeRegistry/fakeForwarder in
// internal/broker/hooks_test.go.
type redisAdminClient interface {
	ping(ctx context.Context, addr string) error
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
	return redis.NewClient(&redis.Options{
		Addr: addr, Password: a.password, Protocol: 2,
		ContextTimeoutEnabled: true,
		MaxRetries:            -1,
		DialerRetries:         1,
		DialTimeout:           redisHealthTimeout,
		ReadTimeout:           redisAdminTimeout,
		WriteTimeout:          redisAdminTimeout,
	})
}

func (a realRedisAdmin) ping(ctx context.Context, addr string) error {
	client := a.dial(addr)
	defer client.Close()
	return client.Ping(ctx).Err()
}

func (m *Membership) pingRedis(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisHealthTimeout)
	defer cancel()
	return m.redisAdmin.ping(ctx, addr)
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

// redisFailoverLoop reconciles the co-located Redis topology on the Raft
// leader. The designated primary is replicated through OpSetRedisPrimary,
// so a new leader reads the committed designation rather than inferring it
// from gossip. Redis admin commands execute outside Raft: leadership checks
// stop work after an observed leadership loss but do not fence Redis writes
// during every network partition.
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
		// A future leadership term must observe its own failure window.
		m.muRedis.Lock()
		clear(m.redisMissingSince)
		m.muRedis.Unlock()
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

	healthy := false
	reason := "core absent from gossip"
	for _, meta := range coreMembers {
		if meta.NodeID == primary {
			if err := m.pingRedis(meta.RedisAddr); err != nil {
				reason = err.Error()
			} else {
				healthy = true
			}
			break
		}
	}
	if !m.raft.IsLeader() {
		return
	}

	now := time.Now()
	m.muRedis.Lock()
	if healthy {
		// Gossip presence is insufficient: a crashed Redis sidecar leaves
		// the core process alive. Only a successful Redis PING resets the
		// continuous failure window.
		delete(m.redisMissingSince, primary)
		m.muRedis.Unlock()
		m.ensureReplicasConfigured(coreMembers, primary)
		return
	}
	missingSince, tracked := m.redisMissingSince[primary]
	if !tracked {
		// Start a grace window even if this leader has never seen a
		// working primary. Do not repoint replicas at a failed primary
		// while waiting for it to recover.
		m.redisMissingSince[primary] = now
		m.muRedis.Unlock()
		m.log.Warn("membership: redis primary unavailable, starting failure window",
			"node_id", primary, "reason", reason, "threshold", m.redisPrimaryDeadThreshold)
		return
	}
	m.muRedis.Unlock()

	if now.Sub(missingSince) <= m.redisPrimaryDeadThreshold {
		return // within the grace window — do nothing yet, not even replica reconfiguration (primary might still be right)
	}

	m.log.Warn("membership: redis primary unavailable beyond threshold, initiating failover",
		"node_id", primary, "reason", reason, "threshold", m.redisPrimaryDeadThreshold)
	m.failoverRedisPrimary(coreMembers, primary)
}

// bootstrapRedisPrimary designates an initial Redis primary when raft has
// never had one set (fresh cluster, or a fresh raft log after a restore).
// Prefers self when self is a Redis-bearing core node, just so the first
// core to reach this code path is usually the one that wins.
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
		if !m.raft.IsLeader() {
			return
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
// primary has been unavailable beyond redisPrimaryDeadThreshold. Promotion
// happens BEFORE the Raft designation is updated. A failed promotion leaves
// the designation unchanged; only an acknowledged promotion advances it.
func (m *Membership) failoverRedisPrimary(coreMembers []NodeMeta, deadPrimary string) {
	// No explicit single-core early-return here either — see
	// ensureReplicasConfigured's doc. The caller already gated on raft
	// voter count; if coreMembers contains no survivor (deadPrimary is the
	// only entry, or the list is otherwise empty), the loop below simply
	// finds nothing and the "no promotable replica found" log
	// below fires, which is the correct, honest outcome for that case.
	// Stable ordering avoids choosing a different candidate on every tick
	// due to map iteration. This is not a replication-freshness ranking:
	// asynchronous replication can still lose unreplicated writes.
	sort.Slice(coreMembers, func(i, j int) bool { return coreMembers[i].NodeID < coreMembers[j].NodeID })
	var candidate NodeMeta
	found := false
	for _, meta := range coreMembers {
		if meta.NodeID == deadPrimary {
			continue
		}
		if err := m.pingRedis(meta.RedisAddr); err != nil {
			m.log.Warn("membership: redis failover candidate unavailable", "node_id", meta.NodeID, "error", err)
			continue
		}
		// Network probes can outlive leadership. Do not issue promotion
		// commands after observing that this node lost its authority.
		if !m.raft.IsLeader() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), redisAdminTimeout)
		err := m.redisAdmin.replicaOfNoOne(ctx, meta.RedisAddr)
		cancel()
		if err != nil {
			m.log.Error("membership: promote redis replica failed", "node_id", meta.NodeID, "error", err)
			// A timeout can mean the command succeeded but its reply was
			// lost. Do not promote a second Redis in the same attempt.
			return
		}
		candidate, found = meta, true
		break
	}
	if !found {
		m.log.Warn("membership: redis primary unavailable but no promotable replica found", "dead_primary", deadPrimary)
		return
	}

	if err := m.raft.Registry.SetRedisPrimary(candidate.NodeID); err != nil {
		m.log.Error("membership: raft designation of new redis primary failed", "node_id", candidate.NodeID, "error", err)
		return
	}
	telemetry.StorageFailoversTotal.Inc()
	m.muRedis.Lock()
	delete(m.redisMissingSince, deadPrimary) // stale now — the map is only ever keyed by the CURRENT primary's nodeID
	m.muRedis.Unlock()
	m.log.Info("membership: redis primary failover complete", "old_primary", deadPrimary, "new_primary", candidate.NodeID)

	m.ensureReplicasConfigured(coreMembers, candidate.NodeID)
}
