// Package membership implements gossip-based discovery (SWIM, via
// hashicorp/memberlist) shared by core and edge nodes. It works
// identically on Docker Compose, bare VMs, or Kubernetes — no dependency
// on the K8s API — and drives two things:
//
//  1. address discovery: every node learns every other node's gRPC
//     address (for raft.RemoteRegistry and dataplane.GRPCForwarder), and
//     core nodes learn each other's raft address.
//  2. raft quorum growth: when the local node is both core and the
//     current raft leader, it AddVoters any newly discovered core peer.
//     Bootstrapping a brand-new cluster is NOT handled here — see
//     raft.Node.Bootstrap, invoked once at startup by the node configured
//     with --bootstrap.
package membership

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// Config configures a Membership instance.
type Config struct {
	NodeID        string
	Role          Role
	BindAddr      string // gossip bind address, e.g. "0.0.0.0"
	BindPort      int
	AdvertiseAddr string   // address other nodes should dial; empty = BindAddr
	Peers         []string // seed addresses ("host:port") to contact on startup

	RaftAddr        string // core only, empty for edge
	GRPCAddr        string // registry + dataplane RPC address, all roles
	OlricAddr       string // core only, empty for edge — see NodeMeta.OlricAddr
	OlricClientAddr string // core only, empty for edge — see NodeMeta.OlricClientAddr
	RedisAddr       string // core only, empty for edge — see NodeMeta.RedisAddr
	HTTPAddr        string // edge (or combined) only, empty for pure core — see NodeMeta.HTTPAddr

	// RedisPassword authenticates admin commands (SLAVEOF/REPLICAOF) this
	// node's failover loop issues against OTHER core nodes' co-located
	// Redis instances — see redis_failover.go. Every core's Redis shares
	// one password (same posture as the rest of this project's Redis
	// config — see config.RedisPassword). Only meaningful when RedisAddr
	// is also set.
	RedisPassword string

	// RedisPrimaryDeadThreshold bounds how long the currently-designated
	// Redis primary can be missing from gossip before the failover loop
	// promotes a replica — same debounce rationale as
	// lifecycle.Monitor.Threshold (a blip must not trigger a failover).
	// Zero defaults to 30s (matching cmd/server/main.go's own
	// --heartbeat-threshold default for the analogous core-node-missing
	// case).
	RedisPrimaryDeadThreshold time.Duration

	// RaftNode is non-nil only for core nodes. Membership uses it to
	// AddVoter newly discovered core peers when this node is leader, and
	// (see redis_failover.go) to read/write the Redis primary designation.
	RaftNode *keelraft.Node
}

// reconcileInterval controls how often the leader re-checks that every
// known core peer is a raft voter. Event-driven AddVoter (NotifyJoin) is
// not sufficient on its own: a peer's join event can arrive before this
// node has finished its own leader election (common during a from-zero
// cluster bootstrap, where all three core nodes start within
// milliseconds of each other), and no further join event will ever fire
// for a peer already known — so a missed AddVoter would otherwise stick
// forever. The periodic sweep guarantees convergence without needing to
// reason about that ordering.
const reconcileInterval = 2 * time.Second

// rejoinInterval controls how often an isolated node (one that currently
// sees no gossip peers at all) retries joining via its originally
// configured seed addresses. Needed because memberlist.Join is otherwise
// only ever called once, at startup (see New) — if every peer a node
// knows about dies at once (e.g. all 3 core nodes going down together)
// and later comes back, a long-running node that never restarts its own
// process (typically an edge) has no other path back into the mesh: SWIM
// anti-entropy only syncs with members it already knows about, and it
// knows about none. Found via test/e2e/olric-quorum-loss.sh: after
// killing and restarting all 3 cores, edges never regained
// CoreGRPCAddrs()/CorePeers() and every new connect stayed refused
// indefinitely (raft.RemoteRegistry: "no known core peers"), even though
// the cores themselves had already re-elected a leader and were healthy.
const rejoinInterval = 3 * time.Second

// Membership wraps a memberlist.Memberlist instance and the address
// directory derived from gossiped NodeMeta.
type Membership struct {
	ml   *memberlist.Memberlist
	self NodeMeta
	raft *keelraft.Node
	log  *slog.Logger

	mu      sync.RWMutex
	members map[string]NodeMeta // keyed by memberlist node name (== NodeID)

	peers         []string // seed addresses from Config.Peers, retried by rejoinLoop when isolated
	stopReconcile chan struct{}
	stopRejoin    chan struct{}

	// Redis failover loop state — see redis_failover.go. Started only when
	// Redis co-location is configured (New's cfg.RedisAddr != "").
	redisPrimaryDeadThreshold time.Duration
	redisAdmin                redisAdminClient    // overridable in tests; real impl otherwise
	testVoterCount            func() (int, error) // overridable in tests only — see coreVoterCount's doc; nil in production (real raft.GetConfiguration() used instead)
	stopRedisFailover         chan struct{}

	muRedis           sync.Mutex
	redisMissingSince map[string]time.Time // designated-primary nodeID -> first tick it was observed absent from gossip (absent entirely, since presence clears it)
}

// New creates and starts gossiping. If cfg.Peers is non-empty it attempts
// to join the existing cluster through them; a failure to join is
// returned as an error — the caller decides whether that's fatal (core
// nodes joining an existing cluster) or expected (first node up).
func New(cfg Config, log *slog.Logger) (*Membership, error) {
	self := NodeMeta{
		NodeID:          cfg.NodeID,
		Role:            cfg.Role,
		RaftAddr:        cfg.RaftAddr,
		GRPCAddr:        cfg.GRPCAddr,
		OlricAddr:       cfg.OlricAddr,
		OlricClientAddr: cfg.OlricClientAddr,
		RedisAddr:       cfg.RedisAddr,
		HTTPAddr:        cfg.HTTPAddr,
	}

	redisPrimaryDeadThreshold := cfg.RedisPrimaryDeadThreshold
	if redisPrimaryDeadThreshold <= 0 {
		redisPrimaryDeadThreshold = 30 * time.Second
	}

	m := &Membership{
		raft:                      cfg.RaftNode,
		self:                      self,
		log:                       log,
		members:                   make(map[string]NodeMeta),
		peers:                     cfg.Peers,
		stopReconcile:             make(chan struct{}),
		stopRejoin:                make(chan struct{}),
		redisPrimaryDeadThreshold: redisPrimaryDeadThreshold,
		redisAdmin:                realRedisAdmin{password: cfg.RedisPassword},
		stopRedisFailover:         make(chan struct{}),
		redisMissingSince:         make(map[string]time.Time),
	}

	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = cfg.NodeID
	mlConfig.BindAddr = cfg.BindAddr
	mlConfig.BindPort = cfg.BindPort
	if cfg.AdvertiseAddr != "" {
		// memberlist gossips AdvertiseAddr as a raw IP, not a hostname — in
		// Docker/K8s the advertise address is usually a container/pod DNS
		// name, so resolve it here rather than pushing that requirement
		// onto every caller.
		ip, err := net.ResolveIPAddr("ip", cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("membership: resolve advertise addr %q: %w", cfg.AdvertiseAddr, err)
		}
		mlConfig.AdvertiseAddr = ip.String()
		mlConfig.AdvertisePort = cfg.BindPort
	}
	mlConfig.Delegate = &delegate{self: self}
	mlConfig.Events = &eventDelegate{m: m}
	mlConfig.LogOutput = slogWriter{log: log}

	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("membership: create: %w", err)
	}
	m.ml = ml

	m.mu.Lock()
	m.members[self.NodeID] = self
	m.mu.Unlock()

	if len(cfg.Peers) > 0 {
		// Non-fatal: on a cold multi-node start every node typically lists
		// its siblings as peers, but they may not be listening yet (e.g.
		// this is core-1 and core-2/3 haven't started). SWIM gossip is
		// symmetric — once any single Join succeeds from either side, full
		// mesh membership is established — so failing here doesn't strand
		// this node, it just waits to be discovered instead of discovering.
		if _, err := ml.Join(cfg.Peers); err != nil {
			log.Warn("membership: initial join failed, waiting to be discovered instead", "peers", cfg.Peers, "error", err)
		}
	}

	if m.raft != nil {
		go m.reconcileVotersLoop()
		// Only meaningful when THIS node has a co-located Redis to
		// coordinate — see redis_failover.go's doc. Zero cost (no
		// goroutine at all) for a core node running without Redis
		// co-location, or for edge/standalone nodes (m.raft nil there
		// already excludes them).
		if cfg.RedisAddr != "" {
			go m.redisFailoverLoop()
		}
	}

	// Runs on every role (core and edge alike) — any node can end up
	// isolated, not just edges. No-op (skipped entirely) when there are
	// no seed addresses to retry against at all.
	if len(cfg.Peers) > 0 {
		go m.rejoinLoop()
	}

	return m, nil
}

// Leave gracefully broadcasts departure to the cluster before shutting
// down the local gossip agent. Called by the lifecycle drain command.
func (m *Membership) Leave(timeout time.Duration) error {
	close(m.stopReconcile)
	close(m.stopRejoin)        // safe even if rejoinLoop was never started (no peers configured) — nothing reads a closed, unstarted channel
	close(m.stopRedisFailover) // safe even if redisFailoverLoop was never started — nothing reads a closed, unstarted channel
	if err := m.ml.Leave(timeout); err != nil {
		return fmt.Errorf("membership: leave: %w", err)
	}
	return m.ml.Shutdown()
}

// reconcileVotersLoop periodically ensures every known core peer is a
// raft voter whenever this node is leader. See reconcileInterval for why
// this can't be purely event-driven.
func (m *Membership) reconcileVotersLoop() {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopReconcile:
			return
		case <-ticker.C:
			m.reconcileVoters()
		}
	}
}

func (m *Membership) reconcileVoters() {
	if !m.raft.IsLeader() {
		return
	}
	future := m.raft.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		m.log.Warn("membership: get raft configuration", "error", err)
		return
	}
	current := make(map[string]bool)
	for _, srv := range future.Configuration().Servers {
		current[string(srv.ID)] = true
	}

	for _, meta := range m.Members() {
		if meta.Role != RoleCore || current[meta.NodeID] {
			continue
		}
		if err := m.raft.AddVoter(meta.NodeID, meta.RaftAddr); err != nil {
			m.log.Warn("membership: reconcile add voter failed", "node_id", meta.NodeID, "error", err)
			continue
		}
		m.log.Info("membership: added core node as raft voter (reconcile)", "node_id", meta.NodeID, "raft_addr", meta.RaftAddr)
	}
}

// rejoinLoop retries the original seed join whenever this node currently
// sees no core peer at all — see rejoinInterval's doc for why this can't
// be event-driven (there is no event to react to once every core peer
// this node knew about is gone). A successful Join triggers memberlist's
// own push/pull anti-entropy, which repopulates m.members via the
// existing NotifyJoin callback — no extra bookkeeping needed here beyond
// calling Join again.
func (m *Membership) rejoinLoop() {
	ticker := time.NewTicker(rejoinInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopRejoin:
			return
		case <-ticker.C:
			m.rejoinIfIsolated()
		}
	}
}

// isIsolated reports whether no core node OTHER than self is currently
// known. This is "no core peer visible", not "no peer visible at all" —
// an edge losing every core still sees its sibling edges over gossip
// (they never went anywhere), so a raw member-count check would never
// trigger. Core peers specifically are what
// raft.RemoteRegistry.CorePeers()/CoreGRPCAddrs() need, so that's the
// condition that actually matters (found the hard way: an earlier version
// of this check used len(m.Members()) and silently never fired against a
// live edge, only against an isolated single-node unit test). Compares by
// NodeID, not GRPCAddr — NodeID is always set and unique, unlike GRPCAddr,
// which some configs may leave empty.
func (m *Membership) isIsolated() bool {
	for _, meta := range m.Members() {
		if meta.Role == RoleCore && meta.NodeID != m.self.NodeID {
			return false
		}
	}
	return true
}

func (m *Membership) rejoinIfIsolated() {
	if !m.isIsolated() {
		return
	}
	n, err := m.ml.Join(m.peers)
	if err != nil {
		m.log.Warn("membership: rejoin attempt failed, still isolated", "peers", m.peers, "error", err)
		return
	}
	if n > 0 {
		m.log.Info("membership: rejoined cluster after isolation", "peers", m.peers, "contacted", n)
	}
}

// Members returns a snapshot of every known member's metadata.
func (m *Membership) Members() []NodeMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]NodeMeta, 0, len(m.members))
	for _, meta := range m.members {
		out = append(out, meta)
	}
	return out
}

// CoreGRPCAddrs returns the gRPC address of every known core node,
// including this one if it is core. Used by raft.RemoteRegistry on edge
// nodes to know who to call.
func (m *Membership) CoreGRPCAddrs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, meta := range m.members {
		if meta.Role == RoleCore {
			out = append(out, meta.GRPCAddr)
		}
	}
	return out
}

// CoreOlricAddrs returns the Olric bind address of every known core node
// other than self. Used to seed internal/cluster/store's embedded Olric
// member's peer list at startup — see that package's doc for why this is
// a one-shot seed, not a live-updated peer list (Olric exposes no
// incremental-join API, unlike this package's own reconcileVotersLoop for
// raft voters).
func (m *Membership) CoreOlricAddrs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, meta := range m.members {
		if meta.Role == RoleCore && meta.OlricAddr != "" && meta.NodeID != m.self.NodeID {
			out = append(out, meta.OlricAddr)
		}
	}
	return out
}

// CoreOlricClientAddrs returns every known core node's Olric *main
// protocol* address (not the gossip address CoreOlricAddrs returns) —
// used by store.NewRemoteOlricStore to build an edge node's thin,
// non-member Olric client (see raft.EdgeRegistry).
func (m *Membership) CoreOlricClientAddrs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for _, meta := range m.members {
		if meta.Role == RoleCore && meta.OlricClientAddr != "" {
			out = append(out, meta.OlricClientAddr)
		}
	}
	return out
}

// NodeGRPCAddr resolves a node ID to its gRPC address. Used by
// dataplane.GRPCForwarder to route Forward calls.
func (m *Membership) NodeGRPCAddr(nodeID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.members[nodeID]
	if !ok {
		return "", false
	}
	return meta.GRPCAddr, true
}

// RedisAddrForNode resolves a core node ID to its co-located Redis
// instance's address. Used by whoever needs to reach a specific node's
// Redis (the failover loop reconfiguring surviving replicas via
// REPLICAOF, or a Redis client provider resolving the current primary's
// address once it learns the nodeID from raft — see OpSetRedisPrimary).
// Returns false for an edge node ID (RedisAddr is core-only) or an unknown
// one.
func (m *Membership) RedisAddrForNode(nodeID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.members[nodeID]
	if !ok || meta.RedisAddr == "" {
		return "", false
	}
	return meta.RedisAddr, true
}

// EdgeHTTPAddrs returns the node_id -> metrics-server address (see
// NodeMeta.HTTPAddr) of every currently known member that has one set —
// edges and combined nodes, never a pure core. Used by
// internal/cluster/management's GET /api/metrics and GET
// /api/live/clients to know which nodes to poll for live broker state.
func (m *Membership) EdgeHTTPAddrs() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]string)
	for _, meta := range m.members {
		if meta.HTTPAddr != "" {
			out[meta.NodeID] = meta.HTTPAddr
		}
	}
	return out
}

// ── memberlist.Delegate — only NodeMeta carries data for this PoC ────────

type delegate struct {
	self NodeMeta
}

func (d *delegate) NodeMeta(limit int) []byte {
	b := d.self.encode()
	if len(b) > limit {
		// memberlist caps metadata (default 512 B); NodeMeta's four short
		// strings comfortably fit, so this only trips on misconfiguration.
		return b[:limit]
	}
	return b
}

func (d *delegate) NotifyMsg([]byte)                           {}
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *delegate) LocalState(join bool) []byte                { return nil }
func (d *delegate) MergeRemoteState(buf []byte, join bool)     {}

// ── memberlist.EventDelegate ──────────────────────────────────────────────

type eventDelegate struct {
	m *Membership
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	meta, err := decodeMeta(n.Meta)
	if err != nil {
		e.m.log.Warn("membership: decode meta on join", "node", n.Name, "error", err)
		return
	}
	e.m.mu.Lock()
	e.m.members[meta.NodeID] = meta
	count := len(e.m.members)
	e.m.mu.Unlock()
	telemetry.ClusterMembers.Set(float64(count))
	telemetry.MembershipChangesTotal.WithLabelValues("join").Inc()
	e.m.log.Info("membership: node joined", "node_id", meta.NodeID, "role", meta.Role)

	e.maybeAddVoter(meta)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	meta, err := decodeMeta(n.Meta)
	nodeID := n.Name
	if err == nil {
		nodeID = meta.NodeID
	}
	e.m.mu.Lock()
	delete(e.m.members, nodeID)
	count := len(e.m.members)
	e.m.mu.Unlock()
	telemetry.ClusterMembers.Set(float64(count))
	telemetry.MembershipChangesTotal.WithLabelValues("leave").Inc()
	e.m.log.Info("membership: node left", "node_id", nodeID)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	meta, err := decodeMeta(n.Meta)
	if err != nil {
		return
	}
	e.m.mu.Lock()
	e.m.members[meta.NodeID] = meta
	e.m.mu.Unlock()
}

// maybeAddVoter implements the design's minimal bootstrap rule: "the
// first core node self-elects (raft.Node.Bootstrap, called at startup);
// every other core node is AddVoter'd once the current leader discovers
// it via gossip." Only the node that is currently raft leader acts; every
// other core node observes the same join event and no-ops.
func (e *eventDelegate) maybeAddVoter(meta NodeMeta) {
	if e.m.raft == nil || meta.Role != RoleCore || meta.NodeID == e.m.self.NodeID {
		return
	}
	if !e.m.raft.IsLeader() {
		return
	}
	if err := e.m.raft.AddVoter(meta.NodeID, meta.RaftAddr); err != nil {
		e.m.log.Warn("membership: add voter failed", "node_id", meta.NodeID, "error", err)
		return
	}
	e.m.log.Info("membership: added core node as raft voter", "node_id", meta.NodeID, "raft_addr", meta.RaftAddr)
}

// slogWriter adapts an *slog.Logger to the io.Writer memberlist wants for
// its own internal (very chatty) logging.
type slogWriter struct{ log *slog.Logger }

func (w slogWriter) Write(p []byte) (int, error) {
	w.log.Debug("memberlist", "msg", string(p))
	return len(p), nil
}
