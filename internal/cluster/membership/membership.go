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

	// RaftNode is non-nil only for core nodes. Membership uses it to
	// AddVoter newly discovered core peers when this node is leader.
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

// Membership wraps a memberlist.Memberlist instance and the address
// directory derived from gossiped NodeMeta.
type Membership struct {
	ml   *memberlist.Memberlist
	self NodeMeta
	raft *keelraft.Node
	log  *slog.Logger

	mu      sync.RWMutex
	members map[string]NodeMeta // keyed by memberlist node name (== NodeID)

	stopReconcile chan struct{}
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
	}

	m := &Membership{
		raft:          cfg.RaftNode,
		self:          self,
		log:           log,
		members:       make(map[string]NodeMeta),
		stopReconcile: make(chan struct{}),
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
	}

	return m, nil
}

// Leave gracefully broadcasts departure to the cluster before shutting
// down the local gossip agent. Called by the lifecycle drain command.
func (m *Membership) Leave(timeout time.Duration) error {
	close(m.stopReconcile)
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
	e.m.mu.Unlock()
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
	e.m.mu.Unlock()
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
