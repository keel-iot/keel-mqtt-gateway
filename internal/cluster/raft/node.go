package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// NodeConfig configures a single core node's raft participation.
type NodeConfig struct {
	// NodeID must be stable across restarts (raft identifies servers by
	// ID, not address) — use the pod name in K8s, a configured flag
	// elsewhere.
	NodeID string
	// RaftBindAddr is the host:port raft's TCP transport listens on and
	// advertises to peers. Must be reachable by every other core node.
	RaftBindAddr string
	// DataDir stores the raft log (boltdb) and snapshots. Must be on a
	// persistent volume for core nodes in K8s (StatefulSet + PVC).
	DataDir string

	// SnapshotInterval/SnapshotThreshold override hashicorp/raft's defaults
	// (120s / 8192 log entries), which assume a much larger, higher-write
	// FSM than this one (session ownership + ACL — see fsm.go's doc
	// comment on why both stay on raft despite being small). Zero value
	// for either falls back to hraft.DefaultConfig()'s own default.
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
}

// Node bundles the raft instance with the FSM and Registry built on top of it.
type Node struct {
	Raft     *hraft.Raft
	FSM      *FSM
	Registry *LocalRegistry
	cfg      NodeConfig
}

// NewNode constructs and starts a raft.Raft instance backed by BoltDB for
// log/stable storage and the local filesystem for snapshots. It does not
// join or bootstrap a cluster — call Bootstrap or rely on membership
// discovery (see internal/cluster/membership) to do that.
func NewNode(cfg NodeConfig) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data dir: %w", err)
	}

	raftCfg := hraft.DefaultConfig()
	raftCfg.LocalID = hraft.ServerID(cfg.NodeID)
	raftCfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft." + cfg.NodeID,
		Level:  hclog.Info,
		Output: os.Stderr,
	})
	if cfg.SnapshotInterval > 0 {
		raftCfg.SnapshotInterval = cfg.SnapshotInterval
	}
	if cfg.SnapshotThreshold > 0 {
		raftCfg.SnapshotThreshold = cfg.SnapshotThreshold
	}

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBindAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: resolve bind addr %q: %w", cfg.RaftBindAddr, err)
	}
	transport, err := hraft.NewTCPTransport(cfg.RaftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: create tcp transport: %w", err)
	}

	snapshots, err := hraft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: create snapshot store: %w", err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("raft: create bolt store: %w", err)
	}

	fsm := NewFSM()
	r, err := hraft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("raft: new raft: %w", err)
	}

	return &Node{
		Raft:     r,
		FSM:      fsm,
		Registry: NewLocalRegistry(r, fsm),
		cfg:      cfg,
	}, nil
}

// Bootstrap forms a brand-new single-node cluster with this node as the
// only voter. Only ever call this for the very first node of a cluster
// that starts from zero — calling it on a node joining an existing
// cluster corrupts the log. Safe to call unconditionally at startup: raft
// no-ops (returns didBootstrap=false, err=nil) if the log already has
// entries, i.e. this node has run before — covers both "restarted after a
// drain" and "restarted after a crash".
func (n *Node) Bootstrap() (didBootstrap bool, err error) {
	cfg := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: hraft.ServerID(n.cfg.NodeID), Address: hraft.ServerAddress(n.cfg.RaftBindAddr)},
		},
	}
	future := n.Raft.BootstrapCluster(cfg)
	if ferr := future.Error(); ferr != nil {
		if ferr == hraft.ErrCantBootstrap {
			return false, nil
		}
		return false, fmt.Errorf("raft: bootstrap: %w", ferr)
	}
	return true, nil
}

// AddVoter adds nodeID/addr as a voting member. Only succeeds when called
// against the current leader — hashicorp/raft does not forward
// configuration changes, so the caller (membership package) must route
// this call to whichever node is currently leader.
func (n *Node) AddVoter(nodeID, addr string) error {
	future := n.Raft.AddVoter(hraft.ServerID(nodeID), hraft.ServerAddress(addr), 0, 10*time.Second)
	return future.Error()
}

// RemoveServer removes a voter or non-voter from the configuration.
// Called by the lifecycle controller after a prolonged, confirmed-dead
// core node (manual trigger only in this phase — see internal/cluster/lifecycle).
func (n *Node) RemoveServer(nodeID string) error {
	future := n.Raft.RemoveServer(hraft.ServerID(nodeID), 0, 10*time.Second)
	return future.Error()
}

// IsLeader reports whether this node currently holds raft leadership.
func (n *Node) IsLeader() bool {
	return n.Raft.State() == hraft.Leader
}

// LeadershipTransfer hands leadership to another voter, if this node is
// leader. No-op (returns nil) if it is not — used by the drain command so
// callers don't need to check IsLeader first.
func (n *Node) LeadershipTransfer() error {
	if !n.IsLeader() {
		return nil
	}
	future := n.Raft.LeadershipTransfer()
	return future.Error()
}

// Shutdown stops the raft instance.
func (n *Node) Shutdown() error {
	return n.Raft.Shutdown().Error()
}

// Snapshot forces an immediate raft snapshot and returns its ID and on-disk
// directory (under DataDir/snapshots), ready to be copied elsewhere by the
// `keel-gateway backup raft` CLI command. Blocks until the snapshot
// completes.
func (n *Node) Snapshot() (id string, dir string, err error) {
	future := n.Raft.Snapshot()
	if err := future.Error(); err != nil {
		return "", "", fmt.Errorf("raft: snapshot: %w", err)
	}
	meta, rc, err := future.Open()
	if err != nil {
		return "", "", fmt.Errorf("raft: open snapshot: %w", err)
	}
	_ = rc.Close()
	return meta.ID, filepath.Join(n.cfg.DataDir, "snapshots", meta.ID), nil
}

// RecoverConfig is the target configuration RecoverCluster forces onto a
// node recovering from total quorum loss.
type RecoverConfig struct {
	NodeID       string
	RaftBindAddr string
	DataDir      string
	// Voters lists every server that will make up the recovered cluster:
	// node ID -> raft bind address. Must be identical on every node
	// running RecoverCluster for this recovery, per hashicorp/raft's own
	// RecoverCluster doc comment.
	Voters map[string]string
}

// RecoverCluster is the disaster-recovery entry point for `keel-gateway
// restore raft`: it stages the snapshot at snapshotSrcDir (produced by
// `keel-gateway backup raft`, run against the leader before quorum was
// lost) into this node's DataDir, then calls hashicorp/raft's own
// RecoverCluster to force the given voter configuration.
//
// Run identically on every node participating in the recovered cluster,
// each against its own copy of the backed-up snapshot directory and DataDir
// — this is a disk-level operation with no running *raft.Raft instance
// involved, so it must complete on all voters before any of them starts
// normally (i.e. via NewNode) afterward. A fresh FSM is used and then
// discarded, per hashicorp/raft's own warning that the FSM passed to its
// RecoverCluster must never be reused by a running server.
func RecoverCluster(cfg RecoverConfig, snapshotSrcDir string) error {
	if len(cfg.Voters) == 0 {
		return fmt.Errorf("raft: recover cluster: at least one voter is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("raft: create data dir: %w", err)
	}
	if err := stageSnapshot(snapshotSrcDir, cfg.DataDir); err != nil {
		return fmt.Errorf("raft: stage snapshot: %w", err)
	}

	raftCfg := hraft.DefaultConfig()
	raftCfg.LocalID = hraft.ServerID(cfg.NodeID)
	raftCfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft-recover." + cfg.NodeID,
		Level:  hclog.Info,
		Output: os.Stderr,
	})

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBindAddr)
	if err != nil {
		return fmt.Errorf("raft: resolve bind addr %q: %w", cfg.RaftBindAddr, err)
	}
	transport, err := hraft.NewTCPTransport(cfg.RaftBindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft: create tcp transport: %w", err)
	}
	defer transport.Close()

	snaps, err := hraft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("raft: create snapshot store: %w", err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("raft: create bolt store: %w", err)
	}
	// Must close explicitly: bbolt holds an exclusive flock on the file for
	// the life of the *bolt.DB — left open, any later NewNode against the
	// same DataDir (e.g. actually starting the recovered node) deadlocks
	// forever waiting for a lock this function's own boltStore never
	// released.
	defer boltStore.Close()

	servers := make([]hraft.Server, 0, len(cfg.Voters))
	for id, addr := range cfg.Voters {
		servers = append(servers, hraft.Server{ID: hraft.ServerID(id), Address: hraft.ServerAddress(addr)})
	}
	configuration := hraft.Configuration{Servers: servers}

	return hraft.RecoverCluster(raftCfg, NewFSM(), boltStore, boltStore, snaps, transport, configuration)
}

// stageSnapshot copies the backed-up snapshot directory srcDir (containing
// meta.json + state.bin, as produced by Node.Snapshot/backup raft) into
// dataDir/snapshots/<id> — the layout hraft.FileSnapshotStore requires,
// with <id> matching meta.json's own ID field so FileSnapshotStore.Open can
// find it by name. <id> is read from meta.json rather than assumed from
// srcDir's own name, since the backup's transport (scp, tar, an object
// store download, ...) may not preserve directory names.
func stageSnapshot(srcDir, dataDir string) error {
	metaRaw, err := os.ReadFile(filepath.Join(srcDir, "meta.json"))
	if err != nil {
		return fmt.Errorf("read meta.json: %w", err)
	}
	var meta hraft.SnapshotMeta
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		return fmt.Errorf("parse meta.json: %w", err)
	}
	if meta.ID == "" {
		return fmt.Errorf("meta.json has an empty snapshot ID")
	}

	destDir := filepath.Join(dataDir, "snapshots", meta.ID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(srcDir, e.Name()))
		if err != nil {
			return fmt.Errorf("read %s: %w", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(destDir, e.Name()), data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", e.Name(), err)
		}
	}
	return nil
}
