package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"google.golang.org/grpc"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/broker"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/lifecycle"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/management"
	"github.com/keel-iot/keel-mqtt-gateway/internal/livestatsapi"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
	clusterstore "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/store"
	"github.com/keel-iot/keel-mqtt-gateway/internal/commander"
	"github.com/keel-iot/keel-mqtt-gateway/internal/config"
	"github.com/keel-iot/keel-mqtt-gateway/internal/db"
	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
	"github.com/keel-iot/keel-mqtt-gateway/internal/httpapi"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
	"github.com/keel/pkg/redpanda"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "drain" {
		runDrainCLI(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "acl" {
		runACLCLI(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "backup" {
		runBackupCLI(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "restore" {
		runRestoreCLI(os.Args[2:])
		return
	}
	runServer()
}

// runDrainCLI implements the `keel-gateway drain` subcommand: a thin HTTP
// client against the local node's own management API, meant to be called
// from a K8s preStop hook. It does not touch raft or gossip directly —
// see internal/cluster/lifecycle.Drain, which runs inside the server
// process and does the actual work.
func runDrainCLI(args []string) {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	mgmtAddr := fs.String("management-addr", "http://localhost:8090", "base URL of this node's management API")
	_ = fs.Parse(args)

	resp, err := http.Post(*mgmtAddr+"/api/cluster/drain", "application/octet-stream", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drain: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "drain: management API returned %s\n", resp.Status)
		os.Exit(1)
	}
	fmt.Println("drain: ok")
}

// runACLCLI implements the `keel-gateway acl ...` subcommands: a thin HTTP
// client against a core node's management API (see
// internal/cluster/management.API's /api/acl/* routes). Like runDrainCLI,
// it does not touch raft directly — mutations are forwarded to the raft
// leader by the management API's ACLAdmin implementation.
func runACLCLI(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "acl: expected a subcommand: roles-list, role-create, role-delete, bindings-list, binding-create, binding-delete, rulesets-list, ruleset-enable, ruleset-disable")
		os.Exit(1)
	}

	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("acl "+sub, flag.ExitOnError)
	mgmtAddr := fs.String("management-addr", "http://localhost:8090", "base URL of a core node's management API")

	switch sub {
	case "roles-list":
		_ = fs.Parse(rest)
		aclGet(*mgmtAddr, "/api/acl/roles")
	case "role-create":
		name := fs.String("name", "", "role name")
		rulesJSON := fs.String("rules", "[]", "JSON array of acl.ACLRule")
		_ = fs.Parse(rest)
		if *name == "" {
			fmt.Fprintln(os.Stderr, "acl role-create: --name is required")
			os.Exit(1)
		}
		rules := json.RawMessage(*rulesJSON)
		body, err := json.Marshal(map[string]any{"name": *name, "rules": rules})
		if err != nil {
			fmt.Fprintf(os.Stderr, "acl role-create: invalid --rules: %v\n", err)
			os.Exit(1)
		}
		aclPost(*mgmtAddr, "/api/acl/roles", body)
	case "role-delete":
		name := fs.String("name", "", "role name")
		_ = fs.Parse(rest)
		if *name == "" {
			fmt.Fprintln(os.Stderr, "acl role-delete: --name is required")
			os.Exit(1)
		}
		aclDelete(*mgmtAddr, "/api/acl/roles/"+*name)
	case "bindings-list":
		_ = fs.Parse(rest)
		aclGet(*mgmtAddr, "/api/acl/bindings")
	case "binding-create":
		principal := fs.String("principal", "", "principal (clientID or username)")
		role := fs.String("role", "", "role name")
		_ = fs.Parse(rest)
		if *principal == "" || *role == "" {
			fmt.Fprintln(os.Stderr, "acl binding-create: --principal and --role are required")
			os.Exit(1)
		}
		body, _ := json.Marshal(map[string]string{"principal": *principal, "role_name": *role})
		aclPost(*mgmtAddr, "/api/acl/bindings", body)
	case "binding-delete":
		principal := fs.String("principal", "", "principal (clientID or username)")
		role := fs.String("role", "", "role name")
		_ = fs.Parse(rest)
		if *principal == "" || *role == "" {
			fmt.Fprintln(os.Stderr, "acl binding-delete: --principal and --role are required")
			os.Exit(1)
		}
		aclDelete(*mgmtAddr, "/api/acl/bindings/"+*principal+"/"+*role)
	case "rulesets-list":
		_ = fs.Parse(rest)
		aclGet(*mgmtAddr, "/api/acl/rulesets")
	case "ruleset-enable":
		name := fs.String("name", "", "ruleset name")
		_ = fs.Parse(rest)
		if *name == "" {
			fmt.Fprintln(os.Stderr, "acl ruleset-enable: --name is required")
			os.Exit(1)
		}
		aclPost(*mgmtAddr, "/api/acl/rulesets/"+*name+"/enable", nil)
	case "ruleset-disable":
		name := fs.String("name", "", "ruleset name")
		_ = fs.Parse(rest)
		if *name == "" {
			fmt.Fprintln(os.Stderr, "acl ruleset-disable: --name is required")
			os.Exit(1)
		}
		aclPost(*mgmtAddr, "/api/acl/rulesets/"+*name+"/disable", nil)
	default:
		fmt.Fprintf(os.Stderr, "acl: unknown subcommand %q\n", sub)
		os.Exit(1)
	}
}

func aclGet(mgmtAddr, path string) {
	resp, err := http.Get(mgmtAddr + path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "acl: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "acl: management API returned %s\n", resp.Status)
		os.Exit(1)
	}
	io.Copy(os.Stdout, resp.Body)
}

func aclPost(mgmtAddr, path string, body []byte) {
	resp, err := http.Post(mgmtAddr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "acl: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "acl: management API returned %s: %s\n", resp.Status, strings.TrimSpace(string(respBody)))
		os.Exit(1)
	}
	fmt.Println("acl: ok")
}

func aclDelete(mgmtAddr, path string) {
	req, err := http.NewRequest(http.MethodDelete, mgmtAddr+path, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "acl: request build failed: %v\n", err)
		os.Exit(1)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "acl: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "acl: management API returned %s: %s\n", resp.Status, strings.TrimSpace(string(respBody)))
		os.Exit(1)
	}
	fmt.Println("acl: ok")
}

// runBackupCLI implements `keel-gateway backup raft --output <path>`: a
// thin client against this node's own management API (like drain/acl),
// meant to be run via `docker exec`/`kubectl exec` into the same
// container/pod as the target node — it copies the snapshot directory the
// management API reports off the node's local filesystem, no network
// transfer of its own.
//
// TODO(backup): upload --output to S3/GCS/etc for off-node durability is
// explicitly out of scope here — left as a follow-up operator step (e.g.
// `aws s3 cp --recursive`) or future flag, not silently unimplemented.
func runBackupCLI(args []string) {
	if len(args) == 0 || args[0] != "raft" {
		fmt.Fprintln(os.Stderr, `backup: expected subcommand "raft"`)
		os.Exit(1)
	}
	fs := flag.NewFlagSet("backup raft", flag.ExitOnError)
	mgmtAddr := fs.String("management-addr", "http://localhost:8090", "base URL of this node's management API")
	output := fs.String("output", "", "local directory to copy the snapshot into (created if missing)")
	_ = fs.Parse(args[1:])

	if *output == "" {
		fmt.Fprintln(os.Stderr, "backup raft: --output is required")
		os.Exit(1)
	}

	resp, err := http.Post(*mgmtAddr+"/api/cluster/snapshot", "application/octet-stream", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "backup raft: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "backup raft: management API returned %s: %s\n", resp.Status, strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	var snap struct {
		ID  string `json:"id"`
		Dir string `json:"dir"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		fmt.Fprintf(os.Stderr, "backup raft: decode response: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*output, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "backup raft: create output dir: %v\n", err)
		os.Exit(1)
	}
	if err := copyFlatDir(snap.Dir, *output); err != nil {
		fmt.Fprintf(os.Stderr, "backup raft: copy snapshot: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("backup raft: ok — snapshot %s copied from %s to %s\n", snap.ID, snap.Dir, *output)
}

// copyFlatDir copies the (non-recursive) file contents of src into dst —
// raft snapshot directories are always flat (meta.json + state.bin), so no
// recursive-copy machinery is needed here.
func copyFlatDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return fmt.Errorf("read %s: %w", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", e.Name(), err)
		}
	}
	return nil
}

// runRestoreCLI implements `keel-gateway restore raft --snapshot <path>
// --voters <node1[@addr1],node2[@addr2],...>`: the disaster-recovery
// procedure for total core quorum loss. Unlike backup/drain/acl, this does
// NOT talk to a running node's management API — it operates directly on
// this node's local raft data directory (see keelraft.RecoverCluster's doc
// comment for why: it's a disk-level operation with no running *raft.Raft
// instance involved). Run identically on every node that will join the
// recovered cluster, each against its own local copy of the same backed-up
// snapshot and its own (empty/fresh) --raft-data-dir, before starting any
// of them normally (no --bootstrap needed afterward — the recovered log
// already encodes the given --voters configuration).
func runRestoreCLI(args []string) {
	if len(args) == 0 || args[0] != "raft" {
		fmt.Fprintln(os.Stderr, `restore: expected subcommand "raft"`)
		os.Exit(1)
	}
	fs := flag.NewFlagSet("restore raft", flag.ExitOnError)
	snapshotPath := fs.String("snapshot", "", "path to a local copy of the backed-up snapshot directory (see `backup raft`)")
	votersCSV := fs.String("voters", "", `comma-separated node-id[@raft-addr] list for the recovered cluster (e.g. "core-1@core-1:7000,core-2@core-2:7000,core-3@core-3:7000"); addr defaults to "<node-id>:7000" when omitted`)
	nodeID := fs.String("node-id", "", "this node's ID (default: hostname)")
	raftBindAddr := fs.String("raft-bind", ":7000", "this node's raft bind address")
	raftDataDir := fs.String("raft-data-dir", "/data/raft", "raft data directory to recover into (should be empty — total-loss recovery)")
	_ = fs.Parse(args[1:])

	if *nodeID == "" {
		*nodeID, _ = os.Hostname()
	}
	if *snapshotPath == "" {
		fmt.Fprintln(os.Stderr, "restore raft: --snapshot is required")
		os.Exit(1)
	}
	if *votersCSV == "" {
		fmt.Fprintln(os.Stderr, "restore raft: --voters is required")
		os.Exit(1)
	}

	voters := make(map[string]string)
	for _, entry := range strings.Split(*votersCSV, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, addr, ok := strings.Cut(entry, "@")
		if !ok {
			id, addr = entry, entry+":7000"
		}
		voters[id] = addr
	}

	if err := keelraft.RecoverCluster(keelraft.RecoverConfig{
		NodeID:       *nodeID,
		RaftBindAddr: *raftBindAddr,
		DataDir:      *raftDataDir,
		Voters:       voters,
	}, *snapshotPath); err != nil {
		fmt.Fprintf(os.Stderr, "restore raft: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("restore raft: ok — %s recovered into %s with %d voter(s); start this node normally (no --bootstrap) to rejoin\n", *nodeID, *raftDataDir, len(voters))
}

// clusterFlags holds the --role/--cluster-* flags for the core/edge
// clustering scaffold (internal/cluster). Cluster wiring is entirely
// opt-in: leaving --role unset preserves today's single-node behaviour.
type clusterFlags struct {
	role            string
	nodeID          string
	bootstrap       bool
	raftBindAddr    string
	raftDataDir     string
	gossipBindAddr  string
	gossipPort      int
	gossipAdvertise string
	gossipPeers     string
	grpcBindAddr    string
	grpcAdvertise   string
	managementAddr  string
	heartbeatTTL    time.Duration
	routingSweepTTL time.Duration

	// raftSnapshotInterval/raftSnapshotThreshold override hashicorp/raft's
	// defaults (120s / 8192 entries) — see keelraft.NodeConfig's doc
	// comment for why: this FSM (session ownership + ACL) is small and
	// low-write-volume, so both defaults are tuned down for a much smaller
	// working set.
	raftSnapshotInterval  time.Duration
	raftSnapshotThreshold uint64

	// routingReconcilerInterval is the base (pre-jitter) check interval for
	// routing.Reconciler — see its doc comment for the total-Olric-data-loss
	// self-healing scenario.
	routingReconcilerInterval time.Duration

	olricBindAddr        string
	olricPort            int
	olricGossipPort      int
	olricAdvertise       string
	olricBootstrapWait   time.Duration
	olricJoinRetry       time.Duration
	olricMaxJoinAttempts int

	// TLS listener flags. Not cluster-specific, but registered here since
	// flag.Parse() is only ever called once, in parseClusterFlags.
	tlsEnabled    bool
	tlsCertDir    string
	tlsClientAuth string

	// forwarderBufferSize is the output-connector (Kafka/Ditto) backpressure
	// buffer capacity, in messages. Not cluster-specific, registered here
	// for the same reason as the TLS flags above.
	forwarderBufferSize int

	// edgeConnectionsLimit/edgeCPULimit/edgeLoadScoreInterval feed
	// telemetry.RunEdgeLoadScoreSampler (keel_gateway_edge_load_score, the
	// real HPA custom metric) — see design doc "HPA sui nodi edge". Not
	// cluster-specific, registered here for the same reason as above.
	edgeConnectionsLimit  int
	edgeCPULimit          float64
	edgeLoadScoreInterval time.Duration
}

func parseClusterFlags() clusterFlags {
	var f clusterFlags
	flag.StringVar(&f.role, "role", "", `cluster role: "core" (raft+olric+mgmt-API only, no broker/HTTP/commander), "edge" (broker/HTTP/commander only, no raft/olric), "combined" (both — single-process all-in-one node, gossips as "core" to peers), or empty for standalone (no clustering, broker/HTTP/commander active)`)
	flag.StringVar(&f.nodeID, "node-id", "", "stable cluster node ID (default: hostname)")
	flag.BoolVar(&f.bootstrap, "bootstrap", false, "core only: form a brand-new single-node raft cluster on startup")
	flag.StringVar(&f.raftBindAddr, "raft-bind", ":7000", "core only: raft TCP transport bind/advertise address")
	flag.StringVar(&f.raftDataDir, "raft-data-dir", "/data/raft", "core only: raft log + snapshot directory (needs a PVC in K8s)")
	flag.StringVar(&f.gossipBindAddr, "gossip-bind", "0.0.0.0", "memberlist gossip bind address")
	flag.IntVar(&f.gossipPort, "gossip-port", 7946, "memberlist gossip bind/advertise port")
	flag.StringVar(&f.gossipAdvertise, "gossip-advertise", "", "memberlist advertise address (default: gossip-bind)")
	flag.StringVar(&f.gossipPeers, "gossip-peers", "", "comma-separated seed addresses (host:port) to join on startup")
	flag.StringVar(&f.grpcBindAddr, "grpc-bind", ":7100", "cluster gRPC (registry + dataplane) bind address")
	flag.StringVar(&f.grpcAdvertise, "grpc-advertise", "", "cluster gRPC address other nodes dial (default: grpc-bind)")
	flag.StringVar(&f.managementAddr, "management-addr", ":8090", "core only: HTTP management API bind address")
	flag.DurationVar(&f.heartbeatTTL, "heartbeat-threshold", 30*time.Second, "core only: how long a core node can be missing from gossip before the lifecycle monitor logs a warning and purges its routing entries")
	flag.DurationVar(&f.routingSweepTTL, "routing-sweep-threshold", 15*time.Minute, "core only: how long a node can be absent from gossip while still holding routing-table entries before the safety-net sweep logs it (observability only, never deletes)")
	flag.DurationVar(&f.raftSnapshotInterval, "raft-snapshot-interval", 30*time.Second, "core only: how often raft checks whether a new snapshot is due (see --raft-snapshot-threshold) — tuned down from hashicorp/raft's 120s default for this FSM's small session-ownership+ACL state")
	flag.Uint64Var(&f.raftSnapshotThreshold, "raft-snapshot-threshold", 128, "core only: minimum number of committed log entries since the last snapshot before one is taken — tuned down from hashicorp/raft's 8192 default, which assumes a much higher-write FSM than this one")
	flag.DurationVar(&f.routingReconcilerInterval, "routing-reconciler-interval", 20*time.Second, "edge/combined only: base (pre-jitter) interval routing.Reconciler checks this node's live local subscriptions against the routing store, re-asserting any missing — the self-heal mechanism for a total routing-store (Olric) data-loss event")
	flag.StringVar(&f.olricBindAddr, "olric-bind", "0.0.0.0", "core only: embedded Olric member (topic-filter routing table) bind address")
	flag.IntVar(&f.olricPort, "olric-port", 7300, "core only: embedded Olric member main protocol bind/advertise port")
	flag.IntVar(&f.olricGossipPort, "olric-gossip-port", 7301, "core only: embedded Olric member's own internal memberlist gossip port — separate from, and unaware of, this project's own --gossip-port")
	flag.StringVar(&f.olricAdvertise, "olric-advertise", "", "core only: Olric advertise address other core nodes dial (default: olric-bind)")
	flag.DurationVar(&f.olricBootstrapWait, "olric-bootstrap-wait", 3*time.Second, "core only: how long to wait after joining gossip before starting the embedded Olric member, so siblings starting at the same time are more likely to already be gossip-visible")
	flag.DurationVar(&f.olricJoinRetry, "olric-join-retry-interval", 300*time.Millisecond, "core only: gap between Olric join attempts against currently-known core peers before falling back to a single-node bootstrap — each attempt re-resolves the peer list live (see internal/cluster/store.OlricConfig.PeersFunc's doc). Widen alongside --olric-max-join-attempts for staggered rollouts (e.g. K8s), since Olric has no way to add a peer once this budget is exhausted and it has moved on")
	flag.IntVar(&f.olricMaxJoinAttempts, "olric-max-join-attempts", 5, "core only: number of Olric join attempts before falling back to a single-node bootstrap — see --olric-join-retry-interval")
	flag.BoolVar(&f.tlsEnabled, "tls-enabled", false, "require the MQTT TLS listener: readiness (/readyz) reports NotReady until a valid certificate is loaded from --tls-cert-dir, instead of silently falling back to plain TCP")
	flag.StringVar(&f.tlsCertDir, "tls-cert-dir", "", "directory containing tls.crt/tls.key (K8s Secret volume layout); watched and reloaded automatically on change, no restart needed. Required when --tls-enabled=true or MQTT_TLS_PORT is set")
	flag.StringVar(&f.tlsClientAuth, "tls-client-auth", "request", `tls.Config.ClientAuth for the TLS listener: "none", "request" (default — optional client cert, needed for the existing X.509 device-auth path), or "require-and-verify"`)
	flag.IntVar(&f.forwarderBufferSize, "forwarder-buffer-size", 1000, "capacity, in messages, of the bounded in-memory backpressure buffer sitting in front of the output connector (Kafka/Ditto); drop-oldest when full, see internal/connector.BufferedConnector")
	flag.IntVar(&f.edgeConnectionsLimit, "edge-connections-limit", 2000, "edge/combined/standalone only: expected max MQTT connections per pod, the denominator of edge_load_score's connections term — tune to the HPA scaling threshold actually chosen for the deployment")
	flag.Float64Var(&f.edgeCPULimit, "edge-cpu-limit", float64(runtime.NumCPU()), "edge/combined/standalone only: CPU cores available to this pod (match the Deployment's CPU request/limit), the denominator of edge_load_score's CPU term; defaults to the host's core count, only correct for non-K8s/no-cgroup-limit deployments")
	flag.DurationVar(&f.edgeLoadScoreInterval, "edge-load-score-interval", 15*time.Second, "edge/combined/standalone only: how often edge_load_score (and its connections/CPU components) is recomputed")
	flag.Parse()

	if f.nodeID == "" {
		f.nodeID, _ = os.Hostname()
	}
	if f.gossipAdvertise == "" {
		f.gossipAdvertise = f.gossipBindAddr
	}
	if f.grpcAdvertise == "" {
		f.grpcAdvertise = f.grpcBindAddr
	}
	if f.olricAdvertise == "" {
		f.olricAdvertise = f.olricBindAddr
	}
	return f
}

func runServer() {
	cf := parseClusterFlags()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	// --tls-enabled is a hard startup requirement (misconfiguration, not a
	// runtime cert problem) — a node started with TLS mandatory but no port
	// or cert dir configured at all should fail fast rather than silently
	// serve plain TCP. A present-but-invalid/expired certificate, by
	// contrast, is a day-2 runtime condition surfaced via /readyz below, not
	// a crash — see broker.CertReloader.
	if cf.tlsEnabled && (cfg.MQTTTLSPort == 0 || cf.tlsCertDir == "") {
		slog.Error("--tls-enabled=true requires MQTT_TLS_PORT and --tls-cert-dir to be set")
		os.Exit(1)
	}

	logLevel := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		logLevel = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("connected to database")

	// Schema owned by this repo (see internal/db) — no longer assumes
	// another service's migrations already created these tables.
	if err := db.Migrate(ctx, pool, log); err != nil {
		log.Error("run database migrations", "error", err)
		os.Exit(1)
	}

	validator := auth.NewValidator(pool)

	// ── Auth provider ─────────────────────────────────────────────────────────
	// The provider abstracts credential validation; defaults to PostgreSQL.
	var provider auth.AuthProvider
	switch cfg.AuthBackend {
	case "file":
		if cfg.CredentialFile == "" {
			log.Error("AUTH_BACKEND=file requires CREDENTIAL_FILE to be set")
			os.Exit(1)
		}
		provider = auth.NewFileProviderWithTTL(cfg.CredentialFile, cfg.CredentialCacheTTL)
		log.Info("auth: using file provider", "path", cfg.CredentialFile, "cache_ttl", cfg.CredentialCacheTTL)
	default: // "postgres" or empty
		provider = auth.NewPostgresProvider(validator)
		log.Info("auth: using postgres provider")
	}
	// ── OpenTelemetry tracer ──────────────────────────────────────────────────
	// tenantCache is not yet constructed at this point — InitTracer accepts nil
	// and re-wires after tenantCache is ready.
	tracerShutdown, err := telemetry.InitTracer(ctx, cfg.OTLPEndpoint, nil, log)
	if err != nil {
		log.Error("init tracer", "error", err)
		os.Exit(1)
	}
	defer func() { _ = tracerShutdown(context.Background()) }()

	// ── Tenant config cache ───────────────────────────────────────────────────
	tenantCache := auth.NewTenantConfigCache(pool, cfg.TenantCacheTTL)

	// ── JWKS cache (per-tenant JWT key rotation, e.g. Clavex) ─────────────────
	jwksCache := auth.NewJWKSCache(cfg.JWKSCacheTTL)

	// Re-init tracer with tenant-aware sampler now that tenantCache is ready.
	if cfg.OTLPEndpoint != "" {
		_ = tracerShutdown(context.Background()) // close the no-op provider
		var newShutdown func(context.Context) error
		newShutdown, err = telemetry.InitTracer(ctx, cfg.OTLPEndpoint, tenantCache, log)
		if err != nil {
			log.Error("re-init tracer with tenant cache", "error", err)
			os.Exit(1)
		}
		tracerShutdown = newShutdown
	}

	// ── Redis (optional) ─────────────────────────────────────────────────────
	// rdb is a *redisrouter.Router, not a raw *redis.Client: the single
	// swappable indirection point every Redis consumer below shares (QoS/
	// session persistence, tenant data-volume limiting), so a primary
	// failover (core-colocated Redis primary+replica — see
	// keel-design-doc.md's risk #6) redirects all of them at once instead
	// of needing a separate swap site per consumer.
	var rdb *redisrouter.Router
	if cfg.RedisAddr != "" {
		var err error
		rdb, err = redisrouter.New(ctx, cfg.RedisAddr, cfg.RedisPassword)
		if err != nil {
			log.Error("connect to Redis", "addr", cfg.RedisAddr, "error", err)
			os.Exit(1)
		}
		defer rdb.Close()
		log.Info("mqtt-gateway: Redis connected", "addr", cfg.RedisAddr)
	} else {
		log.Warn("REDIS_ADDR not set — session persistence and volume limiting disabled")
	}

	// ── Cluster (core/edge) ────────────────────────────────────────────────────
	// Entirely opt-in: --role unset (the default) preserves today's
	// single-node behaviour with every variable below left nil/zero.
	//
	// Session ownership (raft) and topic-filter routing (Olric, via
	// internal/cluster/routing) are two independent backends composed
	// behind keelraft.CoreRegistry — see that type's doc. Olric runs its
	// own embedded gossip ring, entirely separate from this project's own
	// membership package (core/edge discovery) — the two memberships
	// cannot share a single memberlist.Memberlist instance, so instead
	// their peer *configuration* is aligned: Olric's peer list is
	// resolved from this node's own gossip-discovered core siblings, live
	// on every join attempt during startup (see the olricBootstrapWait
	// comment below for the resulting limitation and
	// clusterstore.OlricConfig.PeersFunc's doc for the full mechanism).
	var (
		raftNode          *keelraft.Node
		clusterMembership *membership.Membership
		clusterRegistry   keelraft.Registry
		clusterFwd        dataplane.Forwarder
		gForwarder        *dataplane.GRPCForwarder
		clusterGRPCServer *grpc.Server
		mgmtServer        *http.Server
		olricStore        *clusterstore.OlricStore
		clusterRouter     *routing.Router
	)
	// isCoreRole is true for both "core" (pure) and "combined" (core duties
	// plus a local broker) — everything raft/Olric/mgmt-API-related below
	// gates on this, not on the literal "core" string, so a combined node
	// gets the exact same core-side wiring a pure core node does.
	// brokerRuntimeEnabled is true for every role except pure "core": the
	// MQTT broker, HTTP adapter, commander and device-side Redpanda
	// forwarder are all client-traffic components a pure core node has no
	// use for (see the split-topology isolation test's KNOWN GAP note this
	// closes).
	isCoreRole := cf.role == "core" || cf.role == "combined"
	brokerRuntimeEnabled := cf.role != "core"

	if cf.role != "" {
		log.Info("cluster: starting", "role", cf.role, "node_id", cf.nodeID)

		if isCoreRole {
			raftNode, err = keelraft.NewNode(keelraft.NodeConfig{
				NodeID:            cf.nodeID,
				RaftBindAddr:      cf.raftBindAddr,
				DataDir:           cf.raftDataDir,
				SnapshotInterval:  cf.raftSnapshotInterval,
				SnapshotThreshold: cf.raftSnapshotThreshold,
			})
			if err != nil {
				log.Error("cluster: create raft node", "error", err)
				os.Exit(1)
			}
			if cf.bootstrap {
				did, err := raftNode.Bootstrap()
				if err != nil {
					log.Error("cluster: bootstrap raft cluster", "error", err)
					os.Exit(1)
				}
				if did {
					log.Info("cluster: bootstrapped single-node raft cluster")
				} else {
					log.Info("cluster: raft log already exists, skipping bootstrap (restart)")
				}
			}
		}

		var peers []string
		for _, p := range strings.Split(cf.gossipPeers, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peers = append(peers, p)
			}
		}
		raftAdvertiseAddr := ""
		olricAdvertiseAddr := ""
		olricGossipAdvertiseAddr := ""
		olricClientAdvertiseAddr := ""
		redisAddrForGossip := ""
		if isCoreRole {
			raftAdvertiseAddr = cf.raftBindAddr
			olricAdvertiseAddr = cf.olricAdvertise
			// NodeMeta.OlricAddr seeds siblings' Olric Peers list, which
			// must be Olric's own internal memberlist gossip address
			// (olric-gossip-port), not its main protocol address
			// (olric-port) — the two are entirely separate ports, see
			// internal/cluster/store.OlricConfig's doc.
			olricGossipAdvertiseAddr = net.JoinHostPort(cf.olricAdvertise, strconv.Itoa(cf.olricGossipPort))
			// NodeMeta.OlricClientAddr is the main protocol address (not
			// the gossip one above) — what edge nodes dial to build a
			// thin store.NewRemoteOlricStore client (see EdgeRegistry).
			olricClientAdvertiseAddr = net.JoinHostPort(cf.olricAdvertise, strconv.Itoa(cf.olricPort))
			// NodeMeta.RedisAddr is core-only, same as OlricAddr above —
			// this core's own co-located Redis instance (REDIS_ADDR on a
			// core process IS that instance's address, by deployment
			// convention; see docker-compose.core-edge-split.yml). An
			// edge's own cfg.RedisAddr is just its Router's bootstrap
			// seed, never gossiped — NodeMeta.RedisAddr only ever
			// describes a core's co-located instance.
			redisAddrForGossip = cfg.RedisAddr
		}
		httpAddrForGossip := ""
		if brokerRuntimeEnabled {
			// NodeMeta.HTTPAddr feeds internal/cluster/management's
			// cluster-wide GET /api/metrics/GET /api/live/clients
			// aggregation (see internal/livestatsapi) — same host as
			// grpc-advertise (same pod/container by construction), metrics
			// port instead of the gRPC one.
			if host, _, err := net.SplitHostPort(cf.grpcAdvertise); err == nil {
				if _, metricsPort, err := net.SplitHostPort(cfg.MetricsAddr); err == nil {
					httpAddrForGossip = net.JoinHostPort(host, metricsPort)
				}
			}
		}
		// A combined node gossips as plain "core" — the membership package
		// only knows RoleCore/RoleEdge, and combined nodes must be
		// discoverable as core peers (CoreGRPCAddrs, CoreOlricAddrs, raft
		// voter reconciliation) exactly like a pure core node. The extra
		// local broker a combined node runs is invisible to the cluster
		// protocol — it's just a local mochi-mqtt instance receiving
		// dataplane forwards like any other node's.
		gossipRole := cf.role
		if gossipRole == "combined" {
			gossipRole = "core"
		}
		clusterMembership, err = membership.New(membership.Config{
			NodeID:          cf.nodeID,
			Role:            membership.Role(gossipRole),
			BindAddr:        cf.gossipBindAddr,
			BindPort:        cf.gossipPort,
			AdvertiseAddr:   cf.gossipAdvertise,
			Peers:           peers,
			RaftAddr:        raftAdvertiseAddr,
			GRPCAddr:        cf.grpcAdvertise,
			OlricAddr:       olricGossipAdvertiseAddr,
			OlricClientAddr: olricClientAdvertiseAddr,
			RedisAddr:       redisAddrForGossip,
			RedisPassword:   cfg.RedisPassword,
			HTTPAddr:        httpAddrForGossip,
			RaftNode:        raftNode,
		}, log)
		if err != nil {
			log.Error("cluster: join gossip cluster", "error", err)
			os.Exit(1)
		}

		gForwarder = dataplane.NewGRPCForwarder(clusterMembership.NodeGRPCAddr, log)
		clusterFwd = gForwarder

		clusterGRPCServer = grpc.NewServer()
		dataplane.RegisterServer(clusterGRPCServer, gForwarder)

		if isCoreRole {
			// See the package doc atop this block and
			// clusterstore.OlricConfig.PeersFunc's doc: Olric has no
			// incremental-join API once Start() has returned, so a
			// sibling that only becomes gossip-visible after the join
			// budget below (--olric-join-retry-interval *
			// --olric-max-join-attempts) is exhausted is never discovered
			// by this node's Olric member on its own — a real limitation
			// relative to raft's reconcileVotersLoop, which can correct a
			// late/missed join at any time. PeersFunc re-resolves
			// clusterMembership.CoreOlricAddrs() live on every join
			// attempt *within* that budget, so widen the two join flags
			// (not just this wait) for staggered rollouts. The wait here
			// just improves the odds of a good first attempt.
			if cf.olricBootstrapWait > 0 {
				time.Sleep(cf.olricBootstrapWait)
			}
			olricStore, err = clusterstore.NewEmbeddedOlricStore(clusterstore.OlricConfig{
				BindAddr:      cf.olricBindAddr,
				BindPort:      cf.olricPort,
				GossipPort:    cf.olricGossipPort,
				AdvertiseAddr: olricAdvertiseAddr,
				PeersFunc: func() ([]string, error) {
					return clusterMembership.CoreOlricAddrs(), nil
				},
				JoinRetryInterval: cf.olricJoinRetry,
				MaxJoinAttempts:   cf.olricMaxJoinAttempts,
				Log:               log,
			})
			if err != nil {
				log.Error("cluster: start olric store", "error", err)
				os.Exit(1)
			}
			clusterRouter, err = routing.New(routing.Config{Store: olricStore, Log: log})
			if err != nil {
				log.Error("cluster: start routing router", "error", err)
				os.Exit(1)
			}

			clusterRegistry = keelraft.NewCoreRegistry(raftNode.Registry, clusterRouter, func() (string, bool) {
				leaderID := raftNode.Registry.LeaderID()
				if leaderID == "" {
					return "", false
				}
				return clusterMembership.NodeGRPCAddr(leaderID)
			}, log)
			keelraft.Register(clusterGRPCServer, clusterRegistry)
		} else if cf.role == "edge" {
			remoteRegistry := keelraft.NewRemoteRegistry(clusterMembership.CoreGRPCAddrs, log)

			// Local routing cache: a thin (non-gossip-member) Olric
			// client — store.NewRemoteOlricStore's doc says it's "used by
			// edge nodes", but nothing ever constructed it here before
			// this; NodesFor/Subscribe/Unsubscribe instead fell through
			// to a gRPC call per invocation via remoteRegistry. Same
			// routing.Router local trie cache + pub/sub + periodic Scan
			// core nodes use, just a client-mode store underneath.
			//
			// Retried with live re-resolution of core addresses: right
			// after this node joins gossip, core peers may not be
			// gossip-visible yet, and even once visible, a core node's
			// own embedded Olric member is still warming up
			// (--olric-bootstrap-wait) — a first-attempt failure here is
			// an expected race at cluster startup, not a fatal
			// misconfiguration, so this retries instead of os.Exit(1)-ing
			// the way core's Olric join budget (--olric-join-retry-interval
			// / --olric-max-join-attempts) already does on the core side.
			// Deliberately separate from --olric-join-retry-interval /
			// --olric-max-join-attempts above: those are documented and
			// tuned as "core only" (core-to-core Olric gossip join, ~300ms
			// * 5 attempts). An edge node's wait is for a core's embedded
			// Olric member to finish its own --olric-bootstrap-wait
			// (default 3s) plus startup, a different and longer timing
			// profile, so it gets its own budget rather than overloading
			// those flags' meaning.
			const (
				edgeOlricRetryInterval = time.Second
				edgeOlricMaxAttempts   = 30
			)
			var edgeOlricStore *clusterstore.OlricStore
			var edgeRouter *routing.Router
			for attempt := 1; ; attempt++ {
				addrs := clusterMembership.CoreOlricClientAddrs()
				edgeOlricStore, err = clusterstore.NewRemoteOlricStore(addrs, "")
				if err == nil {
					// routing.New's initial reconcile also dials Olric
					// (a Scan) — construction can succeed against
					// whichever peer answered discovery while the
					// reconcile's Scan lands on a sibling that isn't
					// listening yet, so both steps share one retry loop
					// rather than only guarding the first.
					edgeRouter, err = routing.New(routing.Config{Store: edgeOlricStore, Log: log})
					if err == nil {
						break
					}
					_ = edgeOlricStore.Close(context.Background())
				}
				if attempt >= edgeOlricMaxAttempts {
					log.Error("cluster: start edge olric client/router", "attempts", attempt, "error", err)
					os.Exit(1)
				}
				log.Warn("cluster: edge olric client/router not ready yet, retrying", "attempt", attempt, "addrs", addrs, "error", err)
				time.Sleep(edgeOlricRetryInterval)
			}

			// Local ACL cache: periodically pulls a full snapshot from
			// any reachable core node instead of an EvaluateACL gRPC call
			// per publish/subscribe. See ACLCache's doc for the accepted
			// staleness trade-off (no push invalidation, poll only).
			aclCache := keelraft.NewACLCache(remoteRegistry.ACLSnapshot, 0, log)

			clusterRegistry = keelraft.NewEdgeRegistry(edgeRouter, remoteRegistry, aclCache)
		}

		// Redirect the shared Redis router to whichever node raft has
		// designated primary — runs on every node (core and edge alike),
		// unlike the failover DECISION itself (membership's
		// redisFailoverLoop), which only the raft leader makes. See
		// internal/cluster/redisrouter's package doc and
		// keel-design-doc.md's risk #6.
		if rdb != nil {
			go redisrouter.WatchPrimary(ctx, rdb, clusterRegistry.CurrentRedisPrimary, clusterMembership.RedisAddrForNode, log)
		}

		grpcListener, err := net.Listen("tcp", cf.grpcBindAddr)
		if err != nil {
			log.Error("cluster: listen grpc", "addr", cf.grpcBindAddr, "error", err)
			os.Exit(1)
		}
		go func() {
			log.Info("cluster: grpc server starting", "addr", cf.grpcBindAddr)
			if err := clusterGRPCServer.Serve(grpcListener); err != nil {
				log.Error("cluster: grpc server error", "error", err)
			}
		}()

		if isCoreRole {
			mgmtAPI := &management.API{
				SelfNodeID:      cf.nodeID,
				RaftNode:        raftNode,
				Membership:      clusterMembership,
				ClusterRegistry: clusterRegistry,
				Log:             log,
			}
			mgmtServer = &http.Server{
				Addr:         cf.managementAddr,
				Handler:      mgmtAPI.Router(),
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 5 * time.Second,
			}
			go func() {
				log.Info("cluster: management API starting", "addr", cf.managementAddr)
				if err := mgmtServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Error("cluster: management API error", "error", err)
				}
			}()

			monitor := lifecycle.NewMonitor(clusterMembership.Members, cf.heartbeatTTL, log)
			// clusterRegistry is always a *CoreRegistry in this branch (set
			// above), so this type assertion always succeeds; the ok-check
			// is just defensive against future wiring changes.
			if purger, ok := clusterRegistry.(keelraft.NodePurger); ok {
				monitor.PurgeNode = purger.PurgeNode
			}
			go monitor.Run(ctx)

			if routesProvider, ok := clusterRegistry.(keelraft.NodesWithRoutesProvider); ok {
				sweep := lifecycle.NewRoutingSweep(routesProvider.NodesWithRoutes, clusterMembership.Members, cf.routingSweepTTL, log)
				go sweep.Run(ctx)
			}
		}
	}

	// ── Prometheus metrics server ─────────────────────────────────────────────
	// mqttServer/httpServer/certReloader are declared here (rather than
	// where they're assigned, further down) so the /readyz closure below can
	// capture certReloader by reference and see its final value once the
	// broker is constructed — the metrics server itself doesn't start
	// serving requests until later in this function anyway.
	var mqttServer *mqtt.Server
	var httpServer *http.Server
	var certReloader *broker.CertReloader

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// currentRedisPrimary stays nil outside cluster mode (clusterRegistry
	// nil): readyz's Redis check then reduces to "can we Ping it", which is
	// still the right check for the single-node case.
	var currentRedisPrimary func() (string, bool)
	if clusterRegistry != nil {
		currentRedisPrimary = clusterRegistry.CurrentRedisPrimary
	}
	metricsMux.HandleFunc("/readyz", newReadyzHandler(cf.tlsEnabled, &certReloader, rdb, currentRedisPrimary))
	// TEMPORARY diagnostic instrumentation for the devicesim churn-scenario
	// memory investigation (2026-07-24): mounts net/http/pprof on the same
	// metrics listener, gated behind an env var so it's a no-op unless
	// explicitly requested. Not part of the permanent management surface —
	// remove once the memory question is settled, or promote to a proper
	// flag if it turns out to be generally useful.
	if os.Getenv("KEEL_ENABLE_PPROF") == "true" {
		metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
		metricsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		metricsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		metricsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		metricsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		log.Info("mqtt-gateway: pprof enabled on metrics listener (KEEL_ENABLE_PPROF=true) — temporary diagnostic, not for production use")
	}
	metricsServer := &http.Server{
		Addr:         cfg.MetricsAddr,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("mqtt-gateway: metrics server starting", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", "error", err)
		}
	}()

	// ── Redpanda producer ─────────────────────────────────────────────────────
	// Only needed by the broker/HTTP adapter (device→platform telemetry
	// forwarding) — a pure core node runs neither, so it skips the Redpanda
	// connection entirely rather than holding one open unused.
	var fwd *forwarder.Forwarder
	if brokerRuntimeEnabled && len(cfg.RedpandaBrokers) > 0 {
		producer, err := redpanda.NewProducer(redpanda.ProducerConfig{
			Brokers:      cfg.RedpandaBrokers,
			ClientID:     "mqtt-gateway",
			SASLUsername: cfg.RedpandaSASLUser,
			SASLPassword: cfg.RedpandaSASLPass,
		})
		if err != nil {
			log.Error("connect to redpanda", "error", err)
			os.Exit(1)
		}
		defer producer.Close()
		fwd = forwarder.New(forwarder.Config{
			Producer:          producer,
			TwinInboundTopic:  cfg.TwinInboundTopic,
			OTAStatusTopic:    cfg.OTAStatusTopic,
			CAStatusTopic:     cfg.CAStatusTopic,
			ConnectionTopic:   cfg.DeviceConnectionTopic,
			DittoCompat:       cfg.DittoCompat,
			DittoInboundTopic: cfg.DittoInboundTopic,
			HonoCompat:        cfg.HonoCompat,
			RDB:               rdb,
			TenantCache:       tenantCache,
			Log:               log,
		})
		log.Info("redpanda producer ready", "brokers", cfg.RedpandaBrokers)
	} else {
		fwd = forwarder.NewNoopForwarder(log)
		log.Warn("REDPANDA_BROKERS not set — event forwarding disabled")
	}

	// ── Output connector (external system integration) ─────────────────────────────
	var outputConn connector.OutputConnector
	if cfg.OutputConnector != "" {
		factory, ok := connector.Registry[cfg.OutputConnector]
		if !ok {
			log.Error("output connector: unknown type", "type", cfg.OutputConnector)
			os.Exit(1)
		}

		connConfig := map[string]string{
			"enabled":       "true",
			"brokers":       cfg.KafkaHonoBrokers,
			"sasl_username": cfg.KafkaHonoSASLUser,
			"sasl_password": cfg.KafkaHonoSASLPass,
			"topic_prefix":  cfg.KafkaHonoTopicPrefix,
			"client_id":     "keel-mqtt-gateway-output",
		}

		var built connector.OutputConnector
		var err error
		built, err = factory(connConfig)
		if err != nil {
			log.Error("output connector: create failed", "type", cfg.OutputConnector, "error", err)
			os.Exit(1)
		}

		// Wrapped in a bounded backpressure buffer (drop-oldest when full)
		// so a slow/unavailable Kafka/Ditto never blocks the MQTT publish
		// hot path — see internal/connector.BufferedConnector.
		buffered := connector.NewBuffered(built, cf.forwarderBufferSize, cfg.OutputConnector, log)
		outputConn = buffered

		if err := buffered.Init(ctx, connConfig); err != nil {
			log.Error("output connector: init failed", "type", cfg.OutputConnector, "error", err)
			os.Exit(1)
		}
		buffered.Start(ctx)

		defer func() {
			_ = buffered.Shutdown(context.Background())
		}()
		log.Info("output connector: ready", "type", cfg.OutputConnector, "buffer_size", cf.forwarderBufferSize)
	}

	// ── MQTT broker, commander, HTTP adapter ──────────────────────────────────
	// All three are client-traffic components (device MQTT/HTTP ingestion,
	// platform→device commands) — a pure "core" node has no connected
	// devices to serve, so it runs none of them. "edge", "combined", and
	// standalone (role == "") all run the full set.
	if brokerRuntimeEnabled {
		// Basic monitoring UI (see internal/livestatsapi and
		// internal/cluster/management's aggregation of it): feeds
		// GET /api/live/stats' messages/sec figure. Must exist before
		// broker.New so hooks.go's OnPublish can record into it.
		liveStats := telemetry.NewLiveStats()
		liveStats.Start(ctx, time.Second)

		mqttServer, certReloader, err = broker.New(broker.Config{
			MQTTPort:              cfg.MQTTPort,
			MQTTTLSPort:           cfg.MQTTTLSPort,
			TLSCertDir:            cf.tlsCertDir,
			TLSClientAuth:         cf.tlsClientAuth,
			TenantConfigCache:     tenantCache,
			JWKSCache:             jwksCache,
			AutoProvisioningURL:   cfg.AutoProvisioningURL,
			RedisClient:           rdb,
			ClusterRegistry:       clusterRegistry,
			ClusterFwd:            clusterFwd,
			ClusterNodeID:         cf.nodeID,
			OutputConnector:       outputConn,
			SessionExpiryInterval: cfg.SessionExpiryInterval,
			LiveStats:             liveStats,
		}, provider, fwd, log)
		if err != nil {
			log.Error("create MQTT broker", "error", err)
			os.Exit(1)
		}

		// keel_gateway_edge_load_score: the real HPA custom metric (see
		// design doc "HPA sui nodi edge"). ClientsConnected is tracked
		// natively by mochi-mqtt; read directly, no extra bookkeeping needed.
		srv := mqttServer
		go telemetry.RunEdgeLoadScoreSampler(ctx,
			func() int { return int(atomic.LoadInt64(&srv.Info.ClientsConnected)) },
			cf.edgeConnectionsLimit, cf.edgeCPULimit, cf.edgeLoadScoreInterval)

		// GET /api/live/stats, GET /api/live/clients — mounted on the
		// metrics mux (already serving, safe to add routes to a live
		// http.ServeMux — see net/http's docs), aggregated cluster-wide by
		// internal/cluster/management's GET /api/metrics.
		liveHandlers := &livestatsapi.Handlers{
			Clients: func() []livestatsapi.ClientView {
				all := srv.Clients.GetAll()
				out := make([]livestatsapi.ClientView, 0, len(all))
				for _, cl := range all {
					if cl.Closed() || cl.Net.Inline {
						continue
					}
					subs := cl.State.Subscriptions.GetAll()
					filters := make([]string, 0, len(subs))
					for filter := range subs {
						filters = append(filters, filter)
					}
					out = append(out, livestatsapi.ClientView{
						ClientID:      cl.ID,
						Username:      string(cl.Properties.Username),
						RemoteAddr:    cl.Net.Remote,
						CleanSession:  cl.Properties.Clean,
						Subscriptions: filters,
					})
				}
				return out
			},
			Stats: func() livestatsapi.StatsView {
				snap := liveStats.Snapshot()
				return livestatsapi.StatsView{
					NodeID:            cf.nodeID,
					ActiveConnections: int(atomic.LoadInt64(&srv.Info.ClientsConnected)),
					TotalMessages:     snap.TotalMessages,
					MessagesPerSecond: snap.MessagesPerSecond,
					TotalBytes:        snap.TotalBytes,
					BytesPerSecond:    snap.BytesPerSecond,
				}
			},
		}
		liveHandlers.Register(metricsMux)

		// Inbound side of the cluster data plane: a message another node
		// forwarded to us (because we own a local subscriber for its topic)
		// gets republished into this node's own mochi-mqtt server, which
		// delivers it exactly like any other local publish.
		if gForwarder != nil {
			_ = gForwarder.Subscribe(func(msg *dataplane.Message) {
				if err := mqttServer.Publish(msg.Topic, msg.Payload, false, msg.QoS); err != nil {
					log.Error("cluster: publish forwarded message locally", "topic", msg.Topic, "error", err)
				}
			})

			// Inbound side of session-ownership takeover: another node just
			// won a ClaimSession for a client_id this node still has a local
			// connection for (see internal/broker/hooks.go's
			// OnConnectAuthenticate) — disconnect it locally. mochi-mqtt's
			// own OnDisconnect hook fires normally from this Stop, so routing
			// cleanup/ReleaseSession still happen through the usual path.
			_ = gForwarder.SubscribeEvict(func(clientID string) {
				if cl, ok := mqttServer.Clients.Get(clientID); ok {
					cl.Stop(packets.ErrSessionTakenOver)
				}
			})
		}

		go func() {
			log.Info("mqtt-gateway: MQTT broker starting", "port", cfg.MQTTPort)
			if err := mqttServer.Serve(); err != nil {
				log.Error("MQTT broker error", "error", err)
			}
		}()

		// ── Routing-table self-healing reconciler ──────────────────────────
		// Only meaningful on a node that both serves real MQTT clients and
		// participates in cluster routing (edge, combined — standalone has
		// no clusterRegistry, pure core has no mqttServer). See
		// routing.Reconciler's doc comment for the total-Olric-data-loss
		// scenario this guards against.
		if clusterRegistry != nil {
			if reg, ok := clusterRegistry.(interface {
				Subscribe(topic, nodeID string) error
				TopicsForNode(nodeID string) []string
			}); ok {
				reconciler := &routing.Reconciler{
					Server:   mqttServer,
					Registry: reg,
					NodeID:   cf.nodeID,
					Interval: cf.routingReconcilerInterval,
					Log:      log,
				}
				go reconciler.Run(ctx)
				log.Info("mqtt-gateway: routing self-heal reconciler started", "interval", cf.routingReconcilerInterval)
			}
		}

		// ── Commander (platform→device commands) ──────────────────────────
		if len(cfg.RedpandaBrokers) > 0 {
			cmd, err := commander.New(
				cfg.RedpandaBrokers,
				cfg.RedpandaSASLUser,
				cfg.RedpandaSASLPass,
				cfg.CommandsTopic,
				mqttServer,
				log,
			)
			if err != nil {
				log.Error("create commander", "error", err)
				os.Exit(1)
			}
			defer cmd.Close()
			go func() {
				if err := cmd.Run(ctx); err != nil && ctx.Err() == nil {
					log.Error("commander error", "error", err)
				}
			}()
			log.Info("mqtt-gateway: commander started", "topic", cfg.CommandsTopic)
		}

		// ── HTTP adapter ────────────────────────────────────────────────────
		httpHandler := httpapi.NewWithCache(validator, tenantCache, jwksCache, fwd, log)
		httpServer = &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
			Handler:      httpHandler.Router(),
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		}
		go func() {
			log.Info("mqtt-gateway: HTTP adapter starting", "port", cfg.HTTPPort)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("HTTP adapter error", "error", err)
			}
		}()
	} else {
		log.Info("mqtt-gateway: pure core role — MQTT broker, HTTP adapter, and commander not started")
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("mqtt-gateway: shutting down")
	cancel()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()

	if httpServer != nil {
		_ = httpServer.Shutdown(shutCtx)
	}
	_ = metricsServer.Shutdown(shutCtx)
	if mqttServer != nil {
		mqttServer.Close()
	}

	if mgmtServer != nil {
		_ = mgmtServer.Shutdown(shutCtx)
	}
	if clusterMembership != nil {
		_ = clusterMembership.Leave(5 * time.Second)
	}
	if clusterGRPCServer != nil {
		clusterGRPCServer.GracefulStop()
	}
	if raftNode != nil {
		_ = raftNode.Shutdown()
	}
	if clusterRouter != nil {
		_ = clusterRouter.Close()
	}
	if olricStore != nil {
		_ = olricStore.Close(shutCtx)
	}

	log.Info("mqtt-gateway: stopped")
}
