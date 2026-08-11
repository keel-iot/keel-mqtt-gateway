//go:build cluster

// Package cluster brings up a real, multi-process Keel cluster
// (Core/Edge split, real Postgres, real Redis) via testcontainers-go —
// the harness design specified in docs/testing/CLUSTER_CORRECTNESS_MATRIX.md
// §7. Deterministic process control only: named-container start/stop,
// no random fault injection, no network partition simulation (that's
// Phase 4). Build-tag gated (`cluster`) so it never runs as part of the
// normal `go test ./...` gate — same convention as test/e2e's `e2e` tag.
package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	clusterImageRepo = "keel-mqtt-gateway"
	clusterImageTag  = "cluster-test"

	// Fixed test credentials — see credentialsYAML below. Password for
	// every device is "testpass123" (same bcrypt hash the repo's own
	// deploy/docker-compose fixtures use).
	TenantID      = "11111111-1111-1111-1111-111111111111"
	TenantSlug    = "poc"
	DevicePwd     = "testpass123"
	devicePwdHash = "$2a$10$qg4iMKaWEPTUwuB7ekeutu5r9B1cRK6TtoemXJUmVsaXaqfUmOYCq"
)

// Node is one running container in the cluster — either a Core or an
// Edge, matching cmd/server's own --role values.
type Node struct {
	ID        string // e.g. "core-1", "edge-1"
	Role      string // "core" | "edge"
	Container testcontainers.Container
}

// Harness owns every container/network for one test's cluster and tears
// them all down in Close.
type Harness struct {
	t       *testing.T
	network *testcontainers.DockerNetwork

	Postgres testcontainers.Container
	// Redis holds one instance per Core — real deployment convention
	// (docker-compose.core-edge-split.yml) is one Redis co-located per
	// core, never a single instance shared across cores. Sharing one
	// instance across cores makes internal/cluster/membership's
	// redisFailoverLoop issue a self-referential SLAVEOF against it
	// (every core "co-located" address resolves to the same physical
	// instance), demoting it to a read-only replica of itself.
	Redis []testcontainers.Container
	Cores []*Node
	Edges []*Node
}

// credentialsYAML is a minimal, fixed device-credentials fixture in the
// exact format internal/auth's file provider expects (mirrors
// deploy/docker-compose/credentials.yaml's shape, just far fewer
// devices — this harness doesn't need thousands of simulated devices).
func credentialsYAML(deviceIDs []string) string {
	var sb strings.Builder
	sb.WriteString("devices:\n")
	for _, id := range deviceIDs {
		fmt.Fprintf(&sb, "  - device_id: %s\n", id)
		fmt.Fprintf(&sb, "    tenant_id: %s\n", TenantID)
		fmt.Fprintf(&sb, "    tenant_slug: %s\n", TenantSlug)
		fmt.Fprintf(&sb, "    password_hash: %s\n", devicePwdHash)
	}
	return sb.String()
}

// NewHarness builds the image once (if not already built this run),
// starts Postgres + Redis + numCore Core containers + numEdge Edge
// containers on a shared Docker network, and waits for every one of
// them to report ready. deviceIDs are baked into the AUTH_BACKEND=file
// credentials fixture every Edge gets.
func NewHarness(t *testing.T, numCore, numEdge int, deviceIDs []string) *Harness {
	t.Helper()
	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create docker network: %v", err)
	}
	h := &Harness{t: t, network: net}
	t.Cleanup(h.Close)

	h.Postgres = h.startPostgres(ctx)

	// One Redis per core, matching docker-compose.core-edge-split.yml —
	// see the Redis field's doc for why a single shared instance breaks
	// the raft-driven replica-topology reconciler.
	redisAliases := make([]string, numCore)
	for i := 1; i <= numCore; i++ {
		alias := fmt.Sprintf("redis-core-%d", i)
		redisAliases[i-1] = alias
		h.Redis = append(h.Redis, h.startRedis(ctx, alias))
	}

	creds := credentialsYAML(deviceIDs)

	for i := 1; i <= numCore; i++ {
		nodeID := fmt.Sprintf("core-%d", i)
		bootstrap := i == 1
		// Every core after the first joins via core-1's gossip address;
		// core-1 itself joins via every OTHER core — mirrors
		// docker-compose.core-edge-split.yml's exact peer wiring
		// (core-1: "core-2:7946,core-3:7946", others: "core-1:7946").
		// core-1 pointed at itself instead would be harmless on first
		// cold start (it bootstraps alone, no peers needed yet) but
		// breaks its own rejoin after a restart: with only itself as a
		// configured peer, it can never rediscover core-2/core-3's
		// already-running memberlist cluster.
		var peer string
		if i == 1 {
			var others []string
			for j := 2; j <= numCore; j++ {
				others = append(others, fmt.Sprintf("core-%d:7946", j))
			}
			peer = strings.Join(others, ",")
		} else {
			peer = "core-1:7946"
		}
		h.Cores = append(h.Cores, h.startCore(ctx, nodeID, bootstrap, peer, redisAliases[i-1]+":6379", creds))
	}

	// Edges only need a bootstrap seed address to reach the cluster's
	// initial Redis primary — internal/cluster/redisrouter.WatchPrimary
	// redirects them to whichever core is actually primary shortly after
	// (see docker-compose.core-edge-split.yml's identical comment).
	for i := 1; i <= numEdge; i++ {
		nodeID := fmt.Sprintf("edge-%d", i)
		h.Edges = append(h.Edges, h.startEdge(ctx, nodeID, "core-1:7946", redisAliases[0]+":6379", creds))
	}

	return h
}

func (h *Harness) startPostgres(ctx context.Context) testcontainers.Container {
	h.t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "keel_devices",
		},
		Networks:       []string{h.network.Name},
		NetworkAliases: map[string][]string{h.network.Name: {"postgres"}},
		WaitingFor:     wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		h.t.Fatalf("start postgres: %v", err)
	}
	return c
}

func (h *Harness) startRedis(ctx context.Context, alias string) testcontainers.Container {
	h.t.Helper()
	req := testcontainers.ContainerRequest{
		Image:          "redis:7-alpine",
		ExposedPorts:   []string{"6379/tcp"},
		Networks:       []string{h.network.Name},
		NetworkAliases: map[string][]string{h.network.Name: {alias}},
		WaitingFor:     wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		h.t.Fatalf("start redis %s: %v", alias, err)
	}
	return c
}

// keelImage builds the repo's own Dockerfile once per test process (an
// image tag is process-global in the local Docker daemon — testcontainers
// itself doesn't dedupe builds across separate FromDockerfile requests,
// so this harness does it explicitly instead of rebuilding per node).
var builtImage bool

func (h *Harness) keelContainerRequest(nodeID string, env map[string]string, cmd []string) testcontainers.ContainerRequest {
	req := testcontainers.ContainerRequest{
		Env:            env,
		Cmd:            cmd,
		Networks:       []string{h.network.Name},
		NetworkAliases: map[string][]string{h.network.Name: {nodeID}},
		ExposedPorts:   []string{"1883/tcp", "8090/tcp"},
	}
	if !builtImage {
		req.FromDockerfile = testcontainers.FromDockerfile{
			Context:       "../..",
			Dockerfile:    "Dockerfile",
			Repo:          clusterImageRepo,
			Tag:           clusterImageTag,
			KeepImage:     true,
			PrintBuildLog: false,
		}
		builtImage = true
	} else {
		req.Image = clusterImageRepo + ":" + clusterImageTag
	}
	return req
}

func (h *Harness) startCore(ctx context.Context, nodeID string, bootstrap bool, peer, redisAddr, credsYAML string) *Node {
	h.t.Helper()
	env := map[string]string{
		"DATABASE_URL":         "postgres://postgres:postgres@postgres:5432/keel_devices?sslmode=disable",
		"REDIS_ADDR":           redisAddr,
		"AUTH_BACKEND":         "file",
		"CREDENTIAL_FILE":      "/etc/keel/credentials.yaml",
		"CREDENTIAL_CACHE_TTL": "5s",
		"DEFAULT_TENANT_ID":    TenantID,
		"LOG_LEVEL":            "info",
	}
	cmd := []string{
		"--role=core",
		"--node-id=" + nodeID,
		"--raft-bind=" + nodeID + ":7000",
		"--raft-data-dir=/data/raft",
		"--gossip-bind=0.0.0.0",
		"--gossip-port=7946",
		"--gossip-advertise=" + nodeID,
		"--gossip-peers=" + peer,
		"--grpc-bind=0.0.0.0:7100",
		"--grpc-advertise=" + nodeID + ":7100",
		"--management-addr=0.0.0.0:8090",
		"--olric-bind=0.0.0.0",
		"--olric-port=7300",
		"--olric-gossip-port=7301",
		"--olric-advertise=" + nodeID,
	}
	if bootstrap {
		cmd = append(cmd, "--bootstrap=true")
	}
	req := h.keelContainerRequest(nodeID, env, cmd)
	req.Files = []testcontainers.ContainerFile{{
		Reader:            strings.NewReader(credsYAML),
		ContainerFilePath: "/etc/keel/credentials.yaml",
		FileMode:          0o644,
	}}
	// Core nodes never run mochi-mqtt at all (--role=core is
	// broker/HTTP/commander-free — verified from cmd/server/main.go), so
	// "mochi mqtt server started" (correct for Edge below) never appears
	// in a Core's logs. main.go's own explicit log line for this branch
	// is the last thing it logs before blocking on the shutdown signal —
	// by then raft/gossip/olric/management-API are all already up.
	req.WaitingFor = wait.ForLog("pure core role").WithStartupTimeout(60 * time.Second)

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		h.t.Fatalf("start %s: %v", nodeID, err)
	}
	return &Node{ID: nodeID, Role: "core", Container: c}
}

func (h *Harness) startEdge(ctx context.Context, nodeID, peer, redisAddr, credsYAML string) *Node {
	h.t.Helper()
	env := map[string]string{
		"DATABASE_URL":         "postgres://postgres:postgres@postgres:5432/keel_devices?sslmode=disable",
		"REDIS_ADDR":           redisAddr,
		"AUTH_BACKEND":         "file",
		"CREDENTIAL_FILE":      "/etc/keel/credentials.yaml",
		"CREDENTIAL_CACHE_TTL": "5s",
		"DEFAULT_TENANT_ID":    TenantID,
		"LOG_LEVEL":            "info",
	}
	cmd := []string{
		"--role=edge",
		"--node-id=" + nodeID,
		"--gossip-bind=0.0.0.0",
		"--gossip-port=7946",
		"--gossip-advertise=" + nodeID,
		"--gossip-peers=" + peer,
		"--grpc-bind=0.0.0.0:7100",
		"--grpc-advertise=" + nodeID + ":7100",
	}
	req := h.keelContainerRequest(nodeID, env, cmd)
	req.Files = []testcontainers.ContainerFile{{
		Reader:            strings.NewReader(credsYAML),
		ContainerFilePath: "/etc/keel/credentials.yaml",
		FileMode:          0o644,
	}}
	req.WaitingFor = wait.ForLog("mochi mqtt server started").WithStartupTimeout(60 * time.Second)

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		h.t.Fatalf("start %s: %v", nodeID, err)
	}
	return &Node{ID: nodeID, Role: "edge", Container: c}
}

// MQTTAddr returns the host-reachable "host:port" for edge index i
// (0-based) — for a real MQTT client running on the test host to dial.
func (h *Harness) MQTTAddr(i int) string {
	h.t.Helper()
	ctx := context.Background()
	mapped, err := h.Edges[i].Container.MappedPort(ctx, "1883/tcp")
	if err != nil {
		h.t.Fatalf("mapped port for %s: %v", h.Edges[i].ID, err)
	}
	host, err := h.Edges[i].Container.Host(ctx)
	if err != nil {
		h.t.Fatalf("host for %s: %v", h.Edges[i].ID, err)
	}
	return host + ":" + mapped.Port()
}

// ManagementAddr returns the host-reachable "host:port" for core index i
// (0-based)'s management API — for a real HTTP client running on the
// test host to query real, per-process cluster state (e.g. GET
// /api/cluster/nodes's is_leader field), never a value assumed from
// harness construction order.
func (h *Harness) ManagementAddr(i int) string {
	h.t.Helper()
	ctx := context.Background()
	mapped, err := h.Cores[i].Container.MappedPort(ctx, "8090/tcp")
	if err != nil {
		h.t.Fatalf("mapped management port for %s: %v", h.Cores[i].ID, err)
	}
	host, err := h.Cores[i].Container.Host(ctx)
	if err != nil {
		h.t.Fatalf("host for %s: %v", h.Cores[i].ID, err)
	}
	return host + ":" + mapped.Port()
}

// KillEdge stops (not gracefully — Terminate kills the container
// outright, no DISCONNECT, no drain) the given edge — deterministic,
// named-target process control for the node-loss scenarios, never a
// random kill.
func (h *Harness) KillEdge(i int) {
	h.t.Helper()
	if err := h.Edges[i].Container.Terminate(context.Background()); err != nil {
		h.t.Fatalf("terminate %s: %v", h.Edges[i].ID, err)
	}
}

// KillCore stops the given core outright — same deterministic,
// named-target semantics as KillEdge.
func (h *Harness) KillCore(i int) {
	h.t.Helper()
	if err := h.Cores[i].Container.Terminate(context.Background()); err != nil {
		h.t.Fatalf("terminate %s: %v", h.Cores[i].ID, err)
	}
}

// StopCore stops (not Terminate — the container and its writable layer,
// including the raft data dir under /data/raft, survive) the given
// core, for rejoin/restart scenarios that need real BoltDB state to
// still be there afterward. Pairs with StartCore.
func (h *Harness) StopCore(i int) {
	h.t.Helper()
	if err := h.Cores[i].Container.Stop(context.Background(), nil); err != nil {
		h.t.Fatalf("stop %s: %v", h.Cores[i].ID, err)
	}
}

// StartCore resumes a core previously stopped via StopCore, same
// container/filesystem/data as before — a real restart, not a fresh
// node with a new empty data dir.
func (h *Harness) StartCore(i int) {
	h.t.Helper()
	if err := h.Cores[i].Container.Start(context.Background()); err != nil {
		h.t.Fatalf("start %s: %v", h.Cores[i].ID, err)
	}
}

// Close tears down every container and the network. Registered
// automatically via t.Cleanup in NewHarness — tests don't need to call
// this themselves.
func (h *Harness) Close() {
	ctx := context.Background()
	for _, n := range h.Edges {
		_ = n.Container.Terminate(ctx)
	}
	for _, n := range h.Cores {
		_ = n.Container.Terminate(ctx)
	}
	for _, r := range h.Redis {
		_ = r.Terminate(ctx)
	}
	if h.Postgres != nil {
		_ = h.Postgres.Terminate(ctx)
	}
	if h.network != nil {
		_ = h.network.Remove(ctx)
	}
}
