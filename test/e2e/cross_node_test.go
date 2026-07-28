//go:build e2e

// Package e2e drives the 3-node docker-compose cluster (see
// docker-compose.yml) and validates cross-node MQTT delivery with real,
// authenticated MQTT clients (eclipse/paho.mqtt.golang) — not the gRPC
// debug client used in deploy/docker-compose/README.md's §4b.
//
// The consumer subscribes to the "telemetry/#" wildcard, matching the
// original scenario. This requires the cluster's raft-backed routing table
// (internal/cluster/raft/fsm.go's nodesFor) to do real MQTT wildcard
// matching rather than exact string comparison — see that package's
// TopicsIndex-backed implementation.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	composeFile = "../../docker-compose.yml"

	tenantID  = "11111111-1111-1111-1111-111111111111"
	deviceA   = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	deviceB   = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	devicePwd = "testpass123"

	// Must match internal/broker/hooks.go's testConsumerUsername/Password.
	consumerUser = "test-consumer"
	consumerPwd  = "consumer-e2e-testpass"

	nodeAMQTT = "tcp://localhost:11883"
	nodeBMQTT = "tcp://localhost:21883"
	nodeAMgmt = "http://localhost:18090"
	nodeBMgmt = "http://localhost:28090"
	nodeCMgmt = "http://localhost:38090"

	deliveryTimeout = 2 * time.Second
)

func TestMain(m *testing.M) {
	up := exec.Command("docker", "compose", "-f", composeFile, "up", "-d", "--build")
	up.Stdout, up.Stderr = os.Stdout, os.Stderr
	if err := up.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "docker compose up: %v\n", err)
		os.Exit(1)
	}

	code := 1
	if err := waitForCluster(60 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "cluster did not become ready: %v\n", err)
	} else if err := setupConsumerRBACRole(); err != nil {
		fmt.Fprintf(os.Stderr, "RBAC role setup failed: %v\n", err)
	} else {
		code = m.Run()
	}

	down := exec.Command("docker", "compose", "-f", composeFile, "down", "-v")
	down.Stdout, down.Stderr = os.Stdout, os.Stderr
	_ = down.Run()

	os.Exit(code)
}

// setupConsumerRBACRole replaces the TEMPORARY test-consumer hardcoded ACL
// (internal/broker/hooks.go's isAllowedConsumerSubscribe) with a real RBAC
// role+binding, exercised end-to-end via the management API's /api/acl/*
// REST endpoints (see internal/cluster/management and the `keel-gateway
// acl` CLI, which hits the same routes). Authentication still goes through
// the test-consumer username/password shortcut (rbac-migration deliberately
// left that in place per the additive-integration decision — see hooks.go's
// TEMPORARY comment and the project plan's rbac-hooks-integration entry),
// but ACL authorization now flows entirely through
// internal/cluster/acl.Evaluate via a real custom role bound to the
// "test-consumer" principal (username), not the legacy fallback path —
// proving the full raft-replicated RBAC round-trip (mgmt API → raft Apply →
// FSM state → EvaluateACL on whichever node the consumer connects to,
// which may not be the node the role was created on).
func setupConsumerRBACRole() error {
	roleBody, err := json.Marshal(map[string]any{
		"name": "e2e-consumer",
		"rules": []map[string]any{
			{"topic_filter": "telemetry/#", "actions": []string{"subscribe"}, "effect": "allow"},
		},
	})
	if err != nil {
		return err
	}
	if err := postJSON(nodeAMgmt+"/api/acl/roles", roleBody); err != nil {
		return fmt.Errorf("create role: %w", err)
	}

	bindingBody, err := json.Marshal(map[string]string{
		"principal": consumerUser,
		"role_name": "e2e-consumer",
	})
	if err != nil {
		return err
	}
	if err := postJSON(nodeAMgmt+"/api/acl/bindings", bindingBody); err != nil {
		return fmt.Errorf("create binding: %w", err)
	}

	// Give the raft log a moment to replicate the role+binding to all
	// nodes before any test connects a consumer against node B/C — same
	// small-margin rationale as TestCrossNodeDelivery's post-subscribe
	// sleep below.
	time.Sleep(500 * time.Millisecond)
	return nil
}

func postJSON(url string, body []byte) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("management API returned %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	return nil
}

type nodeView struct {
	NodeID    string `json:"node_id"`
	IsSelf    bool   `json:"is_self"`
	IsLeader  bool   `json:"is_leader"`
	RaftVoter bool   `json:"raft_voter"`
}

// waitForCluster polls every node's own management API until each one
// reports itself as a joined raft voter with a known leader.
//
// Checking core-1 alone is not enough: core-1 self-elects and reports
// "is_leader": true within ~4s of `docker compose up`, well before core-2
// and core-3 have been added as raft voters (see docker-compose/README.md
// §1's reconcileVotersLoop). A test that only waits on core-1 can start
// publishing/subscribing against core-2 or core-3 while their local FSM
// replica is still empty — NodesFor on that node then legitimately returns
// nothing, not because of a matching bug but because that node hasn't
// received any raft log entries yet. Confirmed by reproducing this exact
// race manually: at core-1's 4s "ready" mark, core-2/core-3's own
// /api/cluster/nodes responses had no raft_voter:true anywhere.
func waitForCluster(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = nil
		for _, addr := range []string{nodeAMgmt, nodeBMgmt, nodeCMgmt} {
			if err := nodeReady(addr); err != nil {
				lastErr = fmt.Errorf("%s: %w", addr, err)
				break
			}
		}
		if lastErr == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timed out after %s, last error: %w", timeout, lastErr)
}

// nodeReady reports whether mgmtAddr's own node has joined the raft
// cluster as a voter and knows the current leader — i.e. its local FSM
// replica is live and actively replicating, not just gossip-visible.
func nodeReady(mgmtAddr string) error {
	resp, err := http.Get(mgmtAddr + "/api/cluster/nodes")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("management API returned %d", resp.StatusCode)
	}
	var nodes []nodeView
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return err
	}
	if len(nodes) != 3 {
		return fmt.Errorf("expected 3 nodes, got %d", len(nodes))
	}
	var leaderKnown, selfVoter bool
	for _, n := range nodes {
		if n.IsLeader {
			leaderKnown = true
		}
		if n.IsSelf && n.RaftVoter {
			selfVoter = true
		}
	}
	if !leaderKnown {
		return fmt.Errorf("no leader known yet")
	}
	if !selfVoter {
		return fmt.Errorf("not yet joined as raft voter")
	}
	return nil
}

// TestCrossNodeDelivery validates OnPublish → NodesFor → Forward end-to-end
// with real MQTT clients: a device publishes on one node, a subscriber
// authenticated with the test-consumer credentials (auth bypass only —
// authorization is via the real "e2e-consumer" RBAC role+binding created
// in setupConsumerRBACRole, not the legacy hardcoded
// isAllowedConsumerSubscribe path) receives it on another node. Run in both
// node-role directions to rule out asymmetries.
func TestCrossNodeDelivery(t *testing.T) {
	cases := []struct {
		name           string
		consumerBroker string
		deviceBroker   string
		deviceID       string
	}{
		{"consumer-nodeA_device-nodeB", nodeAMQTT, nodeBMQTT, deviceA},
		{"consumer-nodeB_device-nodeA", nodeBMQTT, nodeAMQTT, deviceB},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const consumerFilter = "telemetry/#" // real wildcard, not a workaround topic
			publishTopic := fmt.Sprintf("telemetry/%s/%s", tenantID, tc.deviceID)
			payload := fmt.Sprintf("payload-%s-%d", tc.name, time.Now().UnixNano())
			received := make(chan string, 1)

			consumer := connectClient(t, tc.consumerBroker, "test-consumer-"+tc.name, consumerUser, consumerPwd)
			defer consumer.Disconnect(250)

			subToken := consumer.Subscribe(consumerFilter, 1, func(_ mqtt.Client, msg mqtt.Message) {
				received <- string(msg.Payload())
			})
			if !subToken.WaitTimeout(5 * time.Second) {
				t.Fatal("subscribe timed out")
			}
			if err := subToken.Error(); err != nil {
				t.Fatalf("subscribe failed: %v", err)
			}

			// OnSubscribed's raft.Apply commits synchronously before SUBACK,
			// but the publishing node's own FSM replica may lag a moment
			// behind the leader's commit — small margin before publishing.
			time.Sleep(500 * time.Millisecond)

			deviceUsername := tc.deviceID + "@" + tenantID
			device := connectClient(t, tc.deviceBroker, tc.deviceID, deviceUsername, devicePwd)
			defer device.Disconnect(250)

			pubToken := device.Publish(publishTopic, 1, false, payload)
			if !pubToken.WaitTimeout(5 * time.Second) {
				t.Fatal("publish timed out")
			}
			if err := pubToken.Error(); err != nil {
				t.Fatalf("publish failed: %v", err)
			}

			select {
			case got := <-received:
				if got != payload {
					t.Fatalf("payload mismatch: got %q, want %q", got, payload)
				}
			case <-time.After(deliveryTimeout):
				t.Fatalf("consumer on %s did not receive device %s's publish from %s within %s",
					tc.consumerBroker, tc.deviceID, tc.deviceBroker, deliveryTimeout)
			}
		})
	}
}

func connectClient(t *testing.T, broker, clientID, username, password string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetUsername(username).
		SetPassword(password).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false)
	c := mqtt.NewClient(opts)
	token := c.Connect()
	if !token.WaitTimeout(5 * time.Second) {
		t.Fatalf("connect to %s timed out", broker)
	}
	if err := token.Error(); err != nil {
		t.Fatalf("connect to %s failed: %v", broker, err)
	}
	return c
}
