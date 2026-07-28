package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// mgmtClient talks to one core node's read-only management API (see
// internal/cluster/management/api.go). Used for two things the MQTT
// protocol itself can't tell the simulator: (1) node list / health, used
// only for a startup sanity check, and (2) the routing snapshot, polled
// during the churn scenario to measure how long a subscribe takes to
// become visible on a node other than the one that received it — the
// convergence-time metric the task asks for.
type mgmtClient struct {
	baseURL string
	http    *http.Client
}

func newMgmtClient(baseURL string) *mgmtClient {
	return &mgmtClient{baseURL: baseURL, http: &http.Client{Timeout: 5 * time.Second}}
}

type mgmtNode struct {
	NodeID   string `json:"node_id"`
	IsSelf   bool   `json:"is_self"`
	IsLeader bool   `json:"is_leader"`
}

func (c *mgmtClient) nodes(ctx context.Context) ([]mgmtNode, error) {
	var out []mgmtNode
	if err := c.getJSON(ctx, "/api/cluster/nodes", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// routesSnapshot returns topic-filter -> []nodeID, matching
// CoreRegistry.RoutesSnapshot's JSON shape.
func (c *mgmtClient) routesSnapshot(ctx context.Context) (map[string][]string, error) {
	var out map[string][]string
	if err := c.getJSON(ctx, "/api/cluster/routes", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// routeHasNode polls routesSnapshot until topic includes nodeID in its node
// list, or the deadline passes. Used to time subscribe-to-visible
// convergence for the churn scenario: the churn loop subscribes on one
// node's broker (device connection) and this polls a *different* node's
// management API for when the routing table there reflects it.
// nodeID == "" means "any node listed at all" (used when the caller only
// wants to know that the subscribe is visible cluster-wide from this node's
// perspective, without needing to identify which node registered it).
func (c *mgmtClient) waitRouteVisible(ctx context.Context, topic, nodeID string, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		snap, err := c.routesSnapshot(ctx)
		if err == nil {
			if nodeID == "" {
				if len(snap[topic]) > 0 {
					return time.Since(start), true
				}
			} else {
				for _, n := range snap[topic] {
					if n == nodeID {
						return time.Since(start), true
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return time.Since(start), false
}

func (c *mgmtClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: unexpected status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
