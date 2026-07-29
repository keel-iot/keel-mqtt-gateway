package management

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/livestatsapi"
)

// liveMetricsCallTimeout bounds a single edge's /api/live/* poll — same
// rationale as raft.RemoteRegistry's remoteCallTimeout: one unreachable
// edge must not stall the whole cluster-wide aggregation.
const liveMetricsCallTimeout = 3 * time.Second

// clusterStatsView is the body of GET /api/metrics — the endpoint
// keel-design-doc.md's "Osservabilità e controllo" section originally
// specified ("GET /api/metrics // msg/sec, connessioni attive, per
// nodo") but was never implemented until now. Aggregates every known
// edge's internal/livestatsapi.StatsView (see NodeMeta.HTTPAddr).
type clusterStatsView struct {
	ActiveConnections int                       `json:"active_connections"`
	TotalMessages     uint64                    `json:"total_messages"`
	MessagesPerSecond float64                   `json:"messages_per_second"`
	TotalBytes        uint64                    `json:"total_bytes"`
	BytesPerSecond    float64                   `json:"bytes_per_second"`
	Nodes             []livestatsapi.StatsView  `json:"nodes"`
	Unreachable       []string                  `json:"unreachable,omitempty"`
}

type clusterClientView struct {
	livestatsapi.ClientView
	NodeID string `json:"node_id"`
}

func (a *API) handleLiveMetrics(w http.ResponseWriter, r *http.Request) {
	edges := a.Membership.EdgeHTTPAddrs()
	stats, unreachable := fetchAll(r.Context(), edges, "/api/live/stats", func() any { return &livestatsapi.StatsView{} })

	view := clusterStatsView{Unreachable: unreachable}
	for _, s := range stats {
		sv := s.(*livestatsapi.StatsView)
		view.ActiveConnections += sv.ActiveConnections
		view.TotalMessages += sv.TotalMessages
		view.MessagesPerSecond += sv.MessagesPerSecond
		view.TotalBytes += sv.TotalBytes
		view.BytesPerSecond += sv.BytesPerSecond
		view.Nodes = append(view.Nodes, *sv)
	}
	writeJSON(w, http.StatusOK, view)
}

func (a *API) handleLiveClients(w http.ResponseWriter, r *http.Request) {
	edges := a.Membership.EdgeHTTPAddrs()
	results, _ := fetchAll(r.Context(), edges, "/api/live/clients", func() any { return &[]livestatsapi.ClientView{} })

	out := []clusterClientView{}
	for nodeID, res := range results {
		clients := res.(*[]livestatsapi.ClientView)
		for _, c := range *clients {
			out = append(out, clusterClientView{ClientView: c, NodeID: nodeID})
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// fetchAll GETs path from every node_id -> addr in edges concurrently,
// decoding each response body into a fresh value produced by newTarget
// (called once per request so concurrent goroutines never share one).
// Returns node_id -> decoded value for every successful call, plus the
// list of node_ids that didn't respond in time — a single unreachable
// edge must not fail the whole aggregation.
func fetchAll(ctx context.Context, edges map[string]string, path string, newTarget func() any) (map[string]any, []string) {
	var (
		mu          sync.Mutex
		results     = make(map[string]any, len(edges))
		unreachable []string
	)
	var wg sync.WaitGroup
	client := &http.Client{Timeout: liveMetricsCallTimeout}
	for nodeID, addr := range edges {
		wg.Add(1)
		go func(nodeID, addr string) {
			defer wg.Done()
			reqCtx, cancel := context.WithTimeout(ctx, liveMetricsCallTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+path, nil)
			if err != nil {
				mu.Lock()
				unreachable = append(unreachable, nodeID)
				mu.Unlock()
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				mu.Lock()
				unreachable = append(unreachable, nodeID)
				mu.Unlock()
				return
			}
			defer resp.Body.Close()
			target := newTarget()
			if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(target) != nil {
				mu.Lock()
				unreachable = append(unreachable, nodeID)
				mu.Unlock()
				return
			}
			mu.Lock()
			results[nodeID] = target
			mu.Unlock()
		}(nodeID, addr)
	}
	wg.Wait()
	return results, unreachable
}
