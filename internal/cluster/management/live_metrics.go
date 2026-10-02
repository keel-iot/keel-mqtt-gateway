package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/livestatsapi"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// liveMetricsCallTimeout bounds a single edge's /api/live/* poll — same
// rationale as raft.RemoteRegistry's remoteCallTimeout: one unreachable
// edge must not stall the whole cluster-wide aggregation.
const liveMetricsCallTimeout = 3 * time.Second

// clusterStatsView is the body of GET /api/metrics, aggregating every
// known edge's internal/livestatsapi.StatsView (see NodeMeta.HTTPAddr).
type clusterStatsView struct {
	ActiveConnections          int                      `json:"active_connections"`
	TotalMessages              uint64                   `json:"total_messages"`
	MessagesPerSecond          float64                  `json:"messages_per_second"`
	MessagesPerSecondAverage1m float64                  `json:"messages_per_second_avg_1m"`
	MessagesPerSecondAverage5m float64                  `json:"messages_per_second_avg_5m"`
	TotalBytes                 uint64                   `json:"total_bytes"`
	BytesPerSecond             float64                  `json:"bytes_per_second"`
	InflightMessages           int64                    `json:"inflight_messages"`
	OfflineSessions            int                      `json:"offline_sessions"`
	DisconnectsTotal           uint64                   `json:"disconnects_total"`
	DisconnectsLast5m          uint64                   `json:"disconnects_last_5m"`
	DroppedMessagesTotal       uint64                   `json:"dropped_messages_total"`
	DroppedMessagesLast5m      uint64                   `json:"dropped_messages_last_5m"`
	Nodes                      []livestatsapi.StatsView `json:"nodes"`
	Unreachable                []string                 `json:"unreachable,omitempty"`
}

type clusterClientView struct {
	livestatsapi.ClientView
	NodeID string `json:"node_id"`
}

func (a *API) handleLiveMetrics(w http.ResponseWriter, r *http.Request) {
	edges := a.Membership.EdgeHTTPAddrs()
	stats, unreachable := fetchAll(r.Context(), edges, "/api/live/stats", func() any { return &livestatsapi.StatsView{} })

	view := clusterStatsView{
		Unreachable:     unreachable,
		OfflineSessions: telemetry.OfflineSessionsSnapshot(),
	}
	if a.InflightMessages != nil {
		view.InflightMessages = a.InflightMessages()
	}
	for _, s := range stats {
		sv := s.(*livestatsapi.StatsView)
		view.ActiveConnections += sv.ActiveConnections
		view.TotalMessages += sv.TotalMessages
		view.MessagesPerSecond += sv.MessagesPerSecond
		view.MessagesPerSecondAverage1m += sv.MessagesPerSecondAverage1m
		view.MessagesPerSecondAverage5m += sv.MessagesPerSecondAverage5m
		view.TotalBytes += sv.TotalBytes
		view.BytesPerSecond += sv.BytesPerSecond
		view.DisconnectsTotal += sv.DisconnectsTotal
		view.DisconnectsLast5m += sv.DisconnectsLast5m
		view.DroppedMessagesTotal += sv.DroppedMessagesTotal
		view.DroppedMessagesLast5m += sv.DroppedMessagesLast5m
		view.Nodes = append(view.Nodes, *sv)
	}
	writeJSON(w, http.StatusOK, view)
}

func (a *API) handleLiveClients(w http.ResponseWriter, r *http.Request) {
	edges := a.Membership.EdgeHTTPAddrs()
	if hasClientPageQuery(r) {
		a.handlePagedLiveClients(w, r, edges)
		return
	}
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

type pagedClusterClients struct {
	Items       []clusterClientView `json:"items"`
	Page        int                 `json:"page"`
	PageSize    int                 `json:"page_size"`
	Total       int                 `json:"total"`
	Unreachable []string            `json:"unreachable,omitempty"`
}

func hasClientPageQuery(r *http.Request) bool {
	q := r.URL.Query()
	return q.Has("page") || q.Has("page_size") || q.Has("search") || q.Has("sort") || q.Has("node_id")
}

func (a *API) handlePagedLiveClients(w http.ResponseWriter, r *http.Request, edges map[string]string) {
	q := r.URL.Query()
	page, pageSize := 1, 50
	var err error
	if raw := q.Get("page"); raw != "" {
		page, err = strconv.Atoi(raw)
		if err != nil || page < 1 || page > 100 {
			http.Error(w, "page must be between 1 and 100", http.StatusBadRequest)
			return
		}
	}
	if raw := q.Get("page_size"); raw != "" {
		pageSize, err = strconv.Atoi(raw)
		if err != nil || pageSize < 1 || pageSize > 100 {
			http.Error(w, "page_size must be between 1 and 100", http.StatusBadRequest)
			return
		}
	}
	if page*pageSize > 1000 {
		http.Error(w, "page and page_size request too large", http.StatusBadRequest)
		return
	}
	search := q.Get("search")
	if len(search) > 128 {
		http.Error(w, "search is too long", http.StatusBadRequest)
		return
	}
	sortBy := q.Get("sort")
	if sortBy != "" && sortBy != "client_id" && sortBy != "username" && sortBy != "remote_addr" {
		http.Error(w, "unsupported sort field", http.StatusBadRequest)
		return
	}

	requestedEdges := edges
	if nodeID := q.Get("node_id"); nodeID != "" {
		addr, ok := edges[nodeID]
		if !ok {
			writeJSON(w, http.StatusOK, pagedClusterClients{Items: []clusterClientView{}, Page: page, PageSize: pageSize})
			return
		}
		requestedEdges = map[string]string{nodeID: addr}
	}
	upstream := url.Values{}
	upstream.Set("page", "1")
	upstream.Set("page_size", strconv.Itoa(page*pageSize))
	if search != "" {
		upstream.Set("search", search)
	}
	if sortBy != "" {
		upstream.Set("sort", sortBy)
	}
	path := "/api/live/clients?" + upstream.Encode()
	results, unreachable := fetchAll(r.Context(), requestedEdges, path, func() any { return &livestatsapi.ClientPage{} })

	items := make([]clusterClientView, 0)
	total := 0
	for nodeID, raw := range results {
		pageResult := raw.(*livestatsapi.ClientPage)
		total += pageResult.Total
		for _, client := range pageResult.Items {
			items = append(items, clusterClientView{ClientView: client, NodeID: nodeID})
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		left, right := clientSortValue(items[i].ClientView, sortBy), clientSortValue(items[j].ClientView, sortBy)
		if left == right {
			if items[i].NodeID == items[j].NodeID {
				return items[i].ClientID < items[j].ClientID
			}
			return items[i].NodeID < items[j].NodeID
		}
		return left < right
	})
	start := (page - 1) * pageSize
	if start > len(items) {
		start = len(items)
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	items = items[start:end]
	if items == nil {
		items = []clusterClientView{}
	}
	writeJSON(w, http.StatusOK, pagedClusterClients{Items: items, Page: page, PageSize: pageSize, Total: total, Unreachable: unreachable})
}

func clientSortValue(client livestatsapi.ClientView, sortBy string) string {
	switch sortBy {
	case "username":
		return client.Username
	case "remote_addr":
		return client.RemoteAddr
	default:
		return client.ClientID
	}
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
