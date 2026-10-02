// Package livestatsapi implements the edge-local half of the basic
// monitoring UI: GET /api/live/stats and GET /api/live/clients, mounted
// on the metrics server (port 9090, alongside /healthz/readyz/metrics) of
// every edge/combined node. internal/cluster/management aggregates these
// across every known edge into the cluster-wide GET /api/metrics the
// design doc originally specified (see that package's live_metrics.go).
//
// Deliberately decoupled from mochi-mqtt and internal/telemetry types —
// callers (cmd/server/main.go) adapt real server/tracker state into the
// plain views below, keeping this package trivially unit-testable.
package livestatsapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// ClientView is one entry in GET /api/live/clients.
type ClientView struct {
	ClientID      string   `json:"client_id"`
	Username      string   `json:"username,omitempty"`
	RemoteAddr    string   `json:"remote_addr,omitempty"`
	CleanSession  bool     `json:"clean_session"`
	Subscriptions []string `json:"subscriptions"`
}

// StatsView is the body of GET /api/live/stats.
type StatsView struct {
	NodeID                     string  `json:"node_id"`
	ActiveConnections          int     `json:"active_connections"`
	TotalMessages              uint64  `json:"total_messages"`
	MessagesPerSecond          float64 `json:"messages_per_second"`
	MessagesPerSecondAverage1m float64 `json:"messages_per_second_avg_1m"`
	MessagesPerSecondAverage5m float64 `json:"messages_per_second_avg_5m"`
	TotalBytes                 uint64  `json:"total_bytes"`
	BytesPerSecond             float64 `json:"bytes_per_second"`
	DisconnectsTotal           uint64  `json:"disconnects_total"`
	DisconnectsLast5m          uint64  `json:"disconnects_last_5m"`
	DroppedMessagesTotal       uint64  `json:"dropped_messages_total"`
	DroppedMessagesLast5m      uint64  `json:"dropped_messages_last_5m"`
}

// ClientPage is returned when the client endpoint is queried with pagination
// or search parameters. The unpaged response remains a plain array for
// compatibility with the original management UI.
type ClientPage struct {
	Items    []ClientView `json:"items"`
	Page     int          `json:"page"`
	PageSize int          `json:"page_size"`
	Total    int          `json:"total"`
}

// Handlers bundles the callbacks needed to serve both endpoints.
type Handlers struct {
	// Clients returns a fresh snapshot of currently connected clients.
	Clients func() []ClientView
	// Stats returns a fresh stats snapshot.
	Stats func() StatsView
}

// Register mounts both endpoints on mux.
func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/live/stats", h.handleStats)
	mux.HandleFunc("GET /api/live/clients", h.handleClients)
}

func (h *Handlers) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.Stats())
}

func (h *Handlers) handleClients(w http.ResponseWriter, r *http.Request) {
	clients := h.Clients()
	if clients == nil {
		clients = []ClientView{}
	}
	w.Header().Set("Content-Type", "application/json")
	if !hasClientQuery(r) {
		_ = json.NewEncoder(w).Encode(clients)
		return
	}

	page, pageSize, search, sortBy, err := parseClientQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	filtered := filterClients(clients, search)
	sortClientViews(filtered, sortBy)
	total := len(filtered)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	items := filtered[start:end]
	if items == nil {
		items = []ClientView{}
	}
	_ = json.NewEncoder(w).Encode(ClientPage{Items: items, Page: page, PageSize: pageSize, Total: total})
}

func hasClientQuery(r *http.Request) bool {
	q := r.URL.Query()
	return q.Has("page") || q.Has("page_size") || q.Has("search") || q.Has("sort")
}

func parseClientQuery(r *http.Request) (page, pageSize int, search, sortBy string, err error) {
	q := r.URL.Query()
	page, pageSize = 1, 50
	var parseErr error
	if raw := q.Get("page"); raw != "" {
		page, parseErr = strconv.Atoi(raw)
		if parseErr != nil || page < 1 || page > 100 {
			err = strconv.ErrSyntax
			return
		}
	}
	if raw := q.Get("page_size"); raw != "" {
		pageSize, parseErr = strconv.Atoi(raw)
		// Cluster aggregation requests page*page_size items from each edge
		// before performing the global merge. Direct callers are still bounded
		// to avoid turning this endpoint into an unbounded memory read.
		if parseErr != nil || pageSize < 1 || pageSize > 1000 {
			err = strconv.ErrSyntax
			return
		}
	}
	search = strings.TrimSpace(q.Get("search"))
	if len(search) > 128 {
		err = strconv.ErrRange
		return
	}
	sortBy = q.Get("sort")
	switch sortBy {
	case "", "client_id", "username", "remote_addr":
	default:
		err = strconv.ErrSyntax
	}
	return
}

func filterClients(clients []ClientView, search string) []ClientView {
	if search == "" {
		return clients
	}
	search = strings.ToLower(search)
	filtered := make([]ClientView, 0, len(clients))
	for _, client := range clients {
		if strings.Contains(strings.ToLower(client.ClientID), search) ||
			strings.Contains(strings.ToLower(client.Username), search) ||
			strings.Contains(strings.ToLower(client.RemoteAddr), search) {
			filtered = append(filtered, client)
		}
	}
	return filtered
}

func sortClientViews(clients []ClientView, sortBy string) {
	if sortBy == "" {
		sortBy = "client_id"
	}
	sort.SliceStable(clients, func(i, j int) bool {
		var left, right string
		switch sortBy {
		case "username":
			left, right = clients[i].Username, clients[j].Username
		case "remote_addr":
			left, right = clients[i].RemoteAddr, clients[j].RemoteAddr
		default:
			left, right = clients[i].ClientID, clients[j].ClientID
		}
		if left == right {
			return clients[i].ClientID < clients[j].ClientID
		}
		return left < right
	})
}
