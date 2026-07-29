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
	NodeID            string  `json:"node_id"`
	ActiveConnections int     `json:"active_connections"`
	TotalMessages     uint64  `json:"total_messages"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	TotalBytes        uint64  `json:"total_bytes"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
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

func (h *Handlers) handleClients(w http.ResponseWriter, _ *http.Request) {
	clients := h.Clients()
	if clients == nil {
		clients = []ClientView{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(clients)
}
