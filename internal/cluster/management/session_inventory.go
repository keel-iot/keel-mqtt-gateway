package management

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/livestatsapi"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
)

type sessionInventoryItem struct {
	ClientID      string   `json:"client_id"`
	Status        string   `json:"status"`
	NodeID        string   `json:"node_id,omitempty"`
	Username      string   `json:"username,omitempty"`
	RemoteAddr    string   `json:"remote_addr,omitempty"`
	CleanSession  bool     `json:"clean_session"`
	Subscriptions []string `json:"subscriptions"`
}

type sessionInventoryResponse struct {
	Items            []sessionInventoryItem `json:"items"`
	Page             int                    `json:"page"`
	PageSize         int                    `json:"page_size"`
	Total            int                    `json:"total"`
	Status           string                 `json:"status"`
	OfflineAvailable bool                   `json:"offline_available"`
	SnapshotAt       string                 `json:"snapshot_at,omitempty"`
	Unreachable      []string               `json:"unreachable,omitempty"`
}

func (a *API) handleSessionInventory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := q.Get("status")
	if status == "" {
		status = "all"
	}
	if status != "all" && status != "online" && status != "offline" {
		http.Error(w, "status must be all, online, or offline", http.StatusBadRequest)
		return
	}

	page, pageSize, err := parseInventoryPage(q.Get("page"), q.Get("page_size"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	search := strings.TrimSpace(q.Get("search"))
	if len(search) > 128 {
		http.Error(w, "search is too long", http.StatusBadRequest)
		return
	}

	var offline []session.OfflineSession
	var offlineUpdatedAt time.Time
	var offlineReady bool
	if a.OfflineSessions != nil {
		offline, offlineUpdatedAt, offlineReady = a.OfflineSessions()
	}
	items, unreachable := a.collectSessionInventory(r, status, search, offline, offlineReady)
	sort.Slice(items, func(i, j int) bool { return items[i].ClientID < items[j].ClientID })

	total := len(items)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	items = items[start:end]
	if items == nil {
		items = []sessionInventoryItem{}
	}

	response := sessionInventoryResponse{
		Items:            items,
		Page:             page,
		PageSize:         pageSize,
		Total:            total,
		Status:           status,
		OfflineAvailable: a.OfflineSessions != nil,
		Unreachable:      unreachable,
	}
	if offlineReady {
		response.SnapshotAt = offlineUpdatedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	writeJSON(w, http.StatusOK, response)
}

func parseInventoryPage(rawPage, rawPageSize string) (int, int, error) {
	page, pageSize := 1, 50
	var err error
	if rawPage != "" {
		page, err = strconv.Atoi(rawPage)
		if err != nil || page < 1 || page > 1000 {
			return 0, 0, &inventoryQueryError{"page must be between 1 and 1000"}
		}
	}
	if rawPageSize != "" {
		pageSize, err = strconv.Atoi(rawPageSize)
		if err != nil || pageSize < 1 || pageSize > 100 {
			return 0, 0, &inventoryQueryError{"page_size must be between 1 and 100"}
		}
	}
	if page*pageSize > 10000 {
		return 0, 0, &inventoryQueryError{"page and page_size request too large"}
	}
	return page, pageSize, nil
}

type inventoryQueryError struct{ message string }

func (e *inventoryQueryError) Error() string { return e.message }

func (a *API) collectSessionInventory(r *http.Request, status, search string, offline []session.OfflineSession, offlineReady bool) ([]sessionInventoryItem, []string) {
	items := make([]sessionInventoryItem, 0)
	liveByID := make(map[string]sessionInventoryItem)
	unreachable := []string{}

	if status == "all" || status == "online" {
		edges := a.Membership.EdgeHTTPAddrs()
		results, missing := fetchAll(r.Context(), edges, "/api/live/clients", func() any {
			return &[]livestatsapi.ClientView{}
		})
		unreachable = missing
		for nodeID, raw := range results {
			for _, client := range *raw.(*[]livestatsapi.ClientView) {
				if _, exists := liveByID[client.ClientID]; exists {
					continue
				}
				liveByID[client.ClientID] = sessionInventoryItem{
					ClientID:      client.ClientID,
					Status:        "online",
					NodeID:        nodeID,
					Username:      client.Username,
					RemoteAddr:    client.RemoteAddr,
					CleanSession:  client.CleanSession,
					Subscriptions: append([]string(nil), client.Subscriptions...),
				}
			}
		}
		if status == "online" {
			for _, item := range liveByID {
				if inventoryMatches(item, search) {
					items = append(items, item)
				}
			}
			return items, unreachable
		}
		for _, item := range liveByID {
			if inventoryMatches(item, search) {
				items = append(items, item)
			}
		}
	}

	if status == "online" || a.OfflineSessions == nil {
		return items, unreachable
	}
	if !offlineReady {
		return items, unreachable
	}
	liveClaims := map[string]string{}
	if a.RaftNode != nil {
		liveClaims = a.RaftNode.Registry.SessionsSnapshot()
	}
	for _, persisted := range offline {
		if _, live := liveClaims[persisted.ClientID]; live {
			continue
		}
		if _, live := liveByID[persisted.ClientID]; live {
			continue
		}
		item := sessionInventoryItem{
			ClientID:      persisted.ClientID,
			Status:        "offline",
			Subscriptions: make([]string, 0, len(persisted.Subscriptions)),
		}
		for _, sub := range persisted.Subscriptions {
			item.Subscriptions = append(item.Subscriptions, sub.Filter)
		}
		if inventoryMatches(item, search) {
			items = append(items, item)
		}
	}
	return items, unreachable
}

func inventoryMatches(item sessionInventoryItem, search string) bool {
	if search == "" {
		return true
	}
	needle := strings.ToLower(search)
	if strings.Contains(strings.ToLower(item.ClientID), needle) ||
		strings.Contains(strings.ToLower(item.Username), needle) ||
		strings.Contains(strings.ToLower(item.RemoteAddr), needle) {
		return true
	}
	for _, sub := range item.Subscriptions {
		if strings.Contains(strings.ToLower(sub), needle) {
			return true
		}
	}
	return false
}
