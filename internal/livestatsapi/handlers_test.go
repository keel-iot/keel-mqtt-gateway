package livestatsapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleStats(t *testing.T) {
	h := &Handlers{
		Stats: func() StatsView {
			return StatsView{NodeID: "edge-1", ActiveConnections: 3, TotalMessages: 42, MessagesPerSecond: 1.5}
		},
	}
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/live/stats", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var got StatsView
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "edge-1", got.NodeID)
	require.Equal(t, 3, got.ActiveConnections)
	require.Equal(t, uint64(42), got.TotalMessages)
	require.InDelta(t, 1.5, got.MessagesPerSecond, 0.001)
}

func TestHandleClients(t *testing.T) {
	h := &Handlers{
		Clients: func() []ClientView {
			return []ClientView{
				{ClientID: "device-1", Username: "device-1@tenant", Subscriptions: []string{"cmd/device-1"}},
			}
		},
	}
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/live/clients", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var got []ClientView
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 1)
	require.Equal(t, "device-1", got[0].ClientID)
}

func TestHandleClients_EmptyNotNull(t *testing.T) {
	h := &Handlers{
		Clients: func() []ClientView { return nil },
	}
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/live/clients", nil))
	require.JSONEq(t, "[]", rec.Body.String())
}

func TestHandleClients_PaginationAndSearch(t *testing.T) {
	h := &Handlers{
		Clients: func() []ClientView {
			return []ClientView{
				{ClientID: "zulu", Username: "alpha", RemoteAddr: "10.0.0.2:1883"},
				{ClientID: "device-2", Username: "beta", RemoteAddr: "10.0.0.3:1883"},
				{ClientID: "device-1", Username: "beta", RemoteAddr: "10.0.0.4:1883"},
			}
		},
	}
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/live/clients?page=1&page_size=1&search=device&sort=client_id", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var got ClientPage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, 2, got.Total)
	require.Equal(t, 1, got.PageSize)
	require.Len(t, got.Items, 1)
	require.Equal(t, "device-1", got.Items[0].ClientID)
}

func TestHandleClients_RejectsInvalidPageSize(t *testing.T) {
	h := &Handlers{Clients: func() []ClientView { return nil }}
	mux := http.NewServeMux()
	h.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/live/clients?page_size=1001", nil))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
