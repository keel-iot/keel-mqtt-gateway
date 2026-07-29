package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/keel-iot/keel-mqtt-gateway/internal/livestatsapi"
)

// TestFetchAll_AggregatesAndToleratesUnreachablePeers is the real risk
// area of the cluster-wide GET /api/metrics aggregation: concurrent
// fetch, one dead/slow edge must not fail (or even slow down) the whole
// response for the others.
func TestFetchAll_AggregatesAndToleratesUnreachablePeers(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node_id":"edge-good","active_connections":5,"total_messages":100,"messages_per_second":2.5}`))
	}))
	defer good.Close()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()

	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	dead.Close() // closed before use — connection refused, not just a 500

	edges := map[string]string{
		"edge-good": strings.TrimPrefix(good.URL, "http://"),
		"edge-slow": strings.TrimPrefix(slow.URL, "http://"),
		"edge-dead": strings.TrimPrefix(dead.URL, "http://"),
	}

	start := time.Now()
	results, unreachable := fetchAll(t.Context(), edges, "/api/live/stats", func() any { return &livestatsapi.StatsView{} })
	elapsed := time.Since(start)

	require.Less(t, elapsed, liveMetricsCallTimeout+2*time.Second, "one slow/dead edge should not block longer than the per-call timeout")
	require.Len(t, results, 1)
	require.Contains(t, results, "edge-good")
	sv := results["edge-good"].(*livestatsapi.StatsView)
	require.Equal(t, "edge-good", sv.NodeID)
	require.Equal(t, 5, sv.ActiveConnections)

	require.ElementsMatch(t, []string{"edge-slow", "edge-dead"}, unreachable)
}

func TestFetchAll_EmptyEdges(t *testing.T) {
	results, unreachable := fetchAll(t.Context(), map[string]string{}, "/api/live/stats", func() any { return &livestatsapi.StatsView{} })
	require.Empty(t, results)
	require.Empty(t, unreachable)
}
