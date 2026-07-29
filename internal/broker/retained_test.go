package broker

import (
	"context"
	"os"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
)

func TestContainsWildcard(t *testing.T) {
	cases := []struct {
		filter string
		want   bool
	}{
		{"state/door1", false},
		{"state/+", true},
		{"state/#", true},
		{"a/b/c", false},
	}
	for _, c := range cases {
		if got := containsWildcard(c.filter); got != c.want {
			t.Errorf("containsWildcard(%q) = %v, want %v", c.filter, got, c.want)
		}
	}
}

// newTestRetainedStore requires a live Redis (TEST_REDIS_ADDR) — skipped
// otherwise. Point it at the docker-compose Redis, e.g.:
//
//	TEST_REDIS_ADDR=localhost:16379 go test ./internal/broker/... -run RetainedStore
func newTestRetainedStore(t *testing.T) (*RetainedStore, context.Context) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set, skipping live-Redis retained-store test")
	}
	ctx := context.Background()
	router, err := redisrouter.New(ctx, addr, "")
	if err != nil {
		t.Fatalf("redisrouter.New: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Client().FlushDB(ctx).Err()
	})
	return NewRetainedStore(router), ctx
}

func TestRetainedStore_SetGetDelete(t *testing.T) {
	s, ctx := newTestRetainedStore(t)

	if err := s.Set(ctx, "state/door1", []byte("open")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	msgs, err := s.Match(ctx, "state/door1", nil)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "open" {
		t.Fatalf("expected 1 message with payload %q, got %+v", "open", msgs)
	}

	// Empty payload deletes, per standard MQTT retained semantics.
	if err := s.Set(ctx, "state/door1", nil); err != nil {
		t.Fatalf("Set (delete via empty payload): %v", err)
	}
	msgs, err = s.Match(ctx, "state/door1", nil)
	if err != nil {
		t.Fatalf("Match after delete: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no messages after delete, got %+v", msgs)
	}
}

func TestRetainedStore_WildcardMatch(t *testing.T) {
	s, ctx := newTestRetainedStore(t)

	for topic, payload := range map[string]string{
		"state/door1":  "open",
		"state/door2":  "closed",
		"telemetry/t1": "22.5",
	} {
		if err := s.Set(ctx, topic, []byte(payload)); err != nil {
			t.Fatalf("Set(%q): %v", topic, err)
		}
	}

	msgs, err := s.Match(ctx, "state/+", nil)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 matches for state/+, got %d: %+v", len(msgs), msgs)
	}

	msgs, err = s.Match(ctx, "state/#", nil)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 matches for state/#, got %d: %+v", len(msgs), msgs)
	}

	msgs, err = s.Match(ctx, "telemetry/+", nil)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Topic != "telemetry/t1" {
		t.Fatalf("expected exactly telemetry/t1, got %+v", msgs)
	}
}

func TestRetainedStore_Exclude(t *testing.T) {
	s, ctx := newTestRetainedStore(t)

	if err := s.Set(ctx, "state/door1", []byte("open")); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, "state/door2", []byte("closed")); err != nil {
		t.Fatal(err)
	}

	msgs, err := s.Match(ctx, "state/+", map[string]struct{}{"state/door1": {}})
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Topic != "state/door2" {
		t.Fatalf("expected only state/door2 (door1 excluded), got %+v", msgs)
	}

	// Exact-filter path must honour exclude too, not just the wildcard path.
	msgs, err = s.Match(ctx, "state/door1", map[string]struct{}{"state/door1": {}})
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no messages, door1 was excluded, got %+v", msgs)
	}
}
