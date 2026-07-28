package raft

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/routing"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/store"
)

// memStore is a minimal in-memory store.ClusterStore, mirroring
// internal/cluster/routing's own test double — used here to prove
// EdgeRegistry.NodesFor/Subscribe/Unsubscribe/EvaluateACL are served
// entirely locally (router + ACL cache), with zero calls into remote
// (RemoteRegistry, the gRPC-to-core path). remote is left nil below:
// any accidental call from EdgeRegistry into it would nil-pointer-panic
// immediately, so a passing test is itself the proof of "no RPC on the
// hot path" the task asked for — no separate call counter needed.
type memStore struct {
	mu   sync.Mutex
	data map[string][]byte

	subMu sync.Mutex
	subs  map[string][]chan []byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte), subs: make(map[string][]chan []byte)}
}

func (s *memStore) Put(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}
func (s *memStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok, nil
}
func (s *memStore) Delete(_ context.Context, keys ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		delete(s.data, k)
	}
	return nil
}
func (s *memStore) Scan(_ context.Context) (store.KeyIterator, error) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	return &memIterator{keys: keys, idx: -1}, nil
}
func (s *memStore) Publish(_ context.Context, channel string, message []byte) error {
	s.subMu.Lock()
	chans := append([]chan []byte(nil), s.subs[channel]...)
	s.subMu.Unlock()
	for _, ch := range chans {
		ch <- message
	}
	return nil
}
func (s *memStore) Subscribe(_ context.Context, channel string) (store.Subscription, error) {
	ch := make(chan []byte, 64)
	s.subMu.Lock()
	s.subs[channel] = append(s.subs[channel], ch)
	s.subMu.Unlock()
	return &memSubscription{store: s, channel: channel, ch: ch}, nil
}
func (s *memStore) Close(_ context.Context) error { return nil }

type memIterator struct {
	keys []string
	idx  int
}

func (i *memIterator) Next() bool  { i.idx++; return i.idx < len(i.keys) }
func (i *memIterator) Key() string { return i.keys[i.idx] }
func (i *memIterator) Close()      {}

type memSubscription struct {
	store   *memStore
	channel string
	ch      chan []byte
}

func (s *memSubscription) Messages() <-chan []byte { return s.ch }

func (s *memSubscription) Close() error {
	s.store.subMu.Lock()
	defer s.store.subMu.Unlock()
	subs := s.store.subs[s.channel]
	for i, c := range subs {
		if c == s.ch {
			s.store.subs[s.channel] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	close(s.ch)
	return nil
}

// TestEdgeRegistry_RoutingServedLocallyNotOverRPC is the "NodesFor no
// longer round-trips to core" test the core/edge follow-up task asked
// for: remote is nil, so any call reaching it would panic immediately —
// a passing test proves Subscribe/Unsubscribe/NodesFor are served
// entirely from the local router.
func TestEdgeRegistry_RoutingServedLocallyNotOverRPC(t *testing.T) {
	router, err := routing.New(routing.Config{Store: newMemStore(), Log: testLog()})
	if err != nil {
		t.Fatalf("routing.New: %v", err)
	}
	defer router.Close()

	cache := NewACLCache(func() (map[string]acl.Role, map[string][]string, []string, error) {
		return map[string]acl.Role{}, map[string][]string{}, nil, nil
	}, 50*time.Millisecond, testLog())
	defer cache.Close()

	e := NewEdgeRegistry(router, nil, cache) // remote intentionally nil

	if err := e.Subscribe("telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Router propagates via its own pub/sub (async, same store here) —
	// poll briefly for the local cache to reflect the write.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if nodes := e.NodesFor("telemetry/x"); len(nodes) == 1 && nodes[0] == "edge-1" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	nodes := e.NodesFor("telemetry/x")
	if len(nodes) != 1 || nodes[0] != "edge-1" {
		t.Fatalf("expected [edge-1], got %v", nodes)
	}

	if err := e.Unsubscribe("telemetry/#", "edge-1"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
}

// TestEdgeRegistry_EvaluateACLServedLocallyNotOverRPC proves EvaluateACL
// is served from the local ACLCache, not RemoteRegistry — same "remote is
// nil, so any RPC attempt panics" proof as above.
func TestEdgeRegistry_EvaluateACLServedLocallyNotOverRPC(t *testing.T) {
	fetchCalls := 0
	cache := NewACLCache(func() (map[string]acl.Role, map[string][]string, []string, error) {
		fetchCalls++
		roles := map[string]acl.Role{
			"custom": {Name: "custom", Rules: []acl.ACLRule{
				{TopicFilter: "telemetry/allowed", Actions: []string{"publish"}, Effect: acl.EffectAllow},
			}},
		}
		bindings := map[string][]string{"device-1": {"custom"}}
		return roles, bindings, nil, nil
	}, time.Hour, testLog())
	defer cache.Close()

	router, err := routing.New(routing.Config{Store: newMemStore(), Log: testLog()})
	if err != nil {
		t.Fatalf("routing.New: %v", err)
	}
	defer router.Close()

	e := NewEdgeRegistry(router, nil, cache) // remote intentionally nil

	if fetchCalls != 1 {
		t.Fatalf("expected exactly 1 synchronous fetch at cache construction, got %d", fetchCalls)
	}

	decision := e.EvaluateACL("device-1", "device-1", "telemetry/allowed", acl.ActionPublish)
	if !decision.Allowed() {
		t.Fatalf("expected allow for device-1 on telemetry/allowed, got deny")
	}

	decision = e.EvaluateACL("device-1", "device-1", "telemetry/other", acl.ActionPublish)
	if decision.Allowed() {
		t.Fatalf("expected fail-closed deny for an unmatched topic, got allow")
	}

	// Still exactly one fetch: reads never trigger a new RPC.
	if fetchCalls != 1 {
		t.Fatalf("expected EvaluateACL reads not to trigger additional fetches, got %d fetches", fetchCalls)
	}
}

func testLog() *slog.Logger { return slog.Default() }
