package routing

import (
	"context"
	"sync"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/store"
)

// memStore is a minimal in-memory ClusterStore used only for fast,
// deterministic unit tests of Router's wildcard-matching logic — not a
// substitute for TestConvergence, which exercises a real multi-node Olric
// cluster.
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
