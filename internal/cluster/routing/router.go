// Package routing implements the MQTT topic-filter routing table as
// distributed state in a store.ClusterStore (Olric in production), with a
// local per-process trie cache for lookup without a network round-trip on
// every publish.
//
// Design: the store holds one entry per (topic filter, nodeID) pair —
// never a shared multi-writer value per topic — so concurrent
// Subscribe/Unsubscribe calls from different nodes to the same or
// overlapping filters never need a merge: each pair is an independent
// key, genuinely conflict-free under an AP store. Every mutation also
// publishes a small event over the store's pub/sub channel so every
// node's local cache updates in well under a second without polling; the
// store itself remains authoritative, and a periodic full reconciliation
// (Router.reconcile, via Scan) rebuilds the local cache from scratch on
// an interval, so convergence holds even if a pub/sub message is dropped
// — the store has no delivery guarantee for a subscriber that's briefly
// unreachable.
//
// Matching semantics (exact match, "#", "+", "$SYS" exclusion, union
// across overlapping filters) are unchanged from the prior Raft-FSM
// implementation: both reuse mochi-mqtt's own TopicsIndex trie.
//
// Router.reconcile (above) only ever pulls the local cache back into sync
// with the store — if the store itself loses all of its data (e.g. Olric
// restarted without persistence, or an admin wipes its dmaps) while a
// node's MQTT clients stay connected, reconcile alone would just converge
// every node's local cache to "empty", not restore the real subscriptions.
// See Reconciler (reconciler.go) for the other direction: it re-asserts a
// node's own live subscriptions (read straight from mochi-mqtt, not from
// this cache) back into the store whenever they've gone missing from it.
package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	mochimqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/store"
)

// keySep separates the topic filter from the node ID in a store key.
// MQTT topic names and filters cannot contain a NUL byte (well-formed
// UTF-8 requirement, MQTT-1.5.4-2), so this delimiter is always
// unambiguous — no escaping needed to split it back apart.
const keySep = "\x00"

// defaultChannel is the pub/sub channel routing events are broadcast on.
const defaultChannel = "keel.routes.events"

// defaultReconcileInterval is how often the local cache is rebuilt from
// scratch via a full store Scan, as a safety net under dropped pub/sub
// messages.
const defaultReconcileInterval = 10 * time.Second

type eventOp string

const (
	eventSubscribe        eventOp = "sub"
	eventUnsubscribe      eventOp = "unsub"
	eventUnsubscribeBatch eventOp = "unsub_batch"
	eventPurgeNode        eventOp = "purge"
)

type routeEvent struct {
	Op     eventOp  `json:"op"`
	Topic  string   `json:"topic,omitempty"`
	Topics []string `json:"topics,omitempty"`
	NodeID string   `json:"node_id"`
}

// Config configures a Router.
type Config struct {
	Store store.ClusterStore
	// Channel is the pub/sub channel for routing events. Default
	// "keel.routes.events".
	Channel string
	// ReconcileInterval is how often the local cache is rebuilt from a
	// full store Scan. Default 10s.
	ReconcileInterval time.Duration
	Log               *slog.Logger
}

// Router is the Olric-backed routing table. It satisfies the same method
// surface the prior Raft-FSM routing implementation exposed to
// internal/cluster/raft.CoreRegistry (Subscribe/Unsubscribe/NodesFor plus
// the BatchUnsubscriber/NodePurger/NodesWithRoutesProvider extensions),
// so hooks.go and the rest of the broker package need no changes.
type Router struct {
	store   store.ClusterStore
	channel string
	log     *slog.Logger

	reconcileInterval time.Duration
	sub               store.Subscription

	mu     sync.RWMutex
	index  *mochimqtt.TopicsIndex
	byNode map[string]map[string]struct{}

	stop      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// New creates a Router: performs an initial full reconciliation from the
// store, subscribes to the routing-events channel, and starts the
// background event-consumption and periodic-reconciliation loops.
func New(cfg Config) (*Router, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("routing: Config.Store is required")
	}
	channel := cfg.Channel
	if channel == "" {
		channel = defaultChannel
	}
	interval := cfg.ReconcileInterval
	if interval <= 0 {
		interval = defaultReconcileInterval
	}

	r := &Router{
		store:             cfg.Store,
		channel:           channel,
		log:               cfg.Log,
		reconcileInterval: interval,
		index:             mochimqtt.NewTopicsIndex(),
		byNode:            make(map[string]map[string]struct{}),
		stop:              make(chan struct{}),
	}

	if err := r.reconcile(context.Background()); err != nil {
		return nil, fmt.Errorf("routing: initial reconcile: %w", err)
	}

	sub, err := cfg.Store.Subscribe(context.Background(), channel)
	if err != nil {
		return nil, fmt.Errorf("routing: subscribe %q: %w", channel, err)
	}
	r.sub = sub

	r.wg.Add(2)
	go r.consumeEvents()
	go r.reconcileLoop()

	return r, nil
}

// Close stops the background loops and closes the pub/sub subscription.
// Does not close the underlying store — the caller owns that.
func (r *Router) Close() error {
	r.closeOnce.Do(func() { close(r.stop) })
	r.wg.Wait()
	if r.sub != nil {
		return r.sub.Close()
	}
	return nil
}

// ── Registry-compatible write path ──────────────────────────────────────

func (r *Router) Subscribe(topic, nodeID string) error {
	ctx := context.Background()
	if err := r.store.Put(ctx, routeKey(topic, nodeID), nowMarker()); err != nil {
		return fmt.Errorf("routing: subscribe: %w", err)
	}
	return r.publish(ctx, routeEvent{Op: eventSubscribe, Topic: topic, NodeID: nodeID})
}

func (r *Router) Unsubscribe(topic, nodeID string) error {
	ctx := context.Background()
	if err := r.store.Delete(ctx, routeKey(topic, nodeID)); err != nil {
		return fmt.Errorf("routing: unsubscribe: %w", err)
	}
	return r.publish(ctx, routeEvent{Op: eventUnsubscribe, Topic: topic, NodeID: nodeID})
}

// UnsubscribeBatch removes multiple filters for nodeID. An empty topics
// list resolves to "everything currently registered for nodeID" via the
// local inverse index.
func (r *Router) UnsubscribeBatch(topics []string, nodeID string) error {
	if len(topics) == 0 {
		topics = r.TopicsForNode(nodeID)
	}
	if len(topics) == 0 {
		return nil
	}
	ctx := context.Background()
	keys := make([]string, len(topics))
	for i, t := range topics {
		keys[i] = routeKey(t, nodeID)
	}
	if err := r.store.Delete(ctx, keys...); err != nil {
		return fmt.Errorf("routing: unsubscribe_batch: %w", err)
	}
	return r.publish(ctx, routeEvent{Op: eventUnsubscribeBatch, Topics: topics, NodeID: nodeID})
}

// PurgeNode removes every routing entry for nodeID.
func (r *Router) PurgeNode(nodeID string) error {
	topics := r.TopicsForNode(nodeID)
	if len(topics) == 0 {
		return nil
	}
	ctx := context.Background()
	keys := make([]string, len(topics))
	for i, t := range topics {
		keys[i] = routeKey(t, nodeID)
	}
	if err := r.store.Delete(ctx, keys...); err != nil {
		return fmt.Errorf("routing: purge_node: %w", err)
	}
	return r.publish(ctx, routeEvent{Op: eventPurgeNode, NodeID: nodeID})
}

func (r *Router) publish(ctx context.Context, ev routeEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("routing: encode event: %w", err)
	}
	if err := r.store.Publish(ctx, r.channel, b); err != nil {
		return fmt.Errorf("routing: publish event: %w", err)
	}
	return nil
}

// ── read path — served entirely from the local cache, no network round-trip ──

// NodesFor returns the node IDs that must receive a message published on
// topic: the union of every node with a non-shared subscription matching
// topic, per standard MQTT wildcard semantics, plus — for each
// shared-subscription group ($share/group/filter) matching topic — either
// nothing (if localNodeID is itself a group member, since that node's own
// mochi-mqtt instance already delivers to one of its local clients in the
// group) or exactly one arbitrarily-selected other member, so a shared
// group receives the message exactly once across the whole cluster rather
// than once per member node. localNodeID may be "" to disable this local
// preference (e.g. no node ID is meaningful).
func (r *Router) NodesFor(topic, localNodeID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	subs := r.index.Subscribers(topic)

	for filter, members := range subs.Shared {
		if _, ok := members[localNodeID]; ok {
			delete(subs.Shared, filter)
		}
	}
	subs.SelectShared()
	subs.MergeSharedSelected()

	out := make([]string, 0, len(subs.Subscriptions))
	for nodeID := range subs.Subscriptions {
		out = append(out, nodeID)
	}
	return out
}

// TopicsForNode returns every filter nodeID is currently registered
// against, per this process's local cache (may lag a live write from
// another node by up to the pub/sub propagation delay, or the
// reconciliation interval if that message was dropped).
func (r *Router) TopicsForNode(nodeID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.topicsForNodeLocked(nodeID)
}

func (r *Router) topicsForNodeLocked(nodeID string) []string {
	topics := r.byNode[nodeID]
	out := make([]string, 0, len(topics))
	for t := range topics {
		out = append(out, t)
	}
	return out
}

// NodesWithRoutes returns every node ID currently holding at least one
// routing entry in the local cache.
func (r *Router) NodesWithRoutes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byNode))
	for nodeID := range r.byNode {
		out = append(out, nodeID)
	}
	return out
}

// Snapshot returns a topic -> node IDs view of the local cache, for the
// management API's /api/cluster/routes endpoint (same shape the prior
// Raft-FSM routing table exposed).
func (r *Router) Snapshot() map[string][]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][]string)
	for nodeID, topics := range r.byNode {
		for t := range topics {
			out[t] = append(out[t], nodeID)
		}
	}
	return out
}

// ── local cache mutation ──────────────────────────────────────────────

func (r *Router) addLocalLocked(topic, nodeID string) {
	topics, ok := r.byNode[nodeID]
	if !ok {
		topics = make(map[string]struct{})
		r.byNode[nodeID] = topics
	}
	topics[topic] = struct{}{}
	r.index.Subscribe(nodeID, packets.Subscription{Filter: topic})
}

func (r *Router) removeLocalLocked(topic, nodeID string) {
	if topics, ok := r.byNode[nodeID]; ok {
		delete(topics, topic)
		if len(topics) == 0 {
			delete(r.byNode, nodeID)
		}
	}
	r.index.Unsubscribe(topic, nodeID)
}

// ── event consumption (fast path) ────────────────────────────────────────

func (r *Router) consumeEvents() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case msg, ok := <-r.sub.Messages():
			if !ok {
				return
			}
			var ev routeEvent
			if err := json.Unmarshal(msg, &ev); err != nil {
				r.logWarn("routing: decode event", "error", err)
				continue
			}
			r.applyEvent(ev)
		}
	}
}

func (r *Router) applyEvent(ev routeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch ev.Op {
	case eventSubscribe:
		r.addLocalLocked(ev.Topic, ev.NodeID)
	case eventUnsubscribe:
		r.removeLocalLocked(ev.Topic, ev.NodeID)
	case eventUnsubscribeBatch:
		for _, t := range ev.Topics {
			r.removeLocalLocked(t, ev.NodeID)
		}
	case eventPurgeNode:
		for _, t := range r.topicsForNodeLocked(ev.NodeID) {
			r.removeLocalLocked(t, ev.NodeID)
		}
	default:
		r.logWarn("routing: unknown event op", "op", ev.Op)
	}
}

// ── periodic reconciliation (safety net) ─────────────────────────────────

func (r *Router) reconcileLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.reconcileInterval)
			if err := r.reconcile(ctx); err != nil {
				r.logWarn("routing: periodic reconcile failed", "error", err)
			}
			cancel()
		}
	}
}

// reconcile rebuilds the local cache from scratch via a full store Scan —
// the convergence guarantee of last resort, independent of pub/sub
// delivery.
func (r *Router) reconcile(ctx context.Context) error {
	it, err := r.store.Scan(ctx)
	if err != nil {
		return err
	}
	defer it.Close()

	index := mochimqtt.NewTopicsIndex()
	byNode := make(map[string]map[string]struct{})
	for it.Next() {
		topic, nodeID, ok := parseRouteKey(it.Key())
		if !ok {
			continue
		}
		topics, exists := byNode[nodeID]
		if !exists {
			topics = make(map[string]struct{})
			byNode[nodeID] = topics
		}
		topics[topic] = struct{}{}
		index.Subscribe(nodeID, packets.Subscription{Filter: topic})
	}

	r.mu.Lock()
	r.index = index
	r.byNode = byNode
	r.mu.Unlock()
	return nil
}

func (r *Router) logWarn(msg string, args ...any) {
	if r.log != nil {
		r.log.Warn(msg, args...)
	}
}

// ── key encoding ──────────────────────────────────────────────────────────

func routeKey(topic, nodeID string) string {
	return topic + keySep + nodeID
}

func parseRouteKey(key string) (topic, nodeID string, ok bool) {
	i := strings.LastIndex(key, keySep)
	if i < 0 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// nowMarker is an informational value stored alongside each routing
// entry (when it was last written) — never read back by this package;
// the key alone carries everything NodesFor/reconcile need.
func nowMarker() []byte {
	return []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
}
