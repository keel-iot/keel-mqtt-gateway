package broker

import (
	"context"
	"fmt"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
)

const (
	retainedKeyPrefix = "keel:retained:"
	retainedIndexKey  = "keel:retained:index"
)

// RetainedStore persists MQTT retained messages in Redis so they survive
// broker restarts and are visible cluster-wide — unlike mochi-mqtt's own
// retained-message store, which is per-process, in-memory only, and
// therefore only ever sees messages published on that exact node. See
// keelHook.OnRetainMessage (write path) and deliverRetainedBackfill (read
// path) for how the two compose without double-delivering to a subscriber.
type RetainedStore struct {
	router *redisrouter.Router
}

// NewRetainedStore creates a RetainedStore backed by router.
func NewRetainedStore(router *redisrouter.Router) *RetainedStore {
	return &RetainedStore{router: router}
}

func retainedKey(topic string) string { return retainedKeyPrefix + topic }

// Set stores payload as the retained message for topic, or — per standard
// MQTT semantics (an empty-payload retained publish clears it) — deletes
// any existing retained message for topic.
func (s *RetainedStore) Set(ctx context.Context, topic string, payload []byte) error {
	if len(payload) == 0 {
		return s.Delete(ctx, topic)
	}
	pipe := s.router.Client().TxPipeline()
	pipe.Set(ctx, retainedKey(topic), payload, 0)
	pipe.SAdd(ctx, retainedIndexKey, topic)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("retained: set %q: %w", topic, err)
	}
	return nil
}

// Delete removes the retained message for topic, if any.
func (s *RetainedStore) Delete(ctx context.Context, topic string) error {
	pipe := s.router.Client().TxPipeline()
	pipe.Del(ctx, retainedKey(topic))
	pipe.SRem(ctx, retainedIndexKey, topic)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("retained: delete %q: %w", topic, err)
	}
	return nil
}

// RetainedMessage is a single (topic, payload) match returned by Match.
type RetainedMessage struct {
	Topic   string
	Payload []byte
}

// Match returns every retained message whose topic matches filter (standard
// MQTT +/# semantics, via acl.MatchTopic), skipping any topic present in
// exclude. exclude lets the caller omit topics mochi-mqtt's own local
// retained store already delivered to this subscriber on this node, so the
// two sources never double-deliver the same retained message.
//
// A literal (wildcard-free) filter is a direct GET — O(1), no index scan.
// A wildcard filter scans the full topic index (SMEMBERS) then MGETs the
// matches — fine at the retained-message volumes a single deployment sees,
// but a known bottleneck at tens of thousands of unique retained topics
// (see CONFIGURATION.md's known limitations).
func (s *RetainedStore) Match(ctx context.Context, filter string, exclude map[string]struct{}) ([]RetainedMessage, error) {
	var topics []string
	if !containsWildcard(filter) {
		if _, skip := exclude[filter]; skip {
			return nil, nil
		}
		topics = []string{filter}
	} else {
		members, err := s.router.Client().SMembers(ctx, retainedIndexKey).Result()
		if err != nil {
			return nil, fmt.Errorf("retained: smembers index: %w", err)
		}
		for _, t := range members {
			if _, skip := exclude[t]; skip {
				continue
			}
			if acl.MatchTopic(filter, t) {
				topics = append(topics, t)
			}
		}
	}
	if len(topics) == 0 {
		return nil, nil
	}

	keys := make([]string, len(topics))
	for i, t := range topics {
		keys[i] = retainedKey(t)
	}
	vals, err := s.router.Client().MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("retained: mget: %w", err)
	}

	out := make([]RetainedMessage, 0, len(topics))
	for i, v := range vals {
		if v == nil {
			continue // race: deleted between the index lookup above and this MGET
		}
		payload, ok := v.(string)
		if !ok {
			continue
		}
		out = append(out, RetainedMessage{Topic: topics[i], Payload: []byte(payload)})
	}
	return out, nil
}

func containsWildcard(filter string) bool {
	for _, r := range filter {
		if r == '+' || r == '#' {
			return true
		}
	}
	return false
}
