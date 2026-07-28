package main

import (
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// message is the wire payload published by simulated devices. sentAtUnixNano
// is what monitors use to compute end-to-end latency; msgID is what the
// pending-message tracker uses to detect loss.
type message struct {
	MsgID        string `json:"msg_id"`
	DeviceID     string `json:"device_id"`
	SentUnixNano int64  `json:"sent_unix_nano"`
	Padding      string `json:"padding,omitempty"`
}

func newMessage(deviceID string, payloadBytes int) message {
	m := message{
		MsgID:        randID(),
		DeviceID:     deviceID,
		SentUnixNano: time.Now().UnixNano(),
	}
	// Pad to roughly the requested size; the JSON envelope itself already
	// accounts for a good chunk of it, so this is approximate, which is
	// fine for load-shape purposes (exact byte-for-byte sizing isn't the point).
	base, _ := json.Marshal(m)
	if extra := payloadBytes - len(base); extra > 0 {
		m.Padding = randPadding(extra)
	}
	return m
}

const idAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

func randID() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = idAlphabet[rand.Intn(len(idAlphabet))]
	}
	return string(b)
}

func randPadding(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

// pendingEntry tracks a single in-flight publish awaiting delivery
// confirmation by any monitor.
type pendingEntry struct {
	sentAt time.Time
}

// numMessageShards is the shard count for the per-message hot path
// (recordPublish/recordDelivery, called once per device per message —
// the operations that turned into a client-side bottleneck at ~5 msg/s/device
// with a single shared mutex). Sharding by msgID hash keeps each shard's lock
// held only by the goroutines whose messages land in it.
const numMessageShards = 32

// messageShard holds the message-rate counters and pending map for one shard.
type messageShard struct {
	mu            sync.Mutex
	pending       map[string]pendingEntry // msgID -> sent time, deleted on first delivery or on loss sweep
	latencies     []time.Duration
	delivered     int
	lost          int
	published     int
	publishErrors int
}

// metrics is the single shared collector for the whole run. Message-rate
// counters live in sharded messageShards to avoid a single central lock on
// the hot path; the lower-frequency counters below (one update per device
// per connect/storm/churn phase, not per message) stay behind mu.
type metrics struct {
	shards [numMessageShards]messageShard

	mu sync.Mutex

	// Reconnect storm bookkeeping.
	stormDisconnected       int
	stormReconnected        int
	stormReconnectFailed    int
	stormReconnectLatencies []time.Duration

	// Subscription churn bookkeeping.
	churnCycles          int
	churnErrors          int
	convergenceLatencies []time.Duration

	// Connection bookkeeping (steady-state connect phase).
	connectSuccesses int
	connectFailures  int
	connectLatencies []time.Duration
}

func newMetrics() *metrics {
	m := &metrics{}
	for i := range m.shards {
		m.shards[i].pending = make(map[string]pendingEntry)
	}
	return m
}

// shardFor picks the shard for a given msgID via FNV-1a; recordPublish and
// recordDelivery for the same msgID must land on the same shard.
func (m *metrics) shardFor(msgID string) *messageShard {
	var h uint32 = 2166136261
	for i := 0; i < len(msgID); i++ {
		h ^= uint32(msgID[i])
		h *= 16777619
	}
	return &m.shards[h%numMessageShards]
}

func (m *metrics) recordPublish(msgID string, sentAt time.Time, err error) {
	s := m.shardFor(msgID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published++
	if err != nil {
		s.publishErrors++
		return
	}
	s.pending[msgID] = pendingEntry{sentAt: sentAt}
}

// recordDelivery is called by a monitor on message receipt. Returns true if
// this was the first monitor to see msgID (so latency is only counted once
// per message even when multiple monitors — one per node — all receive the
// same fanned-out message).
func (m *metrics) recordDelivery(msgID string, recvAt time.Time) bool {
	s := m.shardFor(msgID)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.pending[msgID]
	if !ok {
		return false // already delivered to another monitor, or already swept as lost
	}
	delete(s.pending, msgID)
	s.delivered++
	s.latencies = append(s.latencies, recvAt.Sub(entry.sentAt))
	return true
}

// sweepLost scans pending for entries older than timeout and counts them as
// lost. Called periodically by a background goroutine for the duration of
// the run plus one extra timeout window at the end (drainLost).
func (m *metrics) sweepLost(timeout time.Duration) {
	cutoff := time.Now().Add(-timeout)
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for id, e := range s.pending {
			if e.sentAt.Before(cutoff) {
				delete(s.pending, id)
				s.lost++
			}
		}
		s.mu.Unlock()
	}
}

// messageSnapshot aggregates the sharded message-rate counters. Called once
// at report time, after the run has stopped producing traffic.
func (m *metrics) messageSnapshot() (published, publishErrors, delivered, lost int, latencies []time.Duration) {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		published += s.published
		publishErrors += s.publishErrors
		delivered += s.delivered
		lost += s.lost
		latencies = append(latencies, s.latencies...)
		s.mu.Unlock()
	}
	return
}

func (m *metrics) recordConnect(latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.connectFailures++
		return
	}
	m.connectSuccesses++
	m.connectLatencies = append(m.connectLatencies, latency)
}

func (m *metrics) recordStormDisconnect() {
	m.mu.Lock()
	m.stormDisconnected++
	m.mu.Unlock()
}

func (m *metrics) recordStormReconnect(latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.stormReconnectFailed++
		return
	}
	m.stormReconnected++
	m.stormReconnectLatencies = append(m.stormReconnectLatencies, latency)
}

func (m *metrics) recordChurnCycle(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.churnCycles++
	if err != nil {
		m.churnErrors++
	}
}

func (m *metrics) recordConvergence(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.convergenceLatencies = append(m.convergenceLatencies, latency)
}

// percentiles computes p50/p95/p99 (in that order) from a slice of
// durations. Returns zero values for an empty input.
func percentiles(d []time.Duration) (p50, p95, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(p float64) time.Duration {
		idx := int(p * float64(len(sorted)))
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99)
}

func mean(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	var sum time.Duration
	for _, v := range d {
		sum += v
	}
	return sum / time.Duration(len(d))
}
