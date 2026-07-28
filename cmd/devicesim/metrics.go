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

// metrics is the single shared collector for the whole run. All counters
// are protected by mu; latencies is append-only during the run and sorted
// once at report time.
type metrics struct {
	mu sync.Mutex

	pending       map[string]pendingEntry // msgID -> sent time, deleted on first delivery or on loss sweep
	latencies     []time.Duration
	delivered     int
	lost          int
	published     int
	publishErrors int

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
	return &metrics{pending: make(map[string]pendingEntry)}
}

func (m *metrics) recordPublish(msgID string, sentAt time.Time, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published++
	if err != nil {
		m.publishErrors++
		return
	}
	m.pending[msgID] = pendingEntry{sentAt: sentAt}
}

// recordDelivery is called by a monitor on message receipt. Returns true if
// this was the first monitor to see msgID (so latency is only counted once
// per message even when multiple monitors — one per node — all receive the
// same fanned-out message).
func (m *metrics) recordDelivery(msgID string, recvAt time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.pending[msgID]
	if !ok {
		return false // already delivered to another monitor, or already swept as lost
	}
	delete(m.pending, msgID)
	m.delivered++
	m.latencies = append(m.latencies, recvAt.Sub(entry.sentAt))
	return true
}

// sweepLost scans pending for entries older than timeout and counts them as
// lost. Called periodically by a background goroutine for the duration of
// the run plus one extra timeout window at the end (drainLost).
func (m *metrics) sweepLost(timeout time.Duration) {
	cutoff := time.Now().Add(-timeout)
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, e := range m.pending {
		if e.sentAt.Before(cutoff) {
			delete(m.pending, id)
			m.lost++
		}
	}
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
