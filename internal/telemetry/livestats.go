package telemetry

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// LiveStats tracks message/byte counters for the basic monitoring UI's
// live-stats view (GET /api/live/stats on an edge/combined node) —
// separate from the Prometheus counters above (MessagesPublished,
// BytesPublished) because computing a live messages/sec rate from a
// monotonic prometheus.Counter would mean scraping and parsing this
// process's own registry; a couple of plain atomics fed from the same
// call sites (see internal/broker/hooks.go's OnPublish) is simpler and
// avoids that entirely.
type LiveStats struct {
	totalMessages atomic.Uint64
	totalBytes    atomic.Uint64
	disconnects   atomic.Uint64
	dropped       atomic.Uint64

	mu           sync.Mutex
	prevMessages uint64
	prevBytes    uint64
	prevAt       time.Time
	msgRate      float64
	byteRate     float64
	samples      []rateSample
	events       []liveEvent
}

type rateSample struct {
	at       time.Time
	messages uint64
	bytes    uint64
}

type liveEvent struct {
	at   time.Time
	kind string
}

// NewLiveStats returns a ready-to-use tracker. Call Start to begin
// sampling the rate; RecordPublish is safe to call before Start.
func NewLiveStats() *LiveStats {
	return &LiveStats{prevAt: time.Now()}
}

// RecordPublish records one published message of the given payload size.
func (ls *LiveStats) RecordPublish(payloadBytes int) {
	ls.totalMessages.Add(1)
	ls.totalBytes.Add(uint64(payloadBytes))
}

// RecordDisconnect records a disconnect event for the recent activity view.
func (ls *LiveStats) RecordDisconnect() {
	ls.disconnects.Add(1)
	ls.recordEvent("disconnect")
}

// RecordDrop records a message dropped by the broker data path. The caller
// remains responsible for incrementing the more specific Prometheus metric.
func (ls *LiveStats) RecordDrop() {
	ls.dropped.Add(1)
	ls.recordEvent("drop")
}

func (ls *LiveStats) recordEvent(kind string) {
	ls.mu.Lock()
	ls.events = append(ls.events, liveEvent{at: time.Now(), kind: kind})
	ls.pruneLocked(time.Now())
	ls.mu.Unlock()
}

// Start runs the rate-sampling loop until ctx is done. Safe to call at
// most once per LiveStats instance.
func (ls *LiveStats) Start(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				ls.sample()
			}
		}
	}()
}

func (ls *LiveStats) sample() {
	now := time.Now()
	msgs := ls.totalMessages.Load()
	bytes := ls.totalBytes.Load()

	ls.mu.Lock()
	defer ls.mu.Unlock()
	elapsed := now.Sub(ls.prevAt).Seconds()
	if elapsed > 0 {
		ls.msgRate = float64(msgs-ls.prevMessages) / elapsed
		ls.byteRate = float64(bytes-ls.prevBytes) / elapsed
	}
	ls.prevMessages = msgs
	ls.prevBytes = bytes
	ls.prevAt = now
	ls.samples = append(ls.samples, rateSample{at: now, messages: msgs, bytes: bytes})
	ls.pruneLocked(now)
}

func (ls *LiveStats) pruneLocked(now time.Time) {
	const retention = 5 * time.Minute
	cutoff := now.Add(-retention)
	first := 0
	for first < len(ls.samples) && ls.samples[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		ls.samples = append([]rateSample(nil), ls.samples[first:]...)
	}
	first = 0
	for first < len(ls.events) && ls.events[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		ls.events = append([]liveEvent(nil), ls.events[first:]...)
	}
}

func rateSince(samples []rateSample, messages uint64, now time.Time, window time.Duration) float64 {
	if len(samples) == 0 {
		return 0
	}
	cutoff := now.Add(-window)
	base := samples[0]
	for _, sample := range samples {
		if sample.at.After(cutoff) {
			break
		}
		base = sample
	}
	elapsed := now.Sub(base.at).Seconds()
	if elapsed <= 0 || messages < base.messages {
		return 0
	}
	return float64(messages-base.messages) / elapsed
}

// LiveStatsSnapshot is the JSON shape served by GET /api/live/stats.
type LiveStatsSnapshot struct {
	TotalMessages              uint64  `json:"total_messages"`
	TotalBytes                 uint64  `json:"total_bytes"`
	MessagesPerSecond          float64 `json:"messages_per_second"`
	MessagesPerSecondAverage1m float64 `json:"messages_per_second_avg_1m"`
	MessagesPerSecondAverage5m float64 `json:"messages_per_second_avg_5m"`
	BytesPerSecond             float64 `json:"bytes_per_second"`
	DisconnectsTotal           uint64  `json:"disconnects_total"`
	DisconnectsLast5m          uint64  `json:"disconnects_last_5m"`
	DroppedMessagesTotal       uint64  `json:"dropped_messages_total"`
	DroppedMessagesLast5m      uint64  `json:"dropped_messages_last_5m"`
}

func (ls *LiveStats) Snapshot() LiveStatsSnapshot {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	now := time.Now()
	ls.pruneLocked(now)
	disconnectsLast5m := uint64(0)
	droppedLast5m := uint64(0)
	for _, event := range ls.events {
		switch event.kind {
		case "disconnect":
			disconnectsLast5m++
		case "drop":
			droppedLast5m++
		}
	}
	messages := ls.totalMessages.Load()
	return LiveStatsSnapshot{
		TotalMessages:              messages,
		TotalBytes:                 ls.totalBytes.Load(),
		MessagesPerSecond:          ls.msgRate,
		MessagesPerSecondAverage1m: rateSince(ls.samples, messages, now, time.Minute),
		MessagesPerSecondAverage5m: rateSince(ls.samples, messages, now, 5*time.Minute),
		BytesPerSecond:             ls.byteRate,
		DisconnectsTotal:           ls.disconnects.Load(),
		DisconnectsLast5m:          disconnectsLast5m,
		DroppedMessagesTotal:       ls.dropped.Load(),
		DroppedMessagesLast5m:      droppedLast5m,
	}
}
