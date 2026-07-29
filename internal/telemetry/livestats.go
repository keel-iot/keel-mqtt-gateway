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

	mu           sync.Mutex
	prevMessages uint64
	prevBytes    uint64
	prevAt       time.Time
	msgRate      float64
	byteRate     float64
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
}

// LiveStatsSnapshot is the JSON shape served by GET /api/live/stats.
type LiveStatsSnapshot struct {
	TotalMessages     uint64  `json:"total_messages"`
	TotalBytes        uint64  `json:"total_bytes"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
}

func (ls *LiveStats) Snapshot() LiveStatsSnapshot {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return LiveStatsSnapshot{
		TotalMessages:     ls.totalMessages.Load(),
		TotalBytes:        ls.totalBytes.Load(),
		MessagesPerSecond: ls.msgRate,
		BytesPerSecond:    ls.byteRate,
	}
}
