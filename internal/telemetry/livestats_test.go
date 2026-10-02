package telemetry

import (
	"testing"
	"time"
)

func TestLiveStatsSnapshotIncludesRollingRatesAndRecentEvents(t *testing.T) {
	ls := NewLiveStats()
	now := time.Now()
	ls.mu.Lock()
	ls.samples = []rateSample{{at: now.Add(-2 * time.Minute), messages: 0}}
	ls.mu.Unlock()

	for i := 0; i < 120; i++ {
		ls.RecordPublish(10)
	}
	ls.RecordDisconnect()
	ls.RecordDrop()

	snapshot := ls.Snapshot()
	if snapshot.MessagesPerSecondAverage1m <= 0 {
		t.Fatalf("expected positive one-minute average, got %v", snapshot.MessagesPerSecondAverage1m)
	}
	if snapshot.MessagesPerSecondAverage5m <= 0 {
		t.Fatalf("expected positive five-minute average, got %v", snapshot.MessagesPerSecondAverage5m)
	}
	if snapshot.DisconnectsTotal != 1 || snapshot.DisconnectsLast5m != 1 {
		t.Fatalf("unexpected disconnect counters: total=%d recent=%d", snapshot.DisconnectsTotal, snapshot.DisconnectsLast5m)
	}
	if snapshot.DroppedMessagesTotal != 1 || snapshot.DroppedMessagesLast5m != 1 {
		t.Fatalf("unexpected drop counters: total=%d recent=%d", snapshot.DroppedMessagesTotal, snapshot.DroppedMessagesLast5m)
	}
}
