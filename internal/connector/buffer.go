package connector

import (
	"context"
	"sync"
)

// OutputBuffer is a bounded, in-process queue of ForwardRequests sitting
// between the MQTT publish hot path and an OutputConnector's Forward call.
// It exists so a slow or unavailable downstream (Kafka/Ditto) degrades to
// observable message loss instead of blocking the publish path or an
// unbounded memory leak.
//
// Implementations must never block Push: a full buffer drops the oldest
// queued request to make room, never the newest — the most recent
// telemetry is more useful than stale telemetry. A drop is never silent;
// callers are expected to count it (see internal/telemetry's
// ForwarderDropped).
//
// Kept as an interface, alongside the same pattern used for
// internal/cluster/store.ClusterStore, so a future disk-backed
// implementation (e.g. bbolt-backed, reusing the raft-boltdb dependency)
// can replace MemoryOutputBuffer without touching call sites.
type OutputBuffer interface {
	// Push enqueues req. If the buffer is at capacity, the oldest queued
	// request is dropped and returned as dropped (ok is true); otherwise
	// dropped is nil.
	Push(req *ForwardRequest) (dropped *ForwardRequest, ok bool)

	// Next blocks until a request is available or ctx is done or the
	// buffer is closed. ok is false in the latter two cases.
	Next(ctx context.Context) (req *ForwardRequest, ok bool)

	// Len returns the number of requests currently queued.
	Len() int

	// Capacity returns the maximum number of requests the buffer holds.
	Capacity() int

	// Close releases waiters blocked in Next. Push after Close is a no-op.
	Close()
}

// MemoryOutputBuffer is a bounded, in-memory, drop-oldest FIFO OutputBuffer.
type MemoryOutputBuffer struct {
	mu       sync.Mutex
	items    []*ForwardRequest
	capacity int
	closed   bool

	// signal is sent-to (non-blocking) whenever an item is pushed, and
	// received-from by Next to wake up without polling.
	signal chan struct{}
}

// NewMemoryOutputBuffer creates a MemoryOutputBuffer holding at most
// capacity requests. capacity <= 0 is treated as 1 (a buffer of zero would
// drop every message immediately, which is never the intent of enabling
// buffering at all).
func NewMemoryOutputBuffer(capacity int) *MemoryOutputBuffer {
	if capacity <= 0 {
		capacity = 1
	}
	return &MemoryOutputBuffer{
		capacity: capacity,
		items:    make([]*ForwardRequest, 0, capacity),
		signal:   make(chan struct{}, 1),
	}
}

func (b *MemoryOutputBuffer) Push(req *ForwardRequest) (dropped *ForwardRequest, ok bool) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, false
	}
	if len(b.items) >= b.capacity {
		dropped = b.items[0]
		b.items = b.items[1:]
		ok = true
	}
	b.items = append(b.items, req)
	b.mu.Unlock()

	select {
	case b.signal <- struct{}{}:
	default:
	}
	return dropped, ok
}

func (b *MemoryOutputBuffer) Next(ctx context.Context) (*ForwardRequest, bool) {
	for {
		b.mu.Lock()
		if len(b.items) > 0 {
			req := b.items[0]
			b.items = b.items[1:]
			b.mu.Unlock()
			return req, true
		}
		closed := b.closed
		b.mu.Unlock()
		if closed {
			return nil, false
		}

		select {
		case <-b.signal:
		case <-ctx.Done():
			return nil, false
		}
	}
}

func (b *MemoryOutputBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

func (b *MemoryOutputBuffer) Capacity() int {
	return b.capacity
}

func (b *MemoryOutputBuffer) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.mu.Unlock()

	select {
	case b.signal <- struct{}{}:
	default:
	}
}
