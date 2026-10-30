// SPDX-License-Identifier: MIT

package mqtt

import (
	"sync"

	"github.com/mochi-mqtt/server/v2/packets"
)

// packetQueue is a bounded FIFO with multiple producers and one consumer.
// Unlike a channel buffered to the maximum backlog, its packet storage is
// allocated only when used. The wakeup channel carries no packet references.
type packetQueue struct {
	mu     sync.Mutex
	buf    []*packets.Packet
	head   int
	size   int
	limit  int
	closed bool
	ready  chan struct{}
	// Preserve rendezvous semantics for an explicitly unbuffered queue.
	direct chan *packets.Packet
}

const packetQueueInitialSize = 16

func newPacketQueue(limit int32) *packetQueue {
	if limit < 0 {
		panic("negative outbound queue capacity")
	}
	q := &packetQueue{limit: int(limit), ready: make(chan struct{}, 1)}
	if limit == 0 {
		q.direct = make(chan *packets.Packet)
	}
	return q
}

// tryPush never waits for capacity, matching the former channel select/default.
func (q *packetQueue) tryPush(pk *packets.Packet) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if q.direct != nil {
		q.mu.Unlock()
		select {
		case q.direct <- pk:
			return true
		default:
			return false
		}
	}
	defer q.mu.Unlock()
	if q.size == q.limit {
		return false
	}
	if q.size == len(q.buf) {
		capacity := len(q.buf)
		if capacity == 0 {
			capacity = packetQueueInitialSize
		} else if capacity <= q.limit/2 {
			capacity *= 2
		} else {
			capacity = q.limit
		}
		if capacity > q.limit {
			capacity = q.limit
		}
		buf := make([]*packets.Packet, capacity)
		if q.size != 0 {
			n := copy(buf, q.buf[q.head:])
			copy(buf[n:], q.buf[:q.head])
		}
		q.buf, q.head = buf, 0
	}
	q.buf[(q.head+q.size)%len(q.buf)] = pk
	q.size++
	if q.size == 1 {
		select {
		case q.ready <- struct{}{}:
		default:
		}
	}
	return true
}

func (q *packetQueue) pop(done <-chan struct{}) (*packets.Packet, bool) {
	if q.direct != nil {
		select {
		case <-done:
			return nil, false
		case <-q.ready:
			return nil, false
		case pk := <-q.direct:
			return pk, true
		}
	}
	for {
		select {
		case <-done:
			return nil, false
		default:
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, false
		}
		if q.size > 0 {
			pk := q.buf[q.head]
			q.buf[q.head] = nil // Do not retain delivered packets/payloads.
			q.head = (q.head + 1) % len(q.buf)
			q.size--
			if q.size == 0 {
				q.head = 0
				if len(q.buf) > packetQueueInitialSize {
					q.buf = nil // Reclaim large backlogs when fully drained.
				}
				select {
				case <-q.ready:
				default:
				}
			}
			q.mu.Unlock()
			return pk, true
		}
		q.mu.Unlock()
		select {
		case <-done:
			return nil, false
		case <-q.ready:
		}
	}
}

func (q *packetQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

// close discards transport copies; durable QoS state remains in Inflight.
// It returns the queued count so Client can keep its pending-write counter
// consistent even when another packet is currently being written.
func (q *packetQueue) close() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := q.size
	if !q.closed {
		close(q.ready)
	}
	q.closed, q.buf, q.head, q.size = true, nil, 0, 0
	return n
}
