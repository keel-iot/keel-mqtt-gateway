// SPDX-License-Identifier: MIT

package mqtt

import (
	"context"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/mochi-mqtt/server/v2/system"
	"github.com/stretchr/testify/require"
)

func TestPacketQueueStorageIsLazyAndLimitUnchanged(t *testing.T) {
	cl := newClient(nil, &ops{options: &Options{Capabilities: NewDefaultServerCapabilities()}})
	defer cl.Stop(nil)
	require.Nil(t, cl.State.outbound.buf)
	require.Equal(t, 8192, cl.State.outbound.limit)
}

func TestPacketQueueFIFOThroughWrappedGrowth(t *testing.T) {
	q := newPacketQueue(65)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for id := 0; id < 32; id++ {
		require.True(t, q.tryPush(&packets.Packet{PacketID: uint16(id)}))
	}
	for id := 0; id < 20; id++ {
		pk, ok := q.pop(ctx.Done())
		require.True(t, ok)
		require.Equal(t, uint16(id), pk.PacketID)
	}
	for id := 32; id < 85; id++ {
		require.True(t, q.tryPush(&packets.Packet{PacketID: uint16(id)}))
	}
	require.False(t, q.tryPush(new(packets.Packet)))
	for id := 20; id < 85; id++ {
		pk, ok := q.pop(ctx.Done())
		require.True(t, ok)
		require.Equal(t, uint16(id), pk.PacketID)
	}
	require.Zero(t, q.len())
	require.Nil(t, q.buf, "a drained large queue must release its backing array")
	require.True(t, q.tryPush(&packets.Packet{PacketID: 99}))
	pk, ok := q.pop(ctx.Done())
	require.True(t, ok)
	require.Equal(t, uint16(99), pk.PacketID)
	for _, reference := range q.buf {
		require.Nil(t, reference, "consumed payloads must not stay referenced")
	}
}

func TestPacketQueueFullConfiguredCapacity(t *testing.T) {
	q := newPacketQueue(8192)
	for id := 0; id < 8192; id++ {
		require.True(t, q.tryPush(&packets.Packet{PacketID: uint16(id)}))
	}
	require.Equal(t, 8192, q.len())
	require.False(t, q.tryPush(new(packets.Packet)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for id := 0; id < 8192; id++ {
		pk, ok := q.pop(ctx.Done())
		require.True(t, ok)
		require.Equal(t, uint16(id), pk.PacketID)
	}
	require.Nil(t, q.buf)
}

func TestPacketQueueCancellationAndCloseWakeConsumer(t *testing.T) {
	for _, limit := range []int32{0, 1, 8192} {
		for _, stopWithClose := range []bool{false, true} {
			q := newPacketQueue(limit)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan bool, 1)
			go func() {
				_, ok := q.pop(ctx.Done())
				done <- ok
			}()
			if stopWithClose {
				require.Zero(t, q.close())
			} else {
				cancel()
			}
			select {
			case ok := <-done:
				require.False(t, ok)
			case <-time.After(time.Second):
				t.Fatal("empty queue did not release its consumer")
			}
			cancel()
			q.close()
			require.False(t, q.tryPush(new(packets.Packet)))
		}
	}
}

func TestPacketQueueUnbufferedRendezvous(t *testing.T) {
	q := newPacketQueue(0)
	require.False(t, q.tryPush(new(packets.Packet)), "no waiting reader")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan *packets.Packet, 1)
	go func() {
		pk, _ := q.pop(ctx.Done())
		result <- pk
	}()
	pk := &packets.Packet{PacketID: 42}
	require.Eventually(t, func() bool { return q.tryPush(pk) }, time.Second, time.Millisecond)
	require.Same(t, pk, <-result)
	require.Zero(t, q.len())
	require.Nil(t, q.buf)
}

func TestPacketQueueConcurrentProducers(t *testing.T) {
	const producers, perProducer = 8, 1000
	q := newPacketQueue(64)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan []uint16, 1)
	go func() {
		ids := make([]uint16, 0, producers*perProducer)
		for len(ids) < cap(ids) {
			pk, ok := q.pop(ctx.Done())
			if !ok {
				break
			}
			ids = append(ids, pk.PacketID)
		}
		result <- ids
	}()
	var wg sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				pk := &packets.Packet{PacketID: uint16(producer*perProducer + i)}
				for !q.tryPush(pk) {
					if ctx.Err() != nil {
						return
					}
					runtime.Gosched()
				}
			}
		}(producer)
	}
	wg.Wait()
	ids := <-result
	require.Len(t, ids, producers*perProducer)
	var next [producers]int
	for _, id := range ids {
		producer := int(id) / perProducer
		require.Equal(t, producer*perProducer+next[producer], int(id), "producer order/duplication")
		next[producer]++
	}
	for _, count := range next {
		require.Equal(t, perProducer, count)
	}
}

func TestClientStopReleasesQueueButKeepsQoSState(t *testing.T) {
	cl := newClient(nil, &ops{options: &Options{Capabilities: NewDefaultServerCapabilities()}})
	pk := &packets.Packet{PacketID: 7, Payload: make([]byte, 1024),
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}}
	cl.State.Inflight.SetOutbound(*pk)
	for i := 0; i < 100; i++ {
		require.True(t, cl.State.outbound.tryPush(pk))
		atomic.AddInt32(&cl.State.outboundQty, 1)
	}
	cl.Stop(nil)
	require.Nil(t, cl.State.outbound.buf)
	require.Zero(t, atomic.LoadInt32(&cl.State.outboundQty))
	_, exists := cl.State.Inflight.GetOutbound(pk.PacketID)
	require.True(t, exists, "transport queue cleanup must not erase recoverable QoS state")
}

type queueStalledConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *queueStalledConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

func TestClientSlowWriterPreserves8192PendingLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		qos  byte
	}{{"QoS0", 0}, {"QoS1", 1}, {"QoS2", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			testClientSlowWriterPendingLimit(t, tc.qos)
		})
	}
}

func testClientSlowWriterPendingLimit(t *testing.T, qos byte) {
	s := newServer()
	// Keep the independent inflight quota above the transport queue limit.
	s.Options.Capabilities.MaximumInflight = 8194
	serverConn, clientConn := net.Pipe()
	conn := &queueStalledConn{Conn: serverConn, entered: make(chan struct{})}
	cl := newClient(conn, &ops{info: new(system.Info), hooks: new(Hooks), log: logger,
		options: &Options{Capabilities: NewDefaultServerCapabilities(), ClientNetWriteBufferSize: 2048}})
	cl.ID = "slow-consumer"
	cl.State.Inflight.ResetSendQuota(8194)
	defer clientConn.Close()
	defer cl.Stop(nil)
	done := make(chan struct{})
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
		TopicName: "queue/test", Payload: []byte("payload")}
	sub := packets.Subscription{Filter: "queue/test", Qos: qos}
	// NextPacketID and WritePacket share the existing client mutex. For
	// QoS1/2 fill the queue before starting a socket writer, otherwise a
	// deliberately blocked WritePacket also blocks packet ID allocation.
	if qos == 0 {
		go func() { cl.WriteLoop(); close(done) }()
		_, err := s.publishToClient(cl, sub, pk)
		require.NoError(t, err)
		select {
		case <-conn.entered:
		case <-time.After(time.Second):
			t.Fatal("writer did not reach the blocked socket")
		}
	}
	for i := 0; i < 8192; i++ {
		_, err := s.publishToClient(cl, sub, pk)
		require.NoError(t, err, "queue slot %d", i)
	}
	require.Equal(t, 8192, cl.State.outbound.len())
	quotaBeforeReject := atomic.LoadInt32(&cl.State.Inflight.sendQuota)
	inflightBeforeReject := cl.State.Inflight.LenOutbound()
	_, err := s.publishToClient(cl, sub, pk)
	require.ErrorIs(t, err, packets.ErrPendingClientWritesExceeded)
	require.Equal(t, quotaBeforeReject, atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, inflightBeforeReject, cl.State.Inflight.LenOutbound())
	if qos > 0 {
		go func() { cl.WriteLoop(); close(done) }()
		select {
		case <-conn.entered:
		case <-time.After(time.Second):
			t.Fatal("writer did not reach the blocked socket")
		}
	}
	cl.Stop(nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopped socket writer did not exit")
	}
	require.Zero(t, atomic.LoadInt32(&cl.State.outboundQty))
}
