// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sync/atomic"
	"testing"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestInflightSet(t *testing.T) {
	cl, _, _ := newTestClient()

	r := cl.State.Inflight.Set(packets.Packet{PacketID: 1})
	require.True(t, r)
	require.NotNil(t, cl.State.Inflight.internal[1])
	require.NotEqual(t, 0, cl.State.Inflight.internal[1].PacketID)

	r = cl.State.Inflight.Set(packets.Packet{PacketID: 1})
	require.False(t, r)
}

// TestInflightOppositeDirectionsMaySharePacketID is the regression reproducer
// for upstream issue #545. MQTT permits the client and server to use the same
// numeric Packet Identifier for their independent flows.
func TestInflightOppositeDirectionsMaySharePacketID(t *testing.T) {
	i := NewInflights()
	outbound := packets.Packet{
		PacketID:    7,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		Payload:     []byte("server-to-client"),
	}
	inbound := packets.Packet{
		PacketID:    7,
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec, Qos: 0},
	}

	i.SetOutbound(outbound)
	i.SetInbound(inbound)

	gotOutbound, outboundOK := i.GetOutbound(7)
	gotInbound, inboundOK := i.GetInbound(7)
	require.True(t, outboundOK)
	require.True(t, inboundOK)
	require.Equal(t, outbound, gotOutbound)
	require.Equal(t, inbound, gotInbound)
}

func TestInflightGet(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})

	msg, ok := cl.State.Inflight.Get(2)
	require.True(t, ok)
	require.NotEqual(t, 0, msg.PacketID)
}

func TestInflightGetAllAndImmediate(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Created: 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: 2})
	cl.State.Inflight.Set(packets.Packet{PacketID: 3, Created: 3, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 4, Created: 4, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 5, Created: 5})

	require.Equal(t, []packets.Packet{
		{PacketID: 1, Created: 1},
		{PacketID: 2, Created: 2},
		{PacketID: 3, Created: 3, Expiry: -1},
		{PacketID: 4, Created: 4, Expiry: -1},
		{PacketID: 5, Created: 5},
	}, cl.State.Inflight.GetAll(false))

	require.Equal(t, []packets.Packet{
		{PacketID: 3, Created: 3, Expiry: -1},
		{PacketID: 4, Created: 4, Expiry: -1},
	}, cl.State.Inflight.GetAll(true))
}

func TestInflightGetAllOrdersPacketsCreatedInTheSameSecond(t *testing.T) {
	const sameSecond int64 = 1700000000
	cl, _, _ := newTestClient()
	for _, id := range []uint16{5, 3, 8, 1, 7, 2, 6, 4} {
		cl.State.Inflight.Set(packets.Packet{PacketID: id, Created: sameSecond})
	}

	got := cl.State.Inflight.GetAll(false)
	order := make([]uint16, 0, len(got))
	for _, pk := range got {
		order = append(order, pk.PacketID)
	}
	require.Equal(t, []uint16{1, 2, 3, 4, 5, 6, 7, 8}, order)
}

func TestInflightGetAllDoesNotTruncateCreated(t *testing.T) {
	const boundary int64 = 1786970112
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 9, Created: boundary - 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: boundary + 1})

	got := cl.State.Inflight.GetAll(false)
	require.Len(t, got, 2)
	require.Equal(t, uint16(9), got[0].PacketID)
}

func TestInflightLen(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})
	require.Equal(t, 1, cl.State.Inflight.Len())
}

func TestInflightClone(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2})
	require.Equal(t, 1, cl.State.Inflight.Len())

	cloned := cl.State.Inflight.Clone()
	require.NotNil(t, cloned)
	require.NotSame(t, cloned, cl.State.Inflight)
}

func TestInflightDelete(t *testing.T) {
	cl, _, _ := newTestClient()

	cl.State.Inflight.Set(packets.Packet{PacketID: 3})
	require.NotNil(t, cl.State.Inflight.internal[3])

	r := cl.State.Inflight.Delete(3)
	require.True(t, r)
	require.Equal(t, uint16(0), cl.State.Inflight.internal[3].PacketID)

	_, ok := cl.State.Inflight.Get(3)
	require.False(t, ok)

	r = cl.State.Inflight.Delete(3)
	require.False(t, r)
}

func TestResetReceiveQuota(t *testing.T) {
	i := NewInflights()
	require.Equal(t, int32(0), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.receiveQuota))
	i.ResetReceiveQuota(6)
	require.Equal(t, int32(6), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(6), atomic.LoadInt32(&i.receiveQuota))
}

func TestReceiveQuota(t *testing.T) {
	i := NewInflights()
	i.receiveQuota = 4
	i.maximumReceiveQuota = 5
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(4), atomic.LoadInt32(&i.receiveQuota))

	// Return 1
	i.IncreaseReceiveQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.receiveQuota))

	// Try to go over max limit
	i.IncreaseReceiveQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.receiveQuota))

	// Reset to max 1
	i.ResetReceiveQuota(1)
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(1), atomic.LoadInt32(&i.receiveQuota))

	// Take 1
	i.DecreaseReceiveQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.receiveQuota))

	// Try to go below zero
	i.DecreaseReceiveQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumReceiveQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.receiveQuota))
}

func TestResetSendQuota(t *testing.T) {
	i := NewInflights()
	require.Equal(t, int32(0), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.sendQuota))
	i.ResetSendQuota(6)
	require.Equal(t, int32(6), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(6), atomic.LoadInt32(&i.sendQuota))
}

func TestSendQuota(t *testing.T) {
	i := NewInflights()
	i.sendQuota = 4
	i.maximumSendQuota = 5
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(4), atomic.LoadInt32(&i.sendQuota))

	// Return 1
	i.IncreaseSendQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.sendQuota))

	// Try to go over max limit
	i.IncreaseSendQuota()
	require.Equal(t, int32(5), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(5), atomic.LoadInt32(&i.sendQuota))

	// Reset to max 1
	i.ResetSendQuota(1)
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(1), atomic.LoadInt32(&i.sendQuota))

	// Take 1
	i.DecreaseSendQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.sendQuota))

	// Try to go below zero
	i.DecreaseSendQuota()
	require.Equal(t, int32(1), atomic.LoadInt32(&i.maximumSendQuota))
	require.Equal(t, int32(0), atomic.LoadInt32(&i.sendQuota))
}

func TestNextImmediate(t *testing.T) {
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Created: 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: 2})
	cl.State.Inflight.Set(packets.Packet{PacketID: 3, Created: 3, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 4, Created: 4, Expiry: -1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 5, Created: 5})

	pk, ok := cl.State.Inflight.NextImmediate()
	require.True(t, ok)
	require.Equal(t, packets.Packet{PacketID: 3, Created: 3, Expiry: -1}, pk)

	r := cl.State.Inflight.Delete(3)
	require.True(t, r)

	pk, ok = cl.State.Inflight.NextImmediate()
	require.True(t, ok)
	require.Equal(t, packets.Packet{PacketID: 4, Created: 4, Expiry: -1}, pk)

	r = cl.State.Inflight.Delete(4)
	require.True(t, r)

	_, ok = cl.State.Inflight.NextImmediate()
	require.False(t, ok)
}
