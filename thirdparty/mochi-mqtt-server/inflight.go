// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/mochi-mqtt/server/v2/packets"
)

// Inflight is a map of InflightMessage keyed on packet id.
type Inflight struct {
	sync.RWMutex
	internal            map[uint16]packets.Packet // outbound server-to-client packets
	receive             map[uint16]packets.Packet // inbound client-to-server QoS state
	receiveQuota        int32                     // remaining inbound qos quota for flow control
	sendQuota           int32                     // remaining outbound qos quota for flow control
	maximumReceiveQuota int32                     // maximum allowed receive quota
	maximumSendQuota    int32                     // maximum allowed send quota
}

// NewInflights returns a new instance of an Inflight packets map.
func NewInflights() *Inflight {
	return &Inflight{
		internal: map[uint16]packets.Packet{},
		receive:  map[uint16]packets.Packet{},
	}
}

func isInboundPacket(p packets.Packet) bool {
	switch p.FixedHeader.Type {
	case packets.Puback, packets.Pubrec, packets.Pubcomp:
		return true
	default:
		return false
	}
}

// Set adds or updates an inflight packet by packet id. It is retained for
// compatibility with callers which do not need to name the packet direction.
// Protocol code should use SetInbound or SetOutbound explicitly.
func (i *Inflight) Set(m packets.Packet) bool {
	if isInboundPacket(m) {
		return i.SetInbound(m)
	}
	return i.SetOutbound(m)
}

// SetInbound stores client-to-server QoS state.
func (i *Inflight) SetInbound(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.receive[m.PacketID]
	i.receive[m.PacketID] = m
	return !ok
}

// SetOutbound stores server-to-client QoS state.
func (i *Inflight) SetOutbound(m packets.Packet) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[m.PacketID]
	i.internal[m.PacketID] = m
	return !ok
}

// Get returns an inflight packet by packet id. It is retained for compatibility
// and prefers the outbound domain when both domains contain the same number.
func (i *Inflight) Get(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	if m, ok := i.internal[id]; ok {
		return m, true
	}
	if m, ok := i.receive[id]; ok {
		return m, true
	}

	return packets.Packet{}, false
}

// GetInbound returns client-to-server QoS state.
func (i *Inflight) GetInbound(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()
	m, ok := i.receive[id]
	return m, ok
}

// GetOutbound returns server-to-client QoS state.
func (i *Inflight) GetOutbound(id uint16) (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()
	m, ok := i.internal[id]
	return m, ok
}

// Len returns the size of the inflight messages map.
func (i *Inflight) Len() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal) + len(i.receive)
}

// LenInbound returns the number of client-to-server QoS states.
func (i *Inflight) LenInbound() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.receive)
}

// LenOutbound returns the number of server-to-client QoS states.
func (i *Inflight) LenOutbound() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.internal)
}

// Clone returns a new instance of Inflight with the same message data.
// This is used when transferring inflights from a taken-over session.
func (i *Inflight) Clone() *Inflight {
	c := NewInflights()
	i.RLock()
	defer i.RUnlock()
	for k, v := range i.internal {
		c.internal[k] = v
	}
	for k, v := range i.receive {
		c.receive[k] = v
	}
	return c
}

// GetAll returns all the inflight messages.
func (i *Inflight) GetAll(immediate bool) []packets.Packet {
	i.RLock()
	defer i.RUnlock()

	m := make([]packets.Packet, 0, len(i.internal)+len(i.receive))
	for _, v := range i.internal {
		if !immediate || (immediate && v.Expiry < 0) {
			m = append(m, v)
		}
	}
	for _, v := range i.receive {
		if !immediate || (immediate && v.Expiry < 0) {
			m = append(m, v)
		}
	}

	sort.Slice(m, func(i, j int) bool {
		if m[i].Created != m[j].Created {
			return m[i].Created < m[j].Created
		}
		return m[i].PacketID < m[j].PacketID
	})

	return m
}

// NextImmediate returns the next inflight packet which is indicated to be sent immediately.
// This typically occurs when the quota has been exhausted, and we need to wait until new quota
// is free to continue sending.
func (i *Inflight) NextImmediate() (packets.Packet, bool) {
	i.RLock()
	defer i.RUnlock()

	m := make([]packets.Packet, 0, len(i.internal))
	for _, v := range i.internal {
		if v.Expiry < 0 {
			m = append(m, v)
		}
	}
	sort.Slice(m, func(a, b int) bool {
		if m[a].Created != m[b].Created {
			return m[a].Created < m[b].Created
		}
		return m[a].PacketID < m[b].PacketID
	})
	if len(m) > 0 {
		return m[0], true
	}

	return packets.Packet{}, false
}

// Delete removes an in-flight message from the map. Returns true if the message existed.
func (i *Inflight) Delete(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	_, out := i.internal[id]
	_, in := i.receive[id]
	delete(i.internal, id)
	delete(i.receive, id)
	return out || in
}

// DeleteInbound removes client-to-server QoS state.
func (i *Inflight) DeleteInbound(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.receive[id]
	delete(i.receive, id)
	return ok
}

// DeleteOutbound removes server-to-client QoS state.
func (i *Inflight) DeleteOutbound(id uint16) bool {
	i.Lock()
	defer i.Unlock()

	_, ok := i.internal[id]
	delete(i.internal, id)
	return ok
}

// DeletePacket removes the domain represented by the stored packet.
func (i *Inflight) DeletePacket(m packets.Packet) bool {
	if isInboundPacket(m) {
		return i.DeleteInbound(m.PacketID)
	}
	return i.DeleteOutbound(m.PacketID)
}

// TakeRecieveQuota reduces the receive quota by 1.
func (i *Inflight) DecreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) > 0 {
		atomic.AddInt32(&i.receiveQuota, -1)
	}
}

// TakeRecieveQuota increases the receive quota by 1.
func (i *Inflight) IncreaseReceiveQuota() {
	if atomic.LoadInt32(&i.receiveQuota) < atomic.LoadInt32(&i.maximumReceiveQuota) {
		atomic.AddInt32(&i.receiveQuota, 1)
	}
}

// ResetReceiveQuota resets the receive quota to the maximum allowed value.
func (i *Inflight) ResetReceiveQuota(n int32) {
	atomic.StoreInt32(&i.receiveQuota, n)
	atomic.StoreInt32(&i.maximumReceiveQuota, n)
}

// DecreaseSendQuota reduces the send quota by 1.
func (i *Inflight) DecreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) > 0 {
		atomic.AddInt32(&i.sendQuota, -1)
	}
}

// IncreaseSendQuota increases the send quota by 1.
func (i *Inflight) IncreaseSendQuota() {
	if atomic.LoadInt32(&i.sendQuota) < atomic.LoadInt32(&i.maximumSendQuota) {
		atomic.AddInt32(&i.sendQuota, 1)
	}
}

// ResetSendQuota resets the send quota to the maximum allowed value.
func (i *Inflight) ResetSendQuota(n int32) {
	atomic.StoreInt32(&i.sendQuota, n)
	atomic.StoreInt32(&i.maximumSendQuota, n)
}
