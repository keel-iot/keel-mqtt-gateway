// SPDX-License-Identifier: MIT

package mqtt

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryOfflineDeliveryPreservesQoSState(t *testing.T) {
	for _, protocol := range []byte{4, 5} {
		for _, qos := range []byte{1, 2} {
			for _, disconnected := range []string{"no-connection", "stopped"} {
				t.Run(fmt.Sprintf("mqtt%d/qos%d/%s", protocol, qos, disconnected), func(t *testing.T) {
					s := newServer()
					hook := new(qosResultHook)
					require.NoError(t, s.AddHook(hook, nil))
					cl, r, w := newTestClient()
					t.Cleanup(func() { cl.Stop(nil); _ = r.Close(); _ = w.Close() })
					cl.ID = "persistent-subscriber"
					cl.Properties.ProtocolVersion = protocol
					cl.Properties.Clean = false
					cl.Properties.Props.SessionExpiryInterval = 60
					s.Clients.Add(cl)
					s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "offline/topic", Qos: qos})
					if disconnected == "stopped" {
						cl.Stop(nil)
					} else {
						cl.Net.Conn = nil
					}
					quotaBefore := atomic.LoadInt32(&cl.State.Inflight.sendQuota)
					now := time.Now().Unix()
					s.publishToSubscribers(packets.Packet{
						FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
						TopicName:   "offline/topic", Payload: []byte("recover after reconnect"),
						Created: now, Properties: packets.Properties{MessageExpiryInterval: 30},
					})
					require.Equal(t, 1, cl.State.Inflight.LenOutbound())
					stored, ok := cl.State.Inflight.GetOutbound(1)
					require.True(t, ok)
					require.Equal(t, qos, stored.FixedHeader.Qos)
					require.Equal(t, []byte("recover after reconnect"), stored.Payload)
					require.Equal(t, now+30, stored.Expiry)
					require.Equal(t, quotaBefore-1, atomic.LoadInt32(&cl.State.Inflight.sendQuota))
					require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.Inflight))
					require.Equal(t, 1, hook.publishCalls)
					require.Zero(t, hook.droppedCalls)
					require.Zero(t, cl.State.outbound.len())
					require.Zero(t, atomic.LoadInt32(&cl.State.outboundQty))
				})
			}
		}
	}
}

type disconnectOnQosPersistHook struct{ qosResultHook }

func (h *disconnectOnQosPersistHook) OnQosPublishResult(cl *Client, pk packets.Packet, sent int64, resends int) error {
	cl.Stop(nil)
	return h.qosResultHook.OnQosPublishResult(cl, pk, sent, resends)
}

func TestDisconnectDuringPersistenceSeparatesOrdinaryAndSharedDelivery(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		for _, sharedAttempt := range []bool{false, true} {
			t.Run(fmt.Sprintf("qos%d/shared=%t", qos, sharedAttempt), func(t *testing.T) {
				s := newServer()
				hook := new(disconnectOnQosPersistHook)
				require.NoError(t, s.AddHook(hook, nil))
				cl, r, w := newTestClient()
				t.Cleanup(func() { cl.Stop(nil); _ = r.Close(); _ = w.Close() })
				cl.State.Inflight.SetOutbound(packets.Packet{
					PacketID: 99, TopicName: "previous/topic",
					FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
				})
				atomic.StoreInt64(&s.Info.Inflight, 1)
				quotaBefore := atomic.LoadInt32(&cl.State.Inflight.sendQuota)
				_, err := s.publishToClientMode(cl, packets.Subscription{Filter: "offline/topic", Qos: qos}, packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos}, TopicName: "offline/topic",
				}, sharedAttempt)
				require.ErrorIs(t, err, packets.CodeDisconnect)
				_, previousExists := cl.State.Inflight.GetOutbound(99)
				require.True(t, previousExists)
				wantInflight, wantDropped, wantQuota := 2, 0, quotaBefore-1
				if sharedAttempt {
					wantInflight, wantDropped, wantQuota = 1, 1, quotaBefore
				}
				require.Equal(t, wantInflight, cl.State.Inflight.LenOutbound())
				require.Equal(t, int64(wantInflight), atomic.LoadInt64(&s.Info.Inflight))
				require.Equal(t, wantQuota, atomic.LoadInt32(&cl.State.Inflight.sendQuota))
				require.Equal(t, 1, hook.publishCalls)
				require.Equal(t, wantDropped, hook.droppedCalls)
				require.Zero(t, cl.State.outbound.len())
			})
		}
	}
}
