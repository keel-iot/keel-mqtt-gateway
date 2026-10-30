// SPDX-License-Identifier: MIT
// Regression coverage for selected changes from mochi-mqtt/server PR #543.
// Provenance and the local adaptations are recorded in PATCH.md.

package mqtt

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"
)

func captureBackportWrite(t *testing.T, cl *Client, r net.Conn, action func() error) ([]byte, error) {
	t.Helper()
	t.Cleanup(func() { cl.Stop(nil); _ = r.Close() })
	require.NoError(t, r.SetReadDeadline(time.Now().Add(3*time.Second)))
	type readResult struct {
		wire []byte
		err  error
	}
	read := make(chan readResult, 1)
	go func() { wire, err := io.ReadAll(r); read <- readResult{wire, err} }()
	var err error
	require.NotPanics(t, func() { err = action() })
	cl.Stop(nil)
	got := <-read
	require.NoError(t, got.err)
	return got.wire, err
}

type wrappedBackportRefusalHook struct{ HookBase }

func (*wrappedBackportRefusalHook) ID() string           { return "wrapped-backport-refusal" }
func (*wrappedBackportRefusalHook) Provides(b byte) bool { return b == OnPublish }
func (*wrappedBackportRefusalHook) OnPublish(_ *Client, pk packets.Packet) (packets.Packet, error) {
	return pk, fmt.Errorf("policy refusal: %w", packets.ErrQuotaExceeded)
}

func TestUpstreamWrappedRefusalPreservesQoSAck(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			s := newServer()
			require.NoError(t, s.AddHook(new(wrappedBackportRefusalHook), nil))
			cl, r, _ := newTestClient()
			cl.Properties.ProtocolVersion = 5
			wire, err := captureBackportWrite(t, cl, r, func() error {
				return s.processPublish(cl, packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
					PacketID:    7, TopicName: "allowed/topic",
				})
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(wire), 5)
			want := packets.Puback
			if qos == 2 {
				want = packets.Pubrec
			}
			require.Equal(t, want, wire[0]>>4)
			require.Equal(t, packets.ErrQuotaExceeded.Code, wire[4])
			require.Zero(t, cl.State.Inflight.LenInbound())
		})
	}
}

func TestUpstreamQoS1SuccessReasonIsValidPubackCode(t *testing.T) {
	s := newServer()
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	wire, err := captureBackportWrite(t, cl, r, func() error {
		return s.processPublish(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 7, TopicName: "allowed/topic",
			Properties: packets.Properties{User: []packets.UserProperty{{Key: "key", Val: "value"}}},
		})
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wire), 5)
	require.Equal(t, packets.Puback, wire[0]>>4)
	require.Equal(t, packets.CodeSuccess.Code, wire[4])
}

func TestUpstreamProblemInfoDoesNotStripPublishProperties(t *testing.T) {
	for _, kind := range []byte{packets.Publish, packets.Puback} {
		for _, requested := range []byte{0, 1} {
			t.Run(fmt.Sprintf("packet%d/request%d", kind, requested), func(t *testing.T) {
				cl, r, _ := newTestClient()
				cl.Properties.ProtocolVersion = 5
				cl.Properties.Props.RequestProblemInfoFlag = true
				cl.Properties.Props.RequestProblemInfo = requested
				wire, err := captureBackportWrite(t, cl, r, func() error {
					return cl.WritePacket(packets.Packet{
						FixedHeader: packets.FixedHeader{Type: kind}, PacketID: 7, TopicName: "allowed/topic",
						Properties: packets.Properties{User: []packets.UserProperty{{Key: "application-property", Val: "value"}}},
					})
				})
				require.NoError(t, err)
				require.Equal(t, kind == packets.Publish || requested == 1, bytes.Contains(wire, []byte("application-property")))
			})
		}
	}
}

func TestUpstreamDisconnectRespectsProtocolAndPassiveMode(t *testing.T) {
	for _, protocol := range []byte{3, 4, 5} {
		for _, passive := range []bool{false, true} {
			t.Run(fmt.Sprintf("mqtt%d/passive%t", protocol, passive), func(t *testing.T) {
				s := newServer()
				s.Options.Capabilities.Compatibilities.PassiveClientDisconnect = passive
				cl, r, _ := newTestClient()
				cl.Properties.ProtocolVersion = protocol
				var closed bool
				wire, _ := captureBackportWrite(t, cl, r, func() error {
					err := s.DisconnectClient(cl, packets.ErrNotAuthorized)
					closed = cl.Closed()
					return err
				})
				require.Equal(t, protocol < 5 || !passive, closed)
				if protocol < 5 {
					require.Empty(t, wire)
				} else {
					require.NotEmpty(t, wire)
					require.Equal(t, packets.Disconnect, wire[0]>>4)
				}
			})
		}
	}
}

func TestUpstreamUnregisteredTopicAliasIsRefused(t *testing.T) {
	for _, qos := range []byte{0, 1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			s := newServer()
			cl, r, _ := newTestClient()
			cl.Properties.ProtocolVersion = 5
			wire, err := captureBackportWrite(t, cl, r, func() error {
				return s.processPublish(cl, packets.Packet{
					FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos}, PacketID: 7,
					Properties: packets.Properties{TopicAliasFlag: true, TopicAlias: 1},
				})
			})
			require.ErrorIs(t, err, packets.ErrProtocolViolationNoTopic)
			require.NotEmpty(t, wire)
			require.Equal(t, packets.Disconnect, wire[0]>>4)
			require.NotContains(t, cl.State.TopicAliases.Inbound.internal, uint16(1))
		})
	}
}

// Adapted from ChristophNetsch's regression in upstream PR #539.
func TestUpstreamRetainMessageWhileScanningDoesNotRace(t *testing.T) {
	index := NewTopicsIndex()
	for i := 0; i < 8; i++ {
		index.RetainMessage(packets.Packet{FixedHeader: packets.FixedHeader{Retain: true}, TopicName: fmt.Sprintf("a/b/seed-%d", i), Payload: []byte("x")})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			index.RetainMessage(packets.Packet{FixedHeader: packets.FixedHeader{Retain: true}, TopicName: fmt.Sprintf("a/b/c-%d", i%16), Payload: []byte("hello")})
			index.RetainMessage(packets.Packet{FixedHeader: packets.FixedHeader{Retain: true}, TopicName: fmt.Sprintf("a/b/c-%d", i%16)})
		}
	}()
	for i := 0; i < 2000; i++ {
		_ = index.Messages("a/#")
		_ = index.Messages("a/b/+")
		_ = index.Messages("a/b/c-1")
	}
	<-done
}

type statelessBackportHook struct {
	HookBase
	publishes   int
	completions []packets.Packet
}

func (*statelessBackportHook) ID() string           { return "stateless-backport" }
func (*statelessBackportHook) Provides(b byte) bool { return b == OnQosPublish || b == OnQosComplete }
func (h *statelessBackportHook) OnQosPublish(_ *Client, _ packets.Packet, _ int64, _ int) {
	h.publishes++
}
func (h *statelessBackportHook) OnQosComplete(_ *Client, pk packets.Packet) {
	h.completions = append(h.completions, pk)
}

func TestUpstreamInboundQoS1DoesNotPersistStatelessAck(t *testing.T) {
	s := newServer()
	hook := new(statelessBackportHook)
	require.NoError(t, s.AddHook(hook, nil))
	cl, r, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	outbound := packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, Payload: []byte("outbound")}
	cl.State.Inflight.SetOutbound(outbound)
	atomic.StoreInt64(&s.Info.Inflight, 1)
	quota := atomic.LoadInt32(&cl.State.Inflight.receiveQuota)
	_, err := captureBackportWrite(t, cl, r, func() error {
		return s.processPublish(cl, packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, TopicName: "inbound/topic"})
	})
	require.NoError(t, err)
	require.Zero(t, hook.publishes, "a PUBACK has no inflight transaction to persist")
	require.Len(t, hook.completions, 1, "keep the completion callback contract")
	require.True(t, hook.completions[0].InboundFlow)
	got, exists := cl.State.Inflight.GetOutbound(7)
	require.True(t, exists)
	require.Equal(t, outbound, got)
	require.Zero(t, cl.State.Inflight.LenInbound())
	require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.Inflight))
	require.Equal(t, quota, atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}

func TestUpstreamZeroLimitsAreRefusedOnWire(t *testing.T) {
	for _, properties := range [][]byte{{33, 0, 0}, {39, 0, 0, 0, 0}} {
		t.Run(fmt.Sprint(properties[0]), func(t *testing.T) {
			s := newServer()
			server, client := net.Pipe()
			t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
			require.NoError(t, client.SetDeadline(time.Now().Add(3*time.Second)))
			body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 60, byte(len(properties))}
			body = append(body, properties...)
			body = append(body, 0, 1, 'x')
			frame := append([]byte{packets.Connect << 4, byte(len(body))}, body...)
			result := make(chan error, 1)
			go func() {
				err := s.EstablishConnection("tcp", server)
				_ = server.Close()
				result <- err
			}()
			_, err := client.Write(frame)
			require.NoError(t, err)
			wire, err := io.ReadAll(client)
			require.NoError(t, err)
			require.Error(t, <-result)
			require.GreaterOrEqual(t, len(wire), 4)
			require.Equal(t, packets.Connack, wire[0]>>4)
			require.Equal(t, byte(0x82), wire[3])
		})
	}
}
