package broker

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mochi-mqtt/server/v2/packets"
)

// dialAndConnectV311WithWill connects an MQTT 3.1.1 client with a Will
// message configured, returning the raw connection and its read buffer.
// Does not consume the CONNACK — callers that need to inspect it should
// read it themselves; this only encodes/sends the CONNECT.
func dialAndConnectV311WithWill(t *testing.T, port int, clientID, willTopic, willPayload string, willQos byte) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 4,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: clientID,
			Clean:            true,
			Keepalive:        60,
			WillFlag:         true,
			WillTopic:        willTopic,
			WillPayload:      []byte(willPayload),
			WillQos:          willQos,
		},
	}
	var buf bytes.Buffer
	if err := connectPk.ConnectEncode(&buf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, body := readRawAckBody(t, bufio.NewReader(conn))
	if len(body) < 2 || body[1] != 0 {
		t.Fatalf("connect rejected or malformed connack: %v", body)
	}
	return conn
}

// TestWillMessage_MQTT311_UngracefulDisconnectTriggersWill proves MQTT
// 3.1.1 Will delivery works end to end on this production path — not
// inferred from the MQTT5 Paho conformance result (test_will_message),
// which never exercises a v3.1.1 client at all (Paho's own
// will_message_test in client_test.py isn't "test_"-prefixed, so
// unittest's default discovery never runs it — confirmed by source
// read, not assumed). mochi-mqtt's own sendLWT (server.go) has zero
// protocol-version branching in the delivery path itself, but that's a
// source-level observation, not evidence; this closes the gap with a
// real client.
//
// Requires TEST_DATABASE_URL — see receive_maximum_test.go's doc for why.
func TestWillMessage_MQTT311_UngracefulDisconnectTriggersWill(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:          port,
		TenantConfigCache: tenantCache,
		DefaultTenantID:   uuid.New().String(),
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	willTopic := "command/" + allowAllTestDeviceID.String()

	willConn := dialAndConnectV311WithWill(t, port, "will-311-publisher", willTopic, "goodbye", 0)
	defer willConn.Close()

	subConn, subR := dialAndConnectV311(t, port, "will-311-subscriber")
	defer subConn.Close()

	subPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 4,
		PacketID:        1,
		Filters:         packets.Subscriptions{{Filter: willTopic, Qos: 0}},
	}
	var subBuf bytes.Buffer
	if err := subPk.SubscribeEncode(&subBuf); err != nil {
		t.Fatalf("encode subscribe: %v", err)
	}
	if _, err := subConn.Write(subBuf.Bytes()); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	if err := subConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	subackType, subackBody := readRawAckBody(t, subR)
	if subackType != packets.Suback || len(subackBody) < 3 || subackBody[2] >= 0x80 {
		t.Fatalf("subscribe denied or malformed suback: type=%d body=%v", subackType, subackBody)
	}

	// Ungraceful: close the raw TCP connection without sending DISCONNECT.
	// This is exactly what MQTT-3.1.2-8 requires to trigger the Will.
	willConn.Close()

	if err := subConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	pkType, body := readRawAckBody(t, subR)
	if pkType != packets.Publish {
		t.Fatalf("expected the Will to be delivered as a PUBLISH, got packet type %d", pkType)
	}
	if !bytes.Contains(body, []byte(willTopic)) {
		t.Fatalf("expected will delivered on %q, got %v", willTopic, body)
	}
	if !bytes.Contains(body, []byte("goodbye")) {
		t.Fatalf("expected will payload \"goodbye\" in delivered publish, got %v", body)
	}
}

// TestWillMessage_MQTT311_GracefulDisconnectDoesNotSendWill proves a
// proper DISCONNECT packet suppresses the Will (MQTT-3.1.2-8's other
// half: "unless... the Client sends a DISCONNECT Packet"), on the same
// production path as the ungraceful case above.
//
// Requires TEST_DATABASE_URL — see receive_maximum_test.go's doc for why.
func TestWillMessage_MQTT311_GracefulDisconnectDoesNotSendWill(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:          port,
		TenantConfigCache: tenantCache,
		DefaultTenantID:   uuid.New().String(),
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	willTopic := "command/" + allowAllTestDeviceID.String()

	willConn := dialAndConnectV311WithWill(t, port, "will-311-graceful-publisher", willTopic, "should-not-arrive", 0)
	defer willConn.Close()

	subConn, subR := dialAndConnectV311(t, port, "will-311-graceful-subscriber")
	defer subConn.Close()

	subPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 4,
		PacketID:        1,
		Filters:         packets.Subscriptions{{Filter: willTopic, Qos: 0}},
	}
	var subBuf bytes.Buffer
	if err := subPk.SubscribeEncode(&subBuf); err != nil {
		t.Fatalf("encode subscribe: %v", err)
	}
	if _, err := subConn.Write(subBuf.Bytes()); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	if err := subConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	subackType, subackBody := readRawAckBody(t, subR)
	if subackType != packets.Suback || len(subackBody) < 3 || subackBody[2] >= 0x80 {
		t.Fatalf("subscribe denied or malformed suback: type=%d body=%v", subackType, subackBody)
	}

	// Graceful: a real DISCONNECT packet (type 14, no flags, remaining
	// length 0 — identical encoding in v3.1.1 and v5) before closing.
	if _, err := willConn.Write([]byte{0xE0, 0x00}); err != nil {
		t.Fatalf("write disconnect: %v", err)
	}
	willConn.Close()

	if err := subConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 1)
	if n, err := subConn.Read(buf); err == nil {
		t.Fatalf("expected no Will delivery after a graceful DISCONNECT, got %d bytes", n)
	}
}
