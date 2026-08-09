package broker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/db"
)

// wsRawConn adapts a *websocket.Conn to a plain byte stream carrying raw
// MQTT packets — mirrors mochi-mqtt's own internal wsConn exactly (see
// the vendored module's listeners/websocket.go: Read via NextReader
// expecting a BinaryMessage, Write via WriteMessage(BinaryMessage, ...)).
// Needed for the same reason receive_maximum_test.go's readRawPacket
// exists: exact control over the byte stream to prove the transport
// carries a real MQTT session, not a full client library.
type wsRawConn struct {
	c *websocket.Conn
	r io.Reader
}

func (w *wsRawConn) Read(p []byte) (int, error) {
	if w.r == nil {
		op, r, err := w.c.NextReader()
		if err != nil {
			return 0, err
		}
		if op != websocket.BinaryMessage {
			return 0, fmt.Errorf("unexpected websocket message type %d, want BinaryMessage", op)
		}
		w.r = r
	}
	n, err := w.r.Read(p)
	if err != nil {
		w.r = nil
		if err == io.EOF {
			err = nil
		}
	}
	return n, err
}

func (w *wsRawConn) Write(p []byte) (int, error) {
	if err := w.c.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// TestWebSocket_EndToEnd_ConnectPublishSubscribe proves the WebSocket
// listener carries a real MQTT session, not just that it accepts a TCP
// connection. Deliberately doesn't re-verify general pub/sub correctness
// (already covered via the plain TCP listener elsewhere) — the property
// specific to this transport is that framing survives round-trip in both
// directions: CONNECT/PUBLISH client->server, and a delivered message
// server->client (the write path mochi-mqtt's wsConn.Write exercises,
// most likely to break if the framing were wrong).
//
// Requires TEST_DATABASE_URL — see receive_maximum_test.go's doc for why.
func TestWebSocket_EndToEnd_ConnectPublishSubscribe(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping live-Postgres integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := db.Migrate(ctx, pool, log); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantCache := auth.NewTenantConfigCache(pool, time.Minute)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTWSPort:        port,
		TenantConfigCache: tenantCache,
		DefaultTenantID:   uuid.New().String(),
	}, allowAllAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond) // let the listener actually start accepting

	dialer := websocket.Dialer{
		Subprotocols:     []string{"mqtt"}, // MQTT-over-WebSocket's registered subprotocol name
		HandshakeTimeout: 5 * time.Second,
	}
	wsConn, _, err := dialer.Dial(fmt.Sprintf("ws://localhost:%d/", port), http.Header{})
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer wsConn.Close()

	raw := &wsRawConn{c: wsConn}
	r := bufio.NewReader(raw)

	// CONNECT
	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "ws-test-client",
			Clean:            true,
			Keepalive:        60,
		},
	}
	var connectBuf bytes.Buffer
	if err := connectPk.ConnectEncode(&connectBuf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	if _, err := raw.Write(connectBuf.Bytes()); err != nil {
		t.Fatalf("write connect over websocket: %v", err)
	}

	connack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if connack.ReasonCode != 0 {
		t.Fatalf("connect rejected, reasonCode=%d", connack.ReasonCode)
	}

	// SUBSCRIBE to "command/<deviceID>" — production ACL's non-write
	// branch (internal/broker/hooks.go's OnACLCheck) only allows a device
	// to subscribe to its own command topic, never an arbitrary one (and
	// never the same shape it's allowed to publish to — "telemetry" is
	// publish-only, "command/<id>" is subscribe-only, matching a real
	// device's telemetry-out/commands-in split). allowAllTestDeviceID is
	// fixed specifically so this topic string is predictable here.
	commandTopic := "command/" + allowAllTestDeviceID.String()
	subPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 5,
		PacketID:        1,
		Filters:         packets.Subscriptions{{Filter: commandTopic, Qos: 0}},
	}
	var subBuf bytes.Buffer
	if err := subPk.SubscribeEncode(&subBuf); err != nil {
		t.Fatalf("encode subscribe: %v", err)
	}
	if _, err := raw.Write(subBuf.Bytes()); err != nil {
		t.Fatalf("write subscribe over websocket: %v", err)
	}

	suback, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	if suback.FixedHeader.Type != packets.Suback {
		t.Fatalf("expected SUBACK, got packet type %d", suback.FixedHeader.Type)
	}
	if len(suback.ReasonCodes) != 1 || suback.ReasonCodes[0] >= 0x80 {
		t.Fatalf("subscribe denied, reasonCodes=%v", suback.ReasonCodes)
	}

	// Deliver a message the way the commander (platform->device push)
	// actually does in production: server.Publish via the InlineClient,
	// not a client-side PUBLISH — no client is allowed to publish to
	// "command/*" under production ACL (it's subscribe-only, by design;
	// commands come from the platform, not from another device). This is
	// the write path this test cares about: can the WebSocket connection
	// receive a server-pushed message, not just send one.
	if err := wsConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if err := server.Publish(commandTopic, []byte("hello over websocket"), false, 0); err != nil {
		t.Fatalf("server-side publish: %v", err)
	}

	delivered, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read delivered publish: %v", err)
	}
	if delivered.FixedHeader.Type != packets.Publish {
		t.Fatalf("expected the published message delivered back, got packet type %d", delivered.FixedHeader.Type)
	}
	if delivered.TopicName != commandTopic || string(delivered.Payload) != "hello over websocket" {
		t.Fatalf("delivered publish mismatch: topic=%q payload=%q", delivered.TopicName, delivered.Payload)
	}
}

// TestNew_WSSRequiresTLSCertDir mirrors the existing MQTTTLSPort
// validation — MQTTWSSPort shares the same TLSCertDir/CertReloader, so
// it must fail the same way when misconfigured. Fast, no TEST_DATABASE_URL
// needed: this is a construction-time config error, returned before any
// auth-dependent code path runs.
func TestNew_WSSRequiresTLSCertDir(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, _, err := New(Config{MQTTPort: 0, MQTTWSSPort: 8884}, noopAuthProvider{}, log)
	if err == nil {
		t.Fatal("expected an error when MQTTWSSPort is set without TLSCertDir")
	}
}
