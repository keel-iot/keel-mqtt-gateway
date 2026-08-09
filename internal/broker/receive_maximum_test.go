package broker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/db"
)

// allowAllTestDeviceID is fixed (not a fresh uuid.New() per call) so
// tests can predict ACL-relevant topic shapes ahead of time — e.g.
// isAllowedPublish's non-write branch only allows subscribing to
// "command/<deviceID>", which websocket_test.go needs to know in
// advance to construct that topic string.
var allowAllTestDeviceID = uuid.MustParse("00000000-0000-0000-0000-0000000000a1")

// allowAllAuthProvider accepts any credential with a fixed identity — this
// test cares about MQTT5 wire behavior, not auth policy, so
// authentication itself is deliberately trivial. Distinct from
// internal/conformance's AuthProvider on purpose: internal/broker's own
// tests must not depend on the conformance package (see
// docs/alternatives-and-future-work.md's "conformance-only vs production"
// split — this test protects PRODUCTION protocol behavior). Shared with
// max_keepalive_integration_test.go and websocket_test.go in this same
// package.
type allowAllAuthProvider struct{}

func (allowAllAuthProvider) ValidatePassword(context.Context, string, string) (*auth.DeviceInfo, error) {
	return &auth.DeviceInfo{ID: allowAllTestDeviceID, TenantID: uuid.New(), TenantSlug: "flow-control-test"}, nil
}

func (allowAllAuthProvider) LookupByCN(context.Context, string, string) (*auth.DeviceInfo, error) {
	return &auth.DeviceInfo{ID: allowAllTestDeviceID, TenantID: uuid.New(), TenantSlug: "flow-control-test"}, nil
}

func (allowAllAuthProvider) UpdateLastSeen(context.Context, uuid.UUID) {}

// readRawPacket reads one MQTT5 packet directly off the wire using
// mochi-mqtt's own exported packet codec (packets.DecodeLength +
// the per-type *Decode methods) — deliberately not a full client library:
// this test needs exact control over read/write ordering to prove the
// DISCONNECT-before-close invariant, which a higher-level client (that
// reacts to PUBREC by immediately writing a PUBREL, like
// paho.mqtt.testing's own client — see
// test/conformance/evidence/test_flow_control2.md for the root-cause)
// would race against. Shared with max_keepalive_integration_test.go.
func readRawPacket(r *bufio.Reader) (packets.Packet, error) {
	var pk packets.Packet
	hb, err := r.ReadByte()
	if err != nil {
		return pk, err
	}
	if err := pk.FixedHeader.Decode(hb); err != nil {
		return pk, err
	}
	remaining, _, err := packets.DecodeLength(r)
	if err != nil {
		return pk, err
	}
	pk.FixedHeader.Remaining = remaining
	pk.ProtocolVersion = 5
	buf := make([]byte, remaining)
	if _, err := io.ReadFull(r, buf); err != nil {
		return pk, err
	}
	switch pk.FixedHeader.Type {
	case packets.Connack:
		err = pk.ConnackDecode(buf)
	case packets.Suback:
		err = pk.SubackDecode(buf)
	case packets.Publish:
		err = pk.PublishDecode(buf)
	case packets.Puback:
		err = pk.PubackDecode(buf)
	case packets.Pubrec:
		err = pk.PubrecDecode(buf)
	case packets.Pubrel:
		err = pk.PubrelDecode(buf)
	case packets.Pubcomp:
		err = pk.PubcompDecode(buf)
	case packets.Disconnect:
		err = pk.DisconnectDecode(buf)
	}
	return pk, err
}

// freePort reserves an ephemeral TCP port by binding then immediately
// releasing it — broker.Config.MQTTPort needs a concrete number up front
// (mochi-mqtt's listener doesn't expose its bound address afterward).
// Shared with max_keepalive_integration_test.go.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose
// pins Keel's real (production) MQTT5 flow-control behavior, root-caused
// from paho.mqtt.testing's test_flow_control2 — see
// test/conformance/evidence/test_flow_control2.md for the full
// investigation.
//
// Conclusion of that investigation: Keel is protocol-correct. Sending
// more concurrent QoS1/2 publishes than the advertised Receive Maximum
// gets a DISCONNECT with reason code 147 (0x93, Receive Maximum
// Exceeded), written to the socket BEFORE the connection closes — proven
// here by reading every packet strictly in order (no PUBREC-triggered
// PUBREL writes racing the server's close, unlike the Paho suite's own
// client) and asserting the DISCONNECT is the last packet read, followed
// by a clean EOF, not a connection reset.
//
// Requires TEST_DATABASE_URL (see internal/db/migrate_test.go's own
// doc) — broker.New unconditionally needs a working TenantConfigCache
// once a real CONNECT reaches password auth.
func TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose(t *testing.T) {
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
		MQTTPort:          port,
		TenantConfigCache: tenantCache,
		DefaultTenantID:   uuid.New().String(),
	}, allowAllAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond) // let the listener actually start accepting

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "flow-control-test-client",
			Clean:            true,
			Keepalive:        60,
		},
	}
	var connectBuf bytes.Buffer
	if err := connectPk.ConnectEncode(&connectBuf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	if _, err := conn.Write(connectBuf.Bytes()); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	r := bufio.NewReader(conn)
	connack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if connack.ReasonCode != 0 {
		t.Fatalf("connect rejected, reasonCode=%d", connack.ReasonCode)
	}

	receiveMax := int(connack.Properties.ReceiveMaximum)
	if receiveMax == 0 {
		receiveMax = 65535 // MQTT5 default when the server doesn't advertise a lower one
	}

	// Send exactly one more QoS2 PUBLISH than the server's own advertised
	// Receive Maximum, with no interleaved reads — deliberately mirrors
	// the Paho suite's send-then-drain structure, but this test's drain
	// loop below never writes back, avoiding the exact race that made
	// paho.mqtt.testing's own client fail.
	for i := 1; i <= receiveMax+1; i++ {
		pubPk := packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 2},
			ProtocolVersion: 5,
			// "telemetry" is one of the fixed topic shapes production ACL
			// (internal/broker/hooks.go's isAllowedPublish) allows
			// unconditionally — deliberate, not incidental: an ACL-denied
			// publish never reaches Inflight.DecreaseReceiveQuota (mochi-mqtt
			// server.go's processPublish returns before that on denial), so
			// it would never actually exhaust Receive Maximum. Confirmed by
			// first getting this wrong with an arbitrary topic name.
			TopicName: "telemetry",
			Payload:   []byte("x"),
			PacketID:  uint16(i%65535 + 1),
		}
		var pubBuf bytes.Buffer
		if err := pubPk.PublishEncode(&pubBuf); err != nil {
			t.Fatalf("encode publish #%d: %v", i, err)
		}
		if _, err := conn.Write(pubBuf.Bytes()); err != nil {
			t.Fatalf("write publish #%d: %v", i, err)
		}
	}

	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	var disconnect *packets.Packet
	packetsRead := 0
	for {
		pk, err := readRawPacket(r)
		if err != nil {
			if err != io.EOF {
				t.Fatalf("unexpected read error after %d packets: %v", packetsRead, err)
			}
			break
		}
		packetsRead++
		if pk.FixedHeader.Type == packets.Disconnect {
			disconnect = &pk
			break // DISCONNECT must be the last packet — stop here, verify EOF follows below
		}
	}

	if disconnect == nil {
		t.Fatalf("expected a DISCONNECT packet, got none in %d packets read", packetsRead)
	}
	if disconnect.ReasonCode != 0x93 {
		t.Errorf("expected DISCONNECT reason code 0x93 (Receive Maximum Exceeded), got 0x%x", disconnect.ReasonCode)
	}

	// The DISCONNECT must be immediately followed by a clean connection
	// close (EOF) — not more data, not a reset. This is the exact
	// "DISCONNECT before close" invariant the investigation confirmed.
	if _, err := readRawPacket(r); err != io.EOF {
		t.Errorf("expected clean EOF immediately after DISCONNECT, got: %v", err)
	}
}
