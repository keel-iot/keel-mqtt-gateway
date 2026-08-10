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
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/pires/go-proxyproto"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/db"
)

// nilTenantAuthProvider simulates the hardcoded test-consumer identity
// (see hooks.go's testConsumerDeviceInfo) — the one real path where
// info.TenantID is the zero value, not a resolved tenant.
type nilTenantAuthProvider struct{}

func (nilTenantAuthProvider) ValidatePassword(context.Context, string, string) (*auth.DeviceInfo, error) {
	return &auth.DeviceInfo{ID: allowAllTestDeviceID, TenantID: uuid.Nil, TenantSlug: "test-consumer"}, nil
}

func (nilTenantAuthProvider) LookupByCN(context.Context, string, string) (*auth.DeviceInfo, error) {
	return &auth.DeviceInfo{ID: allowAllTestDeviceID, TenantID: uuid.Nil, TenantSlug: "test-consumer"}, nil
}

func (nilTenantAuthProvider) UpdateLastSeen(context.Context, uuid.UUID) {}

func newTestPool(t *testing.T) (*pgxpool.Pool, *auth.TenantConfigCache) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping live-Postgres integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := db.Migrate(ctx, pool, log); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool, auth.NewTenantConfigCache(pool, time.Minute)
}

func dialAndConnect(t *testing.T, port int, clientID string) (net.Conn, *bufio.Reader, packets.Packet) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: clientID,
			Clean:            true,
			Keepalive:        60,
		},
	}
	var buf bytes.Buffer
	if err := connectPk.ConnectEncode(&buf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	r := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	connack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	return conn, r, connack
}

// ── Connect-attempt rate limiting ───────────────────────────────────────

func TestConnectRateLimit_Disabled_AllConnectsSucceed(t *testing.T) {
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

	for i := 0; i < 5; i++ {
		conn, _, connack := dialAndConnect(t, port, fmt.Sprintf("connect-disabled-%d", i))
		defer conn.Close()
		if connack.ReasonCode != 0 {
			t.Fatalf("call %d: expected connect to succeed with the limiter disabled, reasonCode=%d", i, connack.ReasonCode)
		}
	}
}

func TestConnectRateLimit_OverLimitRejectedBeforeAuth(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:               port,
		TenantConfigCache:      tenantCache,
		DefaultTenantID:        uuid.New().String(),
		ConnectRateLimitPerSec: 0.001, // negligible refill within the test's lifetime
		ConnectRateLimitBurst:  1,
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	conn1, _, connack1 := dialAndConnect(t, port, "connect-over-limit-1")
	defer conn1.Close()
	if connack1.ReasonCode != 0 {
		t.Fatalf("expected the first connect (within burst) to succeed, reasonCode=%d", connack1.ReasonCode)
	}

	// allowAllAuthProvider accepts any credential — if this second connect
	// were rejected for any reason OTHER than the rate limiter, it
	// wouldn't prove the limiter runs before authenticate(). The point is
	// it's rejected despite credentials that would otherwise succeed.
	conn2, _, connack2 := dialAndConnect(t, port, "connect-over-limit-2")
	defer conn2.Close()
	if connack2.ReasonCode == 0 {
		t.Fatal("expected the second connect from the same source IP to be rejected — burst of 1 already consumed")
	}
}

func TestConnectRateLimit_DifferentSourceIPIndependentBucket(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:               port,
		TenantConfigCache:      tenantCache,
		DefaultTenantID:        uuid.New().String(),
		ConnectRateLimitPerSec: 0.001,
		ConnectRateLimitBurst:  1,
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	dialFrom := func(localIP string) (net.Conn, packets.Packet) {
		dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(localIP)}}
		conn, err := dialer.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("dial from %s: %v", localIP, err)
		}
		connectPk := packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Connect},
			ProtocolVersion: 5,
			Connect: packets.ConnectParams{
				ProtocolName:     []byte("MQTT"),
				ClientIdentifier: "connect-diff-ip-" + localIP,
				Clean:            true,
				Keepalive:        60,
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
		connack, err := readRawPacket(bufio.NewReader(conn))
		if err != nil {
			t.Fatalf("read connack: %v", err)
		}
		return conn, connack
	}

	conn1, connack1 := dialFrom("127.0.0.1")
	defer conn1.Close()
	if connack1.ReasonCode != 0 {
		t.Fatalf("expected 127.0.0.1's first connect to succeed, reasonCode=%d", connack1.ReasonCode)
	}

	conn2, connack2 := dialFrom("127.0.0.2")
	defer conn2.Close()
	if connack2.ReasonCode != 0 {
		t.Fatalf("expected 127.0.0.2 to have its own independent bucket, reasonCode=%d", connack2.ReasonCode)
	}
}

func TestConnectRateLimit_RefillAllowsReconnectAfterWait(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:               port,
		TenantConfigCache:      tenantCache,
		DefaultTenantID:        uuid.New().String(),
		ConnectRateLimitPerSec: 100, // refills a 1-token bucket in ~10ms
		ConnectRateLimitBurst:  1,
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	conn1, _, connack1 := dialAndConnect(t, port, "connect-refill-1")
	defer conn1.Close()
	if connack1.ReasonCode != 0 {
		t.Fatalf("expected the first connect to succeed, reasonCode=%d", connack1.ReasonCode)
	}

	conn2, _, connack2 := dialAndConnect(t, port, "connect-refill-2")
	defer conn2.Close()
	if connack2.ReasonCode == 0 {
		t.Fatal("expected the immediate second connect to be rejected — burst of 1")
	}

	time.Sleep(50 * time.Millisecond)
	conn3, _, connack3 := dialAndConnect(t, port, "connect-refill-3")
	defer conn3.Close()
	if connack3.ReasonCode != 0 {
		t.Fatalf("expected a connect after refill to succeed, reasonCode=%d", connack3.ReasonCode)
	}
}

// TestConnectRateLimit_TrustedProxyProtocolIPUsedAsKey proves the connect
// limiter's key is the PROXY-header-derived address, not the raw loopback
// socket address every dial in this test actually uses: two connects
// claiming two DIFFERENT spoofed source IPs get two INDEPENDENT buckets,
// which could only happen if the limiter keyed on the header-derived
// address (if it keyed on the shared raw loopback address instead, the
// second connect would collide with the first and be rejected).
func TestConnectRateLimit_TrustedProxyProtocolIPUsedAsKey(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:                  port,
		TenantConfigCache:         tenantCache,
		DefaultTenantID:           uuid.New().String(),
		ProxyProtocol:             true,
		ProxyProtocolTrustedCIDRs: []string{"127.0.0.1/32"},
		ConnectRateLimitPerSec:    0.001,
		ConnectRateLimitBurst:     1,
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	connectViaProxyProto := func(spoofedIP string, clientID string) packets.Packet {
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()

		spoofed := &net.TCPAddr{IP: net.ParseIP(spoofedIP), Port: 12345}
		header := proxyproto.HeaderProxyFromAddrs(1, spoofed, conn.RemoteAddr())
		if _, err := header.WriteTo(conn); err != nil {
			t.Fatalf("write PROXY header: %v", err)
		}

		connectPk := packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Connect},
			ProtocolVersion: 5,
			Connect: packets.ConnectParams{
				ProtocolName:     []byte("MQTT"),
				ClientIdentifier: clientID,
				Clean:            true,
				Keepalive:        60,
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
		connack, err := readRawPacket(bufio.NewReader(conn))
		if err != nil {
			t.Fatalf("read connack: %v", err)
		}
		return connack
	}

	connack1 := connectViaProxyProto("10.1.1.1", "proxyproto-key-1")
	if connack1.ReasonCode != 0 {
		t.Fatalf("expected the first spoofed-source connect to succeed, reasonCode=%d", connack1.ReasonCode)
	}

	connack2 := connectViaProxyProto("10.2.2.2", "proxyproto-key-2")
	if connack2.ReasonCode != 0 {
		t.Fatalf("expected a connect with a DIFFERENT trusted-header source IP to get its own bucket, reasonCode=%d", connack2.ReasonCode)
	}

	connack3 := connectViaProxyProto("10.1.1.1", "proxyproto-key-3")
	if connack3.ReasonCode == 0 {
		t.Fatal("expected a second connect reusing the SAME header source IP to hit the already-exhausted bucket for that key")
	}
}

// TestConnectRateLimit_UntrustedProxyProtocolNeverReachesLimiter documents
// (and proves) the dependency this feature has on #6's REJECT policy: an
// untrusted sender's spoofed PROXY header is never even parsed as a
// connect attempt — proxyproto.Listener rejects the raw connection at the
// Accept layer, before mochi-mqtt ever constructs a Client or calls
// OnConnectAuthenticate. There is structurally no way for a spoofed key
// from an untrusted source to reach the rate limiter at all — see
// TestProxyProtocol_TCP_UntrustedSourceRejected for the same mechanism
// tested directly against the listener.
func TestConnectRateLimit_UntrustedProxyProtocolNeverReachesLimiter(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)

	server, _, err := New(Config{
		MQTTPort:                  port,
		TenantConfigCache:         tenantCache,
		DefaultTenantID:           uuid.New().String(),
		ProxyProtocol:             true,
		ProxyProtocolTrustedCIDRs: []string{"10.0.0.0/8"}, // does not include the loopback source this test dials from
		ConnectRateLimitPerSec:    1000,                   // generous — a rejection here must come from PROXY policy, not the rate limiter
		ConnectRateLimitBurst:     1000,
	}, allowAllAuthProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	spoofed := &net.TCPAddr{IP: net.ParseIP("10.99.99.1"), Port: 12345}
	header := proxyproto.HeaderProxyFromAddrs(1, spoofed, conn.RemoteAddr())
	if _, err := header.WriteTo(conn); err != nil {
		t.Fatalf("write PROXY header: %v", err)
	}
	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "untrusted-proxyproto",
			Clean:            true,
			Keepalive:        60,
		},
	}
	var buf bytes.Buffer
	if err := connectPk.ConnectEncode(&buf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	_, _ = conn.Write(buf.Bytes())

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if pk, err := readRawPacket(bufio.NewReader(conn)); err == nil {
		t.Fatalf("expected the untrusted connection to be rejected before any CONNACK, got packet type %d", pk.FixedHeader.Type)
	}
}

// ── Publish rate limiting ───────────────────────────────────────────────

func publishConn(t *testing.T, port int, clientID string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, r, connack := dialAndConnect(t, port, clientID)
	if connack.ReasonCode != 0 {
		t.Fatalf("connect rejected, reasonCode=%d", connack.ReasonCode)
	}
	return conn, r
}

func writePublish(t *testing.T, conn net.Conn, protocolVersion byte, qos byte, packetID uint16, topic, payload string) {
	t.Helper()
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: qos},
		ProtocolVersion: protocolVersion,
		TopicName:       topic,
		Payload:         []byte(payload),
	}
	if qos > 0 {
		pk.PacketID = packetID
	}
	var buf bytes.Buffer
	if err := pk.PublishEncode(&buf); err != nil {
		t.Fatalf("encode publish: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write publish: %v", err)
	}
}

func TestPublishRateLimit_Disabled_AllPublishesSucceed(t *testing.T) {
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

	conn, r := publishConn(t, port, "publish-disabled")
	defer conn.Close()

	for i := 0; i < 5; i++ {
		writePublish(t, conn, 5, 1, uint16(i+1), "telemetry", "hello")
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		ack, err := readRawPacket(r)
		if err != nil {
			t.Fatalf("call %d: read puback: %v", i, err)
		}
		if ack.ReasonCode != 0 {
			t.Fatalf("call %d: expected success with the limiter disabled, reasonCode=%d", i, ack.ReasonCode)
		}
	}
}

func newRateLimitedServer(t *testing.T, tenantCache *auth.TenantConfigCache, port int, provider auth.AuthProvider, publishPerSec float64, publishBurst int) *mqtt.Server {
	t.Helper()
	server, _, err := New(Config{
		MQTTPort:               port,
		TenantConfigCache:      tenantCache,
		DefaultTenantID:        uuid.New().String(),
		PublishRateLimitPerSec: publishPerSec,
		PublishRateLimitBurst:  publishBurst,
	}, provider, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)
	return server
}

func TestPublishRateLimit_BurstConsumedThenQuotaExceeded(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)
	h := newRateLimitedServer(t, tenantCache, port, allowAllAuthProvider{}, 0.001, 2)
	defer h.Close()

	conn, r := publishConn(t, port, "publish-burst")
	defer conn.Close()

	for i := 0; i < 2; i++ {
		writePublish(t, conn, 5, 1, uint16(i+1), "telemetry", "hello")
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		ack, err := readRawPacket(r)
		if err != nil {
			t.Fatalf("call %d: read puback: %v", i, err)
		}
		if ack.ReasonCode != 0 {
			t.Fatalf("call %d: expected the burst of 2 to be allowed, reasonCode=%d", i, ack.ReasonCode)
		}
	}

	writePublish(t, conn, 5, 1, 3, "telemetry", "hello")
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read puback: %v", err)
	}
	if ack.ReasonCode != packets.ErrQuotaExceeded.Code {
		t.Fatalf("expected PUBACK reason 0x97 (Quota Exceeded), got 0x%x", ack.ReasonCode)
	}
}

func TestPublishRateLimit_TenantIsolation(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)
	// allowAllAuthProvider mints a fresh random tenant per connect (see its
	// ValidatePassword) — two separate connections are, deliberately for
	// this test, two separate tenants.
	h := newRateLimitedServer(t, tenantCache, port, allowAllAuthProvider{}, 0.001, 1)
	defer h.Close()

	connA, rA := publishConn(t, port, "publish-tenant-a")
	defer connA.Close()
	connB, rB := publishConn(t, port, "publish-tenant-b")
	defer connB.Close()

	writePublish(t, connA, 5, 1, 1, "telemetry", "a")
	if err := connA.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ackA, err := readRawPacket(rA)
	if err != nil {
		t.Fatalf("read tenant A's puback: %v", err)
	}
	if ackA.ReasonCode != 0 {
		t.Fatalf("expected tenant A's first publish (within burst) to succeed, reasonCode=%d", ackA.ReasonCode)
	}

	writePublish(t, connB, 5, 1, 1, "telemetry", "b")
	if err := connB.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ackB, err := readRawPacket(rB)
	if err != nil {
		t.Fatalf("read tenant B's puback: %v", err)
	}
	if ackB.ReasonCode != 0 {
		t.Fatalf("expected tenant B to have its own independent bucket, unaffected by tenant A, reasonCode=%d", ackB.ReasonCode)
	}
}

func TestPublishRateLimit_MQTT5_QoS1_PubackQuotaExceeded(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)
	h := newRateLimitedServer(t, tenantCache, port, allowAllAuthProvider{}, 0.001, 1)
	defer h.Close()

	conn, r := publishConn(t, port, "publish-qos1")
	defer conn.Close()

	writePublish(t, conn, 5, 1, 1, "telemetry", "first") // consumes the burst
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := readRawPacket(r); err != nil {
		t.Fatalf("read first puback: %v", err)
	}

	writePublish(t, conn, 5, 1, 2, "telemetry", "second")
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read second puback: %v", err)
	}
	if ack.FixedHeader.Type != packets.Puback {
		t.Fatalf("expected a PUBACK, got packet type %d", ack.FixedHeader.Type)
	}
	if ack.ReasonCode != packets.ErrQuotaExceeded.Code {
		t.Fatalf("expected PUBACK reason 0x97 (Quota Exceeded), got 0x%x", ack.ReasonCode)
	}
}

func TestPublishRateLimit_MQTT5_QoS2_PubrecQuotaExceeded(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)
	h := newRateLimitedServer(t, tenantCache, port, allowAllAuthProvider{}, 0.001, 1)
	defer h.Close()

	conn, r := publishConn(t, port, "publish-qos2")
	defer conn.Close()

	writePublish(t, conn, 5, 2, 1, "telemetry", "first") // consumes the burst
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := readRawPacket(r); err != nil {
		t.Fatalf("read first pubrec: %v", err)
	}

	writePublish(t, conn, 5, 2, 2, "telemetry", "second")
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read second pubrec: %v", err)
	}
	if ack.FixedHeader.Type != packets.Pubrec {
		t.Fatalf("expected a PUBREC, got packet type %d", ack.FixedHeader.Type)
	}
	if ack.ReasonCode != packets.ErrQuotaExceeded.Code {
		t.Fatalf("expected PUBREC reason 0x97 (Quota Exceeded), got 0x%x", ack.ReasonCode)
	}
}

// TestPublishRateLimit_MQTT5_QoS0_DroppedConnectionSurvives proves a QoS0
// publish over quota is silently dropped — no ack (QoS0 never has one
// anyway), no disconnect — by proving the connection is still usable
// afterward via a QoS1 publish on a topic this test doesn't rate-limit
// against (a fresh client ID means a fresh tenant, so its own burst is
// untouched by the QoS0 attempt above).
func TestPublishRateLimit_MQTT5_QoS0_DroppedConnectionSurvives(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)
	h := newRateLimitedServer(t, tenantCache, port, allowAllAuthProvider{}, 0.001, 1)
	defer h.Close()

	conn, r := publishConn(t, port, "publish-qos0")
	defer conn.Close()

	writePublish(t, conn, 5, 0, 0, "telemetry", "first")  // consumes the burst, QoS0 has no ack either way
	writePublish(t, conn, 5, 0, 0, "telemetry", "second") // over quota — must be dropped silently, not disconnect

	// Prove the connection is still alive: a QoS1 publish still gets a
	// normal PUBACK, not a disconnect/EOF.
	writePublish(t, conn, 5, 1, 1, "telemetry", "still-alive")
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	ack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("expected the connection to survive the dropped QoS0 publish, got: %v", err)
	}
	if ack.FixedHeader.Type != packets.Puback {
		t.Fatalf("expected a PUBACK proving the connection is still alive, got packet type %d", ack.FixedHeader.Type)
	}
}

// dialAndConnectV311 connects with protocol level 4 (MQTT 3.1.1) — a real
// client, not just a PUBLISH encoded with ProtocolVersion 4 over a
// connection that CONNECTed as MQTT5 (mochi-mqtt's per-connection protocol
// version comes from the CONNECT packet itself; a mismatch between that
// and later packets' encoding produces a "malformed packet" decode error
// server-side, not the behavior under test). v3.1.1 CONNACK/PUBACK/PUBREC
// have no properties section and no reason code — remaining length is
// always exactly 2, so a minimal manual read is enough; readRawPacket
// unconditionally assumes protocol 5 and isn't safe to reuse here.
func dialAndConnectV311(t *testing.T, port int, clientID string) (net.Conn, *bufio.Reader) {
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
		},
	}
	var buf bytes.Buffer
	if err := connectPk.ConnectEncode(&buf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	r := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, body := readRawAckBody(t, r)
	if len(body) < 2 || body[1] != 0 {
		t.Fatalf("connect rejected or malformed connack: %v", body)
	}
	return conn, r
}

// readRawAckBody reads one packet's type and raw body without any
// protocol-version-specific decoding.
func readRawAckBody(t *testing.T, r *bufio.Reader) (packetType byte, body []byte) {
	t.Helper()
	hb, err := r.ReadByte()
	if err != nil {
		t.Fatalf("read fixed header: %v", err)
	}
	packetType = hb >> 4
	remaining, _, err := packets.DecodeLength(r)
	if err != nil {
		t.Fatalf("decode remaining length: %v", err)
	}
	body = make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return packetType, body
}

// TestPublishRateLimit_MQTT311_DroppedNotDisconnected covers all three QoS
// levels for MQTT 3.1.1: no per-packet ack reason mechanism exists there
// either, so an over-quota publish is silently dropped, same as MQTT5
// QoS0 — never a disconnect, never an invented reason code the protocol
// can't express.
func TestPublishRateLimit_MQTT311_DroppedNotDisconnected(t *testing.T) {
	for _, qos := range []byte{0, 1, 2} {
		t.Run(fmt.Sprintf("QoS%d", qos), func(t *testing.T) {
			_, tenantCache := newTestPool(t)
			port := freePort(t)
			h := newRateLimitedServer(t, tenantCache, port, allowAllAuthProvider{}, 0.001, 1)
			defer h.Close()

			conn, r := dialAndConnectV311(t, port, fmt.Sprintf("publish-311-qos%d", qos))
			defer conn.Close()

			writePublish(t, conn, 4, qos, 1, "telemetry", "first") // consumes the burst
			if qos > 0 {
				if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatalf("set read deadline: %v", err)
				}
				wantType := byte(packets.Puback)
				if qos == 2 {
					wantType = packets.Pubrec
				}
				gotType, _ := readRawAckBody(t, r)
				if gotType != wantType {
					t.Fatalf("expected first ack packet type %d, got %d", wantType, gotType)
				}
			}

			writePublish(t, conn, 4, qos, 2, "telemetry", "second") // over quota — dropped, no ack, no disconnect

			// A fresh QoS1 publish on a NEW connection (different client
			// ID => different random tenant from allowAllAuthProvider)
			// proves the server itself is still healthy; proving THIS
			// connection specifically survived is done by successfully
			// writing to it without an error below.
			if _, err := conn.Write([]byte{0xC0, 0x00}); err != nil { // PINGREQ
				t.Fatalf("expected the connection to still accept writes after the dropped publish, got: %v", err)
			}
		})
	}
}

func TestPublishRateLimit_EmptyTenantNeverThrottled(t *testing.T) {
	_, tenantCache := newTestPool(t)
	port := freePort(t)
	h := newRateLimitedServer(t, tenantCache, port, nilTenantAuthProvider{}, 0.001, 1)
	defer h.Close()

	conn, r := publishConn(t, port, "publish-niltenant")
	defer conn.Close()

	// Burst is 1 — a real tenant would see call 2 rejected. The
	// uuid.Nil-tenant test-consumer identity must sail through regardless,
	// by explicit policy (see hooks.go's OnPublish comment).
	for i := 0; i < 5; i++ {
		writePublish(t, conn, 5, 1, uint16(i+1), "telemetry", "hello")
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		ack, err := readRawPacket(r)
		if err != nil {
			t.Fatalf("call %d: read puback: %v", i, err)
		}
		if ack.ReasonCode != 0 {
			t.Fatalf("call %d: expected the nil-tenant identity to never be throttled, reasonCode=%d", i, ack.ReasonCode)
		}
	}
}
