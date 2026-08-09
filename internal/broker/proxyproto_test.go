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

// TestNew_ProxyProtocolRequiresTrustedCIDRs guards the fail-closed default:
// enabling ProxyProtocol without an explicit trusted-source allowlist is a
// misconfiguration, not something to silently fall back from (trusting
// every sender lets any client spoof its IP; trusting none makes the
// listener a no-op).
func TestNew_ProxyProtocolRequiresTrustedCIDRs(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, _, err := New(Config{MQTTPort: 0, ProxyProtocol: true}, noopAuthProvider{}, log)
	if err == nil {
		t.Fatal("expected an error when ProxyProtocol is enabled without ProxyProtocolTrustedCIDRs")
	}
}

// TestNew_ProxyProtocolInvalidCIDR proves a malformed CIDR is rejected at
// construction time, not discovered later at the first connection attempt.
func TestNew_ProxyProtocolInvalidCIDR(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, _, err := New(Config{
		MQTTPort:                  0,
		ProxyProtocol:             true,
		ProxyProtocolTrustedCIDRs: []string{"not-a-cidr"},
	}, noopAuthProvider{}, log)
	if err == nil {
		t.Fatal("expected an error for an invalid CIDR")
	}
}

// remoteCapturingHook records the RemoteAddr mochi-mqtt observes for every
// connecting client, letting the test assert on it without depending on any
// exported API from the broker package itself.
type remoteCapturingHook struct {
	mqtt.HookBase
	remotes chan string
}

func (*remoteCapturingHook) ID() string { return "test-remote-capture" }

func (*remoteCapturingHook) Provides(b byte) bool { return b == mqtt.OnConnect }

func (h *remoteCapturingHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	h.remotes <- cl.Net.Remote
	return nil
}

func connectRaw(t *testing.T, conn net.Conn) (*bufio.Reader, packets.Packet) {
	t.Helper()
	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "proxyproto-test-client",
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
	connack, err := readRawPacket(r)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	return r, connack
}

// TestProxyProtocol_TCP_TrustedSourceRealIP proves the real client address
// carried in a PROXY v1 header — not the TCP socket's own peer address
// (loopback, since the test dials locally) — is what mochi-mqtt ends up
// using once the sender is on the trusted allowlist.
//
// Requires TEST_DATABASE_URL — see receive_maximum_test.go's doc for why.
func TestProxyProtocol_TCP_TrustedSourceRealIP(t *testing.T) {
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
		MQTTPort:                  port,
		ProxyProtocol:             true,
		ProxyProtocolTrustedCIDRs: []string{"127.0.0.1/32"},
		TenantConfigCache:         tenantCache,
		DefaultTenantID:           uuid.New().String(),
	}, allowAllAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()

	remotes := make(chan string, 1)
	if err := server.AddHook(&remoteCapturingHook{remotes: remotes}, nil); err != nil {
		t.Fatalf("add capture hook: %v", err)
	}
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	spoofedSource := &net.TCPAddr{IP: net.ParseIP("10.99.99.1"), Port: 12345}
	header := proxyproto.HeaderProxyFromAddrs(1, spoofedSource, conn.RemoteAddr())
	if _, err := header.WriteTo(conn); err != nil {
		t.Fatalf("write PROXY header: %v", err)
	}

	_, connack := connectRaw(t, conn)
	if connack.ReasonCode != 0 {
		t.Fatalf("connect rejected, reasonCode=%d", connack.ReasonCode)
	}

	select {
	case remote := <-remotes:
		if remote != spoofedSource.String() {
			t.Fatalf("expected RemoteAddr from the PROXY header (%s), got %s", spoofedSource, remote)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnConnect")
	}
}

// TestProxyProtocol_TCP_UntrustedSourceRejected proves a sender outside the
// configured trusted CIDRs cannot get a PROXY header honored — the
// connection is dropped outright (REJECT), it does not silently fall back
// to trusting the header or to the raw socket address.
//
// Requires TEST_DATABASE_URL — see receive_maximum_test.go's doc for why.
func TestProxyProtocol_TCP_UntrustedSourceRejected(t *testing.T) {
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

	// The test always dials from 127.0.0.1 — trusting only an unrelated
	// range means the loopback sender is never on the allowlist.
	server, _, err := New(Config{
		MQTTPort:                  port,
		ProxyProtocol:             true,
		ProxyProtocolTrustedCIDRs: []string{"10.0.0.0/8"},
		TenantConfigCache:         tenantCache,
		DefaultTenantID:           uuid.New().String(),
	}, allowAllAuthProvider{}, log)
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

	spoofedSource := &net.TCPAddr{IP: net.ParseIP("10.99.99.1"), Port: 12345}
	header := proxyproto.HeaderProxyFromAddrs(1, spoofedSource, conn.RemoteAddr())
	if _, err := header.WriteTo(conn); err != nil {
		t.Fatalf("write PROXY header: %v", err)
	}

	// Still send a real CONNECT — proves rejection happens at the PROXY
	// policy layer itself, not merely because the test sent nothing further.
	// A REQUIRE-trust-all mutation (see proxyproto_listener.go's Init) would
	// happily process this CONNECT and answer with a CONNACK instead.
	connectPk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: "proxyproto-untrusted-test-client",
			Clean:            true,
			Keepalive:        60,
		},
	}
	var connectBuf bytes.Buffer
	if err := connectPk.ConnectEncode(&connectBuf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	_, _ = conn.Write(connectBuf.Bytes()) // the connection may already be closed by REJECT — a write error here is fine

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	r := bufio.NewReader(conn)
	if pk, err := readRawPacket(r); err == nil {
		t.Fatalf("expected the untrusted connection to be rejected before any CONNACK, got packet type %d", pk.FixedHeader.Type)
	}
}
