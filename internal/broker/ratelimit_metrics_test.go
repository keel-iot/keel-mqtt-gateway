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
	"github.com/prometheus/client_golang/prometheus"
)

// TestRateLimitedTotal_BoundedCardinality proves the design property
// stated in broker.Config's rate-limiter docs and ROADMAP.md: the
// RateLimitedTotal metric must never carry a high-cardinality label
// (source IP, tenant ID, client ID) — only the fixed, two-value "type"
// enum. Deliberately exercises the real production code paths (not a
// synthetic .WithLabelValues call) from several DISTINCT, deliberately
// unusual source IPs and tenants, then asserts the metric family's
// series count stays exactly 2 regardless of how many distinct
// high-cardinality-candidate values were involved — if a future change
// added ip/tenant/client_id to the label set, this test would start
// accumulating a new series per distinct value and fail.
//
// Requires TEST_DATABASE_URL — see receive_maximum_test.go's doc for why.
func TestRateLimitedTotal_BoundedCardinality(t *testing.T) {
	_, tenantCache := newTestPool(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Trigger a connect-rate-limit rejection from three distinct source
	// IPs — if "type" ever grew an ip label, these would be three
	// distinct series instead of contributing to the same one.
	connectPort := freePort(t)
	connectServer, _, err := New(Config{
		MQTTPort:               connectPort,
		TenantConfigCache:      tenantCache,
		DefaultTenantID:        uuid.New().String(),
		ConnectRateLimitPerSec: 0.001,
		ConnectRateLimitBurst:  1,
	}, allowAllAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New (connect limiter): %v", err)
	}
	defer connectServer.Close()
	go func() { _ = connectServer.Serve() }()
	time.Sleep(100 * time.Millisecond)

	for i, localIP := range []string{"127.0.0.3", "127.0.0.4", "127.0.0.5"} {
		dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(localIP)}}
		conn, err := dialer.Dial("tcp", fmt.Sprintf("localhost:%d", connectPort))
		if err != nil {
			t.Fatalf("dial from %s: %v", localIP, err)
		}
		// Two connects from the same IP: first consumes the burst,
		// second is the one that actually increments RateLimitedTotal.
		writeMinimalConnect(t, conn, fmt.Sprintf("cardinality-connect-%d-a", i))
		conn.Close()
		conn2, err := dialer.Dial("tcp", fmt.Sprintf("localhost:%d", connectPort))
		if err != nil {
			t.Fatalf("second dial from %s: %v", localIP, err)
		}
		writeMinimalConnect(t, conn2, fmt.Sprintf("cardinality-connect-%d-b", i))
		conn2.Close()
	}

	// Trigger a publish-rate-limit rejection from three distinct tenants
	// (allowAllAuthProvider mints a fresh random tenant per connect).
	publishPort := freePort(t)
	publishServer, _, err := New(Config{
		MQTTPort:               publishPort,
		TenantConfigCache:      tenantCache,
		DefaultTenantID:        uuid.New().String(),
		PublishRateLimitPerSec: 0.001,
		PublishRateLimitBurst:  1,
	}, allowAllAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New (publish limiter): %v", err)
	}
	defer publishServer.Close()
	go func() { _ = publishServer.Serve() }()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		conn, r := publishConn(t, publishPort, fmt.Sprintf("cardinality-publish-%d", i))
		writePublish(t, conn, 5, 1, 1, "telemetry", "first") // consumes burst
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		if _, err := readRawPacket(r); err != nil {
			t.Fatalf("read first puback: %v", err)
		}
		writePublish(t, conn, 5, 1, 2, "telemetry", "second") // over quota, increments the metric
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		if _, err := readRawPacket(r); err != nil {
			t.Fatalf("read second puback: %v", err)
		}
		conn.Close()
	}

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var found bool
	for _, fam := range families {
		if fam.GetName() != "keel_gateway_rate_limited_total" {
			continue
		}
		found = true
		metrics := fam.GetMetric()
		if len(metrics) > 2 {
			t.Fatalf("expected at most 2 series (type=connect, type=publish) regardless of how many distinct IPs/tenants triggered rejection, got %d", len(metrics))
		}
		for _, m := range metrics {
			labels := m.GetLabel()
			if len(labels) != 1 {
				t.Fatalf("expected exactly one label (\"type\"), got %d: %v", len(labels), labels)
			}
			key, val := labels[0].GetName(), labels[0].GetValue()
			if key != "type" {
				t.Fatalf("expected the only label key to be \"type\", got %q", key)
			}
			if val != "connect" && val != "publish" {
				t.Fatalf("expected type to be \"connect\" or \"publish\", got %q — a high-cardinality value leaked into the label", val)
			}
		}
	}
	if !found {
		t.Fatal("keel_gateway_rate_limited_total metric family not found — did the rejections above actually fire?")
	}
}

// writeMinimalConnect writes a bare MQTT5 CONNECT and discards whatever
// comes back (used only to trigger the connect-rate-limit check itself —
// the CONNACK's own content doesn't matter for this test).
func writeMinimalConnect(t *testing.T, conn net.Conn, clientID string) {
	t.Helper()
	pk := packets.Packet{
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
	if err := pk.ConnectEncode(&buf); err != nil {
		t.Fatalf("encode connect: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return // rejection may close the connection before the write completes; fine
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = bufio.NewReader(conn).ReadByte()
}
