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

// TestMaxKeepAlive_EndToEnd_ConnackServerKeepAliveProperty is the
// integration-level counterpart to max_keepalive_test.go's unit tests: it
// proves the whole wiring (config.Config.MaxKeepAlive -> broker.Config ->
// broker.New registering MaxKeepAliveHook only when configured -> a real
// CONNACK on the wire) actually works, not just the hook's own logic in
// isolation. Requires TEST_DATABASE_URL — see
// receive_maximum_test.go's TestFlowControl_ReceiveMaximumExceeded_DisconnectsWithReasonBeforeClose
// doc for why broker.New needs one; reuses that file's readRawPacket/
// freePort/allowAllAuthProvider helpers (same package).
func TestMaxKeepAlive_EndToEnd_ConnackServerKeepAliveProperty(t *testing.T) {
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
		MaxKeepAlive:      300 * time.Second,
	}, allowAllAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer server.Close()
	go func() { _ = server.Serve() }()
	time.Sleep(100 * time.Millisecond)

	connect := func(t *testing.T, clientID string, keepalive uint16) packets.Packet {
		t.Helper()
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { conn.Close() })

		connectPk := packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Connect},
			ProtocolVersion: 5,
			Connect: packets.ConnectParams{
				ProtocolName:     []byte("MQTT"),
				ClientIdentifier: clientID,
				Clean:            true,
				Keepalive:        keepalive,
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
		if connack.ReasonCode != 0 {
			t.Fatalf("connect rejected, reasonCode=%d", connack.ReasonCode)
		}
		return connack
	}

	t.Run("above configured max gets overridden and announced", func(t *testing.T) {
		connack := connect(t, "max-keepalive-above", 600)
		if !connack.Properties.ServerKeepAliveFlag {
			t.Fatal("expected ServerKeepAliveFlag=true in CONNACK")
		}
		if connack.Properties.ServerKeepAlive != 300 {
			t.Errorf("expected ServerKeepAlive=300, got %d", connack.Properties.ServerKeepAlive)
		}
	})

	t.Run("within configured max left untouched", func(t *testing.T) {
		connack := connect(t, "max-keepalive-within", 60)
		if connack.Properties.ServerKeepAliveFlag {
			t.Errorf("expected no ServerKeepAlive property in CONNACK, got %d", connack.Properties.ServerKeepAlive)
		}
	})
}
