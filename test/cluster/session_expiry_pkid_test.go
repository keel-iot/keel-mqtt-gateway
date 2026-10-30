//go:build cluster

package cluster

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redis/go-redis/v9"
)

// TestSessionExpiry_CleansPerClientRedisState is the regression for the former
// RISK-OWN-004 packet-ID-counter orphan. Expiry must delete the complete v2
// per-client footprint, including CL, SUB, IFM and PKID keys.
//
// PRECONDITION: 3-Core/2-Edge cluster with a short SESSION_EXPIRY_INTERVAL
// (real config surface, internal/config.go's own env var), a persistent
// session with a queued offline message (so all three Redis footprints —
// CL, SUB, PKID — exist before expiry).
// ACTION: no reconnect — let SessionExpiryInterval actually elapse.
// OBSERVABLE TRANSITION: mochi-mqtt's own clearExpiredClients sweep fires
// OnClientExpired, which RedisSessionHook.deleteSessionState (already
// fixed in Phase 2) uses to clean the CL/SUB/IFM hash fields — observed
// here via the per-client CL key disappearing (deterministic poll).
// ASSERT INVARIANT: no per-client Redis state remains after expiry.
// RECOVERY: n/a.
// FINAL ASSERTION: the packet-ID counter is removed with the session.
// LIMITATIONS: does not exercise RISK-OWN-001's collision scenario (a
// reconnect never happens here) — that's TASK-08's dedicated test.
func TestSessionExpiry_CleansPerClientRedisState(t *testing.T) {
	h := NewHarnessWithEdgeEnv(t, 3, 2, []string{deviceAID}, map[string]string{
		"SESSION_EXPIRY_INTERVAL": "3s",
	})

	const clientID = "session-expiry-pkid-subscriber"
	const filter = "telemetry/#"

	sub := connectPersistentClient(t, h.MQTTAddr(0), clientID, consumerUsername, consumerPassword)
	subToken := sub.Subscribe(filter, 1, nil)
	if !subToken.WaitTimeout(10*time.Second) || subToken.Error() != nil {
		t.Fatalf("subscribe: %v", subToken.Error())
	}
	sub.Disconnect(250)

	if !waitForOfflineOwnership(t, h, 0, clientID, 20*time.Second) {
		t.Fatal("offline-ownership record never became observable — cannot proceed to queue a message")
	}

	publisher := connectClient(t, h.MQTTAddr(1), "session-expiry-pkid-publisher", deviceAID, DevicePwd)
	defer publisher.Disconnect(250)
	pubToken := publisher.Publish(publishTopicFor(deviceAID), 1, false, "queued-before-expiry")
	if !pubToken.WaitTimeout(10*time.Second) || pubToken.Error() != nil {
		t.Fatalf("publish while subscriber offline: %v", pubToken.Error())
	}

	rdb := redis.NewClient(&redis.Options{Addr: h.RedisAddr(0)})
	defer rdb.Close()
	ctx := context.Background()
	pkidKey := redisPacketIDKeyFor(clientID)

	// Sanity checkpoint: the PKID key must actually exist before expiry —
	// otherwise this test would trivially pass for the wrong reason.
	if !waitForRedisKeyExists(t, rdb, ctx, pkidKey, 15*time.Second) {
		t.Fatalf("packet-ID counter for %s was never created by the offline delivery", clientID)
	}

	// Deterministic wait for real expiry: poll the per-client CL key
	// (RedisSessionHook.deleteSessionState's own cleanup target) until it's
	// gone — proof OnClientExpired actually fired, not a fixed sleep guess
	// at the 3s+1s-ticker timing.
	if !waitForRedisKeyGone(t, rdb, ctx, redisClientKeyFor(clientID), 20*time.Second) {
		t.Fatal("session never expired (per-client CL key was never removed)")
	}

	// The per-client subscription hash is removed with the client record.
	if n, err := rdb.Exists(ctx, redisSubKeyFor(clientID)).Result(); err != nil || n != 0 {
		t.Errorf("expected per-client SUB key for %s to be cleaned, exists=%d err=%v", clientID, n, err)
	}
	// The packet-ID counter is part of the same session footprint.
	exists, err := rdb.Exists(ctx, pkidKey).Result()
	if err != nil {
		t.Fatalf("redis EXISTS %s: %v", pkidKey, err)
	}
	if exists != 0 {
		t.Fatalf("packet-ID counter for %s remained orphaned after session expiry", clientID)
	}
}

// connectPersistentClient is connectClient's CleanSession=false variant —
// separate from the shared helper rather than adding a parameter to it,
// since every existing caller of connectClient relies on the default
// (clean) session and this scenario's persistence is exactly what's under
// test, not incidental.
func connectPersistentClient(t *testing.T, broker, clientID, username, password string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + broker).
		SetClientID(clientID).
		SetUsername(username).
		SetPassword(password).
		SetCleanSession(false).
		SetConnectTimeout(15 * time.Second).
		SetAutoReconnect(false)
	c := mqtt.NewClient(opts)
	if token := c.Connect(); !token.WaitTimeout(20*time.Second) || token.Error() != nil {
		t.Fatalf("connect %s to %s: %v", clientID, broker, token.Error())
	}
	t.Cleanup(func() { c.Disconnect(250) })
	return c
}

func waitForRedisKeyExists(t *testing.T, rdb *redis.Client, ctx context.Context, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, err := rdb.Exists(ctx, key).Result(); err == nil && n > 0 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func waitForRedisHashFieldGone(t *testing.T, rdb *redis.Client, ctx context.Context, hash, field string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok, err := rdb.HExists(ctx, hash, field).Result(); err == nil && !ok {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func waitForRedisKeyGone(t *testing.T, rdb *redis.Client, ctx context.Context, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, err := rdb.Exists(ctx, key).Result(); err == nil && n == 0 {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func redisSessionToken(clientID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(clientID))
}

func redisClientKeyFor(clientID string) string {
	return "keel:gw:CL:" + redisSessionToken(clientID)
}

func redisSubKeyFor(clientID string) string {
	return "keel:gw:SUB:" + redisSessionToken(clientID)
}

func redisPacketIDKeyFor(clientID string) string {
	return "keel:gw:PKID:" + redisSessionToken(clientID)
}
