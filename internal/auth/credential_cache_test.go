package auth

import (
	"testing"
	"time"
)

// ── credentialCache ─────────────────────────────────────────────────────────────

func TestCredentialCache_Hit(t *testing.T) {
	cache := newCredentialCache(30 * time.Second)

	deviceID := "device-123"
	token := "secret-token"

	// First check should miss
	if cache.check(deviceID, token) {
		t.Fatal("expected cache miss on first check")
	}

	// Set entry
	cache.set(deviceID, token)

	// Second check should hit
	if !cache.check(deviceID, token) {
		t.Fatal("expected cache hit after set")
	}
}

func TestCredentialCache_Expire(t *testing.T) {
	cache := newCredentialCache(50 * time.Millisecond) // Short TTL for test

	deviceID := "device-456"
	token := "another-token"

	cache.set(deviceID, token)

	// Should hit immediately
	if !cache.check(deviceID, token) {
		t.Fatal("expected cache hit immediately after set")
	}

	// Wait for expiry
	time.Sleep(75 * time.Millisecond)

	// Should miss after expiry
	if cache.check(deviceID, token) {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestCredentialCache_DifferentToken(t *testing.T) {
	cache := newCredentialCache(30 * time.Second)

	deviceID := "device-789"

	cache.set(deviceID, "correct-token")

	// Same device, different token should miss
	if cache.check(deviceID, "wrong-token") {
		t.Fatal("expected cache miss for different token")
	}

	// Correct token should still hit
	if !cache.check(deviceID, "correct-token") {
		t.Fatal("expected cache hit for correct token")
	}
}

func TestCredentialCache_DifferentDevice(t *testing.T) {
	cache := newCredentialCache(30 * time.Second)

	token := "shared-token"

	cache.set("device-aaa", token)

	// Different device should miss even with same token
	if cache.check("device-bbb", token) {
		t.Fatal("expected cache miss for different device")
	}
}

func TestCredentialCache_Invalidate(t *testing.T) {
	cache := newCredentialCache(30 * time.Second)

	deviceID := "device-999"
	token := "invalidate-test"

	cache.set(deviceID, token)

	// Should hit
	if !cache.check(deviceID, token) {
		t.Fatal("expected cache hit before invalidate")
	}

	// Invalidate all
	cache.invalidate()

	// Should miss after invalidate
	if cache.check(deviceID, token) {
		t.Fatal("expected cache miss after invalidate")
	}
}

func TestCacheKey_Deterministic(t *testing.T) {
	k1 := cacheKey("device-1", "token-a")
	k2 := cacheKey("device-1", "token-a")

	if k1 != k2 {
		t.Fatalf("cache keys not equal: %q != %q", k1, k2)
	}
}

func TestCacheKey_DifferentInputs(t *testing.T) {
	cases := []struct {
		deviceID string
		token    string
	}{
		{"device-1", "token-a"},
		{"device-1", "token-b"},
		{"device-2", "token-a"},
		{"device-2", "token-b"},
	}

	keys := make(map[string]bool)
	for _, tc := range cases {
		k := cacheKey(tc.deviceID, tc.token)
		if keys[k] {
			t.Fatalf("collision detected: key %q for different inputs", k)
		}
		keys[k] = true
	}
}
