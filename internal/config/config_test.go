package config

import "testing"

func TestLoad_MaxKeepAlive_DefaultDisabled(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxKeepAlive != 0 {
		t.Errorf("expected MaxKeepAlive to default to 0 (disabled) when MAX_KEEPALIVE is unset, got %v", cfg.MaxKeepAlive)
	}
}

func TestLoad_MaxKeepAlive_ValidValue(t *testing.T) {
	t.Setenv("MAX_KEEPALIVE", "5m")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxKeepAlive.String() != "5m0s" {
		t.Errorf("expected MaxKeepAlive=5m, got %v", cfg.MaxKeepAlive)
	}
}

// TestLoad_MaxKeepAlive_BoundaryAtWireLimit confirms exactly 65535s (MQTT's
// uint16-seconds Keep Alive ceiling) is accepted, not rejected off-by-one.
func TestLoad_MaxKeepAlive_BoundaryAtWireLimit(t *testing.T) {
	t.Setenv("MAX_KEEPALIVE", "65535s")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected 65535s to be accepted (exactly at the wire limit), got error: %v", err)
	}
	if cfg.MaxKeepAlive.Seconds() != 65535 {
		t.Errorf("expected MaxKeepAlive=65535s, got %v", cfg.MaxKeepAlive)
	}
}

func TestLoad_MaxKeepAlive_AboveWireLimitRejected(t *testing.T) {
	t.Setenv("MAX_KEEPALIVE", "65536s")
	if _, err := Load(); err == nil {
		t.Fatal("expected an error for MAX_KEEPALIVE exceeding MQTT's 65535s (uint16 seconds) limit, got nil")
	}
}

func TestLoad_MaxKeepAlive_NegativeRejected(t *testing.T) {
	t.Setenv("MAX_KEEPALIVE", "-1s")
	if _, err := Load(); err == nil {
		t.Fatal("expected an error for a negative MAX_KEEPALIVE, got nil")
	}
}

func TestLoad_MaxKeepAlive_MalformedRejected(t *testing.T) {
	t.Setenv("MAX_KEEPALIVE", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected an error for a malformed MAX_KEEPALIVE, got nil")
	}
}

func TestLoad_MaxKeepAlive_ZeroExplicitlyMeansDisabled(t *testing.T) {
	t.Setenv("MAX_KEEPALIVE", "0s")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxKeepAlive != 0 {
		t.Errorf("expected explicit 0s to also mean disabled, got %v", cfg.MaxKeepAlive)
	}
}

// TestLoad_RateLimits_DefaultDisabled covers both the connect and publish
// pairs — unset means 0/0, which parseRateLimitPair treats as the valid
// "disabled" case, not a validation error.
func TestLoad_RateLimits_DefaultDisabled(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ConnectRateLimitPerSec != 0 || cfg.ConnectRateLimitBurst != 0 {
		t.Errorf("expected connect rate limit to default to disabled, got perSec=%v burst=%v", cfg.ConnectRateLimitPerSec, cfg.ConnectRateLimitBurst)
	}
	if cfg.PublishRateLimitPerSec != 0 || cfg.PublishRateLimitBurst != 0 {
		t.Errorf("expected publish rate limit to default to disabled, got perSec=%v burst=%v", cfg.PublishRateLimitPerSec, cfg.PublishRateLimitBurst)
	}
}

func TestLoad_RateLimits_ValidEnabled(t *testing.T) {
	t.Setenv("CONNECT_RATE_LIMIT_PER_SEC", "5")
	t.Setenv("CONNECT_RATE_LIMIT_BURST", "10")
	t.Setenv("PUBLISH_RATE_LIMIT_PER_SEC", "100")
	t.Setenv("PUBLISH_RATE_LIMIT_BURST", "200")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ConnectRateLimitPerSec != 5 || cfg.ConnectRateLimitBurst != 10 {
		t.Errorf("connect rate limit mismatch: perSec=%v burst=%v", cfg.ConnectRateLimitPerSec, cfg.ConnectRateLimitBurst)
	}
	if cfg.PublishRateLimitPerSec != 100 || cfg.PublishRateLimitBurst != 200 {
		t.Errorf("publish rate limit mismatch: perSec=%v burst=%v", cfg.PublishRateLimitPerSec, cfg.PublishRateLimitBurst)
	}
}

// TestLoad_RateLimits_MismatchedPairRejected covers every invalid
// combination the pairing rule is meant to catch — a zero paired with a
// positive, in either direction, on either the connect or publish pair.
func TestLoad_RateLimits_MismatchedPairRejected(t *testing.T) {
	cases := []struct {
		name        string
		perSecEnv   string
		burstEnv    string
		perSecValue string
		burstValue  string
	}{
		{"connect rate>0 burst=0", "CONNECT_RATE_LIMIT_PER_SEC", "CONNECT_RATE_LIMIT_BURST", "5", "0"},
		{"connect rate=0 burst>0", "CONNECT_RATE_LIMIT_PER_SEC", "CONNECT_RATE_LIMIT_BURST", "0", "10"},
		{"publish rate>0 burst=0", "PUBLISH_RATE_LIMIT_PER_SEC", "PUBLISH_RATE_LIMIT_BURST", "5", "0"},
		{"publish rate=0 burst>0", "PUBLISH_RATE_LIMIT_PER_SEC", "PUBLISH_RATE_LIMIT_BURST", "0", "10"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.perSecEnv, tc.perSecValue)
			t.Setenv(tc.burstEnv, tc.burstValue)
			if _, err := Load(); err == nil {
				t.Fatalf("expected an error for mismatched %s=%s %s=%s", tc.perSecEnv, tc.perSecValue, tc.burstEnv, tc.burstValue)
			}
		})
	}
}

func TestLoad_RateLimits_NegativeRejected(t *testing.T) {
	t.Setenv("CONNECT_RATE_LIMIT_PER_SEC", "-1")
	t.Setenv("CONNECT_RATE_LIMIT_BURST", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("expected an error for a negative connect rate limit, got nil")
	}
}
