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
