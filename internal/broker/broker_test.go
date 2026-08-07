package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
)

// noopAuthProvider is a minimal auth.AuthProvider for tests that only need
// New to succeed, not to authenticate anything.
type noopAuthProvider struct{}

func (noopAuthProvider) ValidatePassword(context.Context, string, string) (*auth.DeviceInfo, error) {
	return nil, auth.ErrInvalidCredentials
}

func (noopAuthProvider) LookupByCN(context.Context, string, string) (*auth.DeviceInfo, error) {
	return nil, auth.ErrInvalidCredentials
}

func (noopAuthProvider) UpdateLastSeen(context.Context, uuid.UUID) {}

// TestNew_NoInheritedPropertiesOnAckAlwaysEnabled pins a real MQTT5
// semantics fix (not conformance scaffolding — see internal/conformance's
// package doc and docs/alternatives-and-future-work.md's 2026-08-07
// root-cause) as a default for every deployment: mochi-mqtt's buildAck
// must never echo the original PUBLISH packet's Properties onto the
// PUBACK/PUBREC.
func TestNew_NoInheritedPropertiesOnAckAlwaysEnabled(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, _, err := New(Config{MQTTPort: 0}, noopAuthProvider{}, log)
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	defer server.Close()

	if !server.Options.Capabilities.Compatibilities.NoInheritedPropertiesOnAck {
		t.Error("expected NoInheritedPropertiesOnAck to be enabled unconditionally, not just for --conformance-test")
	}
}
