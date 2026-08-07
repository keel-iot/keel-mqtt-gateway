package conformance

import (
	"context"

	"github.com/google/uuid"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
)

// Fixed synthetic identity for every connection accepted in conformance
// mode. The actual value is arbitrary — nothing downstream is
// cluster-aware in the standalone-only mode this package requires (see
// ValidateRole) — but it must be a valid, stable uuid.UUID since
// auth.DeviceInfo's fields are consumed as such by telemetry labels and
// Config.DefaultTenantID-independent tenant lookups.
var (
	deviceID = uuid.MustParse("00000000-0000-0000-0000-0000000000c0")
	tenantID = uuid.MustParse("00000000-0000-0000-0000-0000000000c1")
)

// AuthProvider implements auth.AuthProvider by accepting every
// credential unconditionally (including no credentials at all — the
// paho.mqtt.testing suite's clients mostly connect without a username or
// password). See the package doc for why this must never be reachable
// outside --conformance-test, standalone-only.
type AuthProvider struct{}

// NewAuthProvider returns a ready-to-use conformance AuthProvider.
func NewAuthProvider() *AuthProvider {
	return &AuthProvider{}
}

func (*AuthProvider) ValidatePassword(_ context.Context, _, _ string) (*auth.DeviceInfo, error) {
	return deviceInfo(), nil
}

func (*AuthProvider) LookupByCN(_ context.Context, _, _ string) (*auth.DeviceInfo, error) {
	return deviceInfo(), nil
}

func (*AuthProvider) UpdateLastSeen(_ context.Context, _ uuid.UUID) {}

func deviceInfo() *auth.DeviceInfo {
	return &auth.DeviceInfo{
		ID:         deviceID,
		TenantID:   tenantID,
		TenantSlug: "conformance",
		FleetIDStr: "nofleet",
	}
}
