// Package auth — AuthProvider defines the pluggable credential-validation
// interface.  Concrete implementations: PostgresProvider (default),
// FileProvider (static YAML), GRPCProvider (future keel-core).
package auth

import (
	"context"

	"github.com/google/uuid"
)

// AuthProvider validates device credentials and resolves device identities.
// It abstracts the underlying storage (PostgreSQL, YAML file, gRPC service)
// from the broker authentication logic.
type AuthProvider interface {
	// ValidatePassword authenticates a device using an MQTT token (password).
	// Returns DeviceInfo on success, ErrInvalidCredentials on failure, or a
	// wrapped error for transient storage problems.
	ValidatePassword(ctx context.Context, deviceID, token string) (*DeviceInfo, error)

	// LookupByCN resolves a device by its certificate CN components
	// ("<deviceID>@<tenantID>").  Returns ErrInvalidCredentials when the
	// device does not exist.
	LookupByCN(ctx context.Context, deviceID, tenantID string) (*DeviceInfo, error)

	// UpdateLastSeen records the current time as the device's last-seen
	// timestamp.  Implementations should swallow errors — this is best-effort.
	UpdateLastSeen(ctx context.Context, deviceID uuid.UUID)
}
