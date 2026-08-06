// Package auth validates device credentials against the devices.device_credentials
// PostgreSQL table and returns the device identity needed for MQTT routing.
package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidCredentials is returned when authentication fails.
var ErrInvalidCredentials = errors.New("invalid credentials")

// DeviceInfo holds the information resolved during authentication.
// It is stored per-connection and used for MQTT topic routing.
type DeviceInfo struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	TenantSlug string
	FleetID    *uuid.UUID
	// FleetIDStr is pre-computed for topic routing.
	// It is set to the fleet UUID string, or "nofleet" when fleet is not assigned.
	FleetIDStr string
	// Identity is the cert-CN identity ("<deviceID>@<tenantID>") this
	// connection authenticated as. Only set by cert auth (authenticateCert)
	// — revocation is a PKI-only concept, so JWT/password auth leaves this
	// empty. Passed to raft.Registry.ClaimSession so a later certificate
	// revocation can find and evict the session.
	Identity string
}

// RedpandaTopic returns the fully qualified Redpanda topic for the given category
// and type (e.g. category="telemetry", typ="metrics").
// Format: keel.{tenant_slug}.{fleet_id}.{device_id}.{category}.{typ}
// The static "keel." prefix allows a single PREFIXED ACL regardless of tenant slug.
func (d *DeviceInfo) RedpandaTopic(category, typ string) string {
	return fmt.Sprintf("keel.%s.%s.%s.%s.%s", d.TenantSlug, d.FleetIDStr, d.ID, category, typ)
}

// Validator validates MQTT credentials against the PostgreSQL devices database.
type Validator struct {
	pool *pgxpool.Pool
}

// NewValidator creates a new Validator using the given connection pool.
func NewValidator(pool *pgxpool.Pool) *Validator {
	return &Validator{pool: pool}
}

// Validate checks that the given deviceID (MQTT clientId) and token
// (MQTT password) correspond to a valid, active device credential of type
// "mqtt_token". Returns ErrInvalidCredentials on auth failure.
func (v *Validator) Validate(ctx context.Context, deviceID, token string) (*DeviceInfo, error) {
	id, err := uuid.Parse(deviceID)
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	const q = `
		SELECT d.id, d.tenant_id, t.slug, d.fleet_id
		FROM   devices.devices d
		JOIN   devices.tenants t ON t.id = d.tenant_id
		JOIN   devices.device_credentials c ON c.device_id = d.id
		WHERE  d.id = $1
		  AND  c.type = 'mqtt_token'
		  AND  c.value = $2
		  AND  (c.expires_at IS NULL OR c.expires_at > now())
		LIMIT 1`

	var info DeviceInfo
	var fleetID *uuid.UUID
	err = v.pool.QueryRow(ctx, q, id, token).Scan(
		&info.ID,
		&info.TenantID,
		&info.TenantSlug,
		&fleetID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidCredentials
	}
	if err != nil {
		return nil, fmt.Errorf("validate device credential: %w", err)
	}

	info.FleetID = fleetID
	if fleetID != nil {
		info.FleetIDStr = fleetID.String()
	} else {
		info.FleetIDStr = "nofleet"
	}

	return &info, nil
}

// UpdateLastSeen writes now() into devices.devices.last_seen for the given device.
// Called on every successful MQTT connect so the field stays current for all
// devices regardless of whether HawkBit polling covers them.
// Errors are intentionally ignored — the connection must not be rejected because
// of a non-critical metadata write failure.
func (v *Validator) UpdateLastSeen(ctx context.Context, deviceID uuid.UUID) {
	_, _ = v.pool.Exec(ctx,
		`UPDATE devices.devices SET last_seen = now() WHERE id = $1`,
		deviceID,
	)
}

// LookupByCN resolves a DeviceInfo using a device+tenant name pair derived from
// a certificate CommonName ("<deviceID>@<tenantID>").
// The device must exist in devices.devices; if it does not, ErrInvalidCredentials
// is returned, and the caller should handle auto-provisioning separately.
func (v *Validator) LookupByCN(ctx context.Context, deviceID, tenantID string) (*DeviceInfo, error) {
	devUUID, err := uuid.Parse(deviceID)
	if err != nil {
		return nil, ErrInvalidCredentials
	}
	tenantUUID, err := uuid.Parse(tenantID)
	if err != nil {
		return nil, ErrInvalidCredentials
	}

	const q = `
		SELECT d.id, d.tenant_id, t.slug, d.fleet_id
		FROM   devices.devices d
		JOIN   devices.tenants t ON t.id = d.tenant_id
		WHERE  d.id = $1 AND d.tenant_id = $2
		LIMIT 1`

	var info DeviceInfo
	var fleetID *uuid.UUID
	err = v.pool.QueryRow(ctx, q, devUUID, tenantUUID).Scan(
		&info.ID,
		&info.TenantID,
		&info.TenantSlug,
		&fleetID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidCredentials
	}
	if err != nil {
		return nil, fmt.Errorf("lookup device by CN: %w", err)
	}

	info.FleetID = fleetID
	if fleetID != nil {
		info.FleetIDStr = fleetID.String()
	} else {
		info.FleetIDStr = "nofleet"
	}
	return &info, nil
}

// Pool exposes the underlying pgxpool for use by TenantConfigCache.
func (v *Validator) Pool() *pgxpool.Pool {
	return v.pool
}

// NewPendingDeviceInfo constructs a synthetic DeviceInfo for a device that
// presented a valid X.509 certificate but does not yet exist in the database.
// This allows the CONNECT to succeed while auto-provisioning runs in parallel.
// UUID parsing is best-effort; invalid strings result in a zero UUID.
func NewPendingDeviceInfo(deviceID, tenantID string) *DeviceInfo {
	devUUID, _ := uuid.Parse(deviceID)
	tenantUUID, _ := uuid.Parse(tenantID)
	return &DeviceInfo{
		ID:         devUUID,
		TenantID:   tenantUUID,
		TenantSlug: "pending",
		FleetIDStr: "nofleet",
	}
}

// isNoRows returns true when err is a pgx "no rows" sentinel.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
