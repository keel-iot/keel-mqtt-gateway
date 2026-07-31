package auth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// fileCredential describes a single device entry in the YAML credential file.
type fileCredential struct {
	// DeviceID is the UUID of the device (required). Always used to
	// populate DeviceInfo.ID, regardless of Username below.
	DeviceID string `yaml:"device_id"`
	// Username is the MQTT CONNECT username this entry authenticates,
	// when it differs from DeviceID — e.g. a small set of static
	// service/bridge accounts (migrated from another broker) that use a
	// human-readable name instead of a UUID on the wire. Empty (the
	// common case: a real per-device credential) means the MQTT username
	// IS DeviceID, same as before this field existed.
	Username string `yaml:"username,omitempty"`
	// TenantID is the UUID of the tenant the device belongs to (required).
	TenantID string `yaml:"tenant_id"`
	// TenantSlug is the human-readable tenant slug used for topic routing.
	TenantSlug string `yaml:"tenant_slug"`
	// FleetID is the optional UUID of the fleet the device belongs to.
	FleetID string `yaml:"fleet_id,omitempty"`
	// PasswordHash is the bcrypt hash of the MQTT token for password auth.
	// Either password_hash or jwt_public_key_pem must be set.
	PasswordHash string `yaml:"password_hash,omitempty"`
	// JWTPublicKeyPEM is the PEM-encoded RSA or EC public key for JWT auth.
	JWTPublicKeyPEM string `yaml:"jwt_public_key_pem,omitempty"`
}

// credentialKey returns the string a connecting MQTT client must present as
// username to match this entry — Username when set, DeviceID otherwise.
func (c *fileCredential) credentialKey() string {
	if c.Username != "" {
		return c.Username
	}
	return c.DeviceID
}

// fileCredentials is the top-level structure of the YAML credential file.
type fileCredentials struct {
	Devices []fileCredential `yaml:"devices"`
}

// FileProvider implements AuthProvider using a static YAML file.
// It is intended for development, air-gapped environments, or bootstrap
// scenarios where no PostgreSQL database is available.
//
// File format (YAML):
//
//	devices:
//	  - device_id: "550e8400-e29b-41d4-a716-446655440000"
//	    tenant_id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
//	    tenant_slug: "acme"
//	    password_hash: "$2a$10$..."   # bcrypt hash of the MQTT token
//
// An entry can optionally set `username` to a non-UUID value when migrating
// a small set of static service/bridge accounts from another broker that
// used human-readable names instead of per-device UUIDs on the wire —
// device_id still populates DeviceInfo.ID and must still be a valid UUID.
type FileProvider struct {
	path string

	mu    sync.RWMutex
	byKey map[string]*fileCredential // keyed by credentialKey() — MQTT username

	credCache *credentialCache // caches recent successful validations (default 30s TTL)
}

// NewFileProviderWithTTL creates an AuthProvider that reads credentials from a YAML
// file at the given path. The file is loaded lazily on first use.
// Successful password validations are cached for the given TTL to reduce
// bcrypt load during reconnect storms.
func NewFileProviderWithTTL(path string, cacheTTL time.Duration) AuthProvider {
	return &FileProvider{
		path:      path,
		byKey:     make(map[string]*fileCredential),
		credCache: newCredentialCache(cacheTTL),
	}
}

// NewFileProvider creates an AuthProvider that reads credentials from a YAML
// file at the given path. The file is loaded lazily on first use.
// Successful password validations are cached for 30 seconds to reduce
// bcrypt load during reconnect storms.
func NewFileProvider(path string) AuthProvider {
	return NewFileProviderWithTTL(path, 30*time.Second)
}

func (f *FileProvider) load() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("file provider: read %q: %w", f.path, err)
	}

	var creds fileCredentials
	if err := yaml.Unmarshal(data, &creds); err != nil {
		return fmt.Errorf("file provider: parse %q: %w", f.path, err)
	}

	m := make(map[string]*fileCredential, len(creds.Devices))
	for i := range creds.Devices {
		c := &creds.Devices[i]
		if _, err := uuid.Parse(c.DeviceID); err != nil {
			return fmt.Errorf("file provider: invalid device_id %q: %w", c.DeviceID, err)
		}
		key := c.credentialKey()
		if _, dup := m[key]; dup {
			return fmt.Errorf("file provider: duplicate credential key %q", key)
		}
		m[key] = c
	}
	f.byKey = m

	// Invalidate credential cache on reload so revoked credentials force re-verification
	f.credCache.invalidate()

	return nil
}

func (f *FileProvider) get(username string) (*fileCredential, bool) {
	f.mu.RLock()
	c, ok := f.byKey[username]
	f.mu.RUnlock()
	return c, ok
}

// ValidatePassword checks the MQTT token against the bcrypt hash stored in the
// YAML file. username is whatever the MQTT client sent — a device UUID for a
// normal per-device entry, or the static value set in that entry's `username`
// field. Successful validations are cached for 30 seconds to reduce bcrypt
// load during reconnect storms.
func (f *FileProvider) ValidatePassword(_ context.Context, username, token string) (*DeviceInfo, error) {
	if len(f.byKey) == 0 {
		if err := f.load(); err != nil {
			return nil, err
		}
	}

	// Fast path: check cache for recent successful validation
	if f.credCache.check(username, token) {
		cred, ok := f.get(username)
		if !ok {
			// Credential disappeared since cache entry — should not happen in
			// practice since cache is invalidated on reload, but handle safely.
			return nil, ErrInvalidCredentials
		}
		return f.toDeviceInfo(cred)
	}

	// Cache miss or expired — do full bcrypt verification
	cred, ok := f.get(username)
	if !ok {
		return nil, ErrInvalidCredentials
	}
	if cred.PasswordHash == "" {
		return nil, ErrInvalidCredentials
	}
	if err := bcrypt.CompareHashAndPassword([]byte(cred.PasswordHash), []byte(token)); err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return nil, ErrInvalidCredentials
		}
		return nil, fmt.Errorf("file provider: bcrypt compare: %w", err)
	}

	// Success — populate cache for next time
	f.credCache.set(username, token)

	return f.toDeviceInfo(cred)
}

// LookupByCN resolves a device from the file by its UUID pair.
func (f *FileProvider) LookupByCN(_ context.Context, deviceID, tenantID string) (*DeviceInfo, error) {
	if len(f.byKey) == 0 {
		if err := f.load(); err != nil {
			return nil, err
		}
	}

	cred, ok := f.get(deviceID)
	if !ok {
		return nil, ErrInvalidCredentials
	}
	if cred.TenantID != tenantID {
		return nil, ErrInvalidCredentials
	}
	return f.toDeviceInfo(cred)
}

// UpdateLastSeen is a no-op for the file provider (no persistent storage).
func (f *FileProvider) UpdateLastSeen(_ context.Context, _ uuid.UUID) {}

func (f *FileProvider) toDeviceInfo(c *fileCredential) (*DeviceInfo, error) {
	devUUID, err := uuid.Parse(c.DeviceID)
	if err != nil {
		return nil, fmt.Errorf("file provider: invalid device_id: %w", err)
	}
	tenUUID, err := uuid.Parse(c.TenantID)
	if err != nil {
		return nil, fmt.Errorf("file provider: invalid tenant_id: %w", err)
	}

	info := &DeviceInfo{
		ID:         devUUID,
		TenantID:   tenUUID,
		TenantSlug: c.TenantSlug,
		FleetIDStr: "nofleet",
	}
	if c.FleetID != "" {
		if fid, err := uuid.Parse(c.FleetID); err == nil {
			info.FleetID = &fid
			info.FleetIDStr = fid.String()
		}
	}
	return info, nil
}
