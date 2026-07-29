package auth

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TenantGatewayConfig holds the per-tenant gateway settings loaded from
// devices.tenant_gateway_config and cached in memory for a short TTL.
type TenantGatewayConfig struct {
	PasswordAuthEnabled bool
	JWTAuthEnabled      bool
	CertAuthEnabled     bool
	JWTPublicKeyPEM     string // RSA or EC public key in PEM format — static key, used when JWKSURL is empty
	// JWKSURL, when set, takes precedence over JWTPublicKeyPEM: JWTs are
	// verified by resolving their "kid" header against this tenant's JWKS
	// endpoint (see JWKSCache) instead of a single static key. Enables key
	// rotation (e.g. Clavex-issued device tokens) without a config update.
	JWKSURL       string
	TrustedCAPEMs []string // PEM CA certificates for X.509 device auth
	AutoProvisioning    bool
	MaxConnections      int   // 0 = unlimited
	MaxBytesPerDay      int64 // 0 = unlimited
	// TracingEnabled forces 100% trace sampling for this tenant when true.
	// When false the global sampling ratio (default 10%) applies.
	TracingEnabled bool
}

// TenantConfigCache loads TenantGatewayConfig from PostgreSQL and caches
// entries for ttl to reduce load on the database.
type TenantConfigCache struct {
	pool *pgxpool.Pool
	ttl  time.Duration

	mu    sync.RWMutex
	cache map[string]*cachedEntry
}

type cachedEntry struct {
	cfg       *TenantGatewayConfig
	expiresAt time.Time
}

// NewTenantConfigCache creates a cache with the given TTL.
// A TTL of 5 minutes is recommended for production.
func NewTenantConfigCache(pool *pgxpool.Pool, ttl time.Duration) *TenantConfigCache {
	return &TenantConfigCache{
		pool:  pool,
		ttl:   ttl,
		cache: make(map[string]*cachedEntry),
	}
}

// Get returns the TenantGatewayConfig for tenantID, loading from the database
// if the cache entry is absent or expired.
// Returns a default (password-auth-only) config when no row exists yet, so that
// tenants that have not configured the gateway still work normally.
func (c *TenantConfigCache) Get(ctx context.Context, tenantID string) (*TenantGatewayConfig, error) {
	c.mu.RLock()
	if entry, ok := c.cache[tenantID]; ok && time.Now().Before(entry.expiresAt) {
		cfg := entry.cfg
		c.mu.RUnlock()
		return cfg, nil
	}
	c.mu.RUnlock()

	cfg, err := c.load(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.cache[tenantID] = &cachedEntry{cfg: cfg, expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()

	return cfg, nil
}

// Invalidate forces the next Get for tenantID to reload from the database.
func (c *TenantConfigCache) Invalidate(tenantID string) {
	c.mu.Lock()
	delete(c.cache, tenantID)
	c.mu.Unlock()
}

const queryTenantGatewayConfig = `
SELECT
    password_auth_enabled,
    jwt_auth_enabled,
    cert_auth_enabled,
    COALESCE(jwt_public_key_pem, ''),
    COALESCE(jwks_url, ''),
    COALESCE(trusted_ca_pems, '{}'),
    auto_provisioning,
    COALESCE(max_connections, 0),
    COALESCE(max_bytes_per_day, 0),
    tracing_enabled
FROM devices.tenant_gateway_config
WHERE tenant_id = $1`

func (c *TenantConfigCache) load(ctx context.Context, tenantID string) (*TenantGatewayConfig, error) {
	cfg := &TenantGatewayConfig{
		PasswordAuthEnabled: true, // safe default when no row exists
	}

	row := c.pool.QueryRow(ctx, queryTenantGatewayConfig, tenantID)
	err := row.Scan(
		&cfg.PasswordAuthEnabled,
		&cfg.JWTAuthEnabled,
		&cfg.CertAuthEnabled,
		&cfg.JWTPublicKeyPEM,
		&cfg.JWKSURL,
		&cfg.TrustedCAPEMs,
		&cfg.AutoProvisioning,
		&cfg.MaxConnections,
		&cfg.MaxBytesPerDay,
		&cfg.TracingEnabled,
	)
	if err != nil {
		// pgx.ErrNoRows → no gateway config row yet — return the safe default.
		// Any other error is a real DB problem.
		if isNoRows(err) {
			return cfg, nil
		}
		return nil, err
	}
	return cfg, nil
}

// TracingEnabled returns true when the tenant has opted into 100% trace
// sampling. It is safe to call from multiple goroutines and returns false
// when the tenant config is not yet cached (fail-safe: use global ratio).
func (c *TenantConfigCache) TracingEnabled(tenantID string) bool {
	c.mu.RLock()
	entry, ok := c.cache[tenantID]
	c.mu.RUnlock()
	if !ok || entry == nil || entry.cfg == nil {
		return false
	}
	return entry.cfg.TracingEnabled
}
