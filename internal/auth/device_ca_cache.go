// Package auth — DeviceCACache fetches and caches per-tenant device CA
// certificates from an external custodian (e.g. Clavex's Device PKI), so
// keel-mqtt-gateway never persists CA material at rest — only a short-lived
// in-memory copy, refreshed periodically. Mirrors JWKSCache's design
// (no-lock reads via atomic.Pointer, singleflight-deduped refresh,
// fail-open to the last known-good value on a transient fetch error).
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// DeviceCACache fetches and caches a tenant's trusted device CA PEM(s) from
// a URL (e.g. Clavex's GET .../organizations/<org_id>/devices/ca),
// authenticated with a bearer token. A cache miss (expired entry, first
// use) triggers at most one HTTP refresh per tenant via singleflight.
type DeviceCACache struct {
	httpClient *http.Client
	ttl        time.Duration

	entries sync.Map // tenantID string -> *deviceCAEntry
	group   singleflight.Group
}

type deviceCAEntry struct {
	caPEMs    atomic.Pointer[[]string]
	expiresAt atomic.Int64 // UnixNano; 0 = never fetched
}

// NewDeviceCACache creates a cache that refreshes a given tenant's device
// CA at most once per ttl. ttl <= 0 defaults to 5 minutes.
func NewDeviceCACache(ttl time.Duration) *DeviceCACache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &DeviceCACache{
		httpClient: &http.Client{Timeout: 5 * time.Second},
		ttl:        ttl,
	}
}

// TrustedCAPEMs returns the cached device CA PEM(s) for tenantID, refreshing
// from caURL (bearer-authenticated with token) when the entry is stale.
// Deduplicates concurrent refreshes for the same tenant, and serves a
// stale-but-known CA rather than fail closed on a transient outage of the
// custodian (same fail-open posture as JWKSCache.Key/TenantConfigCache).
func (c *DeviceCACache) TrustedCAPEMs(ctx context.Context, tenantID, caURL, token string) ([]string, error) {
	e := c.entry(tenantID)

	if pems, ok := lookupPEMs(e); ok && c.fresh(e) {
		return pems, nil
	}

	v, err, _ := c.group.Do(tenantID, func() (any, error) {
		return c.fetch(ctx, caURL, token)
	})
	if err != nil {
		if pems, ok := lookupPEMs(e); ok {
			return pems, nil
		}
		return nil, fmt.Errorf("device-ca: refresh %q: %w", caURL, err)
	}

	pems := v.([]string)
	e.caPEMs.Store(&pems)
	e.expiresAt.Store(time.Now().Add(c.ttl).UnixNano())
	return pems, nil
}

func (c *DeviceCACache) entry(tenantID string) *deviceCAEntry {
	v, _ := c.entries.LoadOrStore(tenantID, &deviceCAEntry{})
	return v.(*deviceCAEntry)
}

func (c *DeviceCACache) fresh(e *deviceCAEntry) bool {
	exp := e.expiresAt.Load()
	return exp != 0 && time.Now().UnixNano() < exp
}

func lookupPEMs(e *deviceCAEntry) ([]string, bool) {
	p := e.caPEMs.Load()
	if p == nil {
		return nil, false
	}
	return *p, true
}

// deviceCAResponse decodes only the field this cache needs from Clavex's
// GetDeviceCA response (internal/repository.DeviceCAConfig upstream) —
// vault_addr/vault_mount/vault_role/etc are ignored, never the Vault token
// itself (Clavex's response never includes it).
type deviceCAResponse struct {
	CACertificatePEM *string `json:"ca_certificate_pem"`
}

func (c *DeviceCACache) fetch(ctx context.Context, caURL, token string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, caURL, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var out deviceCAResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("parse device CA response: %w", err)
	}
	if out.CACertificatePEM == nil || *out.CACertificatePEM == "" {
		return nil, fmt.Errorf("device CA not configured (empty ca_certificate_pem)")
	}
	return []string{*out.CACertificatePEM}, nil
}
