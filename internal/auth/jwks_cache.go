// Package auth — JWKSCache fetches and caches per-tenant JSON Web Key Sets
// so device JWTs can be verified against a rotating key set (e.g. Clavex)
// instead of a single static PEM.
package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// JWKSCache fetches and caches JSON Web Key Sets (RFC 7517) per tenant,
// keyed by "kid". Reads never take a lock — each tenant's key map is held
// behind an atomic.Pointer and swapped wholesale on refresh. A cache miss
// (unknown kid, expired entry, or first use) triggers at most one HTTP
// refresh per tenant via singleflight, so a burst of simultaneous MQTT
// connects behind an unknown or just-rotated kid does not turn into one
// HTTP request per connection.
type JWKSCache struct {
	httpClient *http.Client
	ttl        time.Duration

	entries sync.Map // tenantID string -> *jwksEntry
	group   singleflight.Group
}

type jwksEntry struct {
	keys      atomic.Pointer[map[string]crypto.PublicKey]
	expiresAt atomic.Int64 // UnixNano; 0 = never fetched
}

// NewJWKSCache creates a cache that refreshes a given tenant's JWKS at most
// once per ttl. ttl <= 0 defaults to 5 minutes.
func NewJWKSCache(ttl time.Duration) *JWKSCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &JWKSCache{
		httpClient: &http.Client{Timeout: 5 * time.Second},
		ttl:        ttl,
	}
}

// Key returns the public key for (tenantID, kid). It serves from cache when
// the entry is fresh and the kid is known; otherwise it refreshes jwksURL,
// deduplicating concurrent refreshes for the same tenant so a reconnect
// storm behind an unknown kid results in exactly one HTTP call.
func (c *JWKSCache) Key(ctx context.Context, tenantID, jwksURL, kid string) (crypto.PublicKey, error) {
	e := c.entry(tenantID)

	if k, ok := lookupKey(e, kid); ok && c.fresh(e) {
		return k, nil
	}

	v, err, _ := c.group.Do(tenantID, func() (any, error) {
		return c.fetch(ctx, jwksURL)
	})
	if err != nil {
		// Refresh failed — serve a stale-but-known key rather than fail closed
		// on a transient JWKS endpoint outage (same fail-open posture as
		// TenantConfigCache).
		if k, ok := lookupKey(e, kid); ok {
			return k, nil
		}
		return nil, fmt.Errorf("jwks: refresh %q: %w", jwksURL, err)
	}

	keys := v.(map[string]crypto.PublicKey)
	e.keys.Store(&keys)
	e.expiresAt.Store(time.Now().Add(c.ttl).UnixNano())

	k, ok := keys[kid]
	if !ok {
		return nil, fmt.Errorf("jwks: kid %q not found at %q", kid, jwksURL)
	}
	return k, nil
}

func (c *JWKSCache) entry(tenantID string) *jwksEntry {
	v, _ := c.entries.LoadOrStore(tenantID, &jwksEntry{})
	return v.(*jwksEntry)
}

func (c *JWKSCache) fresh(e *jwksEntry) bool {
	exp := e.expiresAt.Load()
	return exp != 0 && time.Now().UnixNano() < exp
}

func lookupKey(e *jwksEntry, kid string) (crypto.PublicKey, bool) {
	m := e.keys.Load()
	if m == nil {
		return nil, false
	}
	k, ok := (*m)[kid]
	return k, ok
}

// jwkSet is the RFC 7517 JWKS document shape.
type jwkSet struct {
	Keys []jwk `json:"keys"`
}

// jwk decodes only the RSA/EC fields Keel's device JWTs use (RFC 7518);
// unsupported key types are skipped, not treated as a fatal parse error.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (c *JWKSCache) fetch(ctx context.Context, jwksURL string) (map[string]crypto.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jwksURL, nil)
	if err != nil {
		return nil, err
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

	var set jwkSet
	if err := json.Unmarshal(body, &set); err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}

	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kid == "" {
			continue
		}
		pub, err := k.publicKey()
		if err != nil {
			continue // skip key types Keel doesn't support rather than fail the whole set
		}
		keys[k.Kid] = pub
	}
	return keys, nil
}

func (k *jwk) publicKey() (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		nb, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			return nil, fmt.Errorf("jwk %q: decode n: %w", k.Kid, err)
		}
		eb, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			return nil, fmt.Errorf("jwk %q: decode e: %w", k.Kid, err)
		}
		return &rsa.PublicKey{
			N: new(big.Int).SetBytes(nb),
			E: int(new(big.Int).SetBytes(eb).Int64()),
		}, nil

	case "EC":
		curve, err := ecCurve(k.Crv)
		if err != nil {
			return nil, fmt.Errorf("jwk %q: %w", k.Kid, err)
		}
		xb, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, fmt.Errorf("jwk %q: decode x: %w", k.Kid, err)
		}
		yb, err := base64.RawURLEncoding.DecodeString(k.Y)
		if err != nil {
			return nil, fmt.Errorf("jwk %q: decode y: %w", k.Kid, err)
		}
		return &ecdsa.PublicKey{
			Curve: curve,
			X:     new(big.Int).SetBytes(xb),
			Y:     new(big.Int).SetBytes(yb),
		}, nil

	default:
		return nil, fmt.Errorf("jwk %q: unsupported kty %q", k.Kid, k.Kty)
	}
}

func ecCurve(crv string) (elliptic.Curve, error) {
	switch crv {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("unsupported EC curve %q", crv)
	}
}
