package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
)

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func rsaJWK(kid string, priv *rsa.PrivateKey) map[string]any {
	return map[string]any{
		"kty": "RSA",
		"kid": kid,
		"n":   b64u(priv.PublicKey.N.Bytes()),
		"e":   b64u(big64(priv.PublicKey.E)),
	}
}

func big64(e int) []byte {
	// minimal big-endian encoding of a small int (e.g. 65537)
	b := []byte{byte(e >> 16), byte(e >> 8), byte(e)}
	i := 0
	for i < len(b)-1 && b[i] == 0 {
		i++
	}
	return b[i:]
}

func jwksHandler(t *testing.T, reqCount *atomic.Int64, delay time.Duration, keys ...map[string]any) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	}
}

func TestJWKSCache_FetchAndParseRSA(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var reqs atomic.Int64
	srv := httptest.NewServer(jwksHandler(t, &reqs, 0, rsaJWK("kid-1", priv)))
	defer srv.Close()

	c := auth.NewJWKSCache(time.Minute)
	key, err := c.Key(context.Background(), "tenant1", srv.URL, "kid-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pub, ok := key.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("expected *rsa.PublicKey, got %T", key)
	}
	if pub.N.Cmp(priv.PublicKey.N) != 0 || pub.E != priv.PublicKey.E {
		t.Fatal("decoded RSA key does not match source key")
	}
	if reqs.Load() != 1 {
		t.Fatalf("expected 1 HTTP request, got %d", reqs.Load())
	}
}

func TestJWKSCache_UnknownKid(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	var reqs atomic.Int64
	srv := httptest.NewServer(jwksHandler(t, &reqs, 0, rsaJWK("kid-1", priv)))
	defer srv.Close()

	c := auth.NewJWKSCache(time.Minute)
	_, err := c.Key(context.Background(), "tenant1", srv.URL, "kid-does-not-exist")
	if err == nil {
		t.Fatal("expected error for unknown kid, got nil")
	}
}

func TestJWKSCache_CachedHitDoesNotRefetch(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	var reqs atomic.Int64
	srv := httptest.NewServer(jwksHandler(t, &reqs, 0, rsaJWK("kid-1", priv)))
	defer srv.Close()

	c := auth.NewJWKSCache(time.Minute)
	ctx := context.Background()
	if _, err := c.Key(ctx, "tenant1", srv.URL, "kid-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Key(ctx, "tenant1", srv.URL, "kid-1"); err != nil {
		t.Fatal(err)
	}
	if reqs.Load() != 1 {
		t.Fatalf("expected 1 HTTP request across two hits within TTL, got %d", reqs.Load())
	}
}

// TestJWKSCache_SingleflightDedupesConcurrentMisses simulates a reconnect
// storm hitting an unknown/expired kid simultaneously: N goroutines must
// collapse into exactly one HTTP request.
func TestJWKSCache_SingleflightDedupesConcurrentMisses(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	var reqs atomic.Int64
	// Delay forces the goroutines below to actually overlap on the in-flight fetch.
	srv := httptest.NewServer(jwksHandler(t, &reqs, 50*time.Millisecond, rsaJWK("kid-1", priv)))
	defer srv.Close()

	c := auth.NewJWKSCache(time.Minute)

	const n = 100
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := c.Key(context.Background(), "tenant1", srv.URL, "kid-1")
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: unexpected error: %v", i, err)
		}
	}
	if got := reqs.Load(); got != 1 {
		t.Fatalf("expected exactly 1 HTTP request for %d concurrent misses, got %d", n, got)
	}
}

func TestJWKSCache_StaleServedOnRefreshError(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	var reqs atomic.Int64
	var fail atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{rsaJWK("kid-1", priv)}})
	}))
	defer srv.Close()

	// TTL=0 (well, a tiny positive value) so the second call is forced past
	// the freshness window and must attempt a refresh.
	c := auth.NewJWKSCache(1 * time.Millisecond)
	ctx := context.Background()

	if _, err := c.Key(ctx, "tenant1", srv.URL, "kid-1"); err != nil {
		t.Fatalf("initial fetch failed: %v", err)
	}

	time.Sleep(5 * time.Millisecond) // let the entry go stale
	fail.Store(true)

	key, err := c.Key(ctx, "tenant1", srv.URL, "kid-1")
	if err != nil {
		t.Fatalf("expected fail-open (stale key served), got error: %v", err)
	}
	if _, ok := key.(*rsa.PublicKey); !ok {
		t.Fatalf("expected stale *rsa.PublicKey to be served, got %T", key)
	}
	if reqs.Load() < 2 {
		t.Fatalf("expected a refresh attempt to have been made, got %d total requests", reqs.Load())
	}
}

func TestJWKSCache_ECKey(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jwk := map[string]any{
		"kty": "EC",
		"kid": "ec-1",
		"crv": "P-256",
		"x":   b64u(priv.PublicKey.X.Bytes()),
		"y":   b64u(priv.PublicKey.Y.Bytes()),
	}
	var reqs atomic.Int64
	srv := httptest.NewServer(jwksHandler(t, &reqs, 0, jwk))
	defer srv.Close()

	c := auth.NewJWKSCache(time.Minute)
	key, err := c.Key(context.Background(), "tenant1", srv.URL, "ec-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pub, ok := key.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("expected *ecdsa.PublicKey, got %T", key)
	}
	if pub.X.Cmp(priv.PublicKey.X) != 0 || pub.Y.Cmp(priv.PublicKey.Y) != 0 {
		t.Fatal("decoded EC key does not match source key")
	}
}

func TestJWKSCache_MultiTenantIsolation(t *testing.T) {
	priv1, _ := rsa.GenerateKey(rand.Reader, 2048)
	priv2, _ := rsa.GenerateKey(rand.Reader, 2048)
	var reqs1, reqs2 atomic.Int64
	srv1 := httptest.NewServer(jwksHandler(t, &reqs1, 0, rsaJWK("kid-1", priv1)))
	defer srv1.Close()
	srv2 := httptest.NewServer(jwksHandler(t, &reqs2, 0, rsaJWK("kid-1", priv2)))
	defer srv2.Close()

	c := auth.NewJWKSCache(time.Minute)
	ctx := context.Background()

	k1, err := c.Key(ctx, "tenant1", srv1.URL, "kid-1")
	if err != nil {
		t.Fatal(err)
	}
	k2, err := c.Key(ctx, "tenant2", srv2.URL, "kid-1")
	if err != nil {
		t.Fatal(err)
	}
	pub1 := k1.(*rsa.PublicKey)
	pub2 := k2.(*rsa.PublicKey)
	if pub1.N.Cmp(pub2.N) == 0 {
		t.Fatal("expected different tenants with the same kid to resolve to different keys")
	}
}
