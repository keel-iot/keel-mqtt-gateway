package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
)

func deviceCAHandler(t *testing.T, reqCount *atomic.Int64, wantToken, pem string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		if wantToken != "" {
			if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
				t.Errorf("Authorization header = %q, want %q", got, "Bearer "+wantToken)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ca_certificate_pem": pem})
	}
}

func TestDeviceCACache_FetchAndCache(t *testing.T) {
	var reqs atomic.Int64
	srv := httptest.NewServer(deviceCAHandler(t, &reqs, "tok-1", "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"))
	defer srv.Close()

	c := auth.NewDeviceCACache(time.Hour)
	pems, err := c.TrustedCAPEMs(context.Background(), "tenant-1", srv.URL, "tok-1")
	if err != nil {
		t.Fatalf("TrustedCAPEMs: %v", err)
	}
	if len(pems) != 1 || pems[0] == "" {
		t.Fatalf("unexpected pems: %+v", pems)
	}
	if reqs.Load() != 1 {
		t.Fatalf("expected 1 request, got %d", reqs.Load())
	}
}

func TestDeviceCACache_CachedHitDoesNotRefetch(t *testing.T) {
	var reqs atomic.Int64
	srv := httptest.NewServer(deviceCAHandler(t, &reqs, "", "pem-content"))
	defer srv.Close()

	c := auth.NewDeviceCACache(time.Hour)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := c.TrustedCAPEMs(ctx, "tenant-1", srv.URL, ""); err != nil {
			t.Fatalf("TrustedCAPEMs: %v", err)
		}
	}
	if reqs.Load() != 1 {
		t.Fatalf("expected exactly 1 request across 5 calls within TTL, got %d", reqs.Load())
	}
}

func TestDeviceCACache_StaleServedOnRefreshError(t *testing.T) {
	var reqs atomic.Int64
	up := atomic.Bool{}
	up.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		if !up.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ca_certificate_pem": "good-pem"})
	}))
	defer srv.Close()

	c := auth.NewDeviceCACache(10 * time.Millisecond)
	ctx := context.Background()

	pems, err := c.TrustedCAPEMs(ctx, "tenant-1", srv.URL, "")
	if err != nil {
		t.Fatalf("initial fetch: %v", err)
	}
	if pems[0] != "good-pem" {
		t.Fatalf("pems = %+v, want good-pem", pems)
	}

	up.Store(false)
	time.Sleep(15 * time.Millisecond) // let TTL expire

	pems, err = c.TrustedCAPEMs(ctx, "tenant-1", srv.URL, "")
	if err != nil {
		t.Fatalf("expected stale-served success, got error: %v", err)
	}
	if pems[0] != "good-pem" {
		t.Fatalf("expected stale pem served, got %+v", pems)
	}
}

func TestDeviceCACache_NoCachedValueOnErrorReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := auth.NewDeviceCACache(time.Hour)
	if _, err := c.TrustedCAPEMs(context.Background(), "tenant-1", srv.URL, ""); err == nil {
		t.Fatal("expected error when no cached value exists and fetch fails")
	}
}

func TestDeviceCACache_EmptyCertificatePEMIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ca_certificate_pem": nil})
	}))
	defer srv.Close()

	c := auth.NewDeviceCACache(time.Hour)
	if _, err := c.TrustedCAPEMs(context.Background(), "tenant-1", srv.URL, ""); err == nil {
		t.Fatal("expected error for a null/empty ca_certificate_pem (CA not yet configured)")
	}
}

func TestDeviceCACache_MultiTenantIsolation(t *testing.T) {
	srv1 := httptest.NewServer(deviceCAHandler(t, new(atomic.Int64), "", "pem-tenant-1"))
	defer srv1.Close()
	srv2 := httptest.NewServer(deviceCAHandler(t, new(atomic.Int64), "", "pem-tenant-2"))
	defer srv2.Close()

	c := auth.NewDeviceCACache(time.Hour)
	ctx := context.Background()

	pems1, err := c.TrustedCAPEMs(ctx, "tenant-1", srv1.URL, "")
	if err != nil {
		t.Fatalf("tenant-1: %v", err)
	}
	pems2, err := c.TrustedCAPEMs(ctx, "tenant-2", srv2.URL, "")
	if err != nil {
		t.Fatalf("tenant-2: %v", err)
	}
	if pems1[0] != "pem-tenant-1" || pems2[0] != "pem-tenant-2" {
		t.Fatalf("cross-tenant leak: tenant-1=%+v tenant-2=%+v", pems1, pems2)
	}
}
