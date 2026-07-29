package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func generateRSAKey(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}))
	return priv, pubPEM
}

func generateECKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}))
	return priv, pubPEM
}

func makeJWT(t *testing.T, method jwt.SigningMethod, key any, tenantID, deviceID string, exp time.Time) []byte {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": deviceID,
		"tid": tenantID,
		"aud": jwt.ClaimStrings{"keel-gateway"},
		"iat": time.Now().Unix(),
		"exp": exp.Unix(),
	}
	tok, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(tok)
}

func selfSignedCert(t *testing.T, cn string) (*x509.Certificate, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	return cert, caPEM
}

// ── DetectAuthMethod ──────────────────────────────────────────────────────────

func TestDetectAuthMethod(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     auth.AuthMethod
	}{
		{"jwt prefix", "eyJhbGci.eyJ.sig", auth.AuthMethodJWT},
		{"jwt prefix 5 chars", "eyJxx", auth.AuthMethodJWT},
		{"plain token", "some-random-token", auth.AuthMethodPassword},
		{"empty", "", auth.AuthMethodPassword},
		{"short", "abc", auth.AuthMethodPassword},
		{"exactly 4 chars eyJ", "eyJx", auth.AuthMethodPassword}, // len==4 → password (boundary)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := auth.DetectAuthMethod([]byte(tc.password))
			if got != tc.want {
				t.Errorf("DetectAuthMethod(%q) = %q, want %q", tc.password, got, tc.want)
			}
		})
	}
}

// ── ValidateJWT (static PEM path) ──────────────────────────────────────────────

func pemCfg(pubPEM string) *auth.TenantGatewayConfig {
	return &auth.TenantGatewayConfig{JWTPublicKeyPEM: pubPEM}
}

func TestValidateJWT_RSA_Valid(t *testing.T) {
	priv, pubPEM := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", time.Now().Add(time.Hour))
	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, pemCfg(pubPEM), nil); err != nil {
		t.Fatalf("expected valid JWT, got: %v", err)
	}
}

func TestValidateJWT_EC_Valid(t *testing.T) {
	priv, pubPEM := generateECKey(t)
	tok := makeJWT(t, jwt.SigningMethodES256, priv, "tenant2", "device2", time.Now().Add(time.Hour))
	if err := auth.ValidateJWT(context.Background(), "tenant2", "device2", tok, pemCfg(pubPEM), nil); err != nil {
		t.Fatalf("expected valid JWT, got: %v", err)
	}
}

func TestValidateJWT_Expired(t *testing.T) {
	priv, pubPEM := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", time.Now().Add(-time.Minute))
	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, pemCfg(pubPEM), nil); err == nil {
		t.Fatal("expected error for expired JWT, got nil")
	}
}

func TestValidateJWT_WrongSub(t *testing.T) {
	priv, pubPEM := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenant1", "deviceX", time.Now().Add(time.Hour))
	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, pemCfg(pubPEM), nil); err == nil {
		t.Fatal("expected error for mismatched sub, got nil")
	}
}

func TestValidateJWT_WrongTenant(t *testing.T) {
	priv, pubPEM := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenantEVIL", "device1", time.Now().Add(time.Hour))
	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, pemCfg(pubPEM), nil); err == nil {
		t.Fatal("expected error for mismatched tid, got nil")
	}
}

func TestValidateJWT_NoPubKey(t *testing.T) {
	priv, _ := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", time.Now().Add(time.Hour))
	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, pemCfg(""), nil); err == nil {
		t.Fatal("expected error for empty public key, got nil")
	}
}

func TestValidateJWT_NotConfigured(t *testing.T) {
	priv, _ := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", time.Now().Add(time.Hour))
	err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, &auth.TenantGatewayConfig{}, nil)
	if !errors.Is(err, auth.ErrAuthNotConfigured) {
		t.Fatalf("expected ErrAuthNotConfigured, got: %v", err)
	}
}

// ── ValidateJWT (JWKS path) ─────────────────────────────────────────────────────

func rsaJWKForTest(kid string, priv *rsa.PrivateKey) map[string]any {
	eBytes := big.NewInt(int64(priv.PublicKey.E)).Bytes()
	return map[string]any{
		"kty": "RSA",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(eBytes),
	}
}

func jsonKeysHandler(t *testing.T, keys ...map[string]any) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	}
}

func makeJWTWithKid(t *testing.T, method jwt.SigningMethod, key any, tenantID, deviceID, kid string, exp time.Time) []byte {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": deviceID,
		"tid": tenantID,
		"aud": jwt.ClaimStrings{"keel-gateway"},
		"iat": time.Now().Unix(),
		"exp": exp.Unix(),
	}
	tok := jwt.NewWithClaims(method, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(signed)
}

func TestValidateJWT_JWKS_Valid(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(jsonKeysHandler(t, rsaJWKForTest("kid-1", priv)))
	defer srv.Close()

	tok := makeJWTWithKid(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", "kid-1", time.Now().Add(time.Hour))
	cfg := &auth.TenantGatewayConfig{JWKSURL: srv.URL}
	jwks := auth.NewJWKSCache(time.Minute)

	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, cfg, jwks); err != nil {
		t.Fatalf("expected valid JWT via JWKS, got: %v", err)
	}
}

func TestValidateJWT_JWKS_UnknownKid(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(jsonKeysHandler(t, rsaJWKForTest("kid-known", other)))
	defer srv.Close()

	tok := makeJWTWithKid(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", "kid-unknown", time.Now().Add(time.Hour))
	cfg := &auth.TenantGatewayConfig{JWKSURL: srv.URL}
	jwks := auth.NewJWKSCache(time.Minute)

	err = auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, cfg, jwks)
	if !errors.Is(err, auth.ErrInvalidKid) {
		t.Fatalf("expected ErrInvalidKid, got: %v", err)
	}
}

func TestValidateJWT_JWKS_TakesPrecedenceOverPEM(t *testing.T) {
	jwksPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemPriv, pemPub := generateRSAKey(t)
	srv := httptest.NewServer(jsonKeysHandler(t, rsaJWKForTest("kid-1", jwksPriv)))
	defer srv.Close()

	// Token signed by the PEM key, not the JWKS key — must fail because
	// JWKSURL being set means the static PEM is never consulted.
	tok := makeJWTWithKid(t, jwt.SigningMethodRS256, pemPriv, "tenant1", "device1", "kid-1", time.Now().Add(time.Hour))
	cfg := &auth.TenantGatewayConfig{JWKSURL: srv.URL, JWTPublicKeyPEM: pemPub}
	jwks := auth.NewJWKSCache(time.Minute)

	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, cfg, jwks); err == nil {
		t.Fatal("expected signature verification failure — PEM key must not be used when JWKSURL is set")
	}
}

func TestValidateJWT_JWKS_NoCacheProvided(t *testing.T) {
	priv, _ := generateRSAKey(t)
	tok := makeJWT(t, jwt.SigningMethodRS256, priv, "tenant1", "device1", time.Now().Add(time.Hour))
	cfg := &auth.TenantGatewayConfig{JWKSURL: "http://example.invalid/jwks"}
	if err := auth.ValidateJWT(context.Background(), "tenant1", "device1", tok, cfg, nil); err == nil {
		t.Fatal("expected error when JWKSURL is set but no JWKSCache was provided")
	}
}

// ── VerifyCertificate ─────────────────────────────────────────────────────────

func TestVerifyCertificate_CNOnly(t *testing.T) {
	cert, _ := selfSignedCert(t, "device-abc@tenant-123")
	devID, tenID, err := auth.VerifyCertificate(cert, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if devID != "device-abc" || tenID != "tenant-123" {
		t.Fatalf("wrong ids: device=%q tenant=%q", devID, tenID)
	}
}

func TestVerifyCertificate_ValidChain(t *testing.T) {
	cert, caPEM := selfSignedCert(t, "device-abc@tenant-123")
	devID, tenID, err := auth.VerifyCertificate(cert, []string{string(caPEM)})
	if err != nil {
		t.Fatalf("expected valid chain, got: %v", err)
	}
	if devID != "device-abc" || tenID != "tenant-123" {
		t.Fatalf("wrong ids: device=%q tenant=%q", devID, tenID)
	}
}

func TestVerifyCertificate_BadCN(t *testing.T) {
	cert, caPEM := selfSignedCert(t, "no-at-sign")
	_, _, err := auth.VerifyCertificate(cert, []string{string(caPEM)})
	if err == nil {
		t.Fatal("expected error for bad CN, got nil")
	}
}

func TestVerifyCertificate_EmptyCAs(t *testing.T) {
	cert, _ := selfSignedCert(t, "device@tenant")
	_, _, err := auth.VerifyCertificate(cert, []string{})
	if err == nil {
		t.Fatal("expected error for empty trusted CAs, got nil")
	}
}

func TestVerifyCertificate_UntrustedCA(t *testing.T) {
	cert, _ := selfSignedCert(t, "device@tenant")
	_, wrongCA := selfSignedCert(t, "other@other")
	_, _, err := auth.VerifyCertificate(cert, []string{string(wrongCA)})
	if err == nil {
		t.Fatal("expected error for untrusted CA, got nil")
	}
}

// ── ParseClientIdentifier ─────────────────────────────────────────────────────

func TestParseClientIdentifier(t *testing.T) {
	tests := []struct {
		name         string
		clientID     string
		wantTenantID string
		wantDeviceID string
		wantOK       bool
	}{
		{
			name:         "valid format",
			clientID:     "tenants/tid-123/devices/did-456",
			wantTenantID: "tid-123",
			wantDeviceID: "did-456",
			wantOK:       true,
		},
		{
			name:         "valid with UUIDs",
			clientID:     "tenants/550e8400-e29b-41d4-a716-446655440000/devices/6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			wantTenantID: "550e8400-e29b-41d4-a716-446655440000",
			wantDeviceID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			wantOK:       true,
		},
		{
			name:     "standard username format",
			clientID: "device1@tenant1",
			wantOK:   false,
		},
		{
			name:     "bare device id",
			clientID: "device1",
			wantOK:   false,
		},
		{
			name:     "wrong prefix",
			clientID: "clients/tid/devices/did",
			wantOK:   false,
		},
		{
			name:     "missing device part",
			clientID: "tenants/tid/other/did",
			wantOK:   false,
		},
		{
			name:     "empty tenant",
			clientID: "tenants//devices/did",
			wantOK:   false,
		},
		{
			name:     "empty device",
			clientID: "tenants/tid/devices/",
			wantOK:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tid, did, ok := auth.ParseClientIdentifier(tc.clientID)
			if ok != tc.wantOK {
				t.Fatalf("ParseClientIdentifier(%q) ok=%v, want %v", tc.clientID, ok, tc.wantOK)
			}
			if ok {
				if tid != tc.wantTenantID {
					t.Errorf("tenantID = %q, want %q", tid, tc.wantTenantID)
				}
				if did != tc.wantDeviceID {
					t.Errorf("deviceID = %q, want %q", did, tc.wantDeviceID)
				}
			}
		})
	}
}
