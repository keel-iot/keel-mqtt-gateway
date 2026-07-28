package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/broker"
	"github.com/stretchr/testify/require"
)

func writeSelfSignedCert(t *testing.T, dir string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.keel.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0o600))
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0o600))
}

func TestReadyzHandler_TLSDisabled_AlwaysReady(t *testing.T) {
	var reloader *broker.CertReloader
	h := newReadyzHandler(false, &reloader)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 200, rec.Code)
}

func TestReadyzHandler_TLSEnabled_NotReadyWithoutCert(t *testing.T) {
	var reloader *broker.CertReloader
	h := newReadyzHandler(true, &reloader)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, rec.Code)
}

func TestReadyzHandler_TLSEnabled_ReadyOnceCertAppears(t *testing.T) {
	dir := t.TempDir()
	var reloader *broker.CertReloader
	h := newReadyzHandler(true, &reloader)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, rec.Code, "no cert dir configured yet: not ready")

	r, err := broker.NewCertReloader(dir, nil)
	require.NoError(t, err)
	reloader = r

	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, rec.Code, "cert dir empty: still not ready")

	writeSelfSignedCert(t, dir)
	require.Eventually(t, r.Ready, 2*time.Second, 20*time.Millisecond)

	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 200, rec.Code, "valid cert now present: ready")
}
