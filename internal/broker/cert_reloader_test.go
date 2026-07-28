package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeSelfSignedCert writes a self-signed cert/key pair valid for the given
// duration to dir/tls.crt and dir/tls.key.
func writeSelfSignedCert(t *testing.T, dir string, notAfter time.Time) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.keel.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(filepath.Join(dir, tlsCertFileName), certPEM, 0o600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(filepath.Join(dir, tlsKeyFileName), keyPEM, 0o600))
}

func TestCertReloader_NotReadyWhenCertAbsent(t *testing.T) {
	dir := t.TempDir()

	r, err := NewCertReloader(dir, nil)
	require.NoError(t, err)
	require.False(t, r.Ready())
	require.Error(t, r.LastError())

	_, err = r.GetCertificate(nil)
	require.Error(t, err)
}

func TestCertReloader_ReadyAfterCertAppears(t *testing.T) {
	dir := t.TempDir()

	r, err := NewCertReloader(dir, nil)
	require.NoError(t, err)
	require.False(t, r.Ready())

	writeSelfSignedCert(t, dir, time.Now().Add(24*time.Hour))

	require.Eventually(t, r.Ready, 2*time.Second, 20*time.Millisecond, "reloader should become ready after cert appears")

	cert, err := r.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, cert)
}

func TestCertReloader_NotReadyWhenCertExpired(t *testing.T) {
	dir := t.TempDir()
	writeSelfSignedCert(t, dir, time.Now().Add(-time.Hour))

	r, err := NewCertReloader(dir, nil)
	require.NoError(t, err)
	require.False(t, r.Ready())
	require.ErrorContains(t, r.LastError(), "expired")
}

func TestCertReloader_ReloadsOnRotation(t *testing.T) {
	dir := t.TempDir()
	writeSelfSignedCert(t, dir, time.Now().Add(1*time.Hour))

	r, err := NewCertReloader(dir, nil)
	require.NoError(t, err)
	require.Eventually(t, r.Ready, 2*time.Second, 20*time.Millisecond)

	first, err := r.GetCertificate(nil)
	require.NoError(t, err)

	// Rotate to a new cert (new key material) and confirm the reloader picks
	// it up without any restart.
	writeSelfSignedCert(t, dir, time.Now().Add(48*time.Hour))

	require.Eventually(t, func() bool {
		second, err := r.GetCertificate(nil)
		return err == nil && second != nil && string(second.Certificate[0]) != string(first.Certificate[0])
	}, 2*time.Second, 20*time.Millisecond, "reloader should pick up rotated certificate")
}
