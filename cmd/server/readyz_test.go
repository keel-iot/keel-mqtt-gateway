package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/broker"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/stretchr/testify/require"
)

// fakeRedisServer is the same minimal RESP-speaking listener as
// internal/cluster/redisrouter's test helper of the same name (duplicated
// rather than exported/imported across a test boundary — see that
// package's router_test.go for why HELLO/pipelining need special-casing).
// Unlike that version, this one also tracks accepted connections so tests
// can simulate the primary going unreachable *after* Router already holds
// a live connection — closing only the listener leaves an already-accepted
// TCP connection fully functional, since accept/serve run independently.
type fakeRedisServer struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

var respArrayHeader = regexp.MustCompile(`\*\d+\r\n`)

func startFakeRedisServer(t *testing.T) *fakeRedisServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &fakeRedisServer{ln: ln}
	go s.serve()
	t.Cleanup(s.closeAll)
	return s
}

func (s *fakeRedisServer) addr() string { return s.ln.Addr().String() }

// closeAll closes the listener plus every connection accepted so far —
// idempotent, safe to call from t.Cleanup even after an earlier explicit
// call in the test body.
func (s *fakeRedisServer) closeAll() {
	_ = s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
}

func (s *fakeRedisServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go func() {
			defer conn.Close()
			buf := make([]byte, 4096)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					return
				}
				data := buf[:n]
				count := len(respArrayHeader.FindAll(data, -1))
				if count == 0 {
					count = 1
				}
				var out bytes.Buffer
				for i := 0; i < count; i++ {
					if i == 0 && bytes.Contains(bytes.ToUpper(data), []byte("HELLO")) {
						out.WriteString("-ERR unknown command 'HELLO'\r\n")
						continue
					}
					out.WriteString("+PONG\r\n")
				}
				if _, err := conn.Write(out.Bytes()); err != nil {
					return
				}
			}
		}()
	}
}

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
	h := newReadyzHandler(false, &reloader, nil, nil)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 200, rec.Code)
}

func TestReadyzHandler_TLSEnabled_NotReadyWithoutCert(t *testing.T) {
	var reloader *broker.CertReloader
	h := newReadyzHandler(true, &reloader, nil, nil)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, rec.Code)
}

func TestReadyzHandler_TLSEnabled_ReadyOnceCertAppears(t *testing.T) {
	dir := t.TempDir()
	var reloader *broker.CertReloader
	h := newReadyzHandler(true, &reloader, nil, nil)

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

func TestReadyzHandler_NoRedisConfigured_RedisCheckSkipped(t *testing.T) {
	h := newReadyzHandler(false, new(*broker.CertReloader), nil, nil)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 200, rec.Code)
}

func TestReadyzHandler_RedisReachable_PrimaryKnown_Ready(t *testing.T) {
	srv := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rdb, err := redisrouter.New(ctx, srv.addr(), "")
	require.NoError(t, err)
	defer rdb.Close()

	h := newReadyzHandler(false, new(*broker.CertReloader), rdb, func() (string, bool) { return "core-0", true })

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 200, rec.Code)
}

func TestReadyzHandler_RedisConfigured_NoCurrentRedisPrimary_NotReady(t *testing.T) {
	srv := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rdb, err := redisrouter.New(ctx, srv.addr(), "")
	require.NoError(t, err)
	defer rdb.Close()

	h := newReadyzHandler(false, new(*broker.CertReloader), rdb, func() (string, bool) { return "", false })

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, rec.Code, "cluster mode but no primary designated yet: not ready")
}

func TestReadyzHandler_RedisUnreachable_NotReady(t *testing.T) {
	srv := startFakeRedisServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rdb, err := redisrouter.New(ctx, srv.addr(), "")
	require.NoError(t, err)
	defer rdb.Close()
	srv.closeAll() // simulate the primary going unreachable after Router was constructed

	h := newReadyzHandler(false, new(*broker.CertReloader), rdb, nil)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/readyz", nil))
	require.Equal(t, 503, rec.Code)
}
