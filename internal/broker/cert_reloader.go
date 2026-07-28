package broker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	tlsCertFileName = "tls.crt"
	tlsKeyFileName  = "tls.key"

	// reloadDebounce absorbs the burst of fsnotify events a certificate
	// rotation produces (e.g. a Kubernetes Secret volume swaps its "..data"
	// symlink target, which fires several events on the watched directory
	// in quick succession) so the pair is only re-read once, after both
	// files have finished being written.
	reloadDebounce = 200 * time.Millisecond
)

// CertReloader watches a directory containing a tls.crt/tls.key pair (the
// standard Kubernetes Secret volume layout) and serves the current
// certificate via GetCertificate, reloading it automatically on change with
// no process restart. It also tracks whether the currently loaded
// certificate is valid and unexpired, via Ready, so callers can gate
// readiness on it instead of silently falling back to a stale or absent
// certificate.
type CertReloader struct {
	dir string
	log *slog.Logger

	mu      sync.RWMutex
	cert    *tls.Certificate
	loadErr error
}

// NewCertReloader creates a CertReloader watching dir for tls.crt/tls.key
// changes. The initial load is best-effort: if the certificate is missing or
// invalid at startup, NewCertReloader still returns successfully (with
// Ready() == false) so the caller can start serving and surface the problem
// via a readiness endpoint rather than crashing — the watcher keeps retrying
// as files appear or change.
func NewCertReloader(dir string, log *slog.Logger) (*CertReloader, error) {
	if log == nil {
		log = slog.Default()
	}
	r := &CertReloader{dir: dir, log: log}
	r.reload()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}
	if err := watcher.Add(dir); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("watch tls cert dir %s: %w", dir, err)
	}
	go r.watch(watcher)
	return r, nil
}

// watch re-reads the certificate pair on any filesystem event under dir,
// debounced so a burst of events (e.g. a Secret volume's atomic symlink
// swap) triggers exactly one reload.
func (r *CertReloader) watch(watcher *fsnotify.Watcher) {
	var timer *time.Timer
	for {
		select {
		case _, ok := <-watcher.Events:
			if !ok {
				return
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(reloadDebounce, r.reload)
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			r.log.Error("tls cert reloader: watch error", "error", err)
		}
	}
}

func (r *CertReloader) reload() {
	certPath := filepath.Join(r.dir, tlsCertFileName)
	keyPath := filepath.Join(r.dir, tlsKeyFileName)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		r.setError(fmt.Errorf("load tls key pair: %w", err))
		return
	}
	if len(cert.Certificate) == 0 {
		r.setError(fmt.Errorf("tls key pair has an empty certificate chain"))
		return
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		r.setError(fmt.Errorf("parse leaf certificate: %w", err))
		return
	}
	if now := time.Now(); now.After(leaf.NotAfter) {
		r.setError(fmt.Errorf("certificate expired at %s", leaf.NotAfter))
		return
	}
	cert.Leaf = leaf

	r.mu.Lock()
	r.cert = &cert
	r.loadErr = nil
	r.mu.Unlock()
	r.log.Info("tls cert reloader: certificate loaded", "not_after", leaf.NotAfter)
}

func (r *CertReloader) setError(err error) {
	r.mu.Lock()
	r.loadErr = err
	r.mu.Unlock()
	r.log.Error("tls cert reloader: reload failed", "error", err)
}

// GetCertificate implements the tls.Config.GetCertificate callback.
func (r *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.loadErr != nil {
		return nil, r.loadErr
	}
	return r.cert, nil
}

// Ready reports whether a valid, unexpired certificate is currently loaded.
func (r *CertReloader) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert != nil && r.loadErr == nil
}

// LastError returns the most recent reload error, if any (nil when Ready).
func (r *CertReloader) LastError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadErr
}
