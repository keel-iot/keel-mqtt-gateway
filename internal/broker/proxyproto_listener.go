package broker

import (
	"crypto/tls"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/pires/go-proxyproto"
)

// proxyProtoListener wraps a plain or TLS TCP listener with PROXY protocol
// v1/v2 parsing, mirroring mochi-mqtt's own listeners.TCP but inserting a
// proxyproto.Listener between the raw net.Listener and the optional TLS
// handshake so RemoteAddr() reports the real client IP once the header is
// verified. Can't reuse listeners.TCP directly and inject a wrapped
// net.Listener into it — TCP.Init always calls net.Listen itself.
type proxyProtoListener struct {
	id         string
	address    string
	tlsConfig  *tls.Config
	connPolicy proxyproto.ConnPolicyFunc

	mu     sync.RWMutex
	listen net.Listener
	log    *slog.Logger
	end    uint32
}

func newProxyProtoListener(id, address string, tlsConfig *tls.Config, connPolicy proxyproto.ConnPolicyFunc) *proxyProtoListener {
	return &proxyProtoListener{id: id, address: address, tlsConfig: tlsConfig, connPolicy: connPolicy}
}

func (l *proxyProtoListener) ID() string       { return l.id }
func (l *proxyProtoListener) Protocol() string { return "tcp" }

func (l *proxyProtoListener) Address() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.listen != nil {
		return l.listen.Addr().String()
	}
	return l.address
}

// Init opens the raw TCP socket, wraps it with PROXY protocol parsing, and
// — if configured — layers TLS on top of that. Ordering matters: a
// PROXY-protocol-speaking LB sends the header before any TLS ClientHello,
// so the header must be consumed first.
func (l *proxyProtoListener) Init(log *slog.Logger) error {
	l.log = log

	raw, err := net.Listen("tcp", l.address)
	if err != nil {
		return err
	}

	pl := &proxyproto.Listener{Listener: raw, ConnPolicy: l.connPolicy}

	l.mu.Lock()
	if l.tlsConfig != nil {
		l.listen = tls.NewListener(pl, l.tlsConfig)
	} else {
		l.listen = pl
	}
	l.mu.Unlock()

	return nil
}

func (l *proxyProtoListener) Serve(establish listeners.EstablishFn) {
	for {
		if atomic.LoadUint32(&l.end) == 1 {
			return
		}

		conn, err := l.listen.Accept()
		if err != nil {
			return
		}

		if atomic.LoadUint32(&l.end) == 0 {
			go func() {
				if err := establish(l.id, conn); err != nil {
					l.log.Warn("", "error", err)
				}
			}()
		}
	}
}

func (l *proxyProtoListener) Close(closeClients listeners.CloseFn) {
	if atomic.CompareAndSwapUint32(&l.end, 0, 1) {
		closeClients(l.id)
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.listen != nil {
		_ = l.listen.Close()
	}
}
