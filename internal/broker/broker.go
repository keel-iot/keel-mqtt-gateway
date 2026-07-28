// Package broker wraps the mochi-mqtt v2 server and wires together
// authentication, ACL, and message forwarding hooks.
package broker

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/redis/go-redis/v9"
)

// Config holds broker-specific settings extracted from the global config.
type Config struct {
	MQTTPort    int
	MQTTTLSPort int

	// TLSCertDir points at a directory containing tls.crt/tls.key (the
	// standard Kubernetes Secret volume layout). Required when MQTTTLSPort
	// is set. The pair is watched and reloaded automatically — see
	// CertReloader — so rotating the mounted Secret takes effect without a
	// restart.
	TLSCertDir string

	// TLSClientAuth selects tls.Config.ClientAuth: "none" (no client cert
	// requested), "request" (requested but optional — required for the
	// existing X.509 device-auth path in hooks.go's authenticateCert, so
	// this is the default), or "require-and-verify" (client cert mandatory,
	// verified against the configured root pool). Empty defaults to
	// "request".
	TLSClientAuth string

	// TenantConfigCache provides per-tenant gateway settings (JWT keys, trusted CAs, etc.).
	// When nil, all tenant lookups return the safe default (password-auth only).
	TenantConfigCache *auth.TenantConfigCache

	// AutoProvisioningURL is the device-service base URL used to register devices
	// that authenticate via X.509 for the first time. Empty = disabled.
	AutoProvisioningURL string

	// RedisClient is an optional pre-initialised Redis client.
	// When non-nil, a RedisSessionHook is installed to persist sessions,
	// subscriptions, and in-flight messages across broker restarts.
	// When nil the broker uses in-memory session state only.
	RedisClient *redis.Client

	// ClusterRegistry and ClusterForwarder wire this node into a keel MQTT
	// cluster (see internal/cluster): subscribe/unsubscribe/publish hooks
	// use them to keep the cluster-wide routing table in sync and forward
	// cross-node publishes. Both nil = standalone, single-node behaviour
	// (existing default).
	ClusterRegistry keelraft.Registry
	ClusterFwd      dataplane.Forwarder
	ClusterNodeID   string

	// OutputConnector forwards device messages to external systems (e.g., Ditto via Hono Kafka).
	// Nil = no external forwarding (default).
	OutputConnector connector.OutputConnector
}

// parseClientAuth maps a TLSClientAuth config string to a tls.ClientAuthType.
func parseClientAuth(v string) (tls.ClientAuthType, error) {
	switch v {
	case "", "request":
		return tls.RequestClientCert, nil
	case "none":
		return tls.NoClientCert, nil
	case "require-and-verify":
		return tls.RequireAndVerifyClientCert, nil
	default:
		return 0, fmt.Errorf("invalid tls client-auth %q (want none|request|require-and-verify)", v)
	}
}

// New creates and configures a mochi-mqtt v2 server with the Keel authentication
// and forwarding hooks applied. The returned *CertReloader is nil unless a TLS
// listener was configured; callers that expose a readiness endpoint should gate
// on its Ready() method — see cmd/server/main.go's /readyz handler.
func New(cfg Config, provider auth.AuthProvider, fwd *forwarder.Forwarder, log *slog.Logger) (*mqtt.Server, *CertReloader, error) {
	server := mqtt.New(&mqtt.Options{
		Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		// InlineClient is required for any server-side Server.Publish()
		// call — used by the commander (platform→device push) and, new in
		// this change, the cluster dataplane's inbound forward handler
		// (see cmd/server/main.go's gForwarder.Subscribe wiring).
		InlineClient: true,
	})

	// Redis session hook must be registered BEFORE the keel hook so that
	// stored sessions are available by the time the auth hook runs.
	if cfg.RedisClient != nil {
		redisHook := NewRedisSessionHook(cfg.RedisClient, log)
		if err := server.AddHook(redisHook, nil); err != nil {
			return nil, nil, fmt.Errorf("add redis session hook: %w", err)
		}
		log.Info("mqtt-gateway: Redis session persistence enabled")
	}

	hook := &keelHook{
		provider:         provider,
		tenantCache:      cfg.TenantConfigCache,
		fwd:              fwd,
		autoProvURL:      cfg.AutoProvisioningURL,
		log:              log,
		clusterRegistry:  cfg.ClusterRegistry,
		clusterFwd:       cfg.ClusterFwd,
		clusterNodeID:   cfg.ClusterNodeID,
		outputConnector: cfg.OutputConnector,
	}
	if err := server.AddHook(hook, nil); err != nil {
		return nil, nil, fmt.Errorf("add keel hook: %w", err)
	}

	// Plain-text MQTT listener (required)
	tcpListener := listeners.NewTCP(listeners.Config{
		ID:      "tcp",
		Address: fmt.Sprintf(":%d", cfg.MQTTPort),
	})
	if err := server.AddListener(tcpListener); err != nil {
		return nil, nil, fmt.Errorf("add TCP listener on :%d: %w", cfg.MQTTPort, err)
	}

	// TLS MQTT listener (optional — skipped when MQTTTLSPort is 0). The
	// certificate is served via CertReloader.GetCertificate rather than a
	// static tls.Config.Certificates so it can be rotated (K8s Secret
	// update, cert-manager renewal, ...) without a process restart.
	var reloader *CertReloader
	if cfg.MQTTTLSPort > 0 {
		if cfg.TLSCertDir == "" {
			return nil, nil, fmt.Errorf("MQTTTLSPort set but TLSCertDir is empty")
		}
		clientAuth, err := parseClientAuth(cfg.TLSClientAuth)
		if err != nil {
			return nil, nil, err
		}
		reloader, err = NewCertReloader(cfg.TLSCertDir, log)
		if err != nil {
			return nil, nil, fmt.Errorf("create tls cert reloader: %w", err)
		}
		tlsListener := listeners.NewTCP(listeners.Config{
			ID:      "tls",
			Address: fmt.Sprintf(":%d", cfg.MQTTTLSPort),
			TLSConfig: &tls.Config{
				GetCertificate: reloader.GetCertificate,
				ClientAuth:     clientAuth,
				MinVersion:     tls.VersionTLS12,
			},
		})
		if err := server.AddListener(tlsListener); err != nil {
			return nil, nil, fmt.Errorf("add TLS listener on :%d: %w", cfg.MQTTTLSPort, err)
		}
		log.Info("mqtt-gateway: TLS listener configured", "port", cfg.MQTTTLSPort, "cert_dir", cfg.TLSCertDir, "client_auth", cfg.TLSClientAuth)
	}

	return server, reloader, nil
}
