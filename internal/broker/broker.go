// Package broker wraps the mochi-mqtt v2 server and wires together
// authentication, ACL, and message forwarding hooks.
package broker

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/pires/go-proxyproto"
)

// Config holds broker-specific settings extracted from the global config.
type Config struct {
	MQTTPort    int
	MQTTTLSPort int

	// MQTTWSPort and MQTTWSSPort add a WebSocket (RFC 6455, "mqtt"
	// subprotocol) listener alongside the raw TCP one(s) — same auth/ACL
	// hooks, same everything except framing. Zero disables each
	// independently; neither depends on the other or on MQTTPort/
	// MQTTTLSPort being set. MQTTWSSPort shares TLSCertDir/TLSClientAuth
	// with MQTTTLSPort — one certificate, one reload mechanism, not a
	// second TLS story to maintain.
	MQTTWSPort  int
	MQTTWSSPort int

	// ProxyProtocol enables PROXY protocol v1/v2 parsing (github.com/pires/
	// go-proxyproto) on the plain TCP and TLS TCP listeners, for
	// deployments sitting Keel behind an L4 load balancer/proxy that needs
	// to preserve the real client IP. Once parsed, RemoteAddr() on the
	// resulting connection reports the client's address instead of the
	// LB's — everything downstream (audit logs, cl.Net.Remote) picks it up
	// for free, no separate plumbing. Does not apply to the WS/WSS
	// listeners (mochi-mqtt's websocket listener has no equivalent hook to
	// wrap). False by default: zero behaviour change for every deployment
	// not behind a PROXY-protocol-speaking LB.
	ProxyProtocol bool

	// ProxyProtocolTrustedCIDRs restricts which upstream peers are allowed
	// to send a PROXY header at all — required whenever ProxyProtocol is
	// true. A PROXY header is otherwise just an unauthenticated claim about
	// the sender's own address; without a trusted-source allowlist, any
	// client that can reach the port could spoof its IP. Connections from
	// outside every listed CIDR are rejected outright, never silently
	// trusted or silently downgraded to the raw address.
	ProxyProtocolTrustedCIDRs []string

	// TLSCertDir points at a directory containing tls.crt/tls.key (the
	// standard Kubernetes Secret volume layout). Required when MQTTTLSPort
	// or MQTTWSSPort is set. The pair is watched and reloaded automatically
	// — see CertReloader — so rotating the mounted Secret takes effect
	// without a restart.
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

	// JWKSCache resolves per-tenant JWT signing keys by "kid" for tenants
	// configured with TenantGatewayConfig.JWKSURL instead of a static PEM.
	// Nil disables JWKS-based auth — tenants with JWKSURL set will fail every
	// JWT connect until this is configured.
	JWKSCache *auth.JWKSCache

	// DeviceCACache resolves a tenant's device CA live from an external
	// custodian (e.g. Clavex) for tenants configured with
	// TenantGatewayConfig.ClavexCAURL, instead of the static
	// TrustedCAPEMs column. Nil falls back to that static column
	// unconditionally (unchanged behavior).
	DeviceCACache *auth.DeviceCACache

	// AutoProvisioningURL is the device-service base URL used to register devices
	// that authenticate via X.509 for the first time. Empty = disabled.
	AutoProvisioningURL string

	// RedisClient is an optional pre-initialised Redis router (see
	// internal/cluster/redisrouter — the single swappable indirection point
	// every Redis consumer shares, so a primary failover updates all of
	// them at once).
	// When non-nil, a RedisSessionHook is installed to persist sessions,
	// subscriptions, and in-flight messages across broker restarts.
	// When nil the broker uses in-memory session state only.
	RedisClient *redisrouter.Router

	// ClusterRegistry and ClusterForwarder wire this node into a keel MQTT
	// cluster (see internal/cluster): subscribe/unsubscribe/publish hooks
	// use them to keep the cluster-wide routing table in sync and forward
	// cross-node publishes. Both nil = standalone, single-node behaviour
	// (existing default).
	ClusterRegistry keelraft.Registry
	ClusterFwd      dataplane.Forwarder
	ClusterNodeID   string

	// LiveEdgeNodeIDs returns the currently gossip-visible edge nodes, used
	// to eagerly place/clear offline-session ownership on disconnect and
	// reconnect. Nil just disables that shortcut; the periodic
	// session.Reconciler still catches up on its own next tick.
	LiveEdgeNodeIDs func() []string

	// OfflineDedupTTL bounds DeliverOffline's MarkDelivered markers — see
	// that function's doc. Zero uses session.OfflineDelivery's own
	// caller-supplied default (see cmd/server/main.go's wiring).
	OfflineDedupTTL time.Duration

	// OutputConnectors forward device messages to external systems (e.g.,
	// Ditto via Hono Kafka, or an attached plugin sidecar — see
	// internal/connector/pluginhost). Each entry is fanned out to
	// independently and in parallel; empty = no external forwarding
	// (default).
	OutputConnectors []connector.OutputConnector

	// SessionExpiryInterval bounds how long a persistent (clean_session=false)
	// session's offline QoS1/2 queue, ACL identity (keelHook.OnClientExpired),
	// and cluster routing entry survive after a disconnect with no reconnect.
	// Zero uses mochi-mqtt's own default (effectively unbounded).
	SessionExpiryInterval time.Duration

	// LiveStats feeds the basic monitoring UI's messages/sec figure (see
	// internal/telemetry.LiveStats and GET /api/live/stats). Nil disables
	// tracking (a no-op — RecordPublish on a nil *LiveStats is guarded in
	// hooks.go), same posture as every other optional Config field here.
	LiveStats *telemetry.LiveStats

	// DefaultTenantID is used as the tenant identity for password-auth
	// CONNECTs whose username has no "<deviceID>@<tenantID>" separator —
	// e.g. accounts ported as-is from another broker (VerneMQ) that never
	// used that convention. Empty (default) preserves today's behaviour:
	// no "@" means an empty tenantID, which fails tenant-config lookup
	// and rejects the connect. Only meaningful for single-tenant
	// deployments — multi-tenant setups must keep every username
	// tenant-qualified instead of relying on this fallback.
	DefaultTenantID string

	// MaxKeepAlive caps the MQTT5 Keep Alive interval a client may
	// request — see MaxKeepAliveHook's doc for the full semantics
	// (MQTT5-only, deliberately; zero treated as "exceeds the maximum").
	// Zero (default) disables the cap entirely: no hook is even
	// registered, so behaviour is byte-for-byte identical to before this
	// field existed.
	MaxKeepAlive time.Duration

	// ConnectRateLimitPerSec/ConnectRateLimitBurst throttle CONNECT
	// attempts per source IP (cl.Net.Remote's host — the real client IP
	// when behind a trusted PROXY-protocol-speaking LB, see
	// ProxyProtocolTrustedCIDRs above). Checked before authenticate() runs,
	// so brute-force attempts are throttled regardless of credential
	// validity. Rejected the same way an over-MaxConnections attempt
	// already is: false from OnConnectAuthenticate, which mochi-mqtt
	// always answers with CONNACK reason ErrBadUsernameOrPassword — there
	// is no "rate limited" CONNACK reason available through this hook (a
	// mochi-mqtt API limitation, not a Keel design choice). Both zero
	// (default) disables the limiter entirely; config.Load rejects any
	// other zero/nonzero combination of the pair.
	ConnectRateLimitPerSec float64
	ConnectRateLimitBurst  int

	// PublishRateLimitPerSec/PublishRateLimitBurst throttle PUBLISH per
	// tenant. MQTT5 QoS1/2 gets a real PUBACK/PUBREC reason 0x97 (Quota
	// Exceeded); QoS0 and MQTT 3.1.1 (no per-packet ack reason mechanism
	// for either) get the message silently dropped instead — the same
	// "only override what the protocol lets the client learn about"
	// posture as MaxKeepAliveHook. Same zero/nonzero pairing rule as the
	// connect limiter above.
	PublishRateLimitPerSec float64
	PublishRateLimitBurst  int
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
func New(cfg Config, provider auth.AuthProvider, log *slog.Logger) (*mqtt.Server, *CertReloader, error) {
	opts := &mqtt.Options{
		Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		// InlineClient is required for any server-side Server.Publish()
		// call — used by the commander (platform→device push) and, new in
		// this change, the cluster dataplane's inbound forward handler
		// (see cmd/server/main.go's gForwarder.Subscribe wiring).
		InlineClient: true,
	}
	opts.Capabilities = mqtt.NewDefaultServerCapabilities()
	if cfg.SessionExpiryInterval > 0 {
		// Bounds how long mochi-mqtt keeps a persistent (clean_session=false)
		// session's Client object — and therefore its offline QoS1/2 queue —
		// alive after a disconnect with no reconnect. keelHook.OnClientExpired
		// is called exactly when this elapses, which is where the matching
		// ACL identity (h.clients) and cluster routing entry are torn down.
		opts.Capabilities.MaximumSessionExpiryInterval = uint32(cfg.SessionExpiryInterval.Seconds())
	}
	// Not conformance scaffolding — a real MQTT5 semantics fix. Without
	// this, mochi-mqtt's buildAck echoes the original PUBLISH packet's
	// Properties (including arbitrary User Properties) onto the
	// PUBACK/PUBREC — never desired, spec-correct or not. Root-caused
	// 2026-08-07 via internal/conformance's Eclipse Paho suite run (see
	// docs/alternatives-and-future-work.md); mochi-mqtt itself documents
	// this flag as "(paho - spec violation)" but the fix is unconditional
	// on every deployment, not just the conformance harness — unlike
	// ObscureNotAuthorized/KeepAliveHook (see internal/conformance),
	// which change observable client-facing behavior and stay
	// conformance-only until deliberately designed as product features.
	opts.Capabilities.Compatibilities.NoInheritedPropertiesOnAck = true
	server := mqtt.New(opts)

	// Redis session hook must be registered BEFORE the keel hook so that
	// stored sessions are available by the time the auth hook runs.
	var redisHook *RedisSessionHook
	var retainedStore *RetainedStore
	if cfg.RedisClient != nil {
		redisHook = NewRedisSessionHook(cfg.RedisClient, log)
		if err := server.AddHook(redisHook, nil); err != nil {
			return nil, nil, fmt.Errorf("add redis session hook: %w", err)
		}
		log.Info("mqtt-gateway: Redis session persistence enabled")

		retainedStore = NewRetainedStore(cfg.RedisClient)
		log.Info("mqtt-gateway: Redis-backed retained messages enabled")
	}

	// Only constructed when actually configured — zero behaviour change
	// for every deployment that doesn't set either pair, same "absent
	// means untouched" posture as MaxKeepAlive above. config.Load already
	// validated each pair is either both-zero or both-positive.
	var connectLimiter, publishLimiter *keyedRateLimiter
	if cfg.ConnectRateLimitPerSec > 0 {
		connectLimiter = newKeyedRateLimiter(cfg.ConnectRateLimitPerSec, cfg.ConnectRateLimitBurst, rateLimiterTTL, rateLimiterSweepInterval)
	}
	if cfg.PublishRateLimitPerSec > 0 {
		publishLimiter = newKeyedRateLimiter(cfg.PublishRateLimitPerSec, cfg.PublishRateLimitBurst, rateLimiterTTL, rateLimiterSweepInterval)
	}

	hook := &keelHook{
		provider:         provider,
		tenantCache:      cfg.TenantConfigCache,
		jwksCache:        cfg.JWKSCache,
		deviceCACache:    cfg.DeviceCACache,
		retainedStore:    retainedStore,
		rdb:              cfg.RedisClient,
		autoProvURL:      cfg.AutoProvisioningURL,
		log:              log,
		clusterRegistry:  cfg.ClusterRegistry,
		clusterFwd:       cfg.ClusterFwd,
		clusterNodeID:    cfg.ClusterNodeID,
		outputConnectors: cfg.OutputConnectors,
		server:           server,
		liveStats:        cfg.LiveStats,
		defaultTenantID:  cfg.DefaultTenantID,
		liveEdgeNodeIDs:  cfg.LiveEdgeNodeIDs,
		offlineDedupTTL:  cfg.OfflineDedupTTL,
		connectLimiter:   connectLimiter,
		publishLimiter:   publishLimiter,
	}
	// Typed nil guard: an interface value holding a nil *RedisSessionHook is
	// itself non-nil, which would defeat OnSessionEstablish's `h.sessionStore
	// == nil` check — only assign when Redis is actually configured.
	if redisHook != nil {
		hook.sessionStore = redisHook
		hook.offlineDeliveryStore = redisHook
	}
	// Same typed-nil concern as above: only assign when there's an actual
	// cluster registry to back it (offline-session ownership is meaningless
	// standalone — no membership, no rendezvous).
	if cfg.ClusterRegistry != nil {
		hook.offlineOwnership = &keelraft.OfflineOwnership{Registry: cfg.ClusterRegistry}
	}
	if err := server.AddHook(hook, nil); err != nil {
		return nil, nil, fmt.Errorf("add keel hook: %w", err)
	}

	// Only registered when actually configured — zero behaviour change
	// for every deployment that doesn't set MaxKeepAlive (matches the
	// same "absent means untouched" posture as every other optional
	// Config field here).
	if cfg.MaxKeepAlive > 0 {
		maxSeconds := cfg.MaxKeepAlive.Seconds()
		if maxSeconds > 65535 {
			maxSeconds = 65535 // config.Load already rejects this; defensive floor for any other caller
		}
		if err := server.AddHook(NewMaxKeepAliveHook(uint16(maxSeconds)), nil); err != nil {
			return nil, nil, fmt.Errorf("add max keepalive hook: %w", err)
		}
	}

	// PROXY protocol connection policy, shared by the plain TCP and TLS
	// listeners below. Built once and validated up front — an empty
	// TrustedCIDRs list is a misconfiguration, not something to silently
	// fall back from, since the alternative (trust everyone, or trust no
	// one) is exactly the spoofing/no-op failure mode PROXY protocol
	// exists to avoid.
	var connPolicy proxyproto.ConnPolicyFunc
	if cfg.ProxyProtocol {
		if len(cfg.ProxyProtocolTrustedCIDRs) == 0 {
			return nil, nil, fmt.Errorf("ProxyProtocol enabled but ProxyProtocolTrustedCIDRs is empty")
		}
		var err error
		connPolicy, err = proxyproto.TrustProxyHeaderFromRanges(cfg.ProxyProtocolTrustedCIDRs)
		if err != nil {
			return nil, nil, fmt.Errorf("parse ProxyProtocolTrustedCIDRs: %w", err)
		}
	}

	// Plain-text MQTT listener (required)
	var tcpListener listeners.Listener
	if cfg.ProxyProtocol {
		tcpListener = newProxyProtoListener("tcp", fmt.Sprintf(":%d", cfg.MQTTPort), nil, connPolicy)
	} else {
		tcpListener = listeners.NewTCP(listeners.Config{
			ID:      "tcp",
			Address: fmt.Sprintf(":%d", cfg.MQTTPort),
		})
	}
	if err := server.AddListener(tcpListener); err != nil {
		return nil, nil, fmt.Errorf("add TCP listener on :%d: %w", cfg.MQTTPort, err)
	}

	// TLS setup (cert reloader + tls.Config) is shared by the TLS TCP
	// listener and the WSS listener below — one certificate, one reload
	// mechanism, needed whenever either is configured. The certificate is
	// served via CertReloader.GetCertificate rather than a static
	// tls.Config.Certificates so it can be rotated (K8s Secret update,
	// cert-manager renewal, ...) without a process restart.
	var reloader *CertReloader
	var tlsConfig *tls.Config
	if cfg.MQTTTLSPort > 0 || cfg.MQTTWSSPort > 0 {
		if cfg.TLSCertDir == "" {
			return nil, nil, fmt.Errorf("MQTTTLSPort or MQTTWSSPort set but TLSCertDir is empty")
		}
		clientAuth, err := parseClientAuth(cfg.TLSClientAuth)
		if err != nil {
			return nil, nil, err
		}
		reloader, err = NewCertReloader(cfg.TLSCertDir, log)
		if err != nil {
			return nil, nil, fmt.Errorf("create tls cert reloader: %w", err)
		}
		tlsConfig = &tls.Config{
			GetCertificate: reloader.GetCertificate,
			ClientAuth:     clientAuth,
			MinVersion:     tls.VersionTLS12,
		}
	}

	// TLS MQTT listener (optional — skipped when MQTTTLSPort is 0).
	if cfg.MQTTTLSPort > 0 {
		var tlsListener listeners.Listener
		if cfg.ProxyProtocol {
			tlsListener = newProxyProtoListener("tls", fmt.Sprintf(":%d", cfg.MQTTTLSPort), tlsConfig, connPolicy)
		} else {
			tlsListener = listeners.NewTCP(listeners.Config{
				ID:        "tls",
				Address:   fmt.Sprintf(":%d", cfg.MQTTTLSPort),
				TLSConfig: tlsConfig,
			})
		}
		if err := server.AddListener(tlsListener); err != nil {
			return nil, nil, fmt.Errorf("add TLS listener on :%d: %w", cfg.MQTTTLSPort, err)
		}
		log.Info("mqtt-gateway: TLS listener configured", "port", cfg.MQTTTLSPort, "cert_dir", cfg.TLSCertDir, "client_auth", cfg.TLSClientAuth)
	}

	// WebSocket listener (optional — skipped when MQTTWSPort is 0). Same
	// auth/ACL hooks as every other listener: mochi-mqtt establishes
	// connections identically regardless of transport, so there's no
	// separate policy path to keep in sync.
	if cfg.MQTTWSPort > 0 {
		wsListener := listeners.NewWebsocket(listeners.Config{
			ID:      "ws",
			Address: fmt.Sprintf(":%d", cfg.MQTTWSPort),
		})
		if err := server.AddListener(wsListener); err != nil {
			return nil, nil, fmt.Errorf("add WebSocket listener on :%d: %w", cfg.MQTTWSPort, err)
		}
		log.Info("mqtt-gateway: WebSocket listener configured", "port", cfg.MQTTWSPort)
	}

	// WebSocket-over-TLS listener (optional — skipped when MQTTWSSPort is
	// 0). Shares tlsConfig with the TLS TCP listener above.
	if cfg.MQTTWSSPort > 0 {
		wssListener := listeners.NewWebsocket(listeners.Config{
			ID:        "wss",
			Address:   fmt.Sprintf(":%d", cfg.MQTTWSSPort),
			TLSConfig: tlsConfig,
		})
		if err := server.AddListener(wssListener); err != nil {
			return nil, nil, fmt.Errorf("add WSS listener on :%d: %w", cfg.MQTTWSSPort, err)
		}
		log.Info("mqtt-gateway: WSS listener configured", "port", cfg.MQTTWSSPort, "cert_dir", cfg.TLSCertDir, "client_auth", cfg.TLSClientAuth)
	}

	return server, reloader, nil
}
