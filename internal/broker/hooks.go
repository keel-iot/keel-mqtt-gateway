package broker

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/storage"
	"github.com/mochi-mqtt/server/v2/packets"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/redisrouter"
	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
	"github.com/keel-iot/keel-mqtt-gateway/internal/session"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// keelHook implements the mochi-mqtt v2 Hook interface.
type keelHook struct {
	mqtt.HookBase

	provider    auth.AuthProvider
	tenantCache *auth.TenantConfigCache
	jwksCache   *auth.JWKSCache
	// deviceCACache resolves a tenant's device CA live from an external
	// custodian (e.g. Clavex) when TenantGatewayConfig.ClavexCAURL is set —
	// see authenticateCert. Nil disables this path entirely, falling back
	// to the static TrustedCAPEMs column (unchanged behavior).
	deviceCACache *auth.DeviceCACache
	retainedStore *RetainedStore
	// rdb backs the per-tenant daily data-volume quota (see
	// withinDataVolumeLimit) — the only piece of the former
	// internal/forwarder.Forwarder that stayed broker-side, since it's a
	// quota/ACL concern rather than keel-specific output routing. Nil
	// disables the quota check entirely (fail-open).
	rdb         *redisrouter.Router
	autoProvURL string
	log         *slog.Logger
	// outputConnectors is one entry per configured OutputConnector — an
	// in-process one (e.g. kafka-hono) and/or one per attached plugin
	// sidecar (see internal/connector/pluginhost). Nil-safe: loop is a
	// no-op when empty.
	outputConnectors []connector.OutputConnector

	// Cluster wiring (see internal/cluster). Both nil when the gateway
	// runs standalone (no --role flag / single-node mode) — every cluster
	// call site below checks for nil and no-ops.
	clusterRegistry keelraft.Registry
	clusterFwd      dataplane.Forwarder
	clusterNodeID   string

	// offlineOwnership/liveEdgeNodeIDs back OnDisconnect/OnSessionEstablish's
	// eager offline-session ownership placement/clear, a latency shortcut
	// ahead of the periodic session.Reconciler. Nil in standalone mode,
	// same guard-for-nil convention as clusterRegistry above.
	offlineOwnership offlineOwnershipStore
	liveEdgeNodeIDs  func() []string

	// server is this node's own mochi-mqtt server, wired in broker.New right
	// after construction — used by OnSessionEstablish to check whether THIS
	// node already has local state for a reconnecting persistent session
	// before reaching for sessionStore. Nil only in tests that don't exercise
	// OnSessionEstablish's rehydration path.
	server *mqtt.Server

	// sessionStore provides per-client lookup of persisted subscriptions/
	// inflight (backed by RedisSessionHook — see OnSessionEstablish). Nil
	// when Redis is disabled: rehydration then simply doesn't happen, same
	// as today, no different than standalone in-memory-only behavior.
	sessionStore sessionStore

	mu          sync.RWMutex
	clients     map[string]*clientState
	generation  map[string]uint64 // monotonic counter per client_id to detect stale OnDisconnect
	tenantConns map[string]int    // per-tenant active connection counter for rate limiting

	// liveStats feeds the basic monitoring UI's messages/sec figure (see
	// internal/telemetry.LiveStats). Nil when not configured (standalone
	// mode, or tests that don't exercise it) — every call site guards it.
	liveStats *telemetry.LiveStats

	// defaultTenantID, when non-empty, is used as the tenant identity for
	// password-auth CONNECTs whose username has no "@" separator — see
	// Config.DefaultTenantID's doc in broker.go. Empty preserves the
	// original fail-closed behaviour (empty tenantID, rejected at the
	// tenant-config lookup).
	defaultTenantID string
}

// sessionStore is satisfied by *RedisSessionHook; narrowed for testability —
// see fakeRegistry/fakeForwarder for the same pattern elsewhere in this
// package's tests.
type sessionStore interface {
	SubscriptionsForClient(clientID string) ([]storage.Subscription, error)
	InflightForClient(clientID string) ([]storage.Message, error)
}

// offlineOwnershipStore is satisfied by *keelraft.OfflineOwnership —
// narrowed for testability, same pattern as sessionStore above.
type offlineOwnershipStore interface {
	Place(clientID, filter, newOwner string) error
	Clear(clientID, filter string) error
}

type clientState struct {
	info       *auth.DeviceInfo
	method     auth.AuthMethod
	username   string // raw MQTT username, used as the RBAC principal alongside client ID
	generation uint64 // incremented on every new auth for this client_id
}

// ── TEMPORARY: hardcoded test-consumer role ─────────────────────────────
// Single-purpose, subscribe-only identity for the e2e cross-node MQTT
// validation test (test/e2e/cross_node_test.go). Bypasses the tenant/
// provider machinery entirely — not a general RBAC mechanism. Remove
// once the configurable RBAC engine replaces this file's hardcoded ACL
// logic (see docs discussion on the keel-mqtt-gateway PoC).
const (
	testConsumerUsername = "test-consumer"
	testConsumerPassword = "consumer-e2e-testpass"

	authMethodTestConsumer auth.AuthMethod = "test-consumer-role"
)

func testConsumerDeviceInfo() *auth.DeviceInfo {
	return &auth.DeviceInfo{TenantSlug: "test-consumer", FleetIDStr: "nofleet"}
}

func (h *keelHook) ID() string { return "keel-auth-hook" }

func (h *keelHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		byte(mqtt.OnConnectAuthenticate),
		byte(mqtt.OnSessionEstablish),
		byte(mqtt.OnACLCheck),
		byte(mqtt.OnPublish),
		byte(mqtt.OnDisconnect),
		byte(mqtt.OnClientExpired),
		byte(mqtt.OnSubscribed),
		byte(mqtt.OnUnsubscribed),
		byte(mqtt.OnRetainMessage),
	}, []byte{b})
}

func (h *keelHook) Init(_ any) error {
	h.clients = make(map[string]*clientState)
	h.generation = make(map[string]uint64)
	h.tenantConns = make(map[string]int)
	return nil
}

// OnConnectAuthenticate handles CONNECT packets.
// Auth precedence: X.509 cert → JWT → password token.
func (h *keelHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	ctx, span := telemetry.Tracer().Start(context.Background(), "keel-gateway.authenticate",
		oteltrace.WithAttributes(
			attribute.String("mqtt.client_id", cl.ID),
			attribute.String("mqtt.username", string(pk.Connect.Username)),
		),
	)
	defer span.End()

	info, method, ok := h.authenticate(ctx, cl, pk)
	if !ok {
		span.SetStatus(codes.Error, "authentication failed")
		return false
	}

	tenantStr := info.TenantID.String()

	// Rate limiting: check MaxConnections under the lock to prevent TOCTOU.
	tenantCfg, _ := h.tenantCache.Get(ctx, tenantStr)
	h.mu.Lock()
	if tenantCfg != nil && tenantCfg.MaxConnections > 0 && h.tenantConns[tenantStr] >= tenantCfg.MaxConnections {
		h.mu.Unlock()
		telemetry.ConnectionsTotal.WithLabelValues(tenantStr, "rate_limited").Inc()
		span.SetStatus(codes.Error, "rate limited")
		h.log.Warn("mqtt-gateway: connection rejected, max connections reached",
			"tenant", tenantStr, "limit", tenantCfg.MaxConnections)
		return false
	}
	h.generation[cl.ID]++
	h.clients[cl.ID] = &clientState{
		info:       info,
		method:     method,
		username:   string(pk.Connect.Username),
		generation: h.generation[cl.ID],
	}
	h.tenantConns[tenantStr]++
	h.mu.Unlock()

	if !h.claimClusterSession(cl.ID, tenantStr) {
		span.SetStatus(codes.Error, "claim session failed")
		return false
	}

	go h.provider.UpdateLastSeen(context.Background(), info.ID)
	h.forwardConnectionEvent(info, "online")

	telemetry.ActiveConnections.WithLabelValues(tenantStr).Inc()
	telemetry.ConnectionsTotal.WithLabelValues(tenantStr, "success").Inc()

	span.SetAttributes(
		attribute.String("device.id", info.ID.String()),
		attribute.String("tenant.id", tenantStr),
		attribute.String("auth.method", string(method)),
		attribute.Bool("auth.success", true),
	)
	span.SetStatus(codes.Ok, "")

	h.log.Info("mqtt-gateway: device connected",
		"client_id", cl.ID,
		"device_id", info.ID,
		"tenant", info.TenantSlug,
		"auth_method", method,
	)
	return true
}

// OnSessionEstablish seeds a ghost *Client into s.Clients before
// mochi-mqtt's own inheritClientSession runs, so its already-tested resume
// path also works when a persistent session reconnects to a node that
// never had it locally. mochi-mqtt only reads storage once, at boot, so
// without this a reconnect on a different node silently dropped every
// queued QoS1/2 message. No-op for a clean session, no sessionStore
// (standalone), or when this node already has local state (same-node
// resume, already handled by mochi-mqtt itself).
func (h *keelHook) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) {
	if h.server == nil || h.sessionStore == nil || pk.Connect.Clean {
		return
	}
	h.clearOfflineOwnership(cl.ID)

	if _, ok := h.server.Clients.Get(cl.ID); ok {
		return
	}

	subs, err := h.sessionStore.SubscriptionsForClient(cl.ID)
	if err != nil {
		h.log.Error("session rehydrate: fetch subscriptions failed", "client_id", cl.ID, "error", err)
		return
	}
	inflight, err := h.sessionStore.InflightForClient(cl.ID)
	if err != nil {
		h.log.Error("session rehydrate: fetch inflight failed", "client_id", cl.ID, "error", err)
		return
	}
	if len(subs) == 0 && len(inflight) == 0 {
		return // nothing persisted for this client_id anywhere — genuinely new or clean
	}

	ghost := h.server.NewClient(nil, cl.Net.Listener, cl.ID, false)
	ghost.Properties.Clean = false
	ghost.Properties.ProtocolVersion = cl.Properties.ProtocolVersion
	for _, s := range subs {
		ghost.State.Subscriptions.Add(s.Filter, packets.Subscription{
			Filter:            s.Filter,
			RetainHandling:    s.RetainHandling,
			Qos:               s.Qos,
			RetainAsPublished: s.RetainAsPublished,
			NoLocal:           s.NoLocal,
			Identifier:        s.Identifier,
		})
	}
	for _, m := range inflight {
		ghost.State.Inflight.Set(m.ToPacket())
	}
	h.server.Clients.Add(ghost)

	h.log.Info("session rehydrate: seeded from Redis for reconnect on a different node",
		"client_id", cl.ID, "subscriptions", len(subs), "inflight", len(inflight))
}

// clearOfflineOwnership removes any offline-ownership registration for
// clientID's persisted subscription filters — the mirror of
// placeOfflineOwnership below, called when a session comes back online so a
// reconnected client is never left with a stale offline-delivery target
// sitting on some other node until the periodic session.Reconciler's next
// tick notices. Runs for both the same-node and cross-node resume branches
// of OnSessionEstablish (placeOfflineOwnership doesn't know in advance
// which one a given disconnect will end up being), so it's called before
// either path returns. No-op when offlineOwnership isn't configured
// (standalone mode, or a role that doesn't run the offline reconciler).
func (h *keelHook) clearOfflineOwnership(clientID string) {
	if h.offlineOwnership == nil {
		return
	}
	subs, err := h.sessionStore.SubscriptionsForClient(clientID)
	if err != nil {
		h.log.Warn("cluster: offline ownership clear: fetch subscriptions failed", "client_id", clientID, "error", err)
		return
	}
	for _, s := range subs {
		if err := h.offlineOwnership.Clear(clientID, s.Filter); err != nil {
			h.log.Warn("cluster: offline ownership clear failed", "client_id", clientID, "filter", s.Filter, "error", err)
		}
	}
}

// placeOfflineOwnership eagerly registers this session's rendezvous-computed
// owner (internal/session.Owner) for cl's still-active subscription
// filters, right when it goes offline — the same latency optimization as
// clearOfflineOwnership above, over the periodic session.Reconciler, which
// would otherwise take up to its own poll interval to notice and place the
// same thing. No-op when offlineOwnership/liveEdgeNodeIDs aren't configured,
// or there are no live edge nodes to assign an owner from.
func (h *keelHook) placeOfflineOwnership(cl *mqtt.Client) {
	if h.offlineOwnership == nil || h.liveEdgeNodeIDs == nil {
		return
	}
	owner, ok := session.Owner(cl.ID, h.liveEdgeNodeIDs())
	if !ok {
		return
	}
	for filter := range cl.State.Subscriptions.GetAll() {
		if err := h.offlineOwnership.Place(cl.ID, filter, owner); err != nil {
			h.log.Warn("cluster: offline ownership placement failed", "client_id", cl.ID, "filter", filter, "error", err)
		}
	}
}

// claimClusterSession claims clientID for this node in the cluster's
// session-ownership registry, evicting a previous owner on a different
// node if there was one. Called after local connection bookkeeping is
// already in place (h.clients[clientID] set, h.tenantConns[tenantStr]
// incremented) — on failure it rolls both back and returns false, which
// the caller must treat as "reject this connection".
//
// New connection always wins (see raft.Registry.ClaimSession's doc); the
// previous owner is evicted best-effort over the cluster data plane,
// backstopped by that node's own MQTT keepalive if the Evict RPC is lost.
// A ClaimSession error (cluster unreachable/leader unknown) rejects the
// connection rather than silently admitting it with no enforced
// exclusivity — a deliberate fail-closed choice, the same posture as
// EvaluateACL's own transport-error handling.
//
// No-ops (returns true) when clusterRegistry is nil (standalone mode).
func (h *keelHook) claimClusterSession(clientID, tenantStr string) bool {
	if h.clusterRegistry == nil {
		return true
	}

	evictedFrom, err := h.clusterRegistry.ClaimSession(clientID, h.clusterNodeID)
	if err != nil {
		h.mu.Lock()
		delete(h.clients, clientID)
		if h.tenantConns[tenantStr] > 0 {
			h.tenantConns[tenantStr]--
		}
		h.mu.Unlock()
		h.log.Error("cluster: claim session failed, rejecting connection", "client_id", clientID, "error", err)
		return false
	}

	if evictedFrom != "" && evictedFrom != h.clusterNodeID && h.clusterFwd != nil {
		go func() {
			if err := h.clusterFwd.Evict(context.Background(), evictedFrom, clientID); err != nil {
				h.log.Warn("cluster: evict previous session owner failed", "client_id", clientID, "target_node", evictedFrom, "error", err)
			}
		}()
	}
	return true
}

func (h *keelHook) authenticate(ctx context.Context, cl *mqtt.Client, pk packets.Packet) (*auth.DeviceInfo, auth.AuthMethod, bool) {
	// 1. X.509
	if cl.Net.Conn != nil {
		if tlsConn, ok := cl.Net.Conn.(*tls.Conn); ok {
			if state := tlsConn.ConnectionState(); len(state.PeerCertificates) > 0 {
				return h.authenticateCert(ctx, state)
			}
		}
	}

	// 2. JWT or password
	username := string(pk.Connect.Username)
	password := pk.Connect.Password

	// TEMPORARY: test-consumer role — see testConsumerUsername above.
	if username == testConsumerUsername {
		if subtle.ConstantTimeCompare(password, []byte(testConsumerPassword)) == 1 {
			return testConsumerDeviceInfo(), authMethodTestConsumer, true
		}
		telemetry.ConnectionsTotal.WithLabelValues("test-consumer", "auth_failed").Inc()
		return nil, authMethodTestConsumer, false
	}

	// Google IoT Core-style client-id: "tenants/<tid>/devices/<did>".
	// When detected, the password field carries the JWT and username is ignored
	// for identity purposes (tenantID and deviceID come from the client-id).
	if tid, did, ok := auth.ParseClientIdentifier(cl.ID); ok && len(password) > 0 {
		method := auth.DetectAuthMethod(password)
		tenantCfg, err := h.tenantCache.Get(ctx, tid)
		if err != nil {
			h.log.Error("mqtt-gateway: load tenant config (client-id mode)", "tenant", tid, "error", err)
			return nil, method, false
		}
		if !tenantCfg.JWTAuthEnabled {
			h.log.Warn("mqtt-gateway: JWT auth disabled for tenant (client-id mode)", "tenant", tid)
			telemetry.ConnectionsTotal.WithLabelValues(tid, "auth_failed").Inc()
			return nil, method, false
		}
		start := time.Now()
		if err := auth.ValidateJWTFromClientID(ctx, tid, did, password, tenantCfg, h.jwksCache); err != nil {
			h.log.Warn("mqtt-gateway: JWT auth failed (client-id mode)", "tenant", tid, "device", did, "error", err)
			telemetry.ConnectionsTotal.WithLabelValues(tid, "auth_failed").Inc()
			telemetry.AuthDuration.WithLabelValues(tid, string(method)).Observe(time.Since(start).Seconds())
			return nil, method, false
		}
		telemetry.AuthDuration.WithLabelValues(tid, string(method)).Observe(time.Since(start).Seconds())
		info, err := h.provider.LookupByCN(ctx, did, tid)
		if err != nil {
			h.log.Warn("mqtt-gateway: device not found after JWT auth (client-id mode)", "tenant", tid, "device", did)
			telemetry.ConnectionsTotal.WithLabelValues(tid, "auth_failed").Inc()
			return nil, method, false
		}
		return info, method, true
	}

	var deviceID, tenantID string
	if parts := strings.SplitN(username, "@", 2); len(parts) == 2 {
		deviceID, tenantID = parts[0], parts[1]
	} else if username != "" {
		// A username was sent but with no "@<tenantID>" separator — e.g.
		// an account ported as-is from another broker (FileProvider's
		// username-override, see file_provider.go) that never used this
		// convention. The raw username IS the provider's lookup key
		// (FileProvider.credentialKey()) — using cl.ID here instead (as
		// this branch did before) can never match such an entry, since
		// MQTT client IDs are normally distinct per device while the
		// override username is shared across all of them.
		deviceID = username
		tenantID = h.defaultTenantID
	} else {
		// No username at all — client-id-only auth, where cl.ID doubles
		// as the device's own identity (e.g. a real per-device Postgres
		// credential keyed by client ID as device UUID).
		deviceID = cl.ID
		tenantID = h.defaultTenantID
	}

	method := auth.DetectAuthMethod(password)

	tenantCfg, err := h.tenantCache.Get(ctx, tenantID)
	if err != nil {
		h.log.Error("mqtt-gateway: load tenant config", "tenant", tenantID, "error", err)
		return nil, method, false
	}

	start := time.Now()
	defer func() {
		telemetry.AuthDuration.WithLabelValues(tenantID, string(method)).Observe(time.Since(start).Seconds())
	}()

	switch method {
	case auth.AuthMethodJWT:
		if !tenantCfg.JWTAuthEnabled {
			h.log.Warn("mqtt-gateway: JWT auth disabled for tenant", "tenant", tenantID)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
		if err := auth.ValidateJWT(ctx, tenantID, deviceID, password, tenantCfg, h.jwksCache); err != nil {
			h.log.Warn("mqtt-gateway: JWT auth failed", "tenant", tenantID, "device", deviceID, "error", err)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
		info, err := h.provider.LookupByCN(ctx, deviceID, tenantID)
		if err != nil {
			h.log.Warn("mqtt-gateway: device not found after JWT auth", "tenant", tenantID, "device", deviceID)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
		return info, method, true

	default: // password
		if !tenantCfg.PasswordAuthEnabled {
			h.log.Warn("mqtt-gateway: password auth disabled for tenant", "tenant", tenantID)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
		info, err := h.provider.ValidatePassword(ctx, deviceID, string(password))
		if err != nil {
			h.log.Warn("mqtt-gateway: password auth failed", "tenant", tenantID, "device", deviceID, "error", err)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
		return info, method, true
	}
}

func (h *keelHook) authenticateCert(ctx context.Context, state tls.ConnectionState) (*auth.DeviceInfo, auth.AuthMethod, bool) {
	cert := state.PeerCertificates[0]
	method := auth.AuthMethodCertificate

	// Quick CN parse to get tenantID for CA lookup
	deviceID, tenantID, err := auth.VerifyCertificate(cert, nil)
	if err != nil {
		h.log.Warn("mqtt-gateway: cert CN parse failed", "cn", cert.Subject.CommonName, "error", err)
		return nil, method, false
	}

	tenantCfg, err := h.tenantCache.Get(ctx, tenantID)
	if err != nil || !tenantCfg.CertAuthEnabled {
		h.log.Warn("mqtt-gateway: cert auth not enabled for tenant", "tenant", tenantID)
		telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
		return nil, method, false
	}

	// Full verification with tenant's trusted CAs. ClavexCAURL, when set,
	// takes precedence over the static TrustedCAPEMs — resolved live via
	// deviceCACache (see its doc), never persisted in this database.
	trustedCAPEMs := tenantCfg.TrustedCAPEMs
	if tenantCfg.ClavexCAURL != "" && h.deviceCACache != nil {
		pems, caErr := h.deviceCACache.TrustedCAPEMs(ctx, tenantID, tenantCfg.ClavexCAURL, tenantCfg.ClavexAgentToken)
		if caErr != nil {
			h.log.Warn("mqtt-gateway: device CA fetch failed", "tenant", tenantID, "error", caErr)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
		trustedCAPEMs = pems
	}

	deviceID, tenantID, err = auth.VerifyCertificate(cert, trustedCAPEMs)
	if err == nil && h.clusterRegistry != nil && h.clusterRegistry.IsRevoked(deviceID+"@"+tenantID) {
		h.log.Warn("mqtt-gateway: cert revoked", "tenant", tenantID, "device", deviceID)
		telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
		return nil, method, false
	}
	if err != nil {
		h.log.Warn("mqtt-gateway: cert verification failed", "tenant", tenantID, "error", err)
		telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
		return nil, method, false
	}

	start := time.Now()
	defer func() {
		telemetry.AuthDuration.WithLabelValues(tenantID, string(method)).Observe(time.Since(start).Seconds())
	}()

	info, lookupErr := h.provider.LookupByCN(ctx, deviceID, tenantID)
	if lookupErr != nil {
		if tenantCfg.AutoProvisioning {
			telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "started").Inc()
			go h.autoProvision(context.Background(), tenantID, deviceID)
			info = auth.NewPendingDeviceInfo(deviceID, tenantID)
			h.log.Info("mqtt-gateway: auto-provisioning triggered", "tenant", tenantID, "device", deviceID)
		} else {
			h.log.Warn("mqtt-gateway: device not found, auto-provisioning disabled", "tenant", tenantID, "device", deviceID)
			telemetry.ConnectionsTotal.WithLabelValues(tenantID, "auth_failed").Inc()
			return nil, method, false
		}
	} else {
		telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "skipped_exists").Inc()
	}

	return info, method, true
}

// autoProvision asynchronously registers a new device in keel core via the
// device-service REST API. Called as a goroutine from authenticateCert — must
// not block the CONNECT path.
// Retries up to 3 times with exponential backoff starting at 1 s.
// HTTP 409 Conflict is treated as idempotent success (device already registered).
func (h *keelHook) autoProvision(ctx context.Context, tenantID, deviceID string) {
	if h.autoProvURL == "" {
		h.log.Warn("mqtt-gateway: auto-provision URL not configured", "tenant", tenantID, "device", deviceID)
		telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "error").Inc()
		return
	}

	type autoProvBody struct {
		Name string         `json:"name"`
		Tags map[string]any `json:"tags,omitempty"`
	}
	bodyObj := autoProvBody{
		Name: deviceID,
		Tags: map[string]any{"source": "auto-provisioned", "cert_auth": true},
	}
	b, err := json.Marshal(bodyObj)
	if err != nil {
		h.log.Error("mqtt-gateway: marshal auto-prov body", "error", err)
		telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "error").Inc()
		return
	}

	endpoint := fmt.Sprintf("%s/api/v1/tenants/%s/devices", h.autoProvURL, tenantID)
	client := &http.Client{Timeout: 10 * time.Second}

	const maxAttempts = 3
	backoff := time.Second
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
		if err != nil {
			lastErr = err
			break
		}
		req.Header.Set("Content-Type", "application/json")
		// Internal service-to-service: X-User-Sub absent → api-gateway visibility bypass.

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts {
				h.log.Warn("mqtt-gateway: auto-prov request error, retrying",
					"attempt", attempt, "tenant", tenantID, "device", deviceID, "error", err)
				time.Sleep(backoff)
				backoff *= 2
			}
			continue
		}
		_ = resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusCreated, http.StatusOK:
			telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "created").Inc()
			h.log.Info("mqtt-gateway: device auto-provisioned",
				"tenant", tenantID, "device", deviceID)
			return
		case http.StatusConflict: // already exists — idempotent
			telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "skipped_exists").Inc()
			h.log.Info("mqtt-gateway: auto-prov device already exists",
				"tenant", tenantID, "device", deviceID)
			return
		default:
			lastErr = fmt.Errorf("auto-prov: unexpected status %d", resp.StatusCode)
			if attempt < maxAttempts {
				h.log.Warn("mqtt-gateway: auto-prov unexpected response, retrying",
					"attempt", attempt, "status", resp.StatusCode, "tenant", tenantID)
				time.Sleep(backoff)
				backoff *= 2
			}
		}
	}

	h.log.Error("mqtt-gateway: auto-provisioning failed after retries",
		"tenant", tenantID, "device", deviceID, "error", lastErr)
	telemetry.AutoProvisioningTotal.WithLabelValues(tenantID, "error").Inc()
}

// OnACLCheck enforces topic-level access control.
//
// RBAC integration (additive, not a full replacement — see
// internal/cluster/acl and rbac-migration in the project plan for the
// eventual reconciliation): if h.clusterRegistry is set and RBAC produces
// an *explicit* decision (Rule != nil, i.e. some enabled ruleset or custom
// role binding actually matched this clientID/username/topic/action), that
// decision is authoritative — an explicit RBAC deny always wins, and an
// explicit RBAC allow is honored without falling through to the legacy
// hardcoded checks below. When RBAC has no opinion (no matching rule at
// all — the fail-closed default with a nil Rule), control falls through to
// the existing hardcoded ACL logic unchanged, so today's device/consumer
// behavior keeps working exactly as before until rbac-migration defines
// and activates keel-device-default explicitly.
func (h *keelHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	h.mu.RLock()
	state, ok := h.clients[cl.ID]
	h.mu.RUnlock()
	if !ok {
		return false
	}

	action := acl.ActionSubscribe
	if write {
		action = acl.ActionPublish
	}
	if h.clusterRegistry != nil {
		decision := h.clusterRegistry.EvaluateACL(cl.ID, state.username, topic, action)
		if decision.Rule != nil {
			return decision.Allowed()
		}
	}

	// TEMPORARY: test-consumer role — subscribe-only, telemetry topics
	// (see isAllowedConsumerSubscribe for scope).
	if state.method == authMethodTestConsumer {
		return !write && isAllowedConsumerSubscribe(topic)
	}

	info := state.info

	if write {
		return isAllowedPublish(topic, info)
	}
	deviceID := info.ID.String()
	tenantID := info.TenantID.String()
	return topic == "command/"+deviceID ||
		topic == "command/"+deviceID+"/#" ||
		topic == "command/"+tenantID+"//"+deviceID+"/req/#"
}

// OnPublish forwards device messages to Redpanda and records metrics.
func (h *keelHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	h.mu.RLock()
	state, ok := h.clients[cl.ID]
	h.mu.RUnlock()
	if !ok {
		return pk, nil
	}
	info := state.info

	ctx, span := telemetry.Tracer().Start(context.Background(), "keel-gateway.publish",
		oteltrace.WithAttributes(
			attribute.String("device.id", info.ID.String()),
			attribute.String("tenant.id", info.TenantID.String()),
			attribute.String("mqtt.topic", pk.TopicName),
			attribute.Int("mqtt.qos", int(pk.FixedHeader.Qos)),
			attribute.Int("payload.bytes", len(pk.Payload)),
		),
	)
	defer span.End()

	h.forwardToClusterSubscribers(ctx, info, pk)

	// Per-tenant daily data-volume quota gates only the OutputConnector fan-out
	// (the Redpanda/plugin-facing side effects), never real MQTT subscriber
	// delivery above — a noisy tenant over quota still gets normal MQTT
	// service, it just stops feeding downstream systems until the next day.
	tenantStr := info.TenantID.String()
	if h.withinDataVolumeLimit(ctx, tenantStr, len(pk.Payload)) {
		h.forwardToOutputConnector(ctx, info, pk)
	}

	go h.provider.UpdateLastSeen(context.Background(), info.ID)

	telemetry.MessagesPublished.WithLabelValues(tenantStr, strconv.Itoa(int(pk.FixedHeader.Qos))).Inc()
	telemetry.BytesPublished.WithLabelValues(tenantStr).Add(float64(len(pk.Payload)))
	if h.liveStats != nil {
		h.liveStats.RecordPublish(len(pk.Payload))
	}

	span.SetStatus(codes.Ok, "")
	return pk, nil
}

// OnDisconnect always decrements the connections gauge and releases
// cluster session ownership, but only tears down ACL identity (h.clients)
// and cluster routing when the session is truly ending (expire == true).
// mochi-mqtt keeps delivering queued QoS1/2 messages against a persistent
// session's Client object while offline; deleting h.clients or the
// routing entry unconditionally on every disconnect broke both. The
// generation counter guards against removing an entry a newer connection
// already replaced.
func (h *keelHook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	// A disconnect caused by our own cluster-level Evict (see
	// claimClusterSession/cmd/server/main.go's SubscribeEvict handler) means
	// this client_id's session has definitely moved to a different node —
	// unlike an ordinary network disconnect, there's no ambiguity to
	// preserve here even for a persistent session. Treating it like expire
	// avoids leaving a stale "ghost": local ACL identity plus a duplicate
	// cluster routing entry that would otherwise sit on this node until
	// OnClientExpired's sweep eventually clears it. Same signal
	// RedisSessionHook.OnDisconnect already keys off for the same reason
	// (see that method's ErrSessionTakenOver check).
	//
	// Client.Stop only closes the connection and returns — OnDisconnect
	// itself fires later, asynchronously, once the blocked Read notices —
	// so this check (evaluated here, inside the one guaranteed call) is the
	// race-free way to act on it, instead of a second explicit cleanup call
	// racing against this same hook from the Evict handler.
	evicted := cl.StopCause() == packets.ErrSessionTakenOver
	cleanup := expire || evicted

	h.mu.Lock()
	state, ok := h.clients[cl.ID]
	// Only act if this disconnect belongs to the current generation.
	// A higher generation means a newer connection has already taken over.
	if ok && state.generation == h.generation[cl.ID] {
		if ts := state.info.TenantID.String(); h.tenantConns[ts] > 0 {
			h.tenantConns[ts]--
		}
		if cleanup {
			delete(h.clients, cl.ID)
			delete(h.generation, cl.ID)
		}
	} else {
		ok = false // suppress metrics decrement and cluster cleanup below
	}
	h.mu.Unlock()

	if ok {
		telemetry.ActiveConnections.WithLabelValues(state.info.TenantID.String()).Dec()
		h.forwardConnectionEvent(state.info, "offline")
		if cleanup {
			h.unsubscribeClusterFilters(cl)
		} else {
			// Persistent session, genuinely offline (not evicted/expired) —
			// eagerly place its offline-session ownership rather than
			// waiting for the periodic session.Reconciler's next tick. See
			// placeOfflineOwnership's doc.
			h.placeOfflineOwnership(cl)
		}
		if h.clusterRegistry != nil {
			// Guarded by nodeID: a no-op if this node was already
			// superseded by a newer ClaimSession elsewhere (see
			// raft.Registry.ReleaseSession's doc) — safe even when this
			// disconnect was itself caused by an inbound Evict.
			if err := h.clusterRegistry.ReleaseSession(cl.ID, h.clusterNodeID); err != nil {
				h.log.Warn("cluster: release session failed", "client_id", cl.ID, "error", err)
			}
		}
	}
	h.log.Info("mqtt-gateway: device disconnected", "client_id", cl.ID, "session_expired", expire, "evicted", evicted)
}

// OnClientExpired is called by mochi-mqtt when a persistent session's
// offline window has genuinely elapsed with no reconnect — see broker.go's
// New (Options.Capabilities.MaximumSessionExpiryInterval) and mochi-mqtt's
// own clearExpiredClients. This is the true end-of-life event OnDisconnect
// deliberately doesn't act on for a persistent session (expire == false
// there): only now are the ACL identity and cluster routing entry finally
// torn down.
//
// A same-client_id reconnect before expiry needs no explicit invalidation
// here: OnConnectAuthenticate unconditionally overwrites h.clients[cl.ID]
// with the new connection's fresh state, and mochi-mqtt's own s.Clients map
// is similarly overwritten under the same key — so a reconnected client's
// entry is never "disconnected" from clearExpiredClients's point of view,
// and this hook is never called for it.
func (h *keelHook) OnClientExpired(cl *mqtt.Client) {
	h.mu.Lock()
	state, ok := h.clients[cl.ID]
	if ok {
		delete(h.clients, cl.ID)
		delete(h.generation, cl.ID)
	}
	h.mu.Unlock()

	if !ok {
		return // already cleaned up (e.g. reconnected and took over)
	}
	h.unsubscribeClusterFilters(cl)
	h.log.Info("mqtt-gateway: persistent session expired", "client_id", cl.ID, "tenant", state.info.TenantID)
}

// unsubscribeClusterFilters removes this node's cluster-wide routing
// entries for every filter cl was still locally subscribed to, in one
// batched raft.Apply when the Registry backend supports it (core nodes —
// see keelraft.BatchUnsubscriber) or a per-filter loop otherwise (edge
// nodes' RemoteRegistry has no batch RPC; standalone/no-op when
// clusterRegistry is nil).
func (h *keelHook) unsubscribeClusterFilters(cl *mqtt.Client) {
	if h.clusterRegistry == nil {
		return
	}
	subs := cl.State.Subscriptions.GetAll()
	if len(subs) == 0 {
		return
	}
	filters := make([]string, 0, len(subs))
	for filter := range subs {
		filters = append(filters, filter)
	}

	if batch, ok := h.clusterRegistry.(keelraft.BatchUnsubscriber); ok {
		if err := batch.UnsubscribeBatch(filters, h.clusterNodeID); err != nil {
			h.log.Error("cluster: batch unsubscribe on disconnect failed", "client_id", cl.ID, "count", len(filters), "error", err)
		}
		return
	}

	for _, filter := range filters {
		if err := h.clusterRegistry.Unsubscribe(filter, h.clusterNodeID); err != nil {
			h.log.Error("cluster: unsubscribe on disconnect failed", "client_id", cl.ID, "topic", filter, "error", err)
		}
	}
}

// OnSubscribed registers this node in the cluster routing table for every
// filter a client just subscribed to, so OnPublish on any other node
// knows to forward matching messages here. No-op when running standalone
// (clusterRegistry == nil). Also triggers Redis-backed retained-message
// backfill (see deliverRetainedBackfill) when configured.
func (h *keelHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, _ []byte) {
	if h.clusterRegistry != nil {
		for _, f := range pk.Filters {
			if err := h.clusterRegistry.Subscribe(f.Filter, h.clusterNodeID); err != nil {
				h.log.Error("cluster: subscribe failed", "topic", f.Filter, "error", err)
			}
		}
	}
	if h.retainedStore != nil {
		// Dispatched async: mochi-mqtt itself sends SUBACK, then its own
		// (local-only) retained messages, synchronously right after this
		// hook returns. Running our Redis-backed backfill inline here would
		// risk it reaching the client before SUBACK; async also means a
		// slow/unavailable Redis never blocks the subscribe path.
		filters := append([]packets.Subscription(nil), pk.Filters...)
		go h.deliverRetainedBackfill(cl, filters)
	}
}

// deliverRetainedBackfill sends retained messages from Redis that mochi's
// own local (per-node, in-memory) retained store did NOT already deliver
// for these filters — i.e. messages retained on a different node than the
// one this client happens to be connected to, or retained before this
// node's last restart. Local matches are excluded via h.server.Topics so a
// subscriber never receives the same retained message twice. QoS0 only —
// full QoS1/2 delivery would require mochi-mqtt's private inflight/packet-ID
// machinery, which isn't reachable from this package; a known, deliberate
// V1 simplification (see CONFIGURATION.md).
func (h *keelHook) deliverRetainedBackfill(cl *mqtt.Client, filters []packets.Subscription) {
	ctx := context.Background()
	for _, f := range filters {
		if mqtt.IsSharedFilter(f.Filter) {
			continue // 4.8.2 non-normative: shared subscriptions get no retained messages on subscribe
		}

		var exclude map[string]struct{}
		if h.server != nil {
			local := h.server.Topics.Messages(f.Filter)
			exclude = make(map[string]struct{}, len(local))
			for _, pk := range local {
				exclude[pk.TopicName] = struct{}{}
			}
		}

		msgs, err := h.retainedStore.Match(ctx, f.Filter, exclude)
		if err != nil {
			h.log.Error("retained: match failed", "client_id", cl.ID, "filter", f.Filter, "error", err)
			continue
		}
		for _, m := range msgs {
			if !h.OnACLCheck(cl, m.Topic, false) {
				continue
			}
			pk := packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 0, Retain: true},
				TopicName:   m.Topic,
				Payload:     m.Payload,
				Created:     time.Now().Unix(),
			}
			if err := cl.WritePacket(pk); err != nil {
				h.log.Debug("retained: write to client failed", "client_id", cl.ID, "topic", m.Topic, "error", err)
			}
		}
	}
}

// OnRetainMessage mirrors every retained publish (and retained Will, on
// disconnect — mochi-mqtt calls this hook for both) into Redis, so it
// survives this node's restart and is visible to every other node's
// deliverRetainedBackfill. No-op when Redis isn't configured (retained
// then behaves exactly as vanilla mochi-mqtt: per-node, in-memory only).
func (h *keelHook) OnRetainMessage(_ *mqtt.Client, pk packets.Packet, _ int64) {
	if h.retainedStore == nil {
		return
	}
	if err := h.retainedStore.Set(context.Background(), pk.TopicName, pk.Payload); err != nil {
		h.log.Error("retained: persist to redis failed", "topic", pk.TopicName, "error", err)
	}
}

// OnUnsubscribed removes this node from the cluster routing table for
// filters a client just unsubscribed from.
func (h *keelHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	if h.clusterRegistry == nil {
		return
	}
	for _, f := range pk.Filters {
		if err := h.clusterRegistry.Unsubscribe(f.Filter, h.clusterNodeID); err != nil {
			h.log.Error("cluster: unsubscribe failed", "topic", f.Filter, "error", err)
		}
	}
}

// forwardToClusterSubscribers looks up which other nodes own a subscriber
// for pk.TopicName and forwards the payload to each over the cluster data
// plane. The inbound side (dataplane.Forwarder.Subscribe handler,
// registered in cmd/server) republishes it into the receiving node's
// local mochi-mqtt server so its own connected clients get it.
func (h *keelHook) forwardToClusterSubscribers(ctx context.Context, info *auth.DeviceInfo, pk packets.Packet) {
	if h.clusterRegistry == nil || h.clusterFwd == nil {
		return
	}
	nodes := h.clusterRegistry.NodesFor(pk.TopicName, h.clusterNodeID)
	if len(nodes) == 0 {
		return
	}
	msg := &dataplane.Message{
		SourceNodeID: h.clusterNodeID,
		TenantID:     info.TenantID.String(),
		Topic:        pk.TopicName,
		Payload:      pk.Payload,
		QoS:          pk.FixedHeader.Qos,
	}
	for _, nodeID := range nodes {
		if nodeID == h.clusterNodeID {
			continue // local subscribers are already served by mochi-mqtt itself
		}
		if err := h.clusterFwd.Forward(ctx, nodeID, msg); err != nil {
			h.log.Error("cluster: forward publish failed", "target_node", nodeID, "topic", pk.TopicName, "error", err)
		}
	}
}

// forwardToOutputConnector fans a device publish out to every configured
// OutputConnector — this is now the ONLY forwarding path off the MQTT hot
// path (the former in-broker forwarder.Forward, with its keel-specific
// topic taxonomy/Ditto/Hono-compat logic, has moved out to the keel
// OutputConnector plugin; see design doc "Meccanismo di plugin"). Real MQTT
// subscriber delivery (forwardToClusterSubscribers) is separate and comes
// first — this call must never be able to affect it.
func (h *keelHook) forwardToOutputConnector(ctx context.Context, info *auth.DeviceInfo, pk packets.Packet) {
	connector.FanOut(ctx, h.log, info.ID.String(), h.outputConnectors, &connector.ForwardRequest{
		Topic:      pk.TopicName,
		Payload:    pk.Payload,
		Headers:    map[string]string{"content-type": "application/json"},
		DeviceId:   info.ID.String(),
		TenantId:   info.TenantID.String(),
		TenantSlug: info.TenantSlug,
		FleetId:    info.FleetIDStr,
	})
}

// forwardConnectionEvent fans a device connect/disconnect event out to
// every configured OutputConnector, using the reserved
// connector.ConnectionEventTopic — see that constant's doc. Kept as a
// distinct call site (not tied to OnPublish) because connect/disconnect
// happen outside the publish hot path, but it shares the exact same
// fan-out/isolation semantics.
func (h *keelHook) forwardConnectionEvent(info *auth.DeviceInfo, state string) {
	payload, err := json.Marshal(map[string]string{"state": state})
	if err != nil {
		return // unreachable: static map, Marshal never fails
	}
	connector.FanOut(context.Background(), h.log, info.ID.String(), h.outputConnectors, &connector.ForwardRequest{
		Topic:      connector.ConnectionEventTopic,
		Payload:    payload,
		Headers:    map[string]string{"content-type": "application/json"},
		DeviceId:   info.ID.String(),
		TenantId:   info.TenantID.String(),
		TenantSlug: info.TenantSlug,
		FleetId:    info.FleetIDStr,
	})
}

// withinDataVolumeLimit checks and records the tenant's daily Redpanda/plugin
// output byte quota (independent of any specific OutputConnector — this is a
// broker-core ACL/quota concern, not part of any output plugin). Returns
// true when rdb or tenantCache is nil (feature disabled) or the limit
// hasn't been hit, false when the tenant is over quota.
func (h *keelHook) withinDataVolumeLimit(ctx context.Context, tenantID string, payloadBytes int) bool {
	if h.rdb == nil || h.tenantCache == nil {
		return true
	}
	var maxBytes int64
	if cfg, _ := h.tenantCache.Get(ctx, tenantID); cfg != nil {
		maxBytes = cfg.MaxBytesPerDay
	}
	if err := forwarder.CheckAndRecordBytes(ctx, h.rdb.Client(), tenantID, payloadBytes, maxBytes); err != nil {
		h.log.Warn("mqtt-gateway: data volume limit exceeded, dropping output", "tenant", tenantID, "payload_bytes", payloadBytes)
		telemetry.DataVolumeLimitExceeded.WithLabelValues(tenantID).Inc()
		return false
	}
	return true
}

// DeviceInfo returns the cached DeviceInfo for a connected client.
func (h *keelHook) DeviceInfo(clientID string) (*auth.DeviceInfo, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if s, ok := h.clients[clientID]; ok {
		return s.info, true
	}
	return nil, false
}

// ── ACL helpers ────────────────────────────────────────────────────────────────

// isAllowedConsumerSubscribe restricts the TEMPORARY test-consumer role to
// the "telemetry/#" wildcard or a literal telemetry/* topic. Cross-node
// forwarding (internal/cluster/raft's routing table, see
// forwardToClusterSubscribers) now does real MQTT wildcard matching (see
// FSM.nodesFor), so "telemetry/#" correctly receives cross-node publishes
// — no longer restricted to exact-match topics only.
func isAllowedConsumerSubscribe(topic string) bool {
	if topic == "telemetry/#" {
		return true
	}
	return strings.HasPrefix(topic, "telemetry/") && !strings.ContainsAny(topic, "+#")
}

func isAllowedPublish(topic string, info *auth.DeviceInfo) bool {
	// Gateway via-pattern: allow publishing on behalf of any sub-device UUID
	// as long as the delegating client is authenticated.
	// "via/<uuid>/telemetry", "via/<uuid>/event/...", etc.
	if strings.HasPrefix(topic, "via/") {
		rest := strings.TrimPrefix(topic, "via/")
		slashIdx := strings.IndexByte(rest, '/')
		if slashIdx > 0 {
			subID := rest[:slashIdx]
			if _, err := uuid.Parse(subID); err == nil {
				subTopic := rest[slashIdx+1:]
				return isAllowedPublish(subTopic, info) // recursive: validate the inner topic
			}
		}
		return false
	}
	switch {
	case topic == "telemetry", topic == "t",
		topic == "event", topic == "e",
		topic == "status/heartbeat", topic == "status/ota", topic == "status/ca":
		return true
	case len(topic) > 2 && (topic[:2] == "t/" || topic[:2] == "e/"):
		return true
	case len(topic) > 10 && topic[:10] == "telemetry/":
		return isHonoTopicOwned(topic, "telemetry/", info) || !isHonoTopicShape(topic, "telemetry/", info)
	case len(topic) > 6 && topic[:6] == "event/":
		return isHonoTopicOwned(topic, "event/", info) || !isHonoTopicShape(topic, "event/", info)
	}
	return false
}

func isHonoTopicShape(topic, prefix string, info *auth.DeviceInfo) bool {
	rest := topic[len(prefix):]
	tid := info.TenantID.String()
	return len(rest) >= len(tid)+1 && rest[:len(tid)] == tid && rest[len(tid)] == '/'
}

func isHonoTopicOwned(topic, prefix string, info *auth.DeviceInfo) bool {
	rest := topic[len(prefix):]
	tid := info.TenantID.String()
	did := info.ID.String()
	td := tid + "/" + did
	return rest == td || (len(rest) > len(td) && rest[:len(td)] == td && rest[len(td)] == '/')
}
