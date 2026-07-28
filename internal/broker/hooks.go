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
	"github.com/mochi-mqtt/server/v2/packets"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/dataplane"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
	"github.com/keel-iot/keel-mqtt-gateway/internal/connector"
	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// keelHook implements the mochi-mqtt v2 Hook interface.
type keelHook struct {
	mqtt.HookBase

	provider         auth.AuthProvider
	tenantCache      *auth.TenantConfigCache
	fwd              *forwarder.Forwarder
	autoProvURL      string
	log              *slog.Logger
	outputConnector  connector.OutputConnector

	// Cluster wiring (see internal/cluster). Both nil when the gateway
	// runs standalone (no --role flag / single-node mode) — every cluster
	// call site below checks for nil and no-ops.
	clusterRegistry keelraft.Registry
	clusterFwd      dataplane.Forwarder
	clusterNodeID   string

	mu          sync.RWMutex
	clients     map[string]*clientState
	generation  map[string]uint64 // monotonic counter per client_id to detect stale OnDisconnect
	tenantConns map[string]int    // per-tenant active connection counter for rate limiting
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
// logic (see docs discussion on the keel-mqtt-cluster PoC).
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
		byte(mqtt.OnACLCheck),
		byte(mqtt.OnPublish),
		byte(mqtt.OnDisconnect),
		byte(mqtt.OnSubscribed),
		byte(mqtt.OnUnsubscribed),
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
	if h.fwd != nil {
		h.fwd.PublishConnection(context.Background(), tenantStr, info.ID.String(), "online")
	}

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
		if err := auth.ValidateJWTFromClientID(tid, did, password, tenantCfg.JWTPublicKeyPEM); err != nil {
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
	} else {
		deviceID = cl.ID
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
		if err := auth.ValidateJWT(tenantID, deviceID, password, tenantCfg.JWTPublicKeyPEM); err != nil {
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

	// Full verification with tenant's trusted CAs
	deviceID, tenantID, err = auth.VerifyCertificate(cert, tenantCfg.TrustedCAPEMs)
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

	h.fwd.Forward(ctx, info, pk.TopicName, pk.Payload, pk.FixedHeader.Qos)
	h.forwardToClusterSubscribers(ctx, info, pk)
	h.forwardToOutputConnector(ctx, info, pk)

	go h.provider.UpdateLastSeen(context.Background(), info.ID)

	tenantStr := info.TenantID.String()
	telemetry.MessagesPublished.WithLabelValues(tenantStr, strconv.Itoa(int(pk.FixedHeader.Qos))).Inc()
	telemetry.BytesPublished.WithLabelValues(tenantStr).Add(float64(len(pk.Payload)))

	span.SetStatus(codes.Ok, "")
	return pk, nil
}

// OnDisconnect decrements the active-connections gauge and clears this
// node's cluster-wide routing entries for whatever the client was still
// subscribed to.
// Uses the generation counter to avoid removing an entry that was already
// replaced by a newer connection with the same client_id — the same guard
// covers the cluster routing cleanup, since a superseded connection's
// filters may have already been legitimately re-subscribed by the newer
// connection on this same node (routing entries are per (topic, nodeID),
// not per client_id).
func (h *keelHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	h.mu.Lock()
	state, ok := h.clients[cl.ID]
	// Only delete if this disconnect belongs to the current generation.
	// A higher generation means a newer connection has already taken over.
	if ok && state.generation == h.generation[cl.ID] {
		delete(h.clients, cl.ID)
		if ts := state.info.TenantID.String(); h.tenantConns[ts] > 0 {
			h.tenantConns[ts]--
		}
	} else {
		ok = false // suppress metrics decrement and cluster cleanup below
	}
	h.mu.Unlock()

	if ok {
		telemetry.ActiveConnections.WithLabelValues(state.info.TenantID.String()).Dec()
		if h.fwd != nil {
			h.fwd.PublishConnection(context.Background(), state.info.TenantID.String(), state.info.ID.String(), "offline")
		}
		h.unsubscribeClusterFilters(cl)
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
	h.log.Info("mqtt-gateway: device disconnected", "client_id", cl.ID)
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
// (clusterRegistry == nil).
func (h *keelHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, _ []byte) {
	if h.clusterRegistry == nil {
		return
	}
	for _, f := range pk.Filters {
		if err := h.clusterRegistry.Subscribe(f.Filter, h.clusterNodeID); err != nil {
			h.log.Error("cluster: subscribe failed", "topic", f.Filter, "error", err)
		}
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
	nodes := h.clusterRegistry.NodesFor(pk.TopicName)
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

// forwardToOutputConnector forwards the message to the configured OutputConnector (if any).
// This runs in parallel to the existing keel-native forwarding (redpanda, cluster) and
// does not replace it — it's an additional output path for external system integration.
func (h *keelHook) forwardToOutputConnector(ctx context.Context, info *auth.DeviceInfo, pk packets.Packet) {
	if h.outputConnector == nil {
		return
	}

	req := &connector.ForwardRequest{
		Topic:    pk.TopicName,
		Payload:  pk.Payload,
		Headers:  map[string]string{"content-type": "application/json"},
		DeviceId: info.ID.String(),
		TenantId: info.TenantID.String(),
	}

	resp, err := h.outputConnector.Forward(ctx, req)
	if err != nil {
		h.log.Error("output-connector: forward error", "device_id", info.ID, "error", err)
		return
	}
	if !resp.Success {
		h.log.Warn("output-connector: forward failed", "device_id", info.ID, "error", resp.Error)
	}
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
