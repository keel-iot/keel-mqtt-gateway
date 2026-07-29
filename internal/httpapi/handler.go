// Package httpapi implements the Eclipse Hono-compatible HTTP Adapter.
//
// Devices that cannot use MQTT (e.g. constrained HTTP-only devices) can send
// telemetry and events via plain HTTP using the same credentials as MQTT.
//
// Endpoint reference:
//
//	PUT  /telemetry         — at-most-once (QoS 0 semantic)
//	POST /telemetry         — same as PUT
//	PUT  /event             — at-least-once (QoS 1 semantic, acks after Redpanda)
//	POST /event             — same as PUT
//	PUT  /event/{subject}   — named event (live message)
//	GET  /healthz           — liveness probe
//
// Authentication: HTTP Basic with {device_uuid}:{mqtt_token}  OR
// Bearer token in Authorization header (token = mqtt_token, device ID from header
// X-Device-ID or query parameter device-id).
package httpapi

import (
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/forwarder"
)

// Handler holds the HTTP adapter dependencies.
type Handler struct {
	validator   *auth.Validator
	tenantCache *auth.TenantConfigCache
	jwksCache   *auth.JWKSCache
	fwd         *forwarder.Forwarder
	log         *slog.Logger
}

// New creates a new HTTP adapter Handler.
func New(validator *auth.Validator, fwd *forwarder.Forwarder, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{validator: validator, fwd: fwd, log: log}
}

// NewWithCache creates a Handler that also supports per-tenant JWT
// authentication. jwks may be nil if no tenant uses JWKSURL — tenants that
// do will fail JWT auth until it's provided.
func NewWithCache(validator *auth.Validator, cache *auth.TenantConfigCache, jwks *auth.JWKSCache, fwd *forwarder.Forwarder, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	return &Handler{validator: validator, tenantCache: cache, jwksCache: jwks, fwd: fwd, log: log}
}

// Router returns the chi router for the HTTP adapter.
func (h *Handler) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Telemetry (at-most-once) — mirrors Hono PUT /telemetry
	r.Put("/telemetry", h.handleTelemetry)
	r.Post("/telemetry", h.handleTelemetry)
	r.Put("/telemetry/{sub}", h.handleTelemetrySub)
	r.Post("/telemetry/{sub}", h.handleTelemetrySub)

	// Events (at-least-once) — mirrors Hono PUT /event
	r.Put("/event", h.handleEvent("event"))
	r.Post("/event", h.handleEvent("event"))
	r.Put("/event/{subject}", h.handleEventSub)
	r.Post("/event/{subject}", h.handleEventSub)

	return r
}

// handleTelemetry handles PUT/POST /telemetry — routes to topic "telemetry/metrics".
func (h *Handler) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	device, payload, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	// QoS 0 — at-most-once
	h.fwd.Forward(r.Context(), device, "telemetry/metrics", payload, 0)
	w.WriteHeader(http.StatusAccepted)
}

// handleTelemetrySub handles PUT/POST /telemetry/{sub} — routes to "telemetry/{sub}".
func (h *Handler) handleTelemetrySub(w http.ResponseWriter, r *http.Request) {
	device, payload, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	sub := chi.URLParam(r, "sub")
	h.fwd.Forward(r.Context(), device, "telemetry/"+sub, payload, 0)
	w.WriteHeader(http.StatusAccepted)
}

// handleEvent returns an HTTP handler for events with the given fixed subject.
func (h *Handler) handleEvent(subject string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		device, payload, ok := h.authenticate(w, r)
		if !ok {
			return
		}
		// QoS 1 — at-least-once (Redpanda publish is synchronous, so ack = 201)
		h.fwd.Forward(r.Context(), device, "event/"+subject, payload, 1)
		w.WriteHeader(http.StatusCreated)
	}
}

// handleEventSub handles PUT/POST /event/{subject}.
func (h *Handler) handleEventSub(w http.ResponseWriter, r *http.Request) {
	device, payload, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	subject := chi.URLParam(r, "subject")
	h.fwd.Forward(r.Context(), device, "event/"+subject, payload, 1)
	w.WriteHeader(http.StatusCreated)
}

// authenticate extracts credentials and payload from the request.
// Auth precedence: JWT ("eyJ" prefix) → password token.
// It writes the appropriate error response and returns (nil, nil, false) on failure.
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (*auth.DeviceInfo, []byte, bool) {
	deviceID, tenantID, token, ok := extractCredentials(r)
	if !ok {
		http.Error(w, "missing or invalid Authorization header", http.StatusUnauthorized)
		return nil, nil, false
	}

	var device *auth.DeviceInfo
	var err error

	if auth.DetectAuthMethod([]byte(token)) == auth.AuthMethodJWT && h.tenantCache != nil && tenantID != "" {
		// JWT path
		cfg, cfgErr := h.tenantCache.Get(r.Context(), tenantID)
		if cfgErr != nil || !cfg.JWTAuthEnabled {
			h.log.Warn("http-adapter: JWT auth not enabled", "tenant", tenantID)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return nil, nil, false
		}
		if jwtErr := auth.ValidateJWT(r.Context(), tenantID, deviceID, []byte(token), cfg, h.jwksCache); jwtErr != nil {
			h.log.Warn("http-adapter: JWT validation failed", "device", deviceID, "error", jwtErr)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return nil, nil, false
		}
		device, err = h.validator.LookupByCN(r.Context(), deviceID, tenantID)
	} else {
		// Password token path (original behaviour)
		device, err = h.validator.Validate(r.Context(), deviceID, token)
	}

	if err != nil {
		h.log.Warn("http-adapter: auth failed", "device", deviceID, "error", err)
		return nil, nil, false
	}

	// Limit body size to 1 MiB to prevent memory exhaustion.
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || len(body) == 0 {
		http.Error(w, "request body required", http.StatusBadRequest)
		return nil, nil, false
	}

	return device, body, true
}

// extractCredentials tries to extract (deviceID, tenantID, token) from:
//  1. HTTP Basic:  username=<deviceID>@<tenantID>   password=<token or JWT>
//                 username=<deviceID>               password=<token>  (legacy, no tenant)
//  2. Bearer <token> + X-Device-ID: <deviceID>@<tenantID>  (or ?device-id=...)
//
// tenantID may be empty for legacy password-only mode.
func extractCredentials(r *http.Request) (deviceID, tenantID, token string, ok bool) {
	return ExtractCredentials(r)
}

// ExtractCredentials is the exported version of extractCredentials, exposed for
// testing and for potential reuse by other adapters.
func ExtractCredentials(r *http.Request) (deviceID, tenantID, token string, ok bool) {
	hdr := r.Header.Get("Authorization")
	if hdr == "" {
		return
	}

	if strings.HasPrefix(hdr, "Basic ") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(hdr, "Basic "))
		if err != nil {
			return
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			return
		}
		username := parts[0]
		token = parts[1]
		if idx := strings.LastIndex(username, "@"); idx > 0 {
			deviceID = username[:idx]
			tenantID = username[idx+1:]
		} else {
			deviceID = username
		}
		ok = deviceID != "" && token != ""
		return
	}

	if strings.HasPrefix(hdr, "Bearer ") {
		token = strings.TrimPrefix(hdr, "Bearer ")
		raw := r.Header.Get("X-Device-ID")
		if raw == "" {
			raw = r.URL.Query().Get("device-id")
		}
		if idx := strings.LastIndex(raw, "@"); idx > 0 {
			deviceID = raw[:idx]
			tenantID = raw[idx+1:]
		} else {
			deviceID = raw
		}
		ok = deviceID != "" && token != ""
		return
	}

	return
}
