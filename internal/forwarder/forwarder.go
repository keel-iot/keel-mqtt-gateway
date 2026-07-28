// Package forwarder routes device MQTT messages to Redpanda.
//
// By default it speaks keel-native formats only: it publishes to the platform
// topic taxonomy and emits a keel-native twin envelope (twinUpdate) on
// twinInboundTopic, consumed by keel's twin-service. No Ditto/Hono wire formats
// are involved in this default path.
//
// Two optional, config-gated interop modes exist for customers with an existing
// Eclipse stack:
//   - dittoCompat: ALSO emit Eclipse Ditto Protocol envelopes on
//     dittoInboundTopic, so an external Eclipse Ditto can consume them.
//   - honoCompat: ALSO accept Eclipse Hono inbound topic forms (routing infix,
//     via-pattern gateway topics, property bags).
package forwarder

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/keel-iot/keel-mqtt-gateway/internal/auth"
	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
	"github.com/keel/pkg/redpanda"
	"github.com/redis/go-redis/v9"
)

// twinUpdate is keel's native twin-ingestion envelope, published to
// twinInboundTopic and consumed by twin-service. It carries only platform
// concepts (tenant/device IDs + the taxonomy category/type) and the raw device
// payload — no Ditto/Hono references.
type twinUpdate struct {
	TenantID string          `json:"tenant_id"`
	DeviceID string          `json:"device_id"`
	Category string          `json:"category"` // "telemetry" | "event"
	Type     string          `json:"type"`     // feature/subject, e.g. "metrics"
	Payload  json.RawMessage `json:"payload"`
}

// dittoMessage is the Eclipse Ditto Protocol envelope used only in the optional
// dittoCompat interop mode.
//
// Spec: https://eclipse.dev/ditto/protocol-specification.html
type dittoMessage struct {
	Topic   string            `json:"topic"`
	Headers map[string]string `json:"headers"`
	Path    string            `json:"path"`
	Value   json.RawMessage   `json:"value"`
}

// Forwarder routes device MQTT messages to Redpanda, emitting keel-native twin
// updates by default and, optionally, Eclipse Ditto/Hono interop formats.
type Forwarder struct {
	producer          *redpanda.Producer
	twinInboundTopic  string                  // keel-native twin feed; empty disables emission
	otaStatusTopic    string                  // flat OTA-status feed for ota-service; empty disables
	caStatusTopic     string                  // flat CA-anchor-ack feed for provisioning-service; empty disables
	connectionTopic   string                  // device connect/disconnect feed; empty disables
	dittoCompat       bool                    // when true, also emit Ditto Protocol envelopes
	dittoInboundTopic string                  // Ditto interop topic (used only when dittoCompat)
	honoCompat        bool                    // when true, accept Hono inbound topic forms
	rdb               *redis.Client           // optional; nil = no volume tracking
	tenantCache       *auth.TenantConfigCache // optional; nil = no volume limits
	log               *slog.Logger
}

// connectionEvent is the keel.device.connection envelope consumed by
// twin-service to maintain per-device connection state.
type connectionEvent struct {
	TenantID  string `json:"tenant_id"`
	DeviceID  string `json:"device_id"`
	State     string `json:"state"` // "online" | "offline"
	Timestamp string `json:"timestamp"`
}

// Config configures a Forwarder.
type Config struct {
	Producer *redpanda.Producer
	// TwinInboundTopic is the keel-native topic to publish twin updates to
	// (e.g. "keel.twin.inbound"). Empty disables twin emission.
	TwinInboundTopic string
	// OTAStatusTopic is the flat topic that device OTA status (status/ota) is
	// mirrored to for ota-service (e.g. "platform.ota.status"). Empty disables.
	OTAStatusTopic string
	// CAStatusTopic is the flat topic that device SSH CA anchor acks (status/ca)
	// are mirrored to for provisioning-service (e.g. "platform.ca.status"). It
	// gates the staged CA rotation (device adoption). Empty disables.
	CAStatusTopic string
	// ConnectionTopic carries device connect/disconnect events for twin-service
	// (e.g. "keel.device.connection"). Empty disables emission.
	ConnectionTopic string
	// DittoCompat enables optional Eclipse Ditto Protocol emission.
	DittoCompat bool
	// DittoInboundTopic is the topic for Ditto Protocol interop (used only when
	// DittoCompat is true).
	DittoInboundTopic string
	// HonoCompat enables optional Eclipse Hono inbound topic compatibility.
	HonoCompat bool
	// RDB and TenantCache are optional; when both non-nil, per-tenant daily
	// data-volume limits (max_bytes_per_day) are enforced.
	RDB         *redis.Client
	TenantCache *auth.TenantConfigCache
	Log         *slog.Logger
}

// New creates a new Forwarder from cfg.
func New(cfg Config) *Forwarder {
	return &Forwarder{
		producer:          cfg.Producer,
		twinInboundTopic:  cfg.TwinInboundTopic,
		otaStatusTopic:    cfg.OTAStatusTopic,
		caStatusTopic:     cfg.CAStatusTopic,
		connectionTopic:   cfg.ConnectionTopic,
		dittoCompat:       cfg.DittoCompat,
		dittoInboundTopic: cfg.DittoInboundTopic,
		honoCompat:        cfg.HonoCompat,
		rdb:               cfg.RDB,
		tenantCache:       cfg.TenantCache,
		log:               cfg.Log,
	}
}

// NewNoopForwarder returns a Forwarder with no Redpanda producer (dev mode).
func NewNoopForwarder(log *slog.Logger) *Forwarder {
	return &Forwarder{log: log}
}

// PublishConnection emits a device connect/disconnect event on the connection
// topic (keyed by device ID for per-device ordering). No-op when the producer
// or topic is not configured. Failures are logged, never propagated: connection
// tracking must never block the MQTT auth/disconnect path.
func (f *Forwarder) PublishConnection(ctx context.Context, tenantID, deviceID, state string) {
	if f.producer == nil || f.connectionTopic == "" {
		return
	}
	ev := connectionEvent{
		TenantID:  tenantID,
		DeviceID:  deviceID,
		State:     state,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := f.producer.Publish(ctx, f.connectionTopic, deviceID, ev); err != nil {
		f.log.Error("mqtt-gateway: publish connection event",
			"device_id", deviceID, "state", state, "error", err)
	}
}

// Forward routes a device publish to the appropriate Redpanda topics.
//
// mqttTopic is the raw topic the device published to. Both short form ("telemetry")
// and Hono form ("telemetry/<tenantID>/<deviceID>") are accepted.
// A gateway device may publish on behalf of a sub-device using the Hono via-pattern:
//
//	"via/<subDeviceID>/telemetry"
//	"via/<subDeviceID>/telemetry/<sub>"
//	"via/<subDeviceID>/event/<subject>"
//
// In that case the message is forwarded using the sub-device identity derived from
// <subDeviceID> (same tenant/fleet as the gateway).
// payload is the raw MQTT payload bytes.
// qos is the MQTT QoS level (0 = at-most-once telemetry, 1 = at-least-once event).
func (f *Forwarder) Forward(ctx context.Context, device *auth.DeviceInfo, mqttTopic string, payload []byte, qos byte) {
	// Hono MQTT property bag ("/?key=val…") is a Hono wire feature; strip it only
	// in compat mode so the default path stays Hono-free.
	if f.honoCompat {
		mqttTopic, _ = ParsePropertyBag(mqttTopic)
	}

	// Gate on per-tenant daily data-volume limit before any work.
	if f.rdb != nil && f.tenantCache != nil {
		tenantStr := device.TenantID.String()
		tenantCfg, _ := f.tenantCache.Get(ctx, tenantStr)
		var maxBytes int64
		if tenantCfg != nil {
			maxBytes = tenantCfg.MaxBytesPerDay
		}
		if err := CheckAndRecordBytes(ctx, f.rdb, tenantStr, len(payload), maxBytes); err != nil {
			f.log.Warn("mqtt-gateway: data volume limit exceeded, dropping message",
				"tenant", tenantStr, "payload_bytes", len(payload))
			telemetry.DataVolumeLimitExceeded.WithLabelValues(tenantStr).Inc()
			return
		}
	}

	// Resolve the target device. In Hono compat, a gateway device may publish on
	// behalf of a downstream sub-device via the "via/<subDeviceID>/..." pattern.
	target := device
	if f.honoCompat {
		target, mqttTopic = f.resolveViaTopic(device, mqttTopic)
	}

	// In Hono compat, strip the "<tenantID>/<deviceID>" routing infix so parseTopic
	// always receives the short canonical form ("telemetry", "telemetry/sub", …).
	shortTopic := mqttTopic
	if f.honoCompat {
		shortTopic = stripHonoInfix(mqttTopic, target)
	}
	category, typ := parseTopic(shortTopic)
	if category == "" {
		f.log.Warn("mqtt-gateway: unrecognised topic, dropping", "topic", mqttTopic, "device", target.ID)
		return
	}

	rpTopic := target.RedpandaTopic(category, typ)

	if f.producer == nil {
		f.log.Debug("mqtt-gateway: redpanda disabled, dropping", "topic", rpTopic)
		return
	}

	if err := f.producer.PublishRaw(ctx, rpTopic, target.ID.String(), payload); err != nil {
		f.log.Error("mqtt-gateway: publish to redpanda", "topic", rpTopic, "error", err)
		return
	}

	f.log.Debug("mqtt-gateway: forwarded", "rp_topic", rpTopic, "qos", qos, "bytes", len(payload))

	// keel-native twin feed (default). Only telemetry/events feed the twin;
	// heartbeat and OTA-status are skipped. Native (non-Ditto) payloads only —
	// a native Ditto payload is handled by the dittoCompat path below.
	if f.twinInboundTopic != "" && !isDittoPayload(payload) &&
		(category == "telemetry" || category == "event") {
		f.forwardTwin(ctx, target, category, typ, payload)
	}

	// Mirror device OTA status to a flat topic consumed by ota-service (the
	// uplink of the custom MQTT OTA protocol). Keyed by device ID.
	if f.otaStatusTopic != "" && category == "status" && typ == "ota" {
		if err := f.producer.PublishRaw(ctx, f.otaStatusTopic, target.ID.String(), payload); err != nil {
			f.log.Error("mqtt-gateway: publish ota status", "device_id", target.ID, "error", err)
		}
	}

	// Mirror device SSH CA anchor acks (status/ca) to a flat topic consumed by
	// provisioning-service. This is the fleet-adoption uplink that gates a staged
	// Clavex CA rotation: only when every device has acked does provisioning
	// authorise dismissing the old CA. Keyed by device ID.
	if f.caStatusTopic != "" && category == "status" && typ == "ca" {
		if err := f.producer.PublishRaw(ctx, f.caStatusTopic, target.ID.String(), payload); err != nil {
			f.log.Error("mqtt-gateway: publish ca status", "device_id", target.ID, "error", err)
		}
	}

	// Optional Eclipse Ditto Protocol interop (external Ditto / Kanto devices).
	if f.dittoCompat && f.dittoInboundTopic != "" {
		f.forwardDittoCompat(ctx, target, category, typ, payload)
	}
}

// forwardTwin publishes a keel-native twin update envelope to twinInboundTopic.
func (f *Forwarder) forwardTwin(ctx context.Context, device *auth.DeviceInfo, category, typ string, payload []byte) {
	if typ == "" {
		if category == "event" {
			typ = "event"
		} else {
			typ = "metrics"
		}
	}
	msg := twinUpdate{
		TenantID: device.TenantID.String(),
		DeviceID: device.ID.String(),
		Category: category,
		Type:     typ,
		Payload:  json.RawMessage(payload),
	}
	if err := f.producer.Publish(ctx, f.twinInboundTopic, device.ID.String(), msg); err != nil {
		f.log.Error("mqtt-gateway: publish twin update", "device_id", device.ID, "error", err)
	}
}

// forwardDittoCompat emits Eclipse Ditto Protocol for external-Ditto interop.
// A native Ditto payload (e.g. from Kanto) is passed through after injecting the
// resolved tenant-id/device-id headers; anything else is wrapped in a Ditto
// envelope for telemetry and events.
func (f *Forwarder) forwardDittoCompat(ctx context.Context, device *auth.DeviceInfo, category, typ string, payload []byte) {
	if isDittoPayload(payload) {
		augmented := augmentDittoHeaders(payload, device)
		if err := f.producer.PublishRaw(ctx, f.dittoInboundTopic, device.ID.String(), augmented); err != nil {
			f.log.Error("mqtt-gateway: publish ditto passthrough", "device_id", device.ID, "error", err)
		}
		return
	}
	if category == "telemetry" || category == "event" {
		f.forwardDitto(ctx, device, category, typ, payload)
	}
}

// forwardDitto emits a Ditto Protocol envelope to the ditto.inbound topic.
func (f *Forwarder) forwardDitto(
	ctx context.Context,
	device *auth.DeviceInfo,
	category, typ string,
	payload []byte,
) {
	// Ditto thing ID: {namespace}:{entity-id}
	thingID := fmt.Sprintf("%s:%s", device.TenantSlug, device.ID)

	var msg dittoMessage

	switch category {
	case "telemetry":
		// Feature properties update: /features/{typ}/properties/status
		featureName := typ
		if featureName == "" {
			featureName = "metrics"
		}
		msg = dittoMessage{
			Topic: fmt.Sprintf("%s/%s/things/twin/commands/modify", device.TenantSlug, device.ID),
			Headers: map[string]string{
				"content-type":   "application/json",
				"correlation-id": uuid.New().String(),
				"tenant-id":      device.TenantID.String(),
				"device-id":      device.ID.String(),
			},
			Path:  fmt.Sprintf("/features/%s/properties/status", featureName),
			Value: json.RawMessage(payload),
		}

	case "event":
		// Live message to thing inbox
		subject := typ
		if subject == "" {
			subject = "event"
		}
		msg = dittoMessage{
			Topic: fmt.Sprintf("%s/%s/things/live/messages/%s", device.TenantSlug, device.ID, subject),
			Headers: map[string]string{
				"content-type": "application/json",
				"thing-id":     thingID,
				"tenant-id":    device.TenantID.String(),
				"device-id":    device.ID.String(),
			},
			Path:  fmt.Sprintf("/inbox/messages/%s", subject),
			Value: json.RawMessage(payload),
		}
	}

	if err := f.producer.Publish(ctx, f.dittoInboundTopic, thingID, msg); err != nil {
		f.log.Error("mqtt-gateway: publish ditto inbound", "thing_id", thingID, "error", err)
	}
}

// resolveViaTopic checks whether mqttTopic starts with the Hono gateway pattern
// "via/<subDeviceID>/..." and, if so, returns a synthetic DeviceInfo for the
// sub-device (same tenant/fleet as the gateway, different device ID) and the
// remainder of the topic after the "via/<subDeviceID>/" prefix.
//
// The sub-device UUID must be a valid UUID; invalid values are treated as a
// regular (non-via) topic so existing topics beginning with "via" aren't broken.
//
// Example:
//
//	gateway publishes "via/<subID>/telemetry"
//	→ target = subDevice, topic = "telemetry"
func (f *Forwarder) resolveViaTopic(gateway *auth.DeviceInfo, topic string) (*auth.DeviceInfo, string) {
	const prefix = "via/"
	if !strings.HasPrefix(topic, prefix) {
		return gateway, topic
	}
	rest := topic[len(prefix):]
	slashIdx := strings.IndexByte(rest, '/')
	var subIDStr, subTopic string
	if slashIdx < 0 {
		// "via/<subID>" with no trailing topic — treat as gateway's own topic
		return gateway, topic
	}
	subIDStr = rest[:slashIdx]
	subTopic = rest[slashIdx+1:]

	subID, err := uuid.Parse(subIDStr)
	if err != nil {
		// Not a UUID → not a via-pattern, treat as regular topic
		return gateway, topic
	}

	sub := &auth.DeviceInfo{
		ID:         subID,
		TenantID:   gateway.TenantID,
		TenantSlug: gateway.TenantSlug,
		FleetID:    gateway.FleetID,
		FleetIDStr: gateway.FleetIDStr,
	}
	f.log.Debug("mqtt-gateway: via-pattern resolved",
		"gateway", gateway.ID, "sub_device", subID, "topic", subTopic)
	return sub, subTopic
}

// parseTopic maps a device MQTT topic to (category, type) pairs used in the
// Redpanda topic taxonomy.
//
// Device-side topics (relative, short):
//
//	"telemetry"          → ("telemetry", "metrics")
//	"telemetry/metrics"  → ("telemetry", "metrics")
//	"telemetry/events"   → ("telemetry", "events")
//	"event"              → ("telemetry", "events")   Hono event → telemetry.events
//	"event/{subject}"    → ("telemetry", "{subject}")
//	"status/heartbeat"   → ("status", "heartbeat")
//	"status/ota"         → ("status", "ota")
func parseTopic(topic string) (category, typ string) {
	parts := strings.SplitN(topic, "/", 2)
	switch parts[0] {
	case "telemetry", "t":
		category = "telemetry"
		if len(parts) == 2 && parts[1] != "" {
			typ = parts[1]
		} else {
			typ = "metrics"
		}
	case "event", "e":
		// Hono uses "event" for at-least-once telemetry payloads.
		// Map to ("telemetry", "events") to match the platform taxonomy.
		category = "telemetry"
		if len(parts) == 2 && parts[1] != "" {
			typ = parts[1]
		} else {
			typ = "events"
		}
	case "status":
		category = "status"
		if len(parts) == 2 {
			typ = parts[1]
		} else {
			typ = "heartbeat"
		}
	}
	return
}

// stripHonoInfix strips the <tenantID>/<deviceID> routing infix that suite-connector
// inserts when forwarding to keel-gateway in Hono topic format, e.g.:
//
//	"telemetry/<tenantID>/<deviceID>"       → "telemetry"
//	"telemetry/<tenantID>/<deviceID>/sub"   → "telemetry/sub"
//	"event/<tenantID>/<deviceID>/subject"   → "event/subject"
//	"telemetry"                             → "telemetry"  (unchanged)
//
// Two infix formats are tried:
//  1. UUID-based:  <tenantUUID>/<deviceUUID>   (when username format is devID@tenantID)
//  2. Slug-based:  <tenantSlug>/<anything>     (when suite-connector uses tenantId=slug and deviceId=namespace:id)
func stripHonoInfix(topic string, device *auth.DeviceInfo) string {
	// 1. UUID-based infix
	infix := device.TenantID.String() + "/" + device.ID.String()
	if idx := strings.Index(topic, infix); idx >= 0 {
		prefix := strings.TrimSuffix(topic[:idx], "/")
		suffix := strings.TrimPrefix(topic[idx+len(infix):], "/")
		if suffix == "" {
			return prefix
		}
		return prefix + "/" + suffix
	}

	// 2. Slug-based infix: <category>/<tenantSlug>/<deviceId>[/<subject>]
	// suite-connector uses tenantId (slug) + deviceId (Hono namespace:id format).
	slugInfix := device.TenantSlug + "/"
	if idx := strings.Index(topic, slugInfix); idx >= 0 {
		before := strings.TrimSuffix(topic[:idx], "/")
		after := topic[idx+len(slugInfix):] // skip tenantSlug/
		// skip the deviceId segment (everything up to the next /)
		slashIdx := strings.IndexByte(after, '/')
		if slashIdx < 0 {
			// no subject after deviceId
			return before
		}
		rest := after[slashIdx+1:]
		if rest == "" {
			return before
		}
		return before + "/" + rest
	}

	return topic
}

// isDittoPayload returns true when the bytes look like a Ditto protocol message
// (a JSON object with non-empty "topic" and "path" fields).
// This is a fast structural check, not a full validation.
func isDittoPayload(b []byte) bool {
	var m struct {
		Topic string `json:"topic"`
		Path  string `json:"path"`
	}
	return json.Unmarshal(b, &m) == nil && m.Topic != "" && m.Path != ""
}

// augmentDittoHeaders parses b as a Ditto message, injects the resolved
// tenant-id and device-id into its headers map, and re-serialises.
// On any error the original bytes are returned unchanged.
func augmentDittoHeaders(b []byte, device *auth.DeviceInfo) []byte {
	var msg map[string]any
	if err := json.Unmarshal(b, &msg); err != nil {
		return b
	}
	headers, _ := msg["headers"].(map[string]any)
	if headers == nil {
		headers = make(map[string]any)
	}
	headers["tenant-id"] = device.TenantID.String()
	headers["device-id"] = device.ID.String()
	msg["headers"] = headers
	out, err := json.Marshal(msg)
	if err != nil {
		return b
	}
	return out
}
