package acl

import "testing"

// TestKeelDeviceDefaultReproducesLegacyDeviceIDShapes locks down the
// rbac-migration reconciliation: every topic shape internal/broker/
// hooks.go's isAllowedPublish/OnACLCheck grants unconditionally (short
// aliases) or on a device-ID-only basis (no Hono tenant+device ownership
// needed) must be allowed by keel-device-default once enabled, for the
// exact clientID it was resolved against. See standard.go's package
// comment for what is deliberately NOT reproduced here (Hono tenant+
// device shape, via/ delegation, tenant-qualified command topic) and why
// that's safe under the additive-fallback OnACLCheck wiring.
func TestKeelDeviceDefaultReproducesLegacyDeviceIDShapes(t *testing.T) {
	role := StandardRulesets["keel-device-default"]
	enabled := []Role{role}
	const clientID = "device-123"
	const username = "device-123@tenant-abc"

	publishAllowed := []string{
		"telemetry/device-123/temp",
		"telemetry/device-123",
		"event/device-123/motion",
		"event/device-123",
		"t",
		"t/sub",
		"e",
		"e/sub",
		"telemetry",
		"event",
		"status/heartbeat",
		"status/ota",
		"status/ca",
	}
	for _, topic := range publishAllowed {
		d := Evaluate(clientID, username, topic, ActionPublish, enabled, nil)
		if !d.Allowed() {
			t.Errorf("publish %q: want allow, got deny (rule=%v)", topic, d.Rule)
		}
	}

	// A different device must never be allowed to publish under another
	// device's telemetry/event tree via this ruleset (%c-scoped).
	otherDeviceTopics := []string{"telemetry/device-999/temp", "event/device-999/motion"}
	for _, topic := range otherDeviceTopics {
		d := Evaluate(clientID, username, topic, ActionPublish, enabled, nil)
		if d.Allowed() {
			t.Errorf("publish %q by %q: want deny (not owner), got allow (rule=%v)", topic, clientID, d.Rule)
		}
	}

	subscribeAllowed := []string{"cmd/device-123", "command/device-123", "command/device-123/sub"}
	for _, topic := range subscribeAllowed {
		d := Evaluate(clientID, username, topic, ActionSubscribe, enabled, nil)
		if !d.Allowed() {
			t.Errorf("subscribe %q: want allow, got deny (rule=%v)", topic, d.Rule)
		}
	}

	// Another device's command topic must stay denied.
	d := Evaluate(clientID, username, "command/device-999", ActionSubscribe, enabled, nil)
	if d.Allowed() {
		t.Errorf("subscribe to another device's command topic: want deny, got allow (rule=%v)", d.Rule)
	}
}

// TestKeelDeviceDefaultAbstainsOnHonoTenantDeviceShape documents (and
// pins) the deliberately-unreproduced gap: a real Hono-shaped
// "telemetry/<tenant>/<device>/..." topic does NOT match this ruleset's
// %c-scoped rule (clientID is the device ID, not the tenant), so
// EvaluateACL abstains (nil Rule) and internal/broker/hooks.go's
// OnACLCheck safely falls through to the legacy isHonoTopicOwned check
// instead of incorrectly denying it outright.
func TestKeelDeviceDefaultAbstainsOnHonoTenantDeviceShape(t *testing.T) {
	role := StandardRulesets["keel-device-default"]
	enabled := []Role{role}
	const clientID = "device-123"
	const username = "device-123@tenant-abc"

	d := Evaluate(clientID, username, "telemetry/tenant-abc/device-123/temp", ActionPublish, enabled, nil)
	if d.Rule != nil {
		t.Fatalf("expected abstain (nil Rule) on Hono tenant+device topic shape, got rule=%v effect=%v", d.Rule, d.Effect)
	}
}
