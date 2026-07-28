package acl

// StandardRulesets holds the built-in, named rule bundles ("rulesets")
// shipped in Go code. They are only rule *templates*: whether a ruleset's
// rules are actually consulted during evaluation is cluster state (the
// enabled-ruleset set replicated via raft, toggled only by the
// acl.enable_ruleset / acl.disable_ruleset commands) — never implied by a
// binary upgrade. Evaluate callers must look up which of these names are
// currently enabled and pass only those Roles as enabledRulesets.
//
// keel-device-default reproduces, as a ruleset, the ACL shape given in the
// design prompt: a device may publish under its own "telemetry/<id>/..."
// tree and subscribe to its own "cmd/<id>" command topic — PLUS, as of
// rbac-migration, the literal alias/command rules that internal/broker/
// hooks.go's hardcoded isAllowedPublish/OnACLCheck already grant, wherever
// those can be expressed faithfully with the %c/%u placeholder model (see
// acl.ACLRule's doc comment).
//
// RECONCILIATION STATUS (rbac-migration, resolved rather than silently
// glossed over — see the design-questions section of the project plan for
// the full history of this gap):
//
//   - Reproduced 1:1 below: the short aliases hooks.go always allows for
//     ANY authenticated device with no ownership check ("t", "e",
//     "t/#", "e/#" prefixes, and the three literal "status/heartbeat|
//     ota|ca" topics), plus the device's own "cmd/%c" / "command/%c" /
//     "command/%c/#" subscribe topics (device-ID-only patterns, which
//     %c expresses exactly).
//   - Reproduced approximately: "telemetry/%c/#" / "event/%c/#" publish —
//     matches hooks.go's *device-ID-only* ownership shape. hooks.go's
//     isHonoTopicOwned check is actually keyed on Hono's two-segment
//     "telemetry/<tenantID>/<deviceID>/..." shape (tenant THEN device),
//     which %c/%u cannot express: %c is the raw MQTT ClientID and %u is
//     the raw username (for password auth, "<deviceID>@<tenantID>" —
//     device first, tenant second, the opposite order and shape needed).
//     Expressing tenant+device ownership needs either a third placeholder
//     resolved from auth.DeviceInfo (not just ClientID/username, which is
//     all EvaluateACL's signature carries today) or a richer segment-aware
//     rule DSL. NOT implemented here — deliberately left unreproduced, not
//     silently dropped: when a real Hono-shaped topic
//     ("telemetry/<tenant>/<device>/...") is checked, this ruleset's %c
//     rule does not match it (since the topic's first segment is a tenant
//     UUID, not the device ID), so EvaluateACL returns a nil-Rule decision
//     and internal/broker/hooks.go's OnACLCheck safely falls through to
//     the legacy isHonoTopicOwned/isHonoTopicShape logic UNCHANGED, which
//     still performs the real tenant+device ownership check. This ruleset
//     only takes over for the topic shapes it can express correctly.
//   - Not reproduced at all (same fallthrough safety net applies): the
//     "via/<uuid>/..." gateway delegation prefix, and the
//     "command/<tenant>//<device>/req/#" three-segment literal pattern —
//     both need more than clientID/username substitution to express
//     safely; left to hooks.go's legacy logic.
//
// Net effect: enabling keel-device-default takes over policy for the
// topic shapes it can express exactly (short aliases, device-ID-only
// telemetry/event/command patterns); every other existing topic shape
// keeps working exactly as before via the additive-fallback wiring in
// internal/broker/hooks.go's OnACLCheck. This is a deliberately partial,
// but *safe and explicit* (fail-closed on the RBAC side, fall-through on
// abstain) first step — full Hono tenant+device equivalence needs the
// EvaluateACL signature (and its proto/gRPC wire form) extended with a
// tenant identifier, which is out of scope for this step and flagged for
// a future iteration if `keel-device-default` needs to fully subsume
// hooks.go's Hono-shape/via-delegation logic instead of coexisting with it.
var StandardRulesets = map[string]Role{
	"keel-device-default": {
		Name: "keel-device-default",
		Rules: []ACLRule{
			// Device-ID-only telemetry/event ownership (see reconciliation
			// note above for why this doesn't yet cover the Hono
			// tenant+device shape).
			{TopicFilter: "telemetry/%c/#", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "event/%c/#", Actions: []string{"publish"}, Effect: EffectAllow},

			// Short aliases hooks.go grants unconditionally to any
			// authenticated device (no ownership check in the original
			// logic either — reproduced faithfully, not tightened).
			{TopicFilter: "t", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "t/#", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "e", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "e/#", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "telemetry", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "event", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "status/heartbeat", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "status/ota", Actions: []string{"publish"}, Effect: EffectAllow},
			{TopicFilter: "status/ca", Actions: []string{"publish"}, Effect: EffectAllow},

			// Device's own command topic, all three subscribe forms
			// hooks.go supports for the device-ID-only case (the
			// tenant-qualified "command/<tenant>//<device>/req/#" form
			// needs a tenant placeholder — see reconciliation note).
			{TopicFilter: "cmd/%c", Actions: []string{"subscribe"}, Effect: EffectAllow},
			{TopicFilter: "command/%c", Actions: []string{"subscribe"}, Effect: EffectAllow},
			{TopicFilter: "command/%c/#", Actions: []string{"subscribe"}, Effect: EffectAllow},
		},
	},
}
