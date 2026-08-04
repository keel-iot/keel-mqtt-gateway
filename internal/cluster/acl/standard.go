package acl

// StandardRulesets holds the built-in, named rule bundles shipped in Go
// code. They're only templates — whether a ruleset is actually consulted
// is cluster state (the enabled set, replicated via raft), never implied
// by a binary upgrade. Callers must look up which names are enabled and
// pass only those Roles to Evaluate.
//
// keel-device-default reproduces hooks.go's hardcoded ACL for the topic
// shapes %c/%u can express exactly (short aliases, device-ID-only
// telemetry/event/command patterns). It does NOT cover Hono's tenant+device
// ownership shape ("telemetry/<tenant>/<device>/..."), the via/<uuid>
// gateway delegation prefix, or the command/<tenant>//<device>/req/#
// pattern — those need more than clientID/username substitution to
// express safely. When this ruleset's rules don't match, EvaluateACL
// returns a nil-Rule decision and OnACLCheck falls through to the legacy
// logic unchanged, so enabling this ruleset only takes over the shapes it
// can express correctly and never silently drops the rest.
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
