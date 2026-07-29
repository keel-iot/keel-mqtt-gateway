package acl

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"telemetry/device-1/#", "telemetry/device-1/temp", true},
		{"telemetry/device-1/#", "telemetry/device-1", true},
		{"telemetry/device-1/#", "telemetry/device-2/temp", false},
		{"telemetry/#", "telemetry/anything/deep/path", true},
		{"cmd/device-1", "cmd/device-1", true},
		{"cmd/device-1", "cmd/device-2", false},
		{"telemetry/+/status", "telemetry/dev1/status", true},
		{"telemetry/+/status", "telemetry/dev1/dev2/status", false},
		{"$SYS/foo", "$SYS/foo", true},
		{"#", "$SYS/foo", false}, // top-level wildcard must not match $ topics
	}
	for _, c := range cases {
		got := MatchTopic(c.filter, c.topic)
		if got != c.want {
			t.Errorf("MatchTopic(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func TestSpecificityOrdering(t *testing.T) {
	// User's own example: a resolved per-client filter must outrank the
	// generic multi-level wildcard.
	specific := specificity("telemetry/device-1/temp")
	generic := specificity("telemetry/#")
	if specific <= generic {
		t.Fatalf("expected telemetry/device-1/temp (%d) more specific than telemetry/# (%d)", specific, generic)
	}

	plusFilter := specificity("telemetry/+/status")
	hashFilter := specificity("telemetry/#")
	if plusFilter <= hashFilter {
		t.Fatalf("expected + filter (%d) more specific than # filter (%d)", plusFilter, hashFilter)
	}
}

func TestEvaluate_DenyBeatsAllowRegardlessOfSpecificity(t *testing.T) {
	// Broad allow from a standard ruleset, narrow deny from a custom rule.
	// Deny must win even though, if specificity alone decided, the allow
	// isn't necessarily less specific here — deny wins unconditionally by
	// design, not because it's "more specific".
	enabled := []Role{
		{Name: "std", Rules: []ACLRule{
			{TopicFilter: "telemetry/#", Actions: []string{"publish"}, Effect: EffectAllow},
		}},
	}
	custom := []ACLRule{
		{TopicFilter: "telemetry/device-1/secret", Actions: []string{"publish"}, Effect: EffectDeny},
	}
	d := Evaluate("device-1", "device-1", "telemetry/device-1/secret", ActionPublish, enabled, custom)
	if d.Allowed() {
		t.Fatalf("expected deny to win, got %+v", d)
	}

	// Same principal, different (non-denied) topic under the same broad
	// allow, should still be allowed.
	d2 := Evaluate("device-1", "device-1", "telemetry/device-1/temp", ActionPublish, enabled, custom)
	if !d2.Allowed() {
		t.Fatalf("expected unrelated topic to remain allowed, got %+v", d2)
	}
}

func TestEvaluate_MostSpecificAmongSameEffect(t *testing.T) {
	enabled := []Role{
		{Name: "std", Rules: []ACLRule{
			{TopicFilter: "telemetry/#", Actions: []string{"publish"}, Effect: EffectAllow},
		}},
	}
	custom := []ACLRule{
		{TopicFilter: "telemetry/%c/temp", Actions: []string{"publish"}, Effect: EffectAllow},
	}
	d := Evaluate("device-1", "device-1", "telemetry/device-1/temp", ActionPublish, enabled, custom)
	if !d.Allowed() {
		t.Fatalf("expected allow, got %+v", d)
	}
	if d.Rule == nil || d.Rule.TopicFilter != "telemetry/%c/temp" {
		t.Fatalf("expected the more specific rule to be recorded as the explaining rule, got %+v", d.Rule)
	}
}

func TestEvaluate_FailClosedOnUnknownPrincipal(t *testing.T) {
	// No enabled rulesets, no custom rules at all — the natural fallthrough
	// must be deny, not a special early-return.
	d := Evaluate("ghost", "ghost", "telemetry/ghost/temp", ActionPublish, nil, nil)
	if d.Allowed() {
		t.Fatalf("expected fail-closed deny for unknown principal with no rules, got %+v", d)
	}
	if d.Rule != nil {
		t.Fatalf("expected nil explaining rule for the fail-closed default, got %+v", d.Rule)
	}
}

func TestEvaluate_FailClosedOnNoApplicableRule(t *testing.T) {
	// Principal is known (has rules) but none of them apply to this
	// particular action/topic combination — still deny, same fallthrough.
	enabled := []Role{StandardRulesets["keel-device-default"]}
	// device-1 tries to subscribe to a wildcard telemetry topic, which
	// keel-device-default only allows it to *publish* to, not subscribe.
	d := Evaluate("device-1", "device-1", "telemetry/device-1/temp", ActionSubscribe, enabled, nil)
	if d.Allowed() {
		t.Fatalf("expected deny for action with no matching rule, got %+v", d)
	}
}

func TestEvaluate_UnionAcrossRulesetsAndBindings(t *testing.T) {
	enabled := []Role{
		StandardRulesets["keel-device-default"],
		{Name: "extra-std", Rules: []ACLRule{
			{TopicFilter: "shared/status", Actions: []string{"subscribe"}, Effect: EffectAllow},
		}},
	}
	custom := []ACLRule{
		{TopicFilter: "diagnostics/%c", Actions: []string{"publish", "subscribe"}, Effect: EffectAllow},
	}

	// From keel-device-default:
	d1 := Evaluate("device-1", "device-1", "telemetry/device-1/x", ActionPublish, enabled, custom)
	if !d1.Allowed() {
		t.Fatalf("expected keel-device-default publish rule to apply, got %+v", d1)
	}
	d2 := Evaluate("device-1", "device-1", "cmd/device-1", ActionSubscribe, enabled, custom)
	if !d2.Allowed() {
		t.Fatalf("expected keel-device-default subscribe rule to apply, got %+v", d2)
	}
	// From the extra standard ruleset:
	d3 := Evaluate("device-1", "device-1", "shared/status", ActionSubscribe, enabled, custom)
	if !d3.Allowed() {
		t.Fatalf("expected extra-std ruleset rule to apply, got %+v", d3)
	}
	// From the custom binding:
	d4 := Evaluate("device-1", "device-1", "diagnostics/device-1", ActionPublish, enabled, custom)
	if !d4.Allowed() {
		t.Fatalf("expected custom binding rule to apply, got %+v", d4)
	}
	// Nothing grants this:
	d5 := Evaluate("device-1", "device-1", "other/topic", ActionPublish, enabled, custom)
	if d5.Allowed() {
		t.Fatalf("expected deny for topic not covered by any rule, got %+v", d5)
	}
}

func TestEvaluate_PlaceholderSubstitution(t *testing.T) {
	custom := []ACLRule{
		{TopicFilter: "telemetry/%c/#", Actions: []string{"publish"}, Effect: EffectAllow},
	}
	// Substitution must be per-principal: device-2's clientID must not
	// satisfy a rule resolved for device-1.
	dOwn := Evaluate("device-1", "device-1", "telemetry/device-1/temp", ActionPublish, nil, custom)
	if !dOwn.Allowed() {
		t.Fatalf("expected own-topic publish to be allowed, got %+v", dOwn)
	}
	dOther := Evaluate("device-2", "device-2", "telemetry/device-1/temp", ActionPublish, nil, custom)
	if dOther.Allowed() {
		t.Fatalf("expected device-2 to be denied publishing on device-1's topic, got %+v", dOther)
	}

	// %u substitution against username instead of clientID.
	userCustom := []ACLRule{
		{TopicFilter: "acct/%u/inbox", Actions: []string{"subscribe"}, Effect: EffectAllow},
	}
	dUser := Evaluate("client-abc", "alice", "acct/alice/inbox", ActionSubscribe, nil, userCustom)
	if !dUser.Allowed() {
		t.Fatalf("expected %%u substitution to allow alice's own inbox, got %+v", dUser)
	}
}
