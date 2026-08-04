package acl

// Decision is the outcome of an ACL evaluation, together with the rule (if
// any) that produced it — kept for debugging/audit purposes, since the
// design calls for the decision to be explainable, not just a bare bool.
type Decision struct {
	Effect Effect
	Rule   *ACLRule // nil when the decision is the fail-closed default (no matching rule)
}

// Allowed is a small convenience for callers that only care about the
// allow/deny outcome (e.g. the mochi-mqtt OnACLCheck hook).
func (d Decision) Allowed() bool { return d.Effect == EffectAllow }

// Evaluate decides whether principal (clientID/username) may perform
// action on topic, given enabledRulesets and the principal's own
// customRules (resolved from Binding → Role lookups by the caller).
//
// Precedence:
//  1. Any matching deny wins outright, regardless of specificity — a
//     narrow custom deny is the right way to carve an exception out of a
//     broad standard-ruleset allow.
//  2. Among same-effect matches, the most specific filter is recorded as
//     the explaining Rule (audit only, doesn't change the outcome).
//  3. No matching rule at all falls through to deny.
func Evaluate(clientID, username, topic string, action Action, enabledRulesets []Role, customRules []ACLRule) Decision {
	var bestDeny, bestAllow *ACLRule
	bestDenySpec, bestAllowSpec := -1, -1

	consider := func(r ACLRule) {
		if !r.allowsAction(action) {
			return
		}
		if !MatchTopic(r.resolvedFilter(clientID, username), topic) {
			return
		}
		s := specificity(r.TopicFilter)
		switch r.Effect {
		case EffectDeny:
			if s > bestDenySpec {
				bestDenySpec = s
				rc := r
				bestDeny = &rc
			}
		case EffectAllow:
			if s > bestAllowSpec {
				bestAllowSpec = s
				rc := r
				bestAllow = &rc
			}
		}
	}

	for _, role := range enabledRulesets {
		for _, r := range role.Rules {
			consider(r)
		}
	}
	for _, r := range customRules {
		consider(r)
	}

	// Rule 1: any explicit deny beats any allow, unconditionally.
	if bestDeny != nil {
		return Decision{Effect: EffectDeny, Rule: bestDeny}
	}
	if bestAllow != nil {
		return Decision{Effect: EffectAllow, Rule: bestAllow}
	}
	// Rule 3: fail-closed default — no candidate rule matched at all.
	return Decision{Effect: EffectDeny, Rule: nil}
}
