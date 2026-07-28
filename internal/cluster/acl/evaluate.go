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

// Evaluate decides whether principal (identified by clientID/username) may
// perform action on topic, given the set of currently-enabled standard
// ruleset roles (enabledRulesets) and the principal's own custom rules
// (customRules, typically resolved by the caller from Binding → Role
// lookups in FSM state).
//
// Precedence, per design:
//  1. An explicit deny match beats every allow match, no matter which
//     ruleset or binding it came from. This is evaluated as an absolute
//     priority: if *any* candidate rule matching (action, topic) has
//     Effect==deny, the result is deny, full stop — specificity is not
//     even consulted in that case, because "deny always wins" is stronger
//     than "most specific wins". (Specificity only distinguishes between
//     rules of the *same* effect — see Rule below — which is why writing a
//     narrower custom deny is the correct way to carve an exception out of
//     a broad standard-ruleset allow: the deny wins unconditionally once
//     it matches, and specificity there is about making sure the deny's
//     filter targets only the topics you mean to restrict, not about
//     out-ranking the competing allow.)
//  2. Among matches of the *same* effect, the most specific topic filter
//     is recorded as the explaining Rule (useful for audit/debugging);
//     it does not change the outcome, since same-effect matches already
//     agree on the result.
//  3. No applicable rule at all (empty candidate set) → deny. This is the
//     natural fallthrough of the loop below, not a special-cased check for
//     "unknown principal" — an unknown principal simply contributes no
//     custom rules, and if no enabled ruleset matches either, the
//     candidate set is empty and we fall through to deny exactly the same
//     way as for a known-but-unauthorized principal.
func Evaluate(clientID, username, topic string, action Action, enabledRulesets []Role, customRules []ACLRule) Decision {
	var bestDeny, bestAllow *ACLRule
	bestDenySpec, bestAllowSpec := -1, -1

	consider := func(r ACLRule) {
		if !r.allowsAction(action) {
			return
		}
		if !matchTopic(r.resolvedFilter(clientID, username), topic) {
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
