// Package acl implements the keel MQTT cluster's configurable RBAC engine:
// role/binding/ruleset data types and the pure evaluation function that
// decides whether a principal may publish or subscribe to a topic.
//
// State (roles, bindings, active-ruleset set) is authoritative,
// low-frequency-write data and lives in the raft FSM (see
// internal/cluster/raft's ACL command/state additions) — unlike topic
// routing, which is high-frequency and moved to Olric. This package only
// contains the data model and the evaluation logic; it has no knowledge of
// raft, gRPC, or HTTP, so it can be unit-tested in isolation and reused
// identically on core and edge nodes.
package acl

import "strings"

// Action identifies whether a rule/check concerns publishing or
// subscribing. MQTT has no other topic-level operations to authorize.
type Action string

const (
	ActionPublish   Action = "publish"
	ActionSubscribe Action = "subscribe"
)

// Effect is the outcome a rule grants when it matches.
type Effect string

const (
	EffectAllow Effect = "allow"
	EffectDeny  Effect = "deny"
)

// ACLRule is a single (topic filter, actions, effect) statement. TopicFilter
// supports standard MQTT wildcards (+, #) plus two placeholders substituted
// per-principal at evaluation time:
//
//	%c — the connecting client ID
//	%u — the authenticated username/principal identifier
//
// Placeholder substitution happens before wildcard matching, so
// "telemetry/%c/#" becomes "telemetry/device-123/#" for principal
// "device-123" and then matches topics the normal MQTT way.
type ACLRule struct {
	TopicFilter string   `json:"topic_filter"`
	Actions     []string `json:"actions"`
	Effect      Effect   `json:"effect"`
}

// allowsAction reports whether this rule applies to action.
func (r ACLRule) allowsAction(action Action) bool {
	for _, a := range r.Actions {
		if Action(a) == action {
			return true
		}
	}
	return false
}

// resolvedFilter substitutes %c/%u placeholders with the given identifiers.
func (r ACLRule) resolvedFilter(clientID, username string) string {
	f := r.TopicFilter
	f = strings.ReplaceAll(f, "%c", clientID)
	f = strings.ReplaceAll(f, "%u", username)
	return f
}

// Role is a named bundle of rules. Roles are referenced by name from
// Binding and from the standard-ruleset registry (StandardRulesets) —
// nothing else identifies a role.
type Role struct {
	Name  string    `json:"name"`
	Rules []ACLRule `json:"rules"`
}

// Binding attaches a custom Role to a principal. Principal is whatever
// identity string the broker hook resolves for a connection — today that's
// the device UUID (auth.DeviceInfo.ID.String()) for device connections, or
// another stable identifier for non-device principals (e.g. a named
// test/service role). Bindings are always custom (never implicit); a
// principal's standard-ruleset rules come from whichever rulesets are
// enabled cluster-wide (see EnabledRulesets), not from a binding.
type Binding struct {
	Principal string `json:"principal"`
	RoleName  string `json:"role_name"`
}
