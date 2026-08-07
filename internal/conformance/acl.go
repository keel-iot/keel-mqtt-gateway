package conformance

import mqtt "github.com/mochi-mqtt/server/v2"

// nosubscribeTopic replicates paho.mqtt.testing's own documented
// semantics for its -n/--nosubscribe_topic_filter option: "topic filter
// name for which subscriptions aren't allowed" — SUBSCRIBE-only. The
// suite's test_subscribe_failure only ever subscribes to it (asserting a
// 0x80 SUBACK reason code) and never publishes to it, so publish is left
// unrestricted like every other topic — deliberately not the broader
// "deny both directions" mochi-mqtt's own paho-testing example hook uses,
// since that would assert more than the suite actually checks.
const nosubscribeTopic = "test/nosubscribe"

// ACLHook allows every topic except a subscribe to nosubscribeTopic.
// Registered as an EXTRA hook on the existing mochi-mqtt server (see
// cmd/server/main.go) — never modifies internal/broker/hooks.go. mochi-mqtt
// combines every registered hook's OnACLCheck with a logical OR (first
// hook to return true wins; see mochi-mqtt's Hooks.OnACLCheck) —
// registration order relative to keel's own hook doesn't matter: when
// this hook returns true the production hook is never consulted, and
// when this hook returns false (the nosubscribeTopic case) the
// production hook's own verdict is used instead, which independently
// denies the same topic (not a keel-shaped topic), so the net result is
// still a deny — never a hook simply overriding the other's explicit
// deny into an allow.
type ACLHook struct {
	mqtt.HookBase
}

// NewACLHook returns a ready-to-use conformance ACLHook.
func NewACLHook() *ACLHook {
	return &ACLHook{}
}

func (*ACLHook) ID() string { return "conformance-allow-all-acl" }

func (*ACLHook) Provides(b byte) bool {
	return b == mqtt.OnACLCheck
}

func (*ACLHook) OnACLCheck(_ *mqtt.Client, topic string, write bool) bool {
	if !write && topic == nosubscribeTopic {
		return false
	}
	return true
}
