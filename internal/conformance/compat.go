package conformance

import mqtt "github.com/mochi-mqtt/server/v2"

// ApplyCompatibilities sets the mochi-mqtt upstream compatibility flag(s)
// that stay conformance-only, pending a deliberate product decision —
// see docs/alternatives-and-future-work.md's 2026-08-07 root-cause entry:
//
//   - ObscureNotAuthorized: without it, an ACL-denied SUBSCRIBE gets
//     SUBACK reason code 0x87 (Not Authorized); paho.mqtt.testing's
//     test_subscribe_failure only accepts 0x80 (Unspecified Error).
//     Changes client-observable behavior (0x87 vs 0x80 on a live SUBACK),
//     so it is NOT promoted to a broker.New default the way
//     NoInheritedPropertiesOnAck was — that would silently change
//     production semantics for already-deployed clients (e.g. Kimera).
//     If wanted in production, it should become an explicit config
//     option (e.g. security.obscureAuthorizationErrors, default false)
//     in a separate change, not an implicit side effect of this package.
//
// NoInheritedPropertiesOnAck is NOT set here anymore — root-caused as a
// real MQTT5 semantics fix, not conformance scaffolding, so it is now a
// broker.New default for every deployment (see internal/broker/broker.go).
//
// Call only on a *mqtt.Server built for --conformance-test.
func ApplyCompatibilities(caps *mqtt.Capabilities) {
	caps.Compatibilities.ObscureNotAuthorized = true
}
