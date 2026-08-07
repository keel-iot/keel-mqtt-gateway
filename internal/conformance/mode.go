// Package conformance implements keel-mqtt-gateway's --conformance-test
// mode: an allow-all AuthProvider (auth.go) and an allow-all ACL hook
// (acl.go), used ONLY to run the black-box Eclipse Paho MQTT
// interoperability suite (eclipse-paho/paho.mqtt.testing) against the
// real production binary, without keel's own multi-tenant auth/ACL
// policy standing in the way of a generic protocol-conformance test.
//
// Deliberately isolated from internal/broker: production auth
// (internal/auth's providers) and ACL (internal/broker/hooks.go's
// isAllowedPublish and the RBAC path) are never imported or modified by
// this package, and this package is never imported by them either — the
// only connection point is cmd/server/main.go's --conformance-test flag,
// which substitutes this package's AuthProvider for the normal one and
// registers ACLHook as an extra mochi-mqtt hook. See ValidateRole for the
// standalone-only restriction that keeps this mode structurally
// unreachable on a core/edge/combined (cluster) node.
package conformance

import "fmt"

// ValidateRole rejects conformance mode on any clustered role — it makes
// sense only for a single, disposable, standalone test instance (real
// device/tenant routing, session ownership, and ACL replication would be
// meaningless — and actively wrong — for connections that were never
// really authenticated). Returned as an error, not called via
// os.Exit/log.Fatal here, so it stays unit-testable; cmd/server/main.go's
// bootstrap is the only place that turns this into a process exit.
func ValidateRole(role string) error {
	if role != "" {
		return fmt.Errorf("conformance mode is only available in standalone mode (role=%q, want \"\")", role)
	}
	return nil
}

// Banner is logged loudly (slog.Warn, one call per line so it survives a
// JSON log encoder) by cmd/server/main.go on startup when conformance mode
// is active — impossible to miss in a log stream, unlike a single
// low-key line.
var Banner = []string{
	"================================================================",
	"MQTT CONFORMANCE MODE ENABLED",
	"AUTHENTICATION AND ACL ENFORCEMENT ARE DISABLED.",
	"ANY credentials and ANY topic are accepted. NEVER use in production.",
	"================================================================",
}
