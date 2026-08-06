package raft

import (
	"encoding/json"
	"testing"

	hraft "github.com/hashicorp/raft"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

func apply(t *testing.T, f *FSM, cmd Command) applyResult {
	t.Helper()
	b, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	res, ok := f.Apply(&hraft.Log{Data: b}).(applyResult)
	if !ok {
		t.Fatalf("Apply returned unexpected type")
	}
	return res
}

func TestFSMClaimSessionOverride(t *testing.T) {
	f := NewFSM()

	res := apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-1"})
	if !res.ok {
		t.Fatalf("first claim should succeed")
	}
	if res.evictedNode != "" {
		t.Fatalf("first claim (previously unowned) should not report an evicted node, got %q", res.evictedNode)
	}
	owner, ok := f.sessionOwner("device-1")
	if !ok || owner != "core-1" {
		t.Fatalf("expected owner core-1, got %q (ok=%v)", owner, ok)
	}

	// A newer connection landing on a different node takes over —
	// standard MQTT "new connection wins" semantics — and reports the
	// previous owner so the caller can evict it there.
	res = apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-2"})
	if !res.ok {
		t.Fatalf("override claim should succeed")
	}
	if res.evictedNode != "core-1" {
		t.Fatalf("expected evictedNode core-1, got %q", res.evictedNode)
	}
	owner, ok = f.sessionOwner("device-1")
	if !ok || owner != "core-2" {
		t.Fatalf("expected owner core-2 after override, got %q (ok=%v)", owner, ok)
	}

	// Re-claiming from the node that already owns it reports no eviction.
	res = apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-2"})
	if res.evictedNode != "" {
		t.Fatalf("re-claim by the current owner should not report an eviction, got %q", res.evictedNode)
	}
}

func TestFSMClaimSessionIdentity(t *testing.T) {
	f := NewFSM()

	// No identity (JWT/password auth) — clientIDsForIdentity finds nothing.
	apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-1"})
	if got := f.clientIDsForIdentity("device-1@tenant-1"); len(got) != 0 {
		t.Fatalf("expected no clientIDs for an identity-less claim, got %v", got)
	}

	// Cert auth carries an identity — findable by revoke_certificate.
	apply(t, f, Command{Op: OpClaimSession, ClientID: "device-2", NodeID: "core-1", Identity: "device-2@tenant-1"})
	got := f.clientIDsForIdentity("device-2@tenant-1")
	if len(got) != 1 || got[0] != "device-2" {
		t.Fatalf("expected [device-2], got %v", got)
	}

	// Release clears the identity index too, not just ownership.
	apply(t, f, Command{Op: OpReleaseSession, ClientID: "device-2", NodeID: "core-1"})
	if got := f.clientIDsForIdentity("device-2@tenant-1"); len(got) != 0 {
		t.Fatalf("expected no clientIDs after release, got %v", got)
	}
}

func TestFSMReleaseSession(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-1"})

	// ReleaseSessionAs with the wrong node ID must not release — guards
	// against a stale release racing a newer claim.
	apply(t, f, Command{Op: OpReleaseSession, ClientID: "device-1", NodeID: "core-2"})
	if _, ok := f.sessionOwner("device-1"); !ok {
		t.Fatalf("session should still be owned after mismatched release")
	}

	// ReleaseSession with the current owner's NodeID releases it.
	apply(t, f, Command{Op: OpReleaseSession, ClientID: "device-1", NodeID: "core-1"})
	if _, ok := f.sessionOwner("device-1"); ok {
		t.Fatalf("session should be released")
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-1"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	sink := newMemSink()
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	f2 := NewFSM()
	if err := f2.Restore(sink.reader()); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if owner, ok := f2.sessionOwner("device-1"); !ok || owner != "core-1" {
		t.Fatalf("expected restored session owner core-1, got %q (ok=%v)", owner, ok)
	}
}

func TestFSMSetRedisPrimary(t *testing.T) {
	f := NewFSM()

	if _, ok := f.currentRedisPrimary(); ok {
		t.Fatalf("expected no redis primary designated on a fresh FSM")
	}

	res := apply(t, f, Command{Op: OpSetRedisPrimary, NodeID: "core-1"})
	if !res.ok {
		t.Fatalf("set redis primary should succeed")
	}
	primary, ok := f.currentRedisPrimary()
	if !ok || primary != "core-1" {
		t.Fatalf("expected redis primary core-1, got %q (ok=%v)", primary, ok)
	}

	// A later designation (failover to a different node) simply overwrites
	// — there's no "previous owner" concept to report here, unlike
	// OpClaimSession's evictedNode: the failover loop already knows who the
	// old primary was (that's what triggered the failover) and is
	// responsible for reconfiguring it as a replica itself.
	apply(t, f, Command{Op: OpSetRedisPrimary, NodeID: "core-2"})
	primary, ok = f.currentRedisPrimary()
	if !ok || primary != "core-2" {
		t.Fatalf("expected redis primary core-2 after redesignation, got %q (ok=%v)", primary, ok)
	}
}

func TestFSMSnapshotRestoreRedisPrimary(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpSetRedisPrimary, NodeID: "core-3"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sink := newMemSink()
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	f2 := NewFSM()
	if err := f2.Restore(sink.reader()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if primary, ok := f2.currentRedisPrimary(); !ok || primary != "core-3" {
		t.Fatalf("expected restored redis primary core-3, got %q (ok=%v)", primary, ok)
	}
}

// TestFSMRestorePreRedisPrimarySnapshot verifies restoring a snapshot taken
// before this field existed (redis_primary absent from the JSON) leaves the
// FSM with no primary designated, not a decode error — same forward-
// compatibility guarantee TestFSMRestorePreACLSnapshot already covers for
// the ACL fields.
func TestFSMRestorePreRedisPrimarySnapshot(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpClaimSession, ClientID: "device-1", NodeID: "core-1"})

	oldSnapshotJSON := `{"sessions":{"device-1":"core-1"}}`
	sink := newMemSink()
	if _, err := sink.Write([]byte(oldSnapshotJSON)); err != nil {
		t.Fatalf("write pre-redis-primary snapshot: %v", err)
	}

	if err := f.Restore(sink.reader()); err != nil {
		t.Fatalf("restore pre-redis-primary snapshot: %v", err)
	}
	if _, ok := f.currentRedisPrimary(); ok {
		t.Fatalf("expected no redis primary designated after restoring a pre-redis-primary snapshot")
	}
}

func TestFSMCreateRole(t *testing.T) {
	f := NewFSM()
	rules := []acl.ACLRule{
		{TopicFilter: "telemetry/%c/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
	}
	res := apply(t, f, Command{Op: OpCreateRole, RoleName: "device-role", Rules: rules})
	if !res.ok {
		t.Fatalf("create_role should succeed: %v", res.err)
	}

	roles := f.rolesSnapshot()
	role, ok := roles["device-role"]
	if !ok {
		t.Fatalf("expected role device-role to exist")
	}
	if len(role.Rules) != 1 || role.Rules[0].TopicFilter != "telemetry/%c/#" {
		t.Fatalf("unexpected role rules: %+v", role.Rules)
	}
}

func TestFSMDeleteRoleCascadesBindings(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpCreateRole, RoleName: "role-a", Rules: []acl.ACLRule{
		{TopicFilter: "a/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
	}})
	apply(t, f, Command{Op: OpCreateRole, RoleName: "role-b", Rules: []acl.ACLRule{
		{TopicFilter: "b/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
	}})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "role-a"})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "role-b"})

	res := apply(t, f, Command{Op: OpDeleteRole, RoleName: "role-a"})
	if !res.ok {
		t.Fatalf("delete_role should succeed: %v", res.err)
	}

	roles := f.rolesSnapshot()
	if _, ok := roles["role-a"]; ok {
		t.Fatalf("role-a should be deleted")
	}

	bindings := f.bindingsSnapshot()
	names := bindings["device-1"]
	if len(names) != 1 || names[0] != "role-b" {
		t.Fatalf("expected only role-b binding to remain, got %v", names)
	}

	// Deleting the last remaining role's binding should drop the
	// principal's binding entry entirely (map miss must fail-closed).
	apply(t, f, Command{Op: OpDeleteRole, RoleName: "role-b"})
	bindings = f.bindingsSnapshot()
	if _, ok := bindings["device-1"]; ok {
		t.Fatalf("expected device-1 binding entry to be removed once all roles are gone")
	}
}

func TestFSMCreateBindingIdempotent(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpCreateRole, RoleName: "role-a", Rules: nil})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "role-a"})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "role-a"})

	bindings := f.bindingsSnapshot()
	if names := bindings["device-1"]; len(names) != 1 {
		t.Fatalf("expected exactly one binding after duplicate create_binding, got %v", names)
	}
}

func TestFSMDeleteBinding(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpCreateRole, RoleName: "role-a", Rules: nil})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "role-a"})

	res := apply(t, f, Command{Op: OpDeleteBinding, Principal: "device-1", RoleName: "role-a"})
	if !res.ok {
		t.Fatalf("delete_binding should succeed: %v", res.err)
	}
	bindings := f.bindingsSnapshot()
	if _, ok := bindings["device-1"]; ok {
		t.Fatalf("expected binding entry removed once its only role is unbound")
	}
}

func TestFSMEnableDisableRuleset(t *testing.T) {
	f := NewFSM()
	res := apply(t, f, Command{Op: OpEnableRuleset, RulesetName: "keel-device-default"})
	if !res.ok {
		t.Fatalf("enable_ruleset should succeed: %v", res.err)
	}
	enabled := f.enabledRulesetsSnapshot()
	if len(enabled) != 1 || enabled[0] != "keel-device-default" {
		t.Fatalf("expected keel-device-default enabled, got %v", enabled)
	}

	res = apply(t, f, Command{Op: OpDisableRuleset, RulesetName: "keel-device-default"})
	if !res.ok {
		t.Fatalf("disable_ruleset should succeed: %v", res.err)
	}
	enabled = f.enabledRulesetsSnapshot()
	if len(enabled) != 0 {
		t.Fatalf("expected no enabled rulesets after disable, got %v", enabled)
	}
}

func TestFSMEvaluateACLWiring(t *testing.T) {
	f := NewFSM()

	// No roles/rulesets at all -> fail-closed deny.
	d := f.evaluateACL("device-1", "device-1", "telemetry/device-1/temp", acl.ActionPublish)
	if d.Allowed() {
		t.Fatalf("expected deny with no roles/rulesets enabled")
	}

	// Enabling the standard ruleset should allow a device to publish
	// under its own telemetry/%c/# namespace.
	apply(t, f, Command{Op: OpEnableRuleset, RulesetName: "keel-device-default"})
	d = f.evaluateACL("device-1", "device-1", "telemetry/device-1/temp", acl.ActionPublish)
	if !d.Allowed() {
		t.Fatalf("expected allow for own telemetry topic once standard ruleset enabled")
	}
	// ... but not another device's namespace.
	d = f.evaluateACL("device-1", "device-1", "telemetry/device-2/temp", acl.ActionPublish)
	if d.Allowed() {
		t.Fatalf("expected deny for another device's telemetry topic")
	}

	// A custom role bound to device-1 should extend what it can do,
	// without affecting an unrelated principal.
	apply(t, f, Command{Op: OpCreateRole, RoleName: "extra", Rules: []acl.ACLRule{
		{TopicFilter: "misc/device-1/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
	}})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "extra"})
	d = f.evaluateACL("device-1", "device-1", "misc/device-1/x", acl.ActionPublish)
	if !d.Allowed() {
		t.Fatalf("expected allow via custom role binding")
	}
	d = f.evaluateACL("device-2", "device-2", "misc/device-1/x", acl.ActionPublish)
	if d.Allowed() {
		t.Fatalf("expected deny: custom role is bound to device-1 only")
	}

	// A custom deny should win over the standard-ruleset allow.
	apply(t, f, Command{Op: OpCreateRole, RoleName: "lockdown", Rules: []acl.ACLRule{
		{TopicFilter: "telemetry/device-1/secret", Actions: []string{"publish"}, Effect: acl.EffectDeny},
	}})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "lockdown"})
	d = f.evaluateACL("device-1", "device-1", "telemetry/device-1/secret", acl.ActionPublish)
	if d.Allowed() {
		t.Fatalf("expected explicit deny to win over standard-ruleset allow")
	}
}

func TestFSMSnapshotRestoreACLState(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpCreateRole, RoleName: "role-a", Rules: []acl.ACLRule{
		{TopicFilter: "a/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
	}})
	apply(t, f, Command{Op: OpCreateBinding, Principal: "device-1", RoleName: "role-a"})
	apply(t, f, Command{Op: OpEnableRuleset, RulesetName: "keel-device-default"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sink := newMemSink()
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	f2 := NewFSM()
	if err := f2.Restore(sink.reader()); err != nil {
		t.Fatalf("restore: %v", err)
	}

	roles := f2.rolesSnapshot()
	if _, ok := roles["role-a"]; !ok {
		t.Fatalf("expected role-a to survive snapshot/restore")
	}
	bindings := f2.bindingsSnapshot()
	if names := bindings["device-1"]; len(names) != 1 || names[0] != "role-a" {
		t.Fatalf("expected device-1 binding to role-a to survive snapshot/restore, got %v", names)
	}
	enabled := f2.enabledRulesetsSnapshot()
	if len(enabled) != 1 || enabled[0] != "keel-device-default" {
		t.Fatalf("expected keel-device-default to survive snapshot/restore, got %v", enabled)
	}
}

// TestFSMRestorePreACLSnapshot verifies nil-safety when restoring a
// snapshot that predates the ACL fields (Roles/Bindings/EnabledRulesets
// all absent/nil in the decoded JSON) — must not panic on nil-map writes
// afterward, per the Restore nil-safety noted in fsm.go.
func TestFSMRestorePreACLSnapshot(t *testing.T) {
	f := NewFSM()
	old := struct {
		Sessions map[string]string `json:"sessions"`
	}{Sessions: map[string]string{"device-1": "core-1"}}

	b, err := json.Marshal(old)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sink := newMemSink()
	if _, err := sink.Write(b); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := f.Restore(sink.reader()); err != nil {
		t.Fatalf("restore pre-ACL snapshot: %v", err)
	}
	if owner, ok := f.sessionOwner("device-1"); !ok || owner != "core-1" {
		t.Fatalf("expected session to restore normally, got %q (ok=%v)", owner, ok)
	}

	// These must not panic (nil map writes) and should behave as empty.
	apply(t, f, Command{Op: OpCreateRole, RoleName: "role-a", Rules: nil})
	if roles := f.rolesSnapshot(); len(roles) != 1 {
		t.Fatalf("expected role creation to work after restoring a pre-ACL snapshot, got %v", roles)
	}
}

func TestFSMRevokeCertificate(t *testing.T) {
	f := NewFSM()
	identity := "device-1@tenant-1"

	if f.isRevoked(identity) {
		t.Fatal("expected identity not revoked before OpRevokeCertificate")
	}

	res := apply(t, f, Command{Op: OpRevokeCertificate, Identity: identity, Serial: "abc123"})
	if !res.ok {
		t.Fatalf("apply OpRevokeCertificate: %+v", res)
	}

	if !f.isRevoked(identity) {
		t.Fatal("expected identity revoked after OpRevokeCertificate")
	}
	if f.isRevoked("someone-else@tenant-1") {
		t.Fatal("expected unrelated identity to remain not-revoked")
	}

	snap := f.revokedSnapshot()
	revokedAt, ok := snap[identity]
	if !ok || revokedAt <= 0 {
		t.Fatalf("expected revokedSnapshot to include %q with a positive timestamp, got %v", identity, snap)
	}
}

func TestFSMSnapshotRestoreRevokedState(t *testing.T) {
	f := NewFSM()
	apply(t, f, Command{Op: OpRevokeCertificate, Identity: "device-1@tenant-1", Serial: "s1"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	sink := newMemSink()
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	f2 := NewFSM()
	if err := f2.Restore(sink.reader()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !f2.isRevoked("device-1@tenant-1") {
		t.Fatal("expected revocation to survive snapshot/restore")
	}
}

// TestFSMRestorePreRevocationSnapshot verifies nil-safety when restoring a
// snapshot that predates the Revoked field — must not panic on a nil-map
// write afterward.
func TestFSMRestorePreRevocationSnapshot(t *testing.T) {
	f := NewFSM()
	old := struct {
		Sessions map[string]string `json:"sessions"`
	}{Sessions: map[string]string{"device-1": "core-1"}}

	b, err := json.Marshal(old)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sink := newMemSink()
	if _, err := sink.Write(b); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := f.Restore(sink.reader()); err != nil {
		t.Fatalf("restore pre-revocation snapshot: %v", err)
	}

	// Must not panic (nil map write) and should behave as empty before.
	if f.isRevoked("device-1@tenant-1") {
		t.Fatal("expected nothing revoked after restoring a pre-revocation snapshot")
	}
	apply(t, f, Command{Op: OpRevokeCertificate, Identity: "device-1@tenant-1"})
	if !f.isRevoked("device-1@tenant-1") {
		t.Fatal("expected revocation to work after restoring a pre-revocation snapshot")
	}
}
