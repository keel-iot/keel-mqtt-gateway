package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

// applyResult is returned from FSM.Apply and surfaced to the caller of
// raft.Apply via raft.ApplyFuture.Response().
type applyResult struct {
	ok  bool
	err error
	// evictedNode is set by OpClaimSession when the session was
	// previously owned by a *different* node — the caller (broker hook)
	// is responsible for telling that node to locally disconnect the
	// client (see dataplane.Forwarder.Evict). Empty when the session was
	// unowned or already owned by the claiming node.
	evictedNode string
}

// state is the FSM's in-memory data: session ownership (clientID ->
// owning node) and ACL state (roles/bindings/enabled rulesets). Topic
// -filter routing used to live here too (a derived trie + inverse index
// kept in sync with a raft-replicated routes map) but has moved to
// internal/cluster/routing, backed by store.ClusterStore (Olric) —
// routing writes are high-frequency and naturally conflict-free per
// (topic, nodeID) pair, so they don't need raft's strong-consensus log;
// session ownership and ACL rules are both comparatively low-frequency
// and genuinely need a single agreed-upon, strongly-consistent value (who
// owns a session; who is or isn't authorized) — an unknown/not-yet-
// -replicated principal must read as "deny", never as "allow" by
// omission, which rules out an AP store the same way stale session
// ownership would cause double-delivery. Both therefore stay on raft, as
// two distinct sections of the same struct/log rather than two separate
// raft groups: splitting would mean a second leader election, a second
// log/snapshot, and a second quorum to reason about, for state that has
// the same write-frequency profile as sessions and no need to scale
// independently from it.
//
// Reads happen directly against this struct (see Registry read methods)
// — on followers this may lag the leader by one replication round-trip,
// which is an acceptable trade-off for a PoC. ACL evaluation callers must
// treat that lag the same way: fail-closed (see internal/cluster/acl),
// never optimistic-allow.
type state struct {
	mu       sync.RWMutex
	sessions map[string]string

	// ACL section — kept as a distinct sub-struct rather than flattened
	// fields, so it's easy to see (and snapshot/restore) as one unit.
	roles           map[string]acl.Role // role name -> role (rules)
	bindings        map[string][]string // principal -> list of role names bound to it
	enabledRulesets map[string]bool     // standard ruleset name -> enabled

	// redisPrimary is the nodeID of the core currently designated primary
	// for the co-located Redis primary+replica pair (see OpSetRedisPrimary).
	// Empty until the first designation — e.g. before any failover loop has
	// ever run, or on a fresh single-core cluster that hasn't set one yet.
	redisPrimary string

	// revoked maps a device cert's CN identity ("<deviceID>@<tenantID>")
	// to the unix-seconds it was revoked at (see OpRevokeCertificate).
	// Same rationale as ACL state for staying on raft rather than an AP
	// store: "is this identity revoked" must be an authoritative,
	// fail-closed fact, not something a node reconstructs independently.
	revoked map[string]int64
}

func newState() *state {
	return &state{
		sessions:        make(map[string]string),
		revoked:         make(map[string]int64),
		roles:           make(map[string]acl.Role),
		bindings:        make(map[string][]string),
		enabledRulesets: make(map[string]bool),
	}
}

// FSM implements hashicorp/raft's raft.FSM interface.
type FSM struct {
	state *state
}

// NewFSM creates an empty FSM.
func NewFSM() *FSM {
	return &FSM{state: newState()}
}

// Apply decodes a Command from the raft log and mutates FSM state. Called
// only on the node that is currently the raft leader for the Apply that
// originated it, but replayed identically on every voter as the log
// replicates — must be deterministic.
func (f *FSM) Apply(log *hraft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return applyResult{err: fmt.Errorf("fsm: unmarshal command: %w", err)}
	}

	f.state.mu.Lock()
	defer f.state.mu.Unlock()

	switch cmd.Op {
	case OpClaimSession:
		// New connection always wins (standard MQTT "newer connection
		// takes over" semantics — decided here, not left to the caller).
		// If a different node held the session, its identity is returned
		// so the caller can tell that node to evict its local client; the
		// FSM only tracks ownership, never talks to other nodes itself.
		var evicted string
		if owner, exists := f.state.sessions[cmd.ClientID]; exists && owner != cmd.NodeID {
			evicted = owner
		}
		f.state.sessions[cmd.ClientID] = cmd.NodeID
		return applyResult{ok: true, evictedNode: evicted}

	case OpReleaseSession:
		// NodeID must match the current owner, guarding against a stale
		// release (e.g. from a just-evicted node's own disconnect
		// cleanup) racing a newer ClaimSession.
		if owner, exists := f.state.sessions[cmd.ClientID]; exists && owner == cmd.NodeID {
			delete(f.state.sessions, cmd.ClientID)
		}
		return applyResult{ok: true}

	case OpCreateRole:
		f.state.roles[cmd.RoleName] = acl.Role{Name: cmd.RoleName, Rules: cmd.Rules}
		return applyResult{ok: true}

	case OpDeleteRole:
		delete(f.state.roles, cmd.RoleName)
		// Also drop any bindings pointing at the now-deleted role, so a
		// stale binding can never be mistaken for a still-valid grant —
		// evaluation must fail closed on the next lookup, not rely on the
		// role map miss alone (a future role recreated under the same
		// name would otherwise silently resurrect old bindings).
		for principal, roleNames := range f.state.bindings {
			kept := roleNames[:0]
			for _, rn := range roleNames {
				if rn != cmd.RoleName {
					kept = append(kept, rn)
				}
			}
			if len(kept) == 0 {
				delete(f.state.bindings, principal)
			} else {
				f.state.bindings[principal] = kept
			}
		}
		return applyResult{ok: true}

	case OpCreateBinding:
		existing := f.state.bindings[cmd.Principal]
		for _, rn := range existing {
			if rn == cmd.RoleName {
				return applyResult{ok: true} // already bound, idempotent
			}
		}
		f.state.bindings[cmd.Principal] = append(existing, cmd.RoleName)
		return applyResult{ok: true}

	case OpDeleteBinding:
		existing := f.state.bindings[cmd.Principal]
		kept := existing[:0]
		for _, rn := range existing {
			if rn != cmd.RoleName {
				kept = append(kept, rn)
			}
		}
		if len(kept) == 0 {
			delete(f.state.bindings, cmd.Principal)
		} else {
			f.state.bindings[cmd.Principal] = kept
		}
		return applyResult{ok: true}

	case OpEnableRuleset:
		f.state.enabledRulesets[cmd.RulesetName] = true
		return applyResult{ok: true}

	case OpDisableRuleset:
		delete(f.state.enabledRulesets, cmd.RulesetName)
		return applyResult{ok: true}

	case OpSetRedisPrimary:
		f.state.redisPrimary = cmd.NodeID
		return applyResult{ok: true}

	case OpRevokeCertificate:
		f.state.revoked[cmd.Identity] = time.Now().Unix()
		return applyResult{ok: true}

	default:
		return applyResult{err: fmt.Errorf("fsm: unknown op %q", cmd.Op)}
	}
}

// Snapshot returns a point-in-time copy of the FSM state for raft's
// snapshot/compaction mechanism.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()

	snap := &snapshot{
		Sessions:        make(map[string]string, len(f.state.sessions)),
		Roles:           make(map[string]acl.Role, len(f.state.roles)),
		Bindings:        make(map[string][]string, len(f.state.bindings)),
		EnabledRulesets: make(map[string]bool, len(f.state.enabledRulesets)),
		RedisPrimary:    f.state.redisPrimary,
		Revoked:         make(map[string]int64, len(f.state.revoked)),
	}
	for clientID, nodeID := range f.state.sessions {
		snap.Sessions[clientID] = nodeID
	}
	for name, role := range f.state.roles {
		snap.Roles[name] = role
	}
	for principal, roleNames := range f.state.bindings {
		cp := make([]string, len(roleNames))
		copy(cp, roleNames)
		snap.Bindings[principal] = cp
	}
	for name, enabled := range f.state.enabledRulesets {
		snap.EnabledRulesets[name] = enabled
	}
	for identity, revokedAt := range f.state.revoked {
		snap.Revoked[identity] = revokedAt
	}
	return snap, nil
}

// Restore replaces FSM state from a snapshot, called by raft on startup
// when a local snapshot exists or after installing one streamed from the
// leader.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snap snapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("fsm: decode snapshot: %w", err)
	}

	f.state.mu.Lock()
	f.state.sessions = snap.Sessions
	if snap.Roles != nil {
		f.state.roles = snap.Roles
	} else {
		f.state.roles = make(map[string]acl.Role)
	}
	if snap.Bindings != nil {
		f.state.bindings = snap.Bindings
	} else {
		f.state.bindings = make(map[string][]string)
	}
	if snap.EnabledRulesets != nil {
		f.state.enabledRulesets = snap.EnabledRulesets
	} else {
		f.state.enabledRulesets = make(map[string]bool)
	}
	f.state.redisPrimary = snap.RedisPrimary
	if snap.Revoked != nil {
		f.state.revoked = snap.Revoked
	} else {
		f.state.revoked = make(map[string]int64)
	}
	f.state.mu.Unlock()
	return nil
}

// ── read helpers, used by Registry ──────────────────────────────────────

func (f *FSM) sessionOwner(clientID string) (string, bool) {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	owner, ok := f.state.sessions[clientID]
	return owner, ok
}

func (f *FSM) sessionsSnapshot() map[string]string {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	sessions := make(map[string]string, len(f.state.sessions))
	for k, v := range f.state.sessions {
		sessions[k] = v
	}
	return sessions
}

// currentRedisPrimary returns the nodeID currently designated primary for
// the co-located Redis pair, if one has ever been set.
func (f *FSM) currentRedisPrimary() (string, bool) {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	return f.state.redisPrimary, f.state.redisPrimary != ""
}

// ── ACL read helpers ─────────────────────────────────────────────────────

// evaluateACL gathers the currently-enabled standard rulesets (resolved
// against acl.StandardRulesets by name) and the principal's custom
// role-derived rules, then delegates to acl.Evaluate. Called on every
// OnACLCheck, on both core (directly) and edge (via gRPC, see
// remote_registry.go) nodes — must stay fail-closed the same way
// acl.Evaluate itself is: an unknown principal or ruleset name simply
// contributes no rules, never an early-return allow.
func (f *FSM) evaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()

	var enabled []acl.Role
	for name := range f.state.enabledRulesets {
		if role, ok := acl.StandardRulesets[name]; ok {
			enabled = append(enabled, role)
		}
		// A name enabled in FSM state but absent from the Go-level
		// StandardRulesets map (e.g. running an older binary than the one
		// that enabled it) contributes nothing — silently ignored rather
		// than erroring, consistent with fail-closed: missing rules just
		// shrink the allow set, never expand it.
	}

	var custom []acl.ACLRule
	for principal, roleNames := range f.state.bindings {
		if principal != clientID && principal != username {
			continue
		}
		for _, rn := range roleNames {
			if role, ok := f.state.roles[rn]; ok {
				custom = append(custom, role.Rules...)
			}
		}
	}

	return acl.Evaluate(clientID, username, topic, action, enabled, custom)
}

// rolesSnapshot exposes custom roles for the management API.
func (f *FSM) rolesSnapshot() map[string]acl.Role {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	roles := make(map[string]acl.Role, len(f.state.roles))
	for k, v := range f.state.roles {
		roles[k] = v
	}
	return roles
}

// bindingsSnapshot exposes principal -> role-name bindings for the
// management API.
func (f *FSM) bindingsSnapshot() map[string][]string {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	bindings := make(map[string][]string, len(f.state.bindings))
	for k, v := range f.state.bindings {
		cp := make([]string, len(v))
		copy(cp, v)
		bindings[k] = cp
	}
	return bindings
}

// enabledRulesetsSnapshot exposes the set of currently-enabled standard
// ruleset names for the management API.
func (f *FSM) enabledRulesetsSnapshot() []string {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	names := make([]string, 0, len(f.state.enabledRulesets))
	for name, enabled := range f.state.enabledRulesets {
		if enabled {
			names = append(names, name)
		}
	}
	return names
}

// isRevoked reports whether identity ("<deviceID>@<tenantID>") has ever
// been revoked.
func (f *FSM) isRevoked(identity string) bool {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	_, ok := f.state.revoked[identity]
	return ok
}

// revokedSnapshot exposes the full revoked-identity set for the
// management API and RevocationCache's periodic refresh.
func (f *FSM) revokedSnapshot() map[string]int64 {
	f.state.mu.RLock()
	defer f.state.mu.RUnlock()
	out := make(map[string]int64, len(f.state.revoked))
	for k, v := range f.state.revoked {
		out[k] = v
	}
	return out
}

// snapshot is the JSON-serialisable form of state, used for both raft
// snapshots and FSMSnapshot.Persist.
type snapshot struct {
	Sessions        map[string]string   `json:"sessions"`
	Roles           map[string]acl.Role `json:"roles"`
	Bindings        map[string][]string `json:"bindings"`
	EnabledRulesets map[string]bool     `json:"enabled_rulesets"`
	RedisPrimary    string              `json:"redis_primary,omitempty"`
	Revoked         map[string]int64    `json:"revoked,omitempty"`
}

func (s *snapshot) Persist(sink hraft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("fsm: persist snapshot: %w", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
