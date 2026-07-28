// Package management exposes the read-only cluster observability API and
// the drain action described in the design doc's "Livello 1" — enough to
// validate the docker-compose scenarios by hand (curl) before any UI is
// built on top. Mounted only on core nodes: routing table lives in
// internal/cluster/routing (Olric-backed), session ownership in the raft
// FSM.
package management

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/lifecycle"
	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/membership"
	keelraft "github.com/keel-iot/keel-mqtt-gateway/internal/cluster/raft"
)

// API bundles the dependencies the HTTP handlers read from.
type API struct {
	SelfNodeID string
	RaftNode   *keelraft.Node
	Membership *membership.Membership
	// ClusterRegistry backs /api/cluster/routes. Routing moved off raft to
	// internal/cluster/routing (Olric-backed) — this is typed as the
	// generic Registry interface and duck-typed against RoutesSnapshot
	// (implemented by *keelraft.CoreRegistry) rather than importing the
	// routing package directly, mirroring the same optional-capability
	// pattern cmd/server/main.go already uses for NodePurger /
	// NodesWithRoutesProvider.
	ClusterRegistry keelraft.Registry
	Log             *slog.Logger
}

// Router builds the http.Handler exposing the management endpoints.
func (a *API) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/cluster/nodes", a.handleNodes)
	mux.HandleFunc("GET /api/cluster/routes", a.handleRoutes)
	mux.HandleFunc("GET /api/cluster/sessions", a.handleSessions)
	mux.HandleFunc("POST /api/cluster/drain", a.handleDrain)
	mux.HandleFunc("POST /api/cluster/snapshot", a.handleSnapshot)

	// ACL/RBAC management — see internal/cluster/acl and internal/cluster/
	// raft's ACLAdmin interface. Mutations are forwarded to the raft
	// leader by CoreRegistry/RemoteRegistry's ACLAdmin implementation when
	// this node isn't currently leader; reads are always served locally.
	mux.HandleFunc("GET /api/acl/roles", a.handleListRoles)
	mux.HandleFunc("POST /api/acl/roles", a.handleCreateRole)
	mux.HandleFunc("DELETE /api/acl/roles/{name}", a.handleDeleteRole)
	mux.HandleFunc("GET /api/acl/bindings", a.handleListBindings)
	mux.HandleFunc("POST /api/acl/bindings", a.handleCreateBinding)
	mux.HandleFunc("DELETE /api/acl/bindings/{principal}/{role}", a.handleDeleteBinding)
	mux.HandleFunc("GET /api/acl/rulesets", a.handleListRulesets)
	mux.HandleFunc("POST /api/acl/rulesets/{name}/enable", a.handleEnableRuleset)
	mux.HandleFunc("POST /api/acl/rulesets/{name}/disable", a.handleDisableRuleset)
	return mux
}

type nodeView struct {
	NodeID    string `json:"node_id"`
	Role      string `json:"role"`
	GRPCAddr  string `json:"grpc_addr"`
	RaftAddr  string `json:"raft_addr,omitempty"`
	IsSelf    bool   `json:"is_self"`
	IsLeader  bool   `json:"is_leader,omitempty"`
	RaftVoter bool   `json:"raft_voter,omitempty"`
}

func (a *API) handleNodes(w http.ResponseWriter, r *http.Request) {
	var voters map[string]bool
	if a.RaftNode != nil {
		voters = make(map[string]bool)
		future := a.RaftNode.Raft.GetConfiguration()
		if err := future.Error(); err == nil {
			for _, srv := range future.Configuration().Servers {
				voters[string(srv.ID)] = true
			}
		}
	}

	var leaderID string
	if a.RaftNode != nil {
		leaderID = a.RaftNode.Registry.LeaderID()
	}

	views := make([]nodeView, 0)
	for _, meta := range a.Membership.Members() {
		v := nodeView{
			NodeID:    meta.NodeID,
			Role:      string(meta.Role),
			GRPCAddr:  meta.GRPCAddr,
			RaftAddr:  meta.RaftAddr,
			IsSelf:    meta.NodeID == a.SelfNodeID,
			RaftVoter: voters[meta.NodeID],
			IsLeader:  meta.NodeID == leaderID,
		}
		views = append(views, v)
	}

	writeJSON(w, http.StatusOK, views)
}

func (a *API) handleRoutes(w http.ResponseWriter, r *http.Request) {
	router, ok := a.ClusterRegistry.(interface {
		RoutesSnapshot() map[string][]string
	})
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, router.RoutesSnapshot())
}

func (a *API) handleSessions(w http.ResponseWriter, r *http.Request) {
	if a.RaftNode == nil {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, a.RaftNode.Registry.SessionsSnapshot())
}

func (a *API) handleDrain(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	if err := lifecycle.Drain(ctx, a.RaftNode, a.Membership, a.Log); err != nil {
		a.Log.Error("management: drain failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type snapshotResponse struct {
	ID  string `json:"id"`
	Dir string `json:"dir"`
}

// handleSnapshot forces an immediate raft snapshot and returns its on-disk
// directory, so `keel-gateway backup raft` (which runs as a separate
// process via `docker exec`/`kubectl exec` into the same node, exactly like
// the existing `drain` command) knows what to copy out. Local-filesystem
// only — the resulting directory lives on this node's own DataDir, no
// network transfer happens here.
func (a *API) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if a.RaftNode == nil {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	id, dir, err := a.RaftNode.Snapshot()
	if err != nil {
		a.Log.Error("management: snapshot failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, snapshotResponse{ID: id, Dir: dir})
}

// ── ACL/RBAC management ─────────────────────────────────────────────────

// aclAdmin type-asserts a.ClusterRegistry for keelraft.ACLAdmin, mirroring
// the same duck-typed-optional-capability pattern handleRoutes uses for
// RoutesSnapshot. ACLAdmin is implemented by CoreRegistry (with
// leader-forwarding) — RemoteRegistry (edge nodes) also implements it via
// the same forwarding pattern, but the management API is only ever
// mounted on core nodes per this package's doc comment, so in practice
// this only fails to assert if ClusterRegistry itself is nil (standalone
// mode, no --role flag).
func (a *API) aclAdmin() (keelraft.ACLAdmin, bool) {
	admin, ok := a.ClusterRegistry.(keelraft.ACLAdmin)
	return admin, ok
}

type roleView struct {
	Name  string        `json:"name"`
	Rules []acl.ACLRule `json:"rules"`
}

func (a *API) handleListRoles(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	roles := admin.RolesSnapshot()
	views := make([]roleView, 0, len(roles))
	for name, role := range roles {
		views = append(views, roleView{Name: name, Rules: role.Rules})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	writeJSON(w, http.StatusOK, views)
}

type createRoleRequest struct {
	Name  string        `json:"name"`
	Rules []acl.ACLRule `json:"rules"`
}

func (a *API) handleCreateRole(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	var req createRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if err := admin.CreateRole(req.Name, req.Rules); err != nil {
		a.Log.Error("management: create role failed", "role", req.Name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) handleDeleteRole(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "role name is required", http.StatusBadRequest)
		return
	}
	if err := admin.DeleteRole(name); err != nil {
		a.Log.Error("management: delete role failed", "role", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type bindingView struct {
	Principal string   `json:"principal"`
	Roles     []string `json:"roles"`
}

func (a *API) handleListBindings(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	bindings := admin.BindingsSnapshot()
	views := make([]bindingView, 0, len(bindings))
	for principal, roles := range bindings {
		views = append(views, bindingView{Principal: principal, Roles: roles})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Principal < views[j].Principal })
	writeJSON(w, http.StatusOK, views)
}

type createBindingRequest struct {
	Principal string `json:"principal"`
	RoleName  string `json:"role_name"`
}

func (a *API) handleCreateBinding(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	var req createBindingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Principal == "" || req.RoleName == "" {
		http.Error(w, "principal and role_name are required", http.StatusBadRequest)
		return
	}
	if err := admin.CreateBinding(req.Principal, req.RoleName); err != nil {
		a.Log.Error("management: create binding failed", "principal", req.Principal, "role", req.RoleName, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) handleDeleteBinding(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	principal := r.PathValue("principal")
	roleName := r.PathValue("role")
	if principal == "" || roleName == "" {
		http.Error(w, "principal and role are required", http.StatusBadRequest)
		return
	}
	if err := admin.DeleteBinding(principal, roleName); err != nil {
		a.Log.Error("management: delete binding failed", "principal", principal, "role", roleName, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) handleListRulesets(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	enabled := make(map[string]bool)
	for _, name := range admin.EnabledRulesetsSnapshot() {
		enabled[name] = true
	}

	names := make([]string, 0, len(acl.StandardRulesets))
	for name := range acl.StandardRulesets {
		names = append(names, name)
	}
	sort.Strings(names)

	type rulesetView struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	views := make([]rulesetView, 0, len(names))
	for _, name := range names {
		views = append(views, rulesetView{Name: name, Enabled: enabled[name]})
	}
	writeJSON(w, http.StatusOK, views)
}

func (a *API) handleEnableRuleset(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	name := r.PathValue("name")
	if _, known := acl.StandardRulesets[name]; !known {
		http.Error(w, "unknown ruleset "+name, http.StatusNotFound)
		return
	}
	if err := admin.EnableRuleset(name); err != nil {
		a.Log.Error("management: enable ruleset failed", "ruleset", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) handleDisableRuleset(w http.ResponseWriter, r *http.Request) {
	admin, ok := a.aclAdmin()
	if !ok {
		http.Error(w, "not a core node", http.StatusServiceUnavailable)
		return
	}
	name := r.PathValue("name")
	if err := admin.DisableRuleset(name); err != nil {
		a.Log.Error("management: disable ruleset failed", "ruleset", name, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
