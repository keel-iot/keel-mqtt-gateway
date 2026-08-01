package management

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/cluster/acl"
)

// fakeACLRegistry implements both keelraft.Registry and keelraft.ACLAdmin
// with an in-memory map, so the management API's ACL handlers can be
// exercised without any raft/gRPC machinery.
type fakeACLRegistry struct {
	roles           map[string]acl.Role
	bindings        map[string][]string
	enabledRulesets map[string]bool
}

func newFakeACLRegistry() *fakeACLRegistry {
	return &fakeACLRegistry{
		roles:           make(map[string]acl.Role),
		bindings:        make(map[string][]string),
		enabledRulesets: make(map[string]bool),
	}
}

// Registry interface (unused by ACL tests, no-op).
func (f *fakeACLRegistry) Subscribe(topic, nodeID string) error        { return nil }
func (f *fakeACLRegistry) Unsubscribe(topic, nodeID string) error      { return nil }
func (f *fakeACLRegistry) NodesFor(topic, localNodeID string) []string { return nil }
func (f *fakeACLRegistry) ClaimSession(clientID, nodeID string) (string, error) {
	return "", nil
}
func (f *fakeACLRegistry) ReleaseSession(clientID, nodeID string) error { return nil }
func (f *fakeACLRegistry) EvaluateACL(clientID, username, topic string, action acl.Action) acl.Decision {
	return acl.Decision{}
}
func (f *fakeACLRegistry) CurrentRedisPrimary() (string, bool) { return "", false }

// ACLAdmin interface.
func (f *fakeACLRegistry) CreateRole(name string, rules []acl.ACLRule) error {
	f.roles[name] = acl.Role{Name: name, Rules: rules}
	return nil
}
func (f *fakeACLRegistry) DeleteRole(name string) error {
	delete(f.roles, name)
	for principal, roles := range f.bindings {
		kept := roles[:0]
		for _, rn := range roles {
			if rn != name {
				kept = append(kept, rn)
			}
		}
		if len(kept) == 0 {
			delete(f.bindings, principal)
		} else {
			f.bindings[principal] = kept
		}
	}
	return nil
}
func (f *fakeACLRegistry) CreateBinding(principal, roleName string) error {
	f.bindings[principal] = append(f.bindings[principal], roleName)
	return nil
}
func (f *fakeACLRegistry) DeleteBinding(principal, roleName string) error {
	kept := f.bindings[principal][:0]
	for _, rn := range f.bindings[principal] {
		if rn != roleName {
			kept = append(kept, rn)
		}
	}
	if len(kept) == 0 {
		delete(f.bindings, principal)
	} else {
		f.bindings[principal] = kept
	}
	return nil
}
func (f *fakeACLRegistry) EnableRuleset(name string) error {
	f.enabledRulesets[name] = true
	return nil
}
func (f *fakeACLRegistry) DisableRuleset(name string) error {
	delete(f.enabledRulesets, name)
	return nil
}
func (f *fakeACLRegistry) RolesSnapshot() map[string]acl.Role {
	out := make(map[string]acl.Role, len(f.roles))
	for k, v := range f.roles {
		out[k] = v
	}
	return out
}
func (f *fakeACLRegistry) BindingsSnapshot() map[string][]string {
	out := make(map[string][]string, len(f.bindings))
	for k, v := range f.bindings {
		out[k] = v
	}
	return out
}
func (f *fakeACLRegistry) EnabledRulesetsSnapshot() []string {
	out := make([]string, 0, len(f.enabledRulesets))
	for name, enabled := range f.enabledRulesets {
		if enabled {
			out = append(out, name)
		}
	}
	return out
}

func newTestAPI(reg *fakeACLRegistry) *API {
	a := &API{Log: slog.Default()}
	if reg != nil {
		a.ClusterRegistry = reg
	}
	return a
}

func doRequest(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestACLRolesCRUD(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	h := a.Router()

	rec := doRequest(t, h, http.MethodPost, "/api/acl/roles", createRoleRequest{
		Name: "device-role",
		Rules: []acl.ACLRule{
			{TopicFilter: "telemetry/%c/#", Actions: []string{"publish"}, Effect: acl.EffectAllow},
		},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("create role: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, h, http.MethodGet, "/api/acl/roles", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list roles: expected 200, got %d", rec.Code)
	}
	var roles []roleView
	if err := json.Unmarshal(rec.Body.Bytes(), &roles); err != nil {
		t.Fatalf("decode roles: %v", err)
	}
	if len(roles) != 1 || roles[0].Name != "device-role" {
		t.Fatalf("expected exactly one role device-role, got %+v", roles)
	}

	rec = doRequest(t, h, http.MethodDelete, "/api/acl/roles/device-role", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete role: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if _, ok := reg.roles["device-role"]; ok {
		t.Fatalf("expected role to be deleted from registry")
	}
}

func TestACLCreateRoleMissingName(t *testing.T) {
	a := newTestAPI(newFakeACLRegistry())
	h := a.Router()

	rec := doRequest(t, h, http.MethodPost, "/api/acl/roles", createRoleRequest{Rules: nil})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing role name, got %d", rec.Code)
	}
}

func TestACLBindingsCRUD(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	h := a.Router()

	reg.CreateRole("device-role", nil)

	rec := doRequest(t, h, http.MethodPost, "/api/acl/bindings", createBindingRequest{
		Principal: "device-1", RoleName: "device-role",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("create binding: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doRequest(t, h, http.MethodGet, "/api/acl/bindings", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list bindings: expected 200, got %d", rec.Code)
	}
	var bindings []bindingView
	if err := json.Unmarshal(rec.Body.Bytes(), &bindings); err != nil {
		t.Fatalf("decode bindings: %v", err)
	}
	if len(bindings) != 1 || bindings[0].Principal != "device-1" {
		t.Fatalf("expected exactly one binding for device-1, got %+v", bindings)
	}

	rec = doRequest(t, h, http.MethodDelete, "/api/acl/bindings/device-1/device-role", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete binding: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if _, ok := reg.bindings["device-1"]; ok {
		t.Fatalf("expected binding to be removed from registry")
	}
}

func TestACLRulesetsListEnableDisable(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	h := a.Router()

	rec := doRequest(t, h, http.MethodGet, "/api/acl/rulesets", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list rulesets: expected 200, got %d", rec.Code)
	}
	var rulesets []struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rulesets); err != nil {
		t.Fatalf("decode rulesets: %v", err)
	}
	if len(rulesets) == 0 {
		t.Fatalf("expected at least one standard ruleset listed")
	}
	for _, rs := range rulesets {
		if rs.Enabled {
			t.Fatalf("expected no ruleset enabled by default, got %s enabled", rs.Name)
		}
	}

	name := rulesets[0].Name
	rec = doRequest(t, h, http.MethodPost, "/api/acl/rulesets/"+name+"/enable", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("enable ruleset: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !reg.enabledRulesets[name] {
		t.Fatalf("expected ruleset %s to be enabled in registry", name)
	}

	rec = doRequest(t, h, http.MethodPost, "/api/acl/rulesets/"+name+"/disable", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("disable ruleset: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if reg.enabledRulesets[name] {
		t.Fatalf("expected ruleset %s to be disabled in registry", name)
	}
}

func TestACLEnableUnknownRulesetRejected(t *testing.T) {
	a := newTestAPI(newFakeACLRegistry())
	h := a.Router()

	rec := doRequest(t, h, http.MethodPost, "/api/acl/rulesets/does-not-exist/enable", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown ruleset, got %d", rec.Code)
	}
}

func TestACLHandlersRequireClusterRegistry(t *testing.T) {
	a := newTestAPI(nil) // no ClusterRegistry set -> standalone mode
	h := a.Router()

	for _, req := range []struct {
		method, path string
	}{
		{http.MethodGet, "/api/acl/roles"},
		{http.MethodGet, "/api/acl/bindings"},
		{http.MethodGet, "/api/acl/rulesets"},
	} {
		rec := doRequest(t, h, req.method, req.path, nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s: expected 503 without ClusterRegistry, got %d", req.method, req.path, rec.Code)
		}
	}
}
