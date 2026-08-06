package management

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeEvictor is a minimal lifecycle.Evictor stand-in that records calls.
type fakeEvictor struct {
	mu    sync.Mutex
	calls []struct{ nodeID, clientID string }
}

func (f *fakeEvictor) Evict(_ context.Context, nodeID, clientID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, struct{ nodeID, clientID string }{nodeID, clientID})
	return nil
}

func (f *fakeEvictor) callsSnapshot() []struct{ nodeID, clientID string } {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]struct{ nodeID, clientID string }, len(f.calls))
	copy(out, f.calls)
	return out
}

func signedRevocationRequest(t *testing.T, secret string, payload clavexEventPayload) *http.Request {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/cluster/revocations", bytes.NewReader(body))
	if secret != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		req.Header.Set("X-Clavex-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	return req
}

func revokedEventPayload(t *testing.T, orgID, deviceID, serial string) clavexEventPayload {
	t.Helper()
	data, err := json.Marshal(clavexDeviceCertRevokedData{DeviceID: deviceID, Serial: serial})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	return clavexEventPayload{Event: "device.cert.revoked", OrgID: orgID, Data: data}
}

func TestRevocationWebhook_ValidSignatureRecordsRevocation(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	a.ClavexWebhookSecret = "shh-secret"
	h := a.Router()

	req := signedRevocationRequest(t, "shh-secret", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !reg.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected device-1@tenant-1 to be recorded as revoked")
	}
}

func TestRevocationWebhook_MissingSecretConfigFailsClosed(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	// ClavexWebhookSecret intentionally left empty.
	h := a.Router()

	req := signedRevocationRequest(t, "", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when no webhook secret configured, got %d", rec.Code)
	}
	if reg.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected no revocation recorded when secret is unconfigured")
	}
}

func TestRevocationWebhook_WrongSignatureRejected(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	a.ClavexWebhookSecret = "shh-secret"
	h := a.Router()

	req := signedRevocationRequest(t, "wrong-secret", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong signature, got %d", rec.Code)
	}
	if reg.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected no revocation recorded on signature mismatch")
	}
}

func TestRevocationWebhook_MissingSignatureHeaderRejected(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	a.ClavexWebhookSecret = "shh-secret"
	h := a.Router()

	req := signedRevocationRequest(t, "", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing signature header, got %d", rec.Code)
	}
}

func TestRevocationWebhook_IgnoresUnrelatedEventTypes(t *testing.T) {
	reg := newFakeACLRegistry()
	a := newTestAPI(reg)
	a.ClavexWebhookSecret = "shh-secret"
	h := a.Router()

	data, _ := json.Marshal(map[string]any{"user_id": "u1"})
	payload := clavexEventPayload{Event: "user.login", OrgID: "tenant-1", Data: data}

	req := signedRevocationRequest(t, "shh-secret", payload)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (ack, ignore) for unrelated event type, got %d", rec.Code)
	}
	if len(reg.revoked) != 0 {
		t.Fatalf("expected no revocation recorded for unrelated event type, got %v", reg.revoked)
	}
}

func TestRevocationWebhook_EvictsAlreadyConnectedSession(t *testing.T) {
	reg := newFakeACLRegistry()
	reg.ClaimSession("client-abc", "edge-7", "device-1@tenant-1")

	evictor := &fakeEvictor{}
	a := newTestAPI(reg)
	a.ClavexWebhookSecret = "shh-secret"
	a.Evictor = evictor
	h := a.Router()

	req := signedRevocationRequest(t, "shh-secret", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	deadline := time.Now().Add(time.Second)
	for {
		if calls := evictor.callsSnapshot(); len(calls) == 1 {
			if calls[0].nodeID != "edge-7" || calls[0].clientID != "client-abc" {
				t.Fatalf("expected Evict(edge-7, client-abc), got %+v", calls[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected exactly one Evict call, got %v", evictor.callsSnapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRevocationWebhook_NoConnectedSessionNoEvictCall(t *testing.T) {
	reg := newFakeACLRegistry() // no ClaimSession — nothing connected
	evictor := &fakeEvictor{}
	a := newTestAPI(reg)
	a.ClavexWebhookSecret = "shh-secret"
	a.Evictor = evictor
	h := a.Router()

	req := signedRevocationRequest(t, "shh-secret", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	time.Sleep(20 * time.Millisecond)
	if calls := evictor.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("expected no Evict calls, got %v", calls)
	}
}

func TestRevocationWebhook_NotCoreNodeReturns503(t *testing.T) {
	a := newTestAPI(nil) // no ClusterRegistry — standalone mode
	a.ClavexWebhookSecret = "shh-secret"
	h := a.Router()

	req := signedRevocationRequest(t, "shh-secret", revokedEventPayload(t, "tenant-1", "device-1", "serial-abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when not a core node, got %d", rec.Code)
	}
}
