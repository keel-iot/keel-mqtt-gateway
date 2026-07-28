package httpapi_test

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/keel-iot/keel-mqtt-gateway/internal/httpapi"
)

// basicAuth builds a valid HTTP Basic Authorization header value.
func basicAuth(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

func makeRequest(t *testing.T, method, path, authHeader string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader([]byte(`{"v":1}`)))
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	return req
}

// ── ExtractCredentials ────────────────────────────────────────────────────────

var extractTests = []struct {
	name          string
	authHeader    string
	deviceIDHdr   string
	deviceIDQuery string
	wantDevice    string
	wantTenant    string
	wantToken     string
	wantOK        bool
}{
	{
		name:       "basic legacy (no tenant)",
		authHeader: basicAuth("device-123", "tok"),
		wantDevice: "device-123", wantTenant: "", wantToken: "tok", wantOK: true,
	},
	{
		name:       "basic with tenant",
		authHeader: basicAuth("device-123@tenant-456", "tok"),
		wantDevice: "device-123", wantTenant: "tenant-456", wantToken: "tok", wantOK: true,
	},
	{
		name:        "bearer with X-Device-ID and tenant",
		authHeader:  "Bearer jwt-token-here",
		deviceIDHdr: "device-abc@tenant-xyz",
		wantDevice:  "device-abc", wantTenant: "tenant-xyz", wantToken: "jwt-token-here", wantOK: true,
	},
	{
		name:        "bearer with X-Device-ID no tenant",
		authHeader:  "Bearer tok",
		deviceIDHdr: "device-only",
		wantDevice:  "device-only", wantTenant: "", wantToken: "tok", wantOK: true,
	},
	{
		name:       "bearer missing device id",
		authHeader: "Bearer tok",
		wantOK:     false,
	},
	{
		name:   "no auth header",
		wantOK: false,
	},
	{
		name:       "basic malformed base64",
		authHeader: "Basic !!!notbase64!!!",
		wantOK:     false,
	},
	{
		name:       "basic missing colon",
		authHeader: "Basic " + base64.StdEncoding.EncodeToString([]byte("nocolon")),
		wantOK:     false,
	},
}

func TestExtractCredentials(t *testing.T) {
	for _, tc := range extractTests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/telemetry", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			if tc.deviceIDHdr != "" {
				req.Header.Set("X-Device-ID", tc.deviceIDHdr)
			}

			devID, tenID, tok, ok := httpapi.ExtractCredentials(req)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if devID != tc.wantDevice {
				t.Errorf("deviceID=%q want %q", devID, tc.wantDevice)
			}
			if tenID != tc.wantTenant {
				t.Errorf("tenantID=%q want %q", tenID, tc.wantTenant)
			}
			if tok != tc.wantToken {
				t.Errorf("token=%q want %q", tok, tc.wantToken)
			}
		})
	}
}

// ── /healthz ──────────────────────────────────────────────────────────────────

func TestRouter_Healthz(t *testing.T) {
	// New() with nil validator/forwarder is fine for /healthz since it doesn't
	// touch them.
	h := httpapi.New(nil, nil, nil)
	srv := httptest.NewServer(h.Router())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", resp.StatusCode)
	}
}

// ── unauthenticated requests ──────────────────────────────────────────────────

func TestRouter_Telemetry_NoAuth(t *testing.T) {
	h := httpapi.New(nil, nil, nil)
	srv := httptest.NewServer(h.Router())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/telemetry", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("PUT /telemetry no-auth = %d, want 401", resp.StatusCode)
	}
}

func TestRouter_Event_NoAuth(t *testing.T) {
	h := httpapi.New(nil, nil, nil)
	srv := httptest.NewServer(h.Router())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/event", bytes.NewReader([]byte(`{}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /event no-auth = %d, want 401", resp.StatusCode)
	}
}
