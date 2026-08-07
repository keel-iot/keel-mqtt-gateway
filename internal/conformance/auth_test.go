package conformance

import (
	"context"
	"testing"
)

func TestConformanceAuth_AllowsAnonymous(t *testing.T) {
	p := NewAuthProvider()

	info, err := p.ValidatePassword(context.Background(), "", "")
	if err != nil {
		t.Fatalf("ValidatePassword with empty credentials: unexpected error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil DeviceInfo for empty credentials")
	}

	info2, err := p.LookupByCN(context.Background(), "any-device", "any-tenant")
	if err != nil {
		t.Fatalf("LookupByCN: unexpected error: %v", err)
	}
	if info2 == nil {
		t.Fatal("expected non-nil DeviceInfo for arbitrary CN lookup")
	}

	// UpdateLastSeen must not panic — best-effort, no-op.
	p.UpdateLastSeen(context.Background(), info.ID)
}

func TestConformanceAuth_AcceptsArbitraryCredentials(t *testing.T) {
	p := NewAuthProvider()
	info, err := p.ValidatePassword(context.Background(), "whoever", "whatever-password")
	if err != nil || info == nil {
		t.Fatalf("expected any credential to be accepted, got info=%v err=%v", info, err)
	}
}
