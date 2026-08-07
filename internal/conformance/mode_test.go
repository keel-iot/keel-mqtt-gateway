package conformance

import "testing"

func TestConformanceMode_StandaloneAllowed(t *testing.T) {
	if err := ValidateRole(""); err != nil {
		t.Fatalf("standalone (empty role) should be allowed, got error: %v", err)
	}
}

func TestConformanceMode_EdgeRejected(t *testing.T) {
	if err := ValidateRole("edge"); err == nil {
		t.Fatal("expected error for role=edge, got nil")
	}
}

func TestConformanceMode_CoreRejected(t *testing.T) {
	if err := ValidateRole("core"); err == nil {
		t.Fatal("expected error for role=core, got nil")
	}
}

func TestConformanceMode_CombinedRejected(t *testing.T) {
	if err := ValidateRole("combined"); err == nil {
		t.Fatal("expected error for role=combined, got nil")
	}
}
