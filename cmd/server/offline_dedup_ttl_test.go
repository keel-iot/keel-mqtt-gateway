package main

import (
	"testing"
	"time"
)

func TestResolveOfflineDedupTTL_ConfiguredWins(t *testing.T) {
	got := resolveOfflineDedupTTL(5*time.Second, 20*time.Second)
	if got != 5*time.Second {
		t.Fatalf("expected the explicitly configured TTL to win, got %v", got)
	}
}

func TestResolveOfflineDedupTTL_ZeroDerivesFromReconcilerInterval(t *testing.T) {
	got := resolveOfflineDedupTTL(0, 20*time.Second)
	if got != 40*time.Second {
		t.Fatalf("expected 2x the reconciler interval (40s), got %v", got)
	}
}
