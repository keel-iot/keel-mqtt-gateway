package raft

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRevocationCache_FailsClosedBeforeFirstFetch(t *testing.T) {
	block := make(chan struct{})
	c := &RevocationCache{
		fetch: func() (map[string]int64, error) {
			<-block // never resolves during this test
			return nil, nil
		},
		interval: time.Hour,
		log:      testLog(),
		stop:     make(chan struct{}),
	}
	// Bypass NewRevocationCache's synchronous first refresh — we want to
	// observe the pre-ready state deliberately, not race it.
	if !c.IsRevoked("anything@tenant-1") {
		t.Fatal("expected fail-closed (revoked=true) before first successful fetch")
	}
	close(block)
}

func TestRevocationCache_ServesFetchedSnapshot(t *testing.T) {
	c := NewRevocationCache(func() (map[string]int64, error) {
		return map[string]int64{"device-1@tenant-1": 100}, nil
	}, time.Hour, testLog())
	defer c.Close()

	if !c.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected device-1@tenant-1 to be revoked")
	}
	if c.IsRevoked("device-2@tenant-1") {
		t.Fatal("expected device-2@tenant-1 to not be revoked")
	}
}

func TestRevocationCache_RefreshesPeriodically(t *testing.T) {
	var calls atomic.Int64
	c := NewRevocationCache(func() (map[string]int64, error) {
		n := calls.Add(1)
		if n == 1 {
			return map[string]int64{}, nil
		}
		return map[string]int64{"device-1@tenant-1": 200}, nil
	}, 10*time.Millisecond, testLog())
	defer c.Close()

	if c.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected not revoked on first fetch")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if c.IsRevoked("device-1@tenant-1") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected device-1@tenant-1 to become revoked after periodic refresh")
}

func TestRevocationCache_StaleServedOnRefreshError(t *testing.T) {
	var calls atomic.Int64
	c := NewRevocationCache(func() (map[string]int64, error) {
		n := calls.Add(1)
		if n == 1 {
			return map[string]int64{"device-1@tenant-1": 100}, nil
		}
		return nil, fmt.Errorf("simulated transient failure")
	}, 10*time.Millisecond, testLog())
	defer c.Close()

	if !c.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected device-1@tenant-1 revoked from first successful fetch")
	}
	time.Sleep(50 * time.Millisecond) // let several failing refreshes happen
	if !c.IsRevoked("device-1@tenant-1") {
		t.Fatal("expected stale-but-known revocation to still be served after refresh errors")
	}
}
