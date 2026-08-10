package broker

import (
	"testing"
	"time"
)

func TestKeyedRateLimiter_NilAlwaysAllows(t *testing.T) {
	var l *keyedRateLimiter
	for i := 0; i < 1000; i++ {
		if !l.allow("any-key") {
			t.Fatal("a nil *keyedRateLimiter must always allow")
		}
	}
}

func TestKeyedRateLimiter_WithinRateAllowed(t *testing.T) {
	l := newKeyedRateLimiter(1000, 1, time.Minute, time.Hour)
	defer l.close()

	for i := 0; i < 5; i++ {
		if !l.allow("k") {
			t.Fatalf("call %d: expected allow within a generous rate", i)
		}
		time.Sleep(2 * time.Millisecond) // stay well under 1000/s so the single-token bucket keeps refilling
	}
}

func TestKeyedRateLimiter_BurstConsumedThenRejected(t *testing.T) {
	l := newKeyedRateLimiter(0.001, 3, time.Minute, time.Hour) // effectively no refill within the test's lifetime
	defer l.close()

	for i := 0; i < 3; i++ {
		if !l.allow("k") {
			t.Fatalf("call %d: expected the burst of 3 to be allowed", i)
		}
	}
	if l.allow("k") {
		t.Fatal("expected the 4th call to be rejected once the burst is exhausted")
	}
}

func TestKeyedRateLimiter_OverRateRejected(t *testing.T) {
	l := newKeyedRateLimiter(0.001, 1, time.Minute, time.Hour)
	defer l.close()

	if !l.allow("k") {
		t.Fatal("expected the first call to be allowed")
	}
	if l.allow("k") {
		t.Fatal("expected the second call to be rejected — burst of 1, negligible refill rate")
	}
}

func TestKeyedRateLimiter_DifferentKeysIndependentBuckets(t *testing.T) {
	l := newKeyedRateLimiter(0.001, 1, time.Minute, time.Hour)
	defer l.close()

	if !l.allow("a") {
		t.Fatal("expected key a's first call to be allowed")
	}
	if l.allow("a") {
		t.Fatal("expected key a's second call to be rejected")
	}
	if !l.allow("b") {
		t.Fatal("expected key b to have its own independent bucket, unaffected by key a")
	}
}

func TestKeyedRateLimiter_RefillOverTime(t *testing.T) {
	l := newKeyedRateLimiter(1000, 1, time.Minute, time.Hour) // 1000/s => refills a 1-token bucket in ~1ms
	defer l.close()

	if !l.allow("k") {
		t.Fatal("expected the first call to be allowed")
	}
	if l.allow("k") {
		t.Fatal("expected the immediate second call to be rejected — burst of 1")
	}
	time.Sleep(10 * time.Millisecond)
	if !l.allow("k") {
		t.Fatal("expected refill after waiting well past the 1000/s refill interval")
	}
}

// TestKeyedRateLimiter_SweepRemovesExpiredEntries drives sweep directly
// with a manufactured "now" rather than sleeping past a real TTL — the
// property under test is the age-comparison logic itself, not wall-clock
// timing, so making it wait on the real clock would only add flakiness
// risk for no extra coverage.
func TestKeyedRateLimiter_SweepRemovesExpiredEntries(t *testing.T) {
	l := newKeyedRateLimiter(1, 1, time.Minute, time.Hour)
	defer l.close()

	for i := 0; i < 50; i++ {
		l.allow(string(rune('a'+i%26)) + string(rune(i)))
	}
	if got := l.entryCount(); got == 0 {
		t.Fatal("expected at least one live entry before sweeping")
	}

	l.sweep(time.Now().Add(2 * time.Hour)) // well past the 1-minute TTL
	if got := l.entryCount(); got != 0 {
		t.Fatalf("expected sweep to remove every expired entry, got %d remaining", got)
	}
}

func TestKeyedRateLimiter_SweepKeepsFreshEntries(t *testing.T) {
	l := newKeyedRateLimiter(1, 1, time.Minute, time.Hour)
	defer l.close()

	l.allow("k")
	l.sweep(time.Now()) // no time has passed — nothing should be evicted
	if got := l.entryCount(); got != 1 {
		t.Fatalf("expected the fresh entry to survive a sweep, got %d entries", got)
	}
}

func TestRemoteIP_StripsPort(t *testing.T) {
	if got := remoteIP("10.0.0.1:5555"); got != "10.0.0.1" {
		t.Fatalf("expected 10.0.0.1, got %q", got)
	}
}

func TestRemoteIP_FallsBackOnParseFailure(t *testing.T) {
	if got := remoteIP("not-a-host-port"); got != "not-a-host-port" {
		t.Fatalf("expected the raw string as a fallback, got %q", got)
	}
}
