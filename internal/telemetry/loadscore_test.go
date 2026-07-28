package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestClamp01(t *testing.T) {
	cases := map[float64]float64{
		-1:  0,
		0:   0,
		0.5: 0.5,
		1:   1,
		2:   1,
	}
	for in, want := range cases {
		if got := clamp01(in); got != want {
			t.Errorf("clamp01(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestRunEdgeLoadScoreSampler_ConnectionsFraction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connCount := 400
	done := make(chan struct{})
	go func() {
		RunEdgeLoadScoreSampler(ctx, func() int { return connCount }, 2000, 4, 10*time.Millisecond)
		close(done)
	}()

	// First tick runs synchronously before the ticker loop starts (see
	// RunEdgeLoadScoreSampler's doc), so the connections term is already
	// correct immediately — no need to wait for a tick.
	time.Sleep(20 * time.Millisecond)

	got := testutil.ToFloat64(EdgeConnectionsFraction)
	want := 400.0 / 2000.0
	if got != want {
		t.Errorf("EdgeConnectionsFraction = %v, want %v", got, want)
	}

	score := testutil.ToFloat64(EdgeLoadScore)
	if score < want*0.6 {
		t.Errorf("EdgeLoadScore = %v, want at least the connections-only floor %v", score, want*0.6)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not stop after context cancellation")
	}
}

func TestRunEdgeLoadScoreSampler_ZeroLimitAvoidsDivideByZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		RunEdgeLoadScoreSampler(ctx, func() int { return 10 }, 0, 4, 10*time.Millisecond)
	}()
	time.Sleep(15 * time.Millisecond)
	cancel()

	got := testutil.ToFloat64(EdgeConnectionsFraction)
	if got != 0 {
		t.Errorf("connections fraction with zero limit = %v, want 0 (no divide-by-zero panic)", got)
	}
}
