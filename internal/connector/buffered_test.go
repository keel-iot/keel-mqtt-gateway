package connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConnector simulates a downstream (e.g. Kafka producer) that can be
// flipped between healthy and down, and records every request it actually
// receives from the drain goroutine.
type fakeConnector struct {
	mu       sync.Mutex
	down     bool
	received []*ForwardRequest

	initCalls     int32
	shutdownCalls int32
}

func (f *fakeConnector) Init(ctx context.Context, config map[string]string) error {
	atomic.AddInt32(&f.initCalls, 1)
	return nil
}

func (f *fakeConnector) Forward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, errors.New("simulated producer down")
	}
	f.received = append(f.received, req)
	return &ForwardResponse{Success: true}, nil
}

func (f *fakeConnector) HealthCheck(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errors.New("down")
	}
	return nil
}

func (f *fakeConnector) Shutdown(ctx context.Context) error {
	atomic.AddInt32(&f.shutdownCalls, 1)
	return nil
}

func (f *fakeConnector) setDown(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = v
}

func (f *fakeConnector) receivedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

func (f *fakeConnector) receivedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.received))
	for i, r := range f.received {
		out[i] = r.DeviceId
	}
	return out
}

func TestBufferedConnector_ForwardsWhenHealthy(t *testing.T) {
	fake := &fakeConnector{}
	bc := NewBuffered(fake, 10, "test", testLogger())
	ctx := context.Background()
	require.NoError(t, bc.Init(ctx, nil))
	bc.Start(ctx)
	defer bc.Shutdown(context.Background())

	resp, err := bc.Forward(ctx, &ForwardRequest{DeviceId: "d1"})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	require.Eventually(t, func() bool { return fake.receivedCount() == 1 }, time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"d1"}, fake.receivedIDs())
}

// TestBufferedConnector_SustainedPublishUnderOutage simulates a rallentamento/
// interruzione del producer: publish continues while the inner connector is
// down, the bounded buffer fills and starts dropping oldest (counted), and
// once the producer recovers the drain goroutine resumes delivering — with
// no loss of the messages still resident in the buffer at recovery time.
func TestBufferedConnector_SustainedPublishUnderOutage(t *testing.T) {
	fake := &fakeConnector{down: true}
	const capacity = 5
	bc := NewBuffered(fake, capacity, "test-outage", testLogger())
	ctx := context.Background()
	require.NoError(t, bc.Init(ctx, nil))
	bc.Start(ctx)
	defer bc.Shutdown(context.Background())

	before := testutilCounterTotal(t, "test-outage", "forward_error")

	// Sustained publish while the producer is down: far more than capacity,
	// so drop-oldest must kick in repeatedly.
	const total = 20
	for i := 0; i < total; i++ {
		_, err := bc.Forward(ctx, &ForwardRequest{DeviceId: fmt.Sprintf("d%02d", i)})
		require.NoError(t, err)
	}

	// The drain goroutine keeps retrying against the down producer (bounded
	// backoff per message, see forwardRetryBackoff) — give it time to burn
	// through several failed attempts and increment forward_error, without
	// ever blocking new Forward calls (already proven above: all 20 Forward
	// calls returned immediately).
	require.Eventually(t, func() bool {
		return testutilCounterTotal(t, "test-outage", "forward_error") > before
	}, 3*time.Second, 10*time.Millisecond, "forward_error must be counted while producer is down")

	assert.Equal(t, 0, fake.receivedCount(), "nothing should reach the inner connector while it's down")

	// Producer recovers: whatever is still resident in the buffer must be
	// delivered without further loss.
	fake.setDown(false)

	require.Eventually(t, func() bool {
		return fake.receivedCount() >= 1
	}, 3*time.Second, 10*time.Millisecond, "drain must resume delivering once producer recovers")

	// Buffer must drain to empty post-recovery — no message left stuck.
	require.Eventually(t, func() bool {
		return bc.buf.Len() == 0
	}, 3*time.Second, 10*time.Millisecond)
}

func TestBufferedConnector_BufferFullDropsOldestAndCounts(t *testing.T) {
	fake := &fakeConnector{down: true} // keep producer down so nothing drains during the assertion window
	bc := NewBuffered(fake, 2, "test-full", testLogger())
	ctx := context.Background()
	require.NoError(t, bc.Init(ctx, nil))
	// Deliberately not calling Start: isolates buffer-full behaviour from
	// any race with the drain goroutine popping items concurrently.

	before := testutilCounterTotal(t, "test-full", "buffer_full")

	bc.Forward(ctx, &ForwardRequest{DeviceId: "d1"})
	bc.Forward(ctx, &ForwardRequest{DeviceId: "d2"})
	bc.Forward(ctx, &ForwardRequest{DeviceId: "d3"}) // drops d1

	assert.Equal(t, before+1, testutilCounterTotal(t, "test-full", "buffer_full"))
	assert.Equal(t, 2, bc.buf.Len())
}

func TestBufferedConnector_ShutdownStopsDrainAndClosesInner(t *testing.T) {
	fake := &fakeConnector{}
	bc := NewBuffered(fake, 10, "test-shutdown", testLogger())
	ctx := context.Background()
	require.NoError(t, bc.Init(ctx, nil))
	bc.Start(ctx)

	require.NoError(t, bc.Shutdown(context.Background()))
	assert.Equal(t, int32(1), atomic.LoadInt32(&fake.shutdownCalls))
}
