package connector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryOutputBuffer_PushWithinCapacity(t *testing.T) {
	buf := NewMemoryOutputBuffer(2)

	dropped, ok := buf.Push(&ForwardRequest{DeviceId: "a"})
	assert.False(t, ok)
	assert.Nil(t, dropped)
	assert.Equal(t, 1, buf.Len())

	dropped, ok = buf.Push(&ForwardRequest{DeviceId: "b"})
	assert.False(t, ok)
	assert.Nil(t, dropped)
	assert.Equal(t, 2, buf.Len())
	assert.Equal(t, 2, buf.Capacity())
}

func TestMemoryOutputBuffer_DropsOldestWhenFull(t *testing.T) {
	buf := NewMemoryOutputBuffer(2)
	buf.Push(&ForwardRequest{DeviceId: "a"})
	buf.Push(&ForwardRequest{DeviceId: "b"})

	dropped, ok := buf.Push(&ForwardRequest{DeviceId: "c"})
	require.True(t, ok)
	require.NotNil(t, dropped)
	assert.Equal(t, "a", dropped.DeviceId, "oldest ('a') must be dropped, not the newest arrival")
	assert.Equal(t, 2, buf.Len())

	ctx := context.Background()
	req, ok := buf.Next(ctx)
	require.True(t, ok)
	assert.Equal(t, "b", req.DeviceId)

	req, ok = buf.Next(ctx)
	require.True(t, ok)
	assert.Equal(t, "c", req.DeviceId)
}

func TestMemoryOutputBuffer_NextBlocksThenUnblocksOnPush(t *testing.T) {
	buf := NewMemoryOutputBuffer(4)
	ctx := context.Background()

	type result struct {
		req *ForwardRequest
		ok  bool
	}
	resultCh := make(chan result, 1)
	go func() {
		req, ok := buf.Next(ctx)
		resultCh <- result{req, ok}
	}()

	select {
	case <-resultCh:
		t.Fatal("Next returned before any Push")
	case <-time.After(50 * time.Millisecond):
	}

	buf.Push(&ForwardRequest{DeviceId: "delayed"})

	select {
	case r := <-resultCh:
		require.True(t, r.ok)
		assert.Equal(t, "delayed", r.req.DeviceId)
	case <-time.After(time.Second):
		t.Fatal("Next did not unblock after Push")
	}
}

func TestMemoryOutputBuffer_NextRespectsContextCancellation(t *testing.T) {
	buf := NewMemoryOutputBuffer(4)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, ok := buf.Next(ctx)
	assert.False(t, ok)
}

func TestMemoryOutputBuffer_CloseUnblocksNext(t *testing.T) {
	buf := NewMemoryOutputBuffer(4)
	ctx := context.Background()

	done := make(chan bool, 1)
	go func() {
		_, ok := buf.Next(ctx)
		done <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	buf.Close()

	select {
	case ok := <-done:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Next")
	}
}

func TestMemoryOutputBuffer_ZeroOrNegativeCapacityClampsToOne(t *testing.T) {
	buf := NewMemoryOutputBuffer(0)
	assert.Equal(t, 1, buf.Capacity())

	buf = NewMemoryOutputBuffer(-5)
	assert.Equal(t, 1, buf.Capacity())
}
