package connector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/keel-iot/keel-mqtt-gateway/internal/telemetry"
)

// forwardRetryBackoff is the delay before each retry attempt of a single
// buffered request against the inner connector. Bounded (not indefinite)
// so a single failing request can never stall the drain loop for long —
// after the last attempt the request is dropped and counted.
var forwardRetryBackoff = []time.Duration{0, 200 * time.Millisecond, 1 * time.Second}

// BufferedConnector wraps an OutputConnector with an OutputBuffer, so a
// slow or unavailable downstream (Kafka/Ditto) degrades to bounded,
// observable message loss instead of blocking the MQTT publish hot path.
//
// Forward enqueues into the buffer and returns immediately; a single
// background goroutine (started by Start) drains the buffer and calls the
// inner connector's Forward, retrying briefly on failure before dropping.
// Every drop — buffer-full or forward-error — increments
// telemetry.ForwarderDropped, labelled by connector name and reason.
type BufferedConnector struct {
	inner OutputConnector
	buf   OutputBuffer
	name  string
	log   *slog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBuffered wraps inner with a MemoryOutputBuffer of the given capacity
// (in messages). name identifies the connector in the ForwarderDropped
// metric label (e.g. "kafka-hono").
func NewBuffered(inner OutputConnector, capacity int, name string, log *slog.Logger) *BufferedConnector {
	return &BufferedConnector{
		inner: inner,
		buf:   NewMemoryOutputBuffer(capacity),
		name:  name,
		log:   log,
	}
}

// Init delegates to the inner connector. Call Start afterwards to begin
// draining — kept separate so Init's error path (e.g. bad config) is
// reported before any goroutine starts.
func (b *BufferedConnector) Init(ctx context.Context, config map[string]string) error {
	return b.inner.Init(ctx, config)
}

// Start launches the background drain goroutine. ctx bounds the goroutine's
// lifetime independent of Shutdown, so it always stops when the caller's
// top-level context is cancelled (e.g. process shutdown).
func (b *BufferedConnector) Start(ctx context.Context) {
	drainCtx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.wg.Add(1)
	go b.drainLoop(drainCtx)
}

// Forward enqueues req into the buffer and returns immediately; the actual
// downstream call happens asynchronously in the drain goroutine. A dropped
// oldest-request (buffer full) is counted here.
func (b *BufferedConnector) Forward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error) {
	if dropped, ok := b.buf.Push(req); ok && dropped != nil {
		telemetry.ForwarderDropped.WithLabelValues(b.name, "buffer_full").Inc()
		b.log.Warn("output-connector: buffer full, dropped oldest",
			"connector", b.name, "device_id", dropped.DeviceId, "topic", dropped.Topic)
	}
	return &ForwardResponse{Success: true}, nil
}

// HealthCheck delegates to the inner connector.
func (b *BufferedConnector) HealthCheck(ctx context.Context) error {
	return b.inner.HealthCheck(ctx)
}

// Shutdown stops the drain goroutine and shuts down the inner connector.
// Requests still queued in the buffer at shutdown are not flushed —
// bounded, in-memory buffering was chosen precisely to accept that loss
// class rather than add persistence (see design doc).
func (b *BufferedConnector) Shutdown(ctx context.Context) error {
	b.buf.Close()
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	return b.inner.Shutdown(ctx)
}

func (b *BufferedConnector) drainLoop(ctx context.Context) {
	defer b.wg.Done()
	for {
		req, ok := b.buf.Next(ctx)
		if !ok {
			return
		}
		b.forwardWithRetry(ctx, req)
	}
}

// forwardWithRetry attempts to forward req to the inner connector, retrying
// on a short bounded backoff. It never blocks the drain loop indefinitely
// on a single message: after the last attempt fails, the message is
// dropped and counted rather than retried forever.
func (b *BufferedConnector) forwardWithRetry(ctx context.Context, req *ForwardRequest) {
	ctx, span := telemetry.Tracer().Start(ctx, "keel-gateway.forward",
		oteltrace.WithAttributes(
			attribute.String("connector", b.name),
			attribute.String("device.id", req.DeviceId),
			attribute.String("mqtt.topic", req.Topic),
		),
	)
	defer span.End()

	var lastErr error
	for i, d := range forwardRetryBackoff {
		if i > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return
			}
		}

		resp, err := b.inner.Forward(ctx, req)
		if err == nil && resp != nil && resp.Success {
			return
		}
		if err != nil {
			lastErr = err
		} else if resp != nil {
			lastErr = errors.New(resp.Error)
		} else {
			lastErr = errors.New("forward: nil response")
		}
	}

	b.log.Error("output-connector: forward failed after retries, dropping",
		"connector", b.name, "device_id", req.DeviceId, "topic", req.Topic, "error", lastErr)
	telemetry.ForwarderDropped.WithLabelValues(b.name, "forward_error").Inc()
	span.SetStatus(codes.Error, lastErr.Error())
}
