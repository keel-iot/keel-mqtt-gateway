package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "keel-gateway"

// TenantCacheReader is a minimal read-only interface consumed by
// TenantAwareSampler. *auth.TenantConfigCache satisfies this interface.
type TenantCacheReader interface {
	// TracingEnabled returns true when the tenant has opted into 100% sampling.
	// Must be safe to call from multiple goroutines.
	TracingEnabled(tenantID string) bool
}

// TenantAwareSampler wraps a base sampler and forces 100% sampling for any
// span that carries a "tenant.id" attribute belonging to a tenant that has
// opted in via devices.tenant_gateway_config.tracing_enabled = true.
//
// For all other spans the base sampler (10% ratio) is used.
type TenantAwareSampler struct {
	base  sdktrace.Sampler
	cache TenantCacheReader
}

// newTenantAwareSampler constructs the sampler.  When cache is nil it falls
// back to the base sampler for every span (no per-tenant override).
func newTenantAwareSampler(base sdktrace.Sampler, cache TenantCacheReader) sdktrace.Sampler {
	if cache == nil {
		return base
	}
	return &TenantAwareSampler{base: base, cache: cache}
}

func (s *TenantAwareSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	for _, attr := range p.Attributes {
		if attr.Key == attribute.Key("tenant.id") {
			tenantID := attr.Value.AsString()
			if tenantID != "" && s.cache.TracingEnabled(tenantID) {
				// Inherit trace state from parent span context.
				traceState := trace.SpanFromContext(p.ParentContext).SpanContext().TraceState()
				return sdktrace.SamplingResult{
					Decision:   sdktrace.RecordAndSample,
					Tracestate: traceState,
				}
			}
			break
		}
	}
	return s.base.ShouldSample(p)
}

func (s *TenantAwareSampler) Description() string {
	return "TenantAwareSampler{" + s.base.Description() + "}"
}

// InitTracer sets up the OpenTelemetry tracer with an OTLP gRPC exporter.
// When otlpEndpoint is empty, tracing is disabled (no-op provider installed).
// tenantCache may be nil; when provided, tenants with tracing_enabled get 100%
// sampling regardless of the global ratio.
// Returns a shutdown function that must be called on process exit.
func InitTracer(ctx context.Context, otlpEndpoint string, tenantCache TenantCacheReader, log *slog.Logger) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }

	if otlpEndpoint == "" {
		log.Info("tracing disabled (OTLP_ENDPOINT not set)")
		return noop, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return noop, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("keel-gateway"),
		),
	)
	if err != nil {
		// Non-fatal — use empty resource
		res = resource.Empty()
		log.Warn("tracing: failed to build resource", "error", err)
	}

	baseSampler := sdktrace.TraceIDRatioBased(0.1)
	sampler := newTenantAwareSampler(baseSampler, tenantCache)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Sample 10% by default; tenants with tracing_enabled=true are sampled at 100%.
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	log.Info("tracing enabled", "endpoint", otlpEndpoint)
	return tp.Shutdown, nil
}

// Tracer returns the named tracer for keel-gateway.
// Always safe to call — returns no-op tracer if InitTracer was never called.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}
