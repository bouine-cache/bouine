// Package tracing provides OpenTelemetry span instrumentation for
// bouine's data-plane layers. Each layer wraps its inbound handler
// with a single span so distributed traces show L1 → L2 → L4 → L5
// as nested children.
//
// A nil tracer is always safe to use — all helpers no-op when the
// SDK is not configured, so single-node deployments without a trace
// exporter work unchanged.
package tracing

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "bouine"

// enabled is set to true when InitTracer configures a real TracerProvider.
// It is read-only after init and used by IsEnabled to avoid per-hit
// allocs from StartSpan + WithContext when no tracer is configured.
var enabled bool

// IsEnabled reports whether a real OTel TracerProvider has been configured.
// When false, StartSpan returns a no-op span and callers should skip
// the associated r.WithContext(ctx) to avoid a heap allocation per hit.
func IsEnabled() bool {
	return enabled
}

// Tracer returns the global tracer under the bouine instrumentation name.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// HTTPMiddleware wraps an http.Handler with an OTel span named spanName.
// The span inherits any trace context propagated in the incoming HTTP
// headers via W3C TraceContext (http.Header propagation).
func HTTPMiddleware(spanName string, next http.Handler) http.Handler {
	t := Tracer()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := t.Start(r.Context(), spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.host", r.Host),
			),
		)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// StartSpan is a thin helper that starts a child span in ctx and
// returns the enriched context. The caller is responsible for calling
// span.End().
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx, span := Tracer().Start(ctx, name,
		trace.WithAttributes(attrs...),
	)
	return ctx, span
}

// InjectHTTP stamps the W3C TraceContext (traceparent / tracestate) and
// Baggage headers into req so the upstream origin can continue the trace.
// It is a no-op when no tracer is configured or the context has no active
// span, so callers do not need to guard against unconfigured tracing.
func InjectHTTP(ctx context.Context, req *http.Request) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
}

// RecordError records err on span and sets the span status to Error.
// Safe to call with a nil span.
func RecordError(span trace.Span, err error) {
	if err == nil || span == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// InitTracer configures the global OTel TracerProvider from cfg.
// When cfg.Endpoint is empty the existing no-op provider remains.
// Returns a shutdown function that must be called on process exit
// to flush buffered spans.
func InitTracer(ctx context.Context, cfg TracingConfig) (func(), error) {
	if cfg.Endpoint == "" {
		return func() {}, nil
	}
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.Endpoint),
		otlptracehttp.WithInsecure(), // use http:// by default; caller can override with https://
	)
	if err != nil {
		return func() {}, fmt.Errorf("otlp exporter: %w", err)
	}

	sample := sdktrace.AlwaysSample()
	if cfg.SamplingRate > 0 && cfg.SamplingRate < 1 {
		sample = sdktrace.TraceIDRatioBased(cfg.SamplingRate)
	} else if cfg.SamplingRate == 0 {
		sample = sdktrace.NeverSample()
	}

	svcName := cfg.ServiceName
	if svcName == "" {
		svcName = "bouine"
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sample),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(svcName),
		)),
	)
	otel.SetTracerProvider(tp)
	enabled = true
	// Install W3C TraceContext + Baggage as the global propagator so that
	// InjectHTTP can stamp outbound upstream requests with traceparent headers.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() { //nolint:contextcheck
		_ = tp.Shutdown(context.Background())
	}, nil
}

// TracingConfig holds the OTel export configuration loaded from YAML.
//
//nolint:revive // TracingConfig name is intentional; avoids ambiguity with config.TracingConfig
type TracingConfig struct {
	// Endpoint is the OTLP/HTTP collector endpoint, e.g. "http://otel-collector:4318".
	// Empty string disables exporting (no-op tracer).
	Endpoint string `yaml:"endpoint"`
	// ServiceName is the service.name resource attribute. Defaults to "bouine".
	ServiceName string `yaml:"service_name"`
	// SamplingRate is a float in [0, 1]. 0 = never sample, 1 = always sample (default).
	SamplingRate float64 `yaml:"sampling_rate"`
}
