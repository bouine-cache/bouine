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
	"strings"
	"sync/atomic"

	"github.com/valyala/fasthttp"
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

// Span re-exports otel's trace.Span so cache-layer (L3) code can hold
// and end spans without importing go.opentelemetry.io directly
// (depguard: L3 reaches this package through the observability kernel
// only).
type Span = trace.Span

// otelUserValueKey is the RequestCtx user-value key under which
// FastHTTPMiddleware stores the server span context. It is
// Background-based, so it is safe to retain past handler return —
// unlike the RequestCtx itself.
const otelUserValueKey = "otel.ctx"

// tracerEnabled is set to true when InitTracer configures a real exporter.
// When false, StartSpan returns a no-op span without calling into the OTel
// global tracer, avoiding ~3 allocations per fetch on the miss path.
var tracerEnabled atomic.Bool

// Tracer returns the global tracer under the bouine instrumentation name.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// HTTPMiddleware wraps an http.Handler with an OTel span named spanName.
// The span inherits any trace context propagated in the incoming HTTP
// headers via W3C TraceContext (http.Header propagation).

// FastHTTPMiddleware wraps a fasthttp.RequestHandler with an OTel span
// named spanName. The span inherits trace context from the incoming
// request headers via W3C TraceContext (fasthttpHeaderCarrier).
func FastHTTPMiddleware(spanName string, next fasthttp.RequestHandler) fasthttp.RequestHandler {
	t := Tracer()
	return func(ctx *fasthttp.RequestCtx) {
		scheme := "http"
		if ctx.IsTLS() {
			scheme = "https"
		}
		// Use context.Background() as the extraction base — NOT ctx,
		// which is *fasthttp.RequestCtx. The RequestCtx implements
		// context.Context but gets reset by fasthttp after the handler
		// returns. If it's in the span context's parent chain, the
		// ReverseProxy's http.Transport goroutine (which outlives the
		// handler) will access freed RequestCtx memory via
		// context.parentCancelCtx.
		extractedCtx := ExtractFastHTTP(context.Background(), &ctx.Request.Header)
		spanCtx, span := t.Start(extractedCtx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", string(ctx.Method())),
				attribute.String("http.scheme", scheme),
				attribute.String("http.host", string(ctx.Host())),
				attribute.String("http.path", string(ctx.Path())),
			),
		)
		defer span.End()
		ctx.SetUserValue(otelUserValueKey, spanCtx)
		next(ctx)
	}
}

// SpanContextFromRequest returns the span context stored by
// FastHTTPMiddleware, or Background when the request never passed
// through it (detached-root behavior). Never carries the RequestCtx.
func SpanContextFromRequest(ctx *fasthttp.RequestCtx) context.Context {
	if c, ok := ctx.UserValue(otelUserValueKey).(context.Context); ok {
		return c
	}
	return context.Background()
}

// StartOriginSpan starts the "bouine.origin" span for an origin fetch,
// parented on the client's trace (SpanContextFromRequest). method, path
// are byte slices so the disabled-tracer path converts nothing; pool and
// route are strings the caller already owns. route is the bounded route
// label (http.route) — never the raw path. The slice is built
// conditionally so empty values are omitted, not passed as zero-value
// KeyValues the SDK counts as dropped attributes on every fetch.
// The returned context never carries the RequestCtx and is safe to
// retain past handler return.
func StartOriginSpan(parent context.Context, method, path []byte, pool, route string) (context.Context, trace.Span) {
	if !tracerEnabled.Load() {
		return parent, trace.SpanFromContext(parent)
	}
	attrs := make([]attribute.KeyValue, 0, 4)
	attrs = append(attrs,
		attribute.String("http.method", string(method)),
		attribute.String("http.path", string(path)),
	)
	if pool != "" {
		attrs = append(attrs, attribute.String("upstream_pool", pool))
	}
	if route != "" {
		attrs = append(attrs, attribute.String("http.route", route))
	}
	ctx, span := Tracer().Start(parent, "bouine.origin",
		trace.WithAttributes(attrs...),
	)
	return ctx, span
}

// StartSpan is a thin helper that starts a child span in ctx and
// returns the enriched context. The caller is responsible for calling
// span.End(). When no tracer is configured (InitTracer was not called
// or had no endpoint), this is a no-op that returns ctx and a no-op
// span without allocating.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !tracerEnabled.Load() {
		return ctx, trace.SpanFromContext(ctx)
	}
	ctx, span := Tracer().Start(ctx, name,
		trace.WithAttributes(attrs...),
	)
	return ctx, span
}

// TracerEnabled reports whether InitTracer has configured a real tracer.
// Callers on hot paths can use this to skip span creation overhead
// (context.WithValue, cancelCtx allocation) when tracing is not configured.
func TracerEnabled() bool {
	return tracerEnabled.Load()
}

// InjectHTTP stamps the W3C TraceContext (traceparent / tracestate) and
// Baggage headers into req so the upstream origin can continue the trace.
// It is a no-op when no tracer is configured or the context has no active
// span, so callers do not need to guard against unconfigured tracing.

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
	tracerEnabled.Store(true)
	// otlptracehttp.WithEndpoint expects "host:port" without a scheme.
	// The OTEL_EXPORTER_OTLP_ENDPOINT env var (and some YAML configs)
	// include the "http://" or "https://" prefix, so strip it here.
	endpoint := strings.TrimPrefix(cfg.Endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
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

// EnableForTest toggles span creation for tests in other packages that
// install their own tracer provider via a test-support helper. Production
// code must use InitTracer instead.
func EnableForTest(enabled bool) {
	tracerEnabled.Store(enabled)
}
