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

// Span re-exports otel's trace.Span so L3 code can hold and end spans
// without importing go.opentelemetry.io directly (depguard).
type Span = trace.Span

// otelUserValueKey is where FastHTTPMiddleware stores the server span
// context. It is Background-based, so it is safe to retain past handler
// return, unlike the RequestCtx itself.
const otelUserValueKey = "otel.ctx"

// tracerEnabled is set to true when InitTracer configures a real exporter.
// When false, StartSpan returns a no-op span without calling into the OTel
// global tracer, avoiding ~3 allocations per fetch on the miss path.
var tracerEnabled atomic.Bool

// hoistMode records what InitTracer's sampler deterministically drops, so
// the middleware and origin-span helpers can skip the attribute building
// and the SDK Start call entirely for requests the sampler provably will
// not record. It is the mirror of the sampler InitTracer installs and
// must only be written by InitTracer — a directly-installed provider
// (tests, tracingtest) leaves it at hoistNone, which degrades to the
// pre-hoist behavior: call the SDK and build attributes unconditionally.
type hoistMode int32

const (
	// hoistNone mirrors AlwaysSample: every span records, so there is
	// nothing to hoist and attributes are never wasted.
	hoistNone hoistMode = iota
	// hoistAll mirrors NeverSample: nothing records, ever.
	hoistAll
	// hoistUnsampledParent mirrors ParentBased(TraceIDRatioBased):
	// an unsampled parent (remote or local) deterministically drops the
	// child, so the whole span pipeline can be skipped for it.
	hoistUnsampledParent
)

var samplerHoist atomic.Int32

// Tracer returns the global tracer under the bouine instrumentation name.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// FastHTTPMiddleware wraps a fasthttp.RequestHandler with an OTel span
// named spanName. The span inherits trace context from the incoming
// request headers via W3C TraceContext (fasthttpHeaderCarrier).
// When no tracer is configured, the wrapped handler runs untouched.
//
// The sampler's drop decision is hoisted ahead of the attribute building
// (finding F4): the four attribute strings are byte→string conversions
// the request goroutine pays only to learn "don't sample" when the
// sampler drops. For the configurations where the drop is provable
// (NeverSample; ParentBased with an unsampled parent) the whole SDK
// Start call is skipped too; for indeterminate cases (root spans under
// a ratio sampler — the trace ID is generated inside the SDK) the span
// starts without attributes and sets them only when recording, so a
// dropped root never pays for attributes either.
func FastHTTPMiddleware(spanName string, next fasthttp.RequestHandler) fasthttp.RequestHandler {
	t := Tracer()
	return func(ctx *fasthttp.RequestCtx) {
		if !tracerEnabled.Load() {
			next(ctx)
			return
		}
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
		switch hoistMode(samplerHoist.Load()) {
		case hoistAll:
			// NeverSample drops every span; the parent span context is
			// still published so downstream origin spans keep the
			// (unsampled) trace-id chain.
			ctx.SetUserValue(otelUserValueKey, extractedCtx)
			next(ctx)
			return
		case hoistUnsampledParent:
			// ParentBased(TraceIDRatioBased): an unsampled remote parent
			// deterministically drops the child. Keep extractedCtx as
			// the published context — it already carries the remote
			// span context, so children and logs can still read the trace
			// ID without a span ever being created.
			if sc := trace.SpanContextFromContext(extractedCtx); sc.IsRemote() && !sc.IsSampled() {
				ctx.SetUserValue(otelUserValueKey, extractedCtx)
				next(ctx)
				return
			}
		}
		spanCtx, span := t.Start(extractedCtx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		if span.IsRecording() {
			span.SetAttributes(
				attribute.String("http.method", string(ctx.Method())),
				attribute.String("http.scheme", scheme),
				attribute.String("http.host", string(ctx.Host())),
				attribute.String("http.path", string(ctx.Path())),
			)
		}
		defer span.End()
		ctx.SetUserValue(otelUserValueKey, spanCtx)
		next(ctx)
	}
}

// SpanContextFromRequest returns the span context stored by
// FastHTTPMiddleware, or Background when the request never passed
// through it. Never carries the RequestCtx.
func SpanContextFromRequest(ctx *fasthttp.RequestCtx) context.Context {
	if c, ok := ctx.UserValue(otelUserValueKey).(context.Context); ok {
		return c
	}
	return context.Background()
}

// StartOriginSpan starts the "bouine.origin" span for an origin fetch,
// parented on the client's trace. method and path are byte slices so the
// disabled-tracer path converts nothing; route is the bounded route label
// (http.route), never the raw path. The sampler-drop hoist mirrors
// FastHTTPMiddleware (finding F4): under NeverSample the span pipeline is
// skipped outright, and under ParentBased a parent span context that is
// valid but unsampled deterministically drops the child — origin spans
// are only ever children, so no attribute is built for a span that cannot
// record. Indeterminate parents (no valid span context — a ratio-sampled
// root) start the span bare and set attributes only when recording.
func StartOriginSpan(parent context.Context, method, path []byte, pool, route string) (context.Context, trace.Span) {
	if !tracerEnabled.Load() {
		return parent, trace.SpanFromContext(parent)
	}
	switch hoistMode(samplerHoist.Load()) {
	case hoistAll:
		return parent, trace.SpanFromContext(parent)
	case hoistUnsampledParent:
		if sc := trace.SpanContextFromContext(parent); sc.IsValid() && !sc.IsSampled() {
			return parent, trace.SpanFromContext(parent)
		}
	}
	ctx, span := Tracer().Start(parent, "bouine.origin")
	if span.IsRecording() {
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
		span.SetAttributes(attrs...)
	}
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

	// ParentBased(TraceIDRatioBased) fixes trace stitching for chains that
	// arrive sampled (CDN → L1 → L2): under the bare TraceIDRatioBased the
	// sampled-flag of an incoming traceparent was ignored and a sampled
	// trace could silently drop hops (finding F4/D2). SamplingRate 0 and 1
	// keep their exact Always/Never semantics. The hoist mode published
	// alongside is the mirror of this choice; both must change together.
	sample := sdktrace.AlwaysSample()
	hoist := hoistNone
	if cfg.SamplingRate > 0 && cfg.SamplingRate < 1 {
		sample = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRate))
		hoist = hoistUnsampledParent
	} else if cfg.SamplingRate == 0 {
		sample = sdktrace.NeverSample()
		hoist = hoistAll
	}
	samplerHoist.Store(int32(hoist))

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
