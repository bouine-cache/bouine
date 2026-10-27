package main

import (
	"context"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/observability/tracing"
)

// The middleware benchmarks install the tracer via tracing.InitTracer with
// the fake OTLP collector — the exact production SDK shape (OTLP batcher,
// resource, W3C propagator, and the hoist mode InitTracer publishes) — so
// the numbers reflect the code the data plane runs, including the sampler
// hoist and the span export path.
func initTracer(b *testing.B, samplingRate float64) {
	b.Helper()
	coll, err := startCollector("ok")
	if err != nil {
		b.Fatalf("collector: %v", err)
	}
	shutdown, err := tracing.InitTracer(context.Background(), tracing.TracingConfig{
		Endpoint:     "http://" + collAddr(coll),
		ServiceName:  "bench",
		SamplingRate: samplingRate,
	})
	if err != nil {
		b.Fatalf("InitTracer: %v", err)
	}
	b.Cleanup(func() {
		shutdown()
		coll.stop()
	})
}

func benchRequest() *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://bench.local/ssr/key-000123")
	return ctx
}

func runMiddleware(b *testing.B, traceparent string) {
	b.Helper()
	inner := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	h := tracing.FastHTTPMiddleware("bouine.pipeline", inner)
	ctx := benchRequest()
	if traceparent != "" {
		ctx.Request.Header.Set("traceparent", traceparent)
	}
	h(ctx) // warm
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx.Response.Reset()
		h(ctx)
	}
}

// BenchmarkTopology_TracingMiddleware_Disabled pins the disabled
// short-circuit (no OTLP endpoint) — the default deployment.
func BenchmarkTopology_TracingMiddleware_Disabled(b *testing.B) {
	prev := tracing.TracerEnabled()
	tracing.EnableForTest(false)
	b.Cleanup(func() { tracing.EnableForTest(prev) })
	runMiddleware(b, "")
}

// BenchmarkTopology_TracingMiddleware_Unsampled is the enabled-but-dropped
// root case (sampling_rate 0): every request pays extraction plus the SDK
// Start to decide "don't sample" — unless the hoist skips it all.
func BenchmarkTopology_TracingMiddleware_Unsampled(b *testing.B) {
	initTracer(b, 0)
	runMiddleware(b, "")
}

// BenchmarkTopology_TracingMiddleware_UnsampledRemote is the CDN-fronted
// common case under a ratio sampler: a traceparent header whose sampled
// flag is clear. ParentBased drops it from the parent's sampled flag
// alone, so the whole span pipeline can be skipped.
func BenchmarkTopology_TracingMiddleware_UnsampledRemote(b *testing.B) {
	initTracer(b, 0.01)
	runMiddleware(b, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
}

// BenchmarkTopology_TracingMiddleware_SampledRemote is a sampled incoming
// traceparent under a ratio sampler: always recorded (the stitching fix),
// attributes built once per span.
func BenchmarkTopology_TracingMiddleware_SampledRemote(b *testing.B) {
	initTracer(b, 0.01)
	runMiddleware(b, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
}

// BenchmarkTopology_TracingMiddleware_Sampled is AlwaysSample: every
// request builds attributes, records, and ends a span.
func BenchmarkTopology_TracingMiddleware_Sampled(b *testing.B) {
	initTracer(b, 1)
	runMiddleware(b, "")
}
