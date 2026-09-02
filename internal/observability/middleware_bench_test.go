package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"
)

// BenchmarkGate_Middleware_Miss measures the middleware wrapper cost per
// request on a MISS (the non-hit path where metrics/rings/logging all run).
// The inner handler is a no-op — this isolates the middleware itself.
func BenchmarkGate_Middleware_Miss(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	// Production always pre-resolves the route table (engine wiring);
	// the benchmark must measure that shape, not the WithLabelValues
	// fallback.
	m.PreResolveRoutes([]string{"bench"})
	m.SetAccessLog(NoopLogger{}, 0) // sampling off: worst case for the log path
	inner := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("X-Cache", "MISS")
		ctx.Response.Header.Set("X-Cache-Source", "origin")
		ctx.SetStatusCode(200)
	}
	h := m.FastHTTPMiddleware(inner)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://bench.local/miss")
	h(ctx) // warm

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx.Response.Reset()
		h(ctx)
	}
}

// BenchmarkGate_Middleware_Miss_NoLog is the default deployment shape:
// access log off entirely.
func BenchmarkGate_Middleware_Miss_NoLog(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"bench"})
	inner := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("X-Cache", "MISS")
		ctx.Response.Header.Set("X-Cache-Source", "origin")
		ctx.SetStatusCode(200)
	}
	h := m.FastHTTPMiddleware(inner)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://bench.local/miss")
	h(ctx)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx.Response.Reset()
		h(ctx)
	}
}
