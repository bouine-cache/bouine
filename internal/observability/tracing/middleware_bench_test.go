package tracing

import (
	"testing"

	"github.com/valyala/fasthttp"
)

// BenchmarkGate_TracingPipeline_Disabled pins the disabled short-circuit
// (default deployment, no OTLP endpoint) at zero allocations.
func BenchmarkGate_TracingPipeline_Disabled(b *testing.B) {
	prev := tracerEnabled.Load()
	tracerEnabled.Store(false)
	b.Cleanup(func() { tracerEnabled.Store(prev) })

	inner := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(200) }
	h := FastHTTPMiddleware("bouine.pipeline", inner)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://bench.local/products/sku-123")
	h(ctx) // warm

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx.Response.Reset()
		h(ctx)
	}
}
