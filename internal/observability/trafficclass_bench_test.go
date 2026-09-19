package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// newBenchRegistry is a fresh registry per benchmark (the metrics
// register on construction and a package-global would collide).
func newBenchRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

// benchRouterShape mirrors the engine's router-attribution wiring for
// the traffic_class axis: the router sets the XBouineTrafficClass
// UserValue and the middleware's attribution() reads it. The inner
// handler replays the router's stamp so the benchmark measures the
// class axis end to end (slot lookup + label resolution), not the
// classifier itself (gated in internal/server).
func benchTrafficClassShape(class string) func(*fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.SetUserValue(header.XBouineTrafficClass, class)
		ctx.Response.Header.Set("X-Cache", "MISS")
		ctx.Response.Header.Set("X-Cache-Source", "origin")
		ctx.SetStatusCode(200)
	}
}

// BenchmarkGate_Middleware_Miss_TrafficClass measures the middleware
// cost per request on a MISS with the traffic_class axis active
// (ADR-0047): attribution reads one extra UserValue, the slot resolvers
// index one deeper, and WithLabelValues carries one more label. Must
// stay within the middleware's alloc budget — the class string is the
// classifier's stable config-owned value, never a per-request copy.
func BenchmarkGate_Middleware_Miss_TrafficClass(b *testing.B) {
	reg := newBenchRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"bench"})
	m.PreResolveTrafficClasses([]string{"csr", "ssr"})
	h := m.FastHTTPMiddleware(benchTrafficClassShape("ssr"))

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://bench.local/miss")
	h(ctx) // warm

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx.Response.Reset()
		ctx.ResetUserValues()
		h(ctx)
	}
}

// BenchmarkGate_Middleware_Miss_TrafficClass_MixedCaseHost is the
// allocation-hazard variant: a ToLower-based classifier would allocate
// only on non-lowercase input, so an all-lowercase benchmark would hide
// it. The middleware reads the UserValue (already a stable class
// string), so the mixed-case Host exercises the attribution path's
// non-allocating reads over adversarial input.
func BenchmarkGate_Middleware_Miss_TrafficClass_MixedCaseHost(b *testing.B) {
	reg := newBenchRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"bench"})
	m.PreResolveTrafficClasses([]string{"csr", "ssr"})
	h := m.FastHTTPMiddleware(benchTrafficClassShape("csr"))

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	// Adversarial mixed-case Host: the classifier itself is gated in
	// internal/server (BenchmarkGate_RoutedFastPath_Hit_TrafficClass);
	// here it pins the middleware over mixed-case request input.
	ctx.Request.SetRequestURI("http://BeNcH.LoCaL/miss")
	h(ctx) // warm

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx.Response.Reset()
		ctx.ResetUserValues()
		h(ctx)
	}
}

// BenchmarkGate_RecordHit_TrafficClass measures the fast-path metrics
// hook (DataPlaneMetrics.RecordHit) with the class axis active — the
// exact call the h1parser and the reactor drainer make per hit. Must
// stay zero-alloc: class strings come from the pre-resolved table.
func BenchmarkGate_RecordHit_TrafficClass(b *testing.B) {
	reg := newBenchRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"bench"})
	m.PreResolveTrafficClasses([]string{"csr", "ssr"})

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		m.RecordHit("bench", "ssr", "HIT", "hot", 200, 128, 1_000_000)
	}
}
