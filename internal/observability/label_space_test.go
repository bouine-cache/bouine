package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestRequestDuration_LabelSpaceBounded pins the label contract: the
// histogram carries status classes and no method or source dimension,
// and spoofed inbound X-Bouine-Route/X-Bouine-Pool headers never
// reach it.
func TestRequestDuration_LabelSpaceBounded(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"api"})

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.Response.Header.Set(header.XCacheSource, "origin")
		ctx.SetStatusCode(404)
	})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/x")
	ctx.Request.Header.SetMethod("PROPFIND")
	ctx.Request.Header.Set(header.XBouineRoute, "spoofed-route-99999")
	ctx.Request.Header.Set(header.XBouinePool, "spoofed-pool-99999")
	h(ctx)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "bouine_request_duration_seconds" {
			continue
		}
		require.NotEmpty(t, mf.GetMetric())
		for _, met := range mf.GetMetric() {
			var names []string
			for _, l := range met.GetLabel() {
				names = append(names, l.GetName())
			}
			assert.NotContains(t, names, "method",
				"histogram must not carry a method label")
			assert.NotContains(t, names, "source",
				"histogram must not carry a source label")
			for _, l := range met.GetLabel() {
				if l.GetName() == "status" {
					assert.Equal(t, "4xx", l.GetValue(),
						"histogram status must be the response class")
				}
				if l.GetName() == "route" || l.GetName() == "upstream_pool" {
					assert.NotContains(t, l.GetValue(), "spoofed",
						"spoofed route/pool header must not appear on the histogram")
				}
			}
		}
		return
	}
	t.Fatal("bouine_request_duration_seconds not gathered")
}

// TestRequestsTotal_NoMethodLabel pins the label contract: the metrics
// carry no method axis, so arbitrary or exotic method tokens cannot
// mint or alter any label value.
func TestRequestsTotal_NoMethodLabel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"api"})

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.SetStatusCode(404)
	})
	for _, method := range []string{"GET", "PROPFIND", "TRACK", "X9"} {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI("/api/x")
		ctx.Request.Header.SetMethod(method)
		h(ctx)
	}

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, met := range mf.GetMetric() {
			for _, l := range met.GetLabel() {
				assert.NotEqual(t, "method", l.GetName(),
					"no metric may carry a method label")
			}
		}
	}
}

// TestTrafficClass_LabelSpaceClosed pins the ADR-0047 label contract:
// the traffic_class label values on all three data-plane families come
// exclusively from the pre-resolved config set plus "unclassified" —
// spoofed inbound X-Bouine-Traffic-Class headers never mint a label,
// and unknown RecordHit classes collapse into "unclassified".
func TestTrafficClass_LabelSpaceClosed(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"api"})
	m.PreResolveTrafficClasses([]string{"csr", "ssr"})

	// Middleware path with a spoofed header form (the router's
	// UserValue form is the only reader; an inbound header is noise).
	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "HIT")
		ctx.Response.Header.Set(header.XCacheSource, "hot")
		ctx.SetStatusCode(200)
	})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/x")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set(header.XBouineTrafficClass, "spoofed-class-99999")
	h(ctx)

	// Fast-path path with an out-of-table class (a handler bug would be
	// the only producer; the fallback must still close the set).
	m.RecordHit("api", "totally-unknown-class", "HIT", "hot", 200, 10, time.Millisecond)

	allowed := map[string]bool{
		api.TrafficClassUnclassified: true,
		"csr":                        true,
		"ssr":                        true,
	}
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bouine_requests_total", "bouine_request_duration_seconds", "bouine_response_bytes_total":
		default:
			continue
		}
		require.NotEmpty(t, mf.GetMetric(), mf.GetName())
		for _, met := range mf.GetMetric() {
			for _, l := range met.GetLabel() {
				if l.GetName() != "traffic_class" {
					continue
				}
				assert.True(t, allowed[l.GetValue()],
					"%s: traffic_class %q outside the config-closed set", mf.GetName(), l.GetValue())
				assert.NotContains(t, l.GetValue(), "spoofed", mf.GetName())
			}
		}
	}
}

// TestTrafficClass_AlwaysPresent pins the always-present semantics:
// every series on the three data-plane families carries a traffic_class
// label — deployments without configured classes differ only in that
// the single value is "unclassified", never in label presence.
func TestTrafficClass_AlwaysPresent(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"api"})
	m.PreResolveTrafficClasses(nil) // no classes configured

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.Response.Header.Set(header.XCacheSource, "origin")
		ctx.SetStatusCode(200)
	})
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/x")
	ctx.Request.Header.SetMethod("GET")
	h(ctx)
	m.RecordHit("api", "", "HIT", "hot", 200, 10, time.Millisecond)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bouine_requests_total", "bouine_request_duration_seconds", "bouine_response_bytes_total":
		default:
			continue
		}
		for _, met := range mf.GetMetric() {
			var found bool
			for _, l := range met.GetLabel() {
				if l.GetName() == "traffic_class" {
					found = true
					assert.Equal(t, api.TrafficClassUnclassified, l.GetValue(),
						mf.GetName()+": the no-classes deployment shape is unclassified")
				}
			}
			assert.True(t, found, mf.GetName()+": every series must carry traffic_class")
		}
	}
}
