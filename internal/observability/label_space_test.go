package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestStatusClassString pins the zero-alloc status-class table.
func TestStatusClassString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "2xx", statusClassString(200))
	assert.Equal(t, "2xx", statusClassString(299))
	assert.Equal(t, "3xx", statusClassString(301))
	assert.Equal(t, "4xx", statusClassString(404))
	assert.Equal(t, "5xx", statusClassString(500))
	assert.Equal(t, "5xx", statusClassString(599))
	assert.Equal(t, "0", statusClassString(0))
	assert.Equal(t, "0", statusClassString(99))
	assert.Equal(t, "0", statusClassString(600))
	assert.Equal(t, "1xx", statusClassString(100))
	assert.Equal(t, "1xx", statusClassString(199))
}
