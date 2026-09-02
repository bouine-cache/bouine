package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestRequestDuration_LabelSpaceBounded pins the phase-1 label contract
// (issue #607): the histogram carries status classes and no method
// dimension, and the spoofed inbound X-Bouine-Route header never reaches it.
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
				"histogram must not carry a method label (issue #607 phase 1.1)")
			for _, l := range met.GetLabel() {
				if l.GetName() == "status" {
					assert.Equal(t, "4xx", l.GetValue(),
						"histogram status must be the response class")
				}
				if l.GetName() == "route" {
					assert.NotEqual(t, "spoofed-route-99999", l.GetValue(),
						"spoofed route header must not appear on the histogram")
				}
			}
		}
		return
	}
	t.Fatal("bouine_request_duration_seconds not gathered")
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
