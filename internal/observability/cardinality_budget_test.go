package observability

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"

	"github.com/valyala/fasthttp"
)

// gatherHistogramTuples returns the set of active label tuples for
// bouine_request_duration_seconds, formatted "statusClass|cache_result|source|route".
func gatherHistogramTuples(t *testing.T, reg *prometheus.Registry) map[string]int {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err, "gather")
	out := map[string]int{}
	for _, mf := range mfs {
		if mf.GetName() != "bouine_request_duration_seconds" {
			continue
		}
		for _, met := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range met.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			key := fmt.Sprintf("%s|%s|%s|%s",
				labels["status"], labels["cache_result"], labels["source"], labels["route"])
			out[key]++
		}
	}
	return out
}

// TestMetricCardinalityBudget pins the AGENTS.md §9 cardinality budget:
// a realistic mixed workload over the maximum configured routes must keep
// bouine_request_duration_seconds under 10k series (target < 5k), and idle
// routes must cost zero series (lazy pre-resolution).
func TestMetricCardinalityBudget(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	const routes = 33 // 32 capped routes + _default
	names := make([]string, routes-1)
	for i := range names {
		names[i] = fmt.Sprintf("route-%02d", i)
	}
	m.PreResolveRoutes(names)

	middleware := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {})
	hits := []struct {
		method, route, cacheResult, source string
		status                             int
	}{
		// Hot tuples across every route.
		{"GET", "_default", "HIT", "hot", 200},
		{"GET", "_default", "MISS", "origin", 200},
		{"GET", "route-00", "HIT", "hot", 200},
		{"GET", "route-00", "MISS", "origin", 200},
		{"GET", "route-00", "STALE", "warm", 200},
		{"GET", "route-01", "HIT", "hot", 200},
		{"HEAD", "route-01", "HIT", "hot", 200},
		// Fast-path hits.
		{"GET", "route-02", "HIT", "hot", 200},
		// Error classes.
		{"GET", "route-03", "MISS", "origin", 404},
		{"GET", "route-03", "MISS", "origin", 500},
		{"GET", "route-03", "MISS", "origin", 503},
	}
	for i, h := range hits {
		route := h.route
		if route == "_default" {
			route = ""
		}
		m.RecordHit(h.method, route, h.cacheResult, h.source, h.status, 100, 1234567)
		_ = i
	}

	// Middleware path: 404 no-route traffic with an arbitrary method token
	// (the unbounded-label attack shape from the issue).
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/x")
	ctx.Request.Header.SetMethod("PROPFIND")
	ctx.SetStatusCode(fasthttp.StatusNotFound)
	middleware(ctx)

	tuples := gatherHistogramTuples(t, reg)

	// Every configured route must exist once observed...
	assert.Contains(t, tuples, "2xx|HIT|hot|route-00", "observed route must have series")
	// ...but idle routes cost nothing.
	for i := 4; i < routes-1; i++ {
		for key := range tuples {
			assert.NotContains(t, key, fmt.Sprintf("route-%02d", i),
				"idle route route-%02d must have zero series", i)
		}
	}
	// 11 observations collapse to 9 histogram tuples: 500 and 503 share
	// the 5xx class, and HEAD shares GET's tuple (no method dimension) —
	// that collapsing is the phase-1.2 win. Plus the middleware 404 tuple.
	assert.Len(t, tuples, 10, "one tuple per observed class combination, no more")

	total := 0
	for _, n := range tuples {
		total += n
	}
	// 16 series per tuple (13 buckets + +Inf + _sum + _count).
	assert.Less(t, total*16, 10000, "AGENTS.md §9: histogram series must stay under 10k")
	assert.Less(t, total*16, 5000, "issue #607 acceptance: histogram series under 5k")
}

// TestMetricCardinalityBudget_IdleRoutesZeroSeries is the explicit lazy
// pre-resolution proof: PreResolveRoutes must not create a single series.
func TestMetricCardinalityBudget_IdleRoutesZeroSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"a", "b", "c", "d"})

	tuples := gatherHistogramTuples(t, reg)
	assert.Empty(t, tuples, "pre-resolution must be lazy: no series before the first observation")

	// Observing one tuple on route "a" creates exactly one tuple; "b"/"c"/"d" stay empty.
	m.RecordHit("GET", "a", "HIT", "hot", 200, 10, 1_000_000)
	tuples = gatherHistogramTuples(t, reg)
	assert.Len(t, tuples, 1, "exactly one tuple after one observation")
	assert.Contains(t, tuples, "2xx|HIT|hot|a")
}

// TestMetricCardinalityBudget_RouteCap pins the config cap: 32 routes.
func TestMetricCardinalityBudget_RouteCap(t *testing.T) {
	t.Parallel()
	newCfg := func(n int) *config.Config {
		pool := config.UpstreamPool{Name: "app", Targets: []string{"a:1"}}
		routes := make([]config.Route, n)
		for i := range routes {
			routes[i] = config.Route{Pool: "app", Match: config.RouteMatch{PathPrefix: fmt.Sprintf("/r%02d", i)}}
		}
		return &config.Config{Listen: config.Listen{Admin: ":9000"}, UpstreamPools: []config.UpstreamPool{pool}, Routes: routes}
	}
	require.NoError(t, newCfg(32).Validate(), "32 routes must validate")
	err := newCfg(33).Validate()
	require.Error(t, err, "33 routes must be rejected")
	assert.Contains(t, err.Error(), "routes")
}
