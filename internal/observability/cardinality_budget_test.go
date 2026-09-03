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
// bouine_request_duration_seconds, formatted "statusClass|cache_result|upstream_pool".
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
			key := fmt.Sprintf("%s|%s|%s",
				labels["status"], labels["cache_result"], labels["upstream_pool"])
			out[key]++
		}
	}
	return out
}

// TestMetricCardinalityBudget pins the AGENTS.md §9 cardinality budget:
// a realistic mixed workload over the maximum configured pools must keep
// bouine_request_duration_seconds under 10k series (target < 5k), and idle
// pools must cost zero series (lazy pre-resolution).
func TestMetricCardinalityBudget(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	const pools = 33 // configured pools + _default
	names := make([]string, pools-1)
	for i := range names {
		names[i] = fmt.Sprintf("pool-%02d", i)
	}
	m.PreResolveRoutes(names)

	middleware := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {})
	hits := []struct {
		pool, cacheResult, source string
		status                    int
	}{
		// Hot tuples across every pool.
		{"_default", "HIT", "hot", 200},
		{"_default", "MISS", "origin", 200},
		{"pool-00", "HIT", "hot", 200},
		{"pool-00", "MISS", "origin", 200},
		{"pool-00", "STALE", "warm", 200},
		{"pool-01", "HIT", "hot", 200},
		{"pool-01", "BYPASS", "origin", 200},
		// Fast-path hits.
		{"pool-02", "HIT", "hot", 200},
		// Error classes.
		{"pool-03", "MISS", "origin", 404},
		{"pool-03", "MISS", "origin", 500},
		{"pool-03", "MISS", "origin", 503},
	}
	for _, h := range hits {
		pool := h.pool
		if pool == "_default" {
			pool = ""
		}
		m.RecordHit(pool, h.cacheResult, h.source, h.status, 100, 1234567)
	}

	// Middleware path: 404 no-route traffic. No method axis exists, so
	// arbitrary method tokens cannot influence the label space at all.
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/x")
	ctx.Request.Header.SetMethod("PROPFIND")
	ctx.SetStatusCode(fasthttp.StatusNotFound)
	middleware(ctx)

	tuples := gatherHistogramTuples(t, reg)

	// Every configured pool must exist once observed...
	assert.Contains(t, tuples, "2xx|HIT|pool-00", "observed pool must have series")
	// ...but idle pools cost nothing.
	for i := 4; i < pools-1; i++ {
		for key := range tuples {
			assert.NotContains(t, key, fmt.Sprintf("pool-%02d", i),
				"idle pool pool-%02d must have zero series", i)
		}
	}
	// 11 observations collapse to 11 histogram tuples: 500 and 503 share
	// the 5xx class, HEAD and POST share GET's 2xx/HIT tuple shape where
	// applicable (no method dimension) — that collapsing is the win.
	// Plus the middleware 404 tuple.
	assert.Len(t, tuples, 11, "one tuple per observed class combination, no more")

	total := 0
	for _, n := range tuples {
		total += n
	}
	// 16 series per tuple (13 buckets + +Inf + _sum + _count).
	assert.Less(t, total*16, 10000, "AGENTS.md §9: histogram series must stay under 10k")
	assert.Less(t, total*16, 5000, "histogram series must stay under the 5k target")
}

// TestMetricCardinalityBudget_IdlePoolsZeroSeries is the explicit lazy
// pre-resolution proof: PreResolveRoutes must not create a single series.
func TestMetricCardinalityBudget_IdlePoolsZeroSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"a", "b", "c", "d"})

	tuples := gatherHistogramTuples(t, reg)
	assert.Empty(t, tuples, "pre-resolution must be lazy: no series before the first observation")

	// Observing one tuple on pool "a" creates exactly one tuple; "b"/"c"/"d" stay empty.
	m.RecordHit("a", "HIT", "hot", 200, 10, 1_000_000)
	tuples = gatherHistogramTuples(t, reg)
	assert.Len(t, tuples, 1, "exactly one tuple after one observation")
	assert.Contains(t, tuples, "2xx|HIT|a")
}

// TestMetricCardinalityBudget_ManyPoolsValidate is the flip side of the
// budget: pool count is deliberately uncapped in config validation. The
// upstream_pool label set is bounded by how many pools actually receive
// traffic (lazy slot fill), not by a config cap, so a pool-heavy topology
// stays a valid configuration.
func TestMetricCardinalityBudget_ManyPoolsValidate(t *testing.T) {
	t.Parallel()
	newCfg := func(n int) *config.Config {
		pools := make([]config.UpstreamPool, n)
		for i := range pools {
			pools[i] = config.UpstreamPool{Name: fmt.Sprintf("p%02d", i), Targets: []string{"a:1"}}
		}
		return &config.Config{Listen: config.Listen{Admin: ":9000"}, UpstreamPools: pools}
	}
	require.NoError(t, newCfg(64).Validate(), "64 pools must validate: no pool-count cap")
}
