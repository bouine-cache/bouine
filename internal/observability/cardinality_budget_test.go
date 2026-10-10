package observability

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// gatherFamilyTuples returns the set of distinct label tuples for one
// metric family, keyed by the traffic_class|status|cache_result|source|
// upstream_pool label values (absent axes render empty), with the
// duplicate count. Any duplicate would indicate a slot-table bug:
// identical label tuples must collapse to one series.
func gatherFamilyTuples(t *testing.T, reg *prometheus.Registry, family string) map[string]int {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err, "gather")
	out := map[string]int{}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, met := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range met.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			key := fmt.Sprintf("%s|%s|%s|%s|%s",
				labels["traffic_class"], labels["status"], labels["cache_result"],
				labels["source"], labels["upstream_pool"])
			out[key]++
		}
	}
	return out
}

// gatherHistogramTuples returns the set of active label tuples for
// bouine_request_duration_seconds, formatted
// "statusClass|cache_result|upstream_pool|traffic_class".
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
				labels["status"], labels["cache_result"], labels["upstream_pool"], labels["traffic_class"])
			out[key]++
		}
	}
	return out
}

// TestMetricCardinalityBudget pins the AGENTS.md §9 cardinality budget
// across ALL three data-plane families (ADR-0047 §2.4): with a closed
// label set the observed series count is the closed-form product of
// the driven axes, so any excess is a leak, not arithmetic. The
// histogram is checked against the 10k line and the 5k target; the
// class axis multiplies it exactly like the pool axis.
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
	// Three class slots driven: unclassified + csr + ssr.
	m.PreResolveTrafficClasses([]string{"csr", "ssr"})

	middleware := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {})
	hits := []struct {
		pool, trafficClass, cacheResult, source string
		status                                  int
	}{
		// Hot tuples across every pool.
		{"_default", "unclassified", "HIT", "hot", 200},
		{"_default", "unclassified", "MISS", "origin", 200},
		{"pool-00", "csr", "HIT", "hot", 200},
		{"pool-00", "csr", "MISS", "origin", 200},
		{"pool-00", "ssr", "STALE", "warm", 200},
		{"pool-01", "csr", "HIT", "hot", 200},
		{"pool-01", "ssr", "BYPASS", "origin", 200},
		// Fast-path hits.
		{"pool-02", "csr", "HIT", "hot", 200},
		// Error classes.
		{"pool-03", "ssr", "MISS", "origin", 404},
		{"pool-03", "ssr", "MISS", "origin", 500},
		{"pool-03", "unclassified", "MISS", "origin", 503},
	}
	for _, h := range hits {
		pool := h.pool
		if pool == "_default" {
			pool = ""
		}
		m.RecordHit(pool, h.trafficClass, h.cacheResult, h.source, h.status, 100, 1234567)
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
	assert.Contains(t, tuples, "2xx|HIT|pool-00|csr", "observed pool must have series")
	// ...but idle pools cost nothing.
	for i := 4; i < pools-1; i++ {
		for key := range tuples {
			assert.NotContains(t, key, fmt.Sprintf("pool-%02d", i),
				"idle pool pool-%02d must have zero series", i)
		}
	}
	// 11 RecordHit observations collapse to 11 histogram tuples: 500
	// and 503 share the 5xx class, and identical (class, result, pool)
	// shapes merge. Plus the middleware 404 tuple.
	assert.Len(t, tuples, 12, "one tuple per observed class combination, no more")

	total := 0
	for _, n := range tuples {
		total += n
	}
	// 16 series per tuple (13 buckets + +Inf + _sum + _count); the
	// tail buckets (2.5/5/10s) distinguish slow misses from hung-fetches
	// without adding label dimensions. The observed count scales with
	// driven tuples only — the class axis multiplies the ceiling, not
	// the cost, because slot fill stays lazy.
	assert.Less(t, total*16, 10000, "AGENTS.md §9: histogram series must stay under 10k")
	assert.Less(t, total*16, 5000, "histogram series must stay under the 5k target")

	// Closed-form product check on the two counter families (ADR-0047
	// §2.4): with a closed label set every observation shape must
	// appear exactly once — duplicates mean a slot-table leak.
	// requests_total carries the status axis, so the 404/500 ssr misses
	// stay distinct: 11 RecordHit shapes + the middleware tuple.
	reqTuples := gatherFamilyTuples(t, reg, "bouine_requests_total")
	assert.Len(t, reqTuples, 12, "one requests_total tuple per observation shape")
	for _, n := range reqTuples {
		assert.Equal(t, 1, n, "identical label tuples must collapse to one series")
	}
	// response_bytes has no status axis: the 404/500 ssr misses collapse
	// into one tuple — 10 RecordHit shapes + the middleware tuple.
	bytesTuples := gatherFamilyTuples(t, reg, "bouine_response_bytes_total")
	assert.Len(t, bytesTuples, 11, "one response_bytes tuple per axis-distinct observation shape")
	for _, n := range bytesTuples {
		assert.Equal(t, 1, n, "identical label tuples must collapse to one series")
	}

	// Label-space sanity across every family: traffic_class values come
	// exclusively from the pre-resolved set plus the fallback.
	allowedClasses := map[string]bool{
		api.TrafficClassUnclassified: true, "csr": true, "ssr": true,
	}
	for _, family := range []string{
		"bouine_requests_total",
		"bouine_response_bytes_total",
		"bouine_request_duration_seconds",
	} {
		for key := range gatherFamilyTuples(t, reg, family) {
			class, _, _ := strings.Cut(key, "|")
			assert.True(t, allowedClasses[class],
				"%s: traffic_class %q must come from the pre-resolved set", family, class)
		}
	}
}

// TestMetricCardinalityBudget_IdlePoolsZeroSeries is the explicit lazy
// pre-resolution proof: PreResolveRoutes must not create a single series.
func TestMetricCardinalityBudget_IdlePoolsZeroSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"a", "b", "c", "d"})
	m.PreResolveTrafficClasses([]string{"csr", "ssr"})

	tuples := gatherHistogramTuples(t, reg)
	assert.Empty(t, tuples, "pre-resolution must be lazy: no series before the first observation")

	// Observing one tuple on pool "a" creates exactly one tuple; "b"/"c"/"d" stay empty.
	m.RecordHit("a", "csr", "HIT", "hot", 200, 10, 1_000_000)
	tuples = gatherHistogramTuples(t, reg)
	assert.Len(t, tuples, 1, "exactly one tuple after one observation")
	assert.Contains(t, tuples, "2xx|HIT|a|csr")
}

// TestMetricCardinalityBudget_ClassSlotsBoundCeiling pins the §9 slot
// arithmetic at the config cap: the class axis tops out at
// unclassified + MaxTrafficClasses, and the config validation cap must
// fill the static array bound exactly (a cap/constant drift would
// either overflow the array or waste slots).
func TestMetricCardinalityBudget_ClassSlotsBoundCeiling(t *testing.T) {
	t.Parallel()
	classes := make([]string, config.MaxTrafficClasses)
	for i := range classes {
		classes[i] = fmt.Sprintf("c%d", i)
	}
	require.Equal(t, metricClassSlots, 1+len(classes),
		"config cap must match the static slot-table bound")

	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes(nil)
	m.PreResolveTrafficClasses(classes)

	// Drive one tuple per class slot on the _default pool: unclassified
	// first (slot 0), then every configured class.
	m.RecordHit("", "", "HIT", "hot", 200, 10, 1_000_000)
	for _, class := range classes {
		m.RecordHit("", class, "HIT", "hot", 200, 10, 1_000_000)
	}
	tuples := gatherHistogramTuples(t, reg)
	assert.Len(t, tuples, metricClassSlots,
		"the class axis must top out at unclassified + configured classes")
	for _, class := range append([]string{api.TrafficClassUnclassified}, classes...) {
		assert.Contains(t, tuples, "2xx|HIT|_default|"+class)
	}
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
