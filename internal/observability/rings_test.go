package observability

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestRing_RecordAndFlush(t *testing.T) {
	t.Parallel()
	r := &RequestRing{}

	r.RecordRequest("HIT", 200, 5)
	r.RecordRequest("HIT", 200, 15)
	r.RecordRequest("MISS", 500, 3)

	r.Flush(time.Now())

	snap := r.Snapshot(1)
	require.Len(t, snap, 1)
	b := snap[0]
	assert.Equal(t, int64(3), b.Requests)
	assert.Equal(t, int64(2), b.Hits)
	assert.Equal(t, int64(1), b.Misses)
	assert.Equal(t, int64(1), b.Errors)
	assert.Equal(t, int64(15), b.P99MS)
	assert.Equal(t, int64(0), r.liveRequests.Load())
}

func TestRequestRing_AllCategories(t *testing.T) {
	t.Parallel()
	r := &RequestRing{}
	r.RecordRequest("HIT", 200, 1)
	r.RecordRequest("MISS", 200, 1)
	r.RecordRequest("STALE", 200, 1)
	r.RecordRequest("BYPASS", 200, 1)
	r.RecordRequest("REVALIDATED", 200, 1)
	r.Flush(time.Now())
	snap := r.Snapshot(1)
	b := snap[0]
	assert.Equal(t, int64(5), b.Requests)
	if b.Hits != 1 || b.Misses != 1 || b.StaleHits != 1 || b.Bypasses != 1 || b.Revalidated != 1 {
		t.Errorf("category counts wrong: %+v", b)
	}
}

func TestRequestRing_SnapshotOrder(t *testing.T) {
	t.Parallel()
	r := &RequestRing{}
	base := time.Now().Truncate(10 * time.Second)

	for i := range 5 {
		r.RecordRequest("MISS", 200, int64(i+1))
		r.Flush(base.Add(time.Duration(i) * 10 * time.Second))
	}

	snap := r.Snapshot(5)
	require.Len(t, snap, 5)
	for i := range 4 {
		if snap[i].Timestamp > snap[i+1].Timestamp {
			t.Errorf("buckets not in ascending order at index %d: %d > %d",
				i, snap[i].Timestamp, snap[i+1].Timestamp)
		}
	}
}

func TestRequestRing_SnapshotWraparound(t *testing.T) {
	t.Parallel()
	r := &RequestRing{}
	now := time.Now()
	for i := range requestBuckets + 10 {
		r.RecordRequest("HIT", 200, 1)
		r.Flush(now.Add(time.Duration(i) * requestBucketSecs * time.Second))
	}
	snap := r.Snapshot(requestBuckets)
	assert.Len(t, snap, requestBuckets)
}

func TestRouteRing_RecordAndFlush(t *testing.T) {
	t.Parallel()
	r := &RouteRing{}
	r.RecordRoute("/api/v1", "HIT", 200, 10)
	r.RecordRoute("/api/v1", "HIT", 200, 10)
	r.RecordRoute("/api/v1", "MISS", 200, 20)
	r.RecordRoute("/static", "HIT", 200, 5)
	r.Flush(time.Now())

	stats := r.RouteStats(1)
	byRoute := map[string]RouteStat{}
	for _, s := range stats {
		byRoute[s.Route] = s
	}

	api, ok := byRoute["/api/v1"]
	require.True(t, ok)
	assert.Equal(t, int64(3), api.Requests)
	assert.Equal(t, int64(2), api.Hits)

	st, ok := byRoute["/static"]
	require.True(t, ok)
	if st.HitPct < 99.9 {
		t.Errorf("/static HitPct: want ~100, got %.1f", st.HitPct)
	}
}

func TestRouteRing_Sparkline(t *testing.T) {
	t.Parallel()
	r := &RouteRing{}
	now := time.Now()
	for i := range 10 {
		for range i + 1 {
			r.RecordRoute("/test", "HIT", 200, 10)
		}
		r.Flush(now.Add(time.Duration(i) * time.Minute))
	}
	stats := r.RouteStats(30)
	require.NotEqual(t, 0, len(stats))
	assert.Len(t, stats[0].Sparkline, sparklinePoints)
}

func TestOpsLogRing_RecordAndSnapshot(t *testing.T) {
	t.Parallel()
	r := &OpsLogRing{}
	r.Record("purge", "https://example.com/foo", "ok")
	r.Record("ban", "^/api/", "ok")

	snap := r.Snapshot(10)
	require.Len(t, snap, 2)
	assert.Equal(t, "purge", snap[0].Op)
	assert.Equal(t, "ban", snap[1].Op)
}

func TestOpsLogRing_Wraparound(t *testing.T) {
	t.Parallel()
	r := &OpsLogRing{}
	for range opsLogCap + 5 {
		r.Record("purge", "url", "ok")
	}
	snap := r.Snapshot(opsLogCap)
	assert.Len(t, snap, opsLogCap)
}

func TestRings_SaveLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.snap")

	ri := NewRings("node-1")
	ri.Request.RecordRequest("HIT", 200, 10)
	ri.Request.Flush(time.Now())
	ri.Route.RecordRoute("/test", "HIT", 200, 10)
	ri.Route.Flush(time.Now())

	err := ri.Save(path)
	require.NoErrorf(t, err, "Save: %v", err)

	ri2 := NewRings("node-1")
	err = ri2.Load(path)
	require.NoErrorf(t, err, "Load: %v", err)

	snap := ri2.Request.Snapshot(requestBuckets)
	require.Len(t, snap, requestBuckets)
	var found bool
	for _, b := range snap {
		if b.Requests > 0 {
			assert.Equal(t, int64(1), b.Requests)
			found = true
		}
	}
	assert.True(t, found)
}

func TestRings_LoadMissingFile(t *testing.T) {
	t.Parallel()
	ri := NewRings("node-x")
	err := ri.Load("/tmp/bouine-nonexistent-snap-12345.snap")
	assert.Nil(t, err)
}

func TestMergeSummaries(t *testing.T) {
	t.Parallel()
	now := time.Now()
	a := MetricsSummary{
		NodeName:    "a",
		CollectedAt: now,
		RequestSnap: make([]RequestBucket, requestBuckets),
		RouteStats:  []RouteStat{{Route: "/foo", Requests: 10, Hits: 8}},
	}
	a.RequestSnap[0] = RequestBucket{Requests: 100, Hits: 80, P99MS: 20}

	b := MetricsSummary{
		NodeName:    "b",
		CollectedAt: now,
		RequestSnap: make([]RequestBucket, requestBuckets),
		RouteStats:  []RouteStat{{Route: "/foo", Requests: 5, Hits: 3}},
	}
	b.RequestSnap[0] = RequestBucket{Requests: 50, Hits: 30, P99MS: 50}

	merged := MergeSummaries([]MetricsSummary{a, b})
	assert.Equal(t, int64(150), merged.RequestSnap[0].Requests)
	assert.Equal(t, int64(110), merged.RequestSnap[0].Hits)
	assert.Equal(t, int64(50), merged.RequestSnap[0].P99MS)
	require.Len(t, merged.RouteStats, 1)
	assert.Equal(t, int64(15), merged.RouteStats[0].Requests)
}

func TestMergeSummaries_Empty(t *testing.T) {
	t.Parallel()
	m := MergeSummaries(nil)
	assert.Equal(t, "", m.NodeName)
}

// TestRequestRing_RecordRequestZeroAllocs asserts the hot-path constraint
// from AGENTS.md §7 and exit criterion §6.8: zero allocations per call.
func TestRequestRing_RecordRequestZeroAllocs(t *testing.T) {
	r := &RequestRing{}
	// Warm up once so no lazy initialisation skews the count.
	r.RecordRequest("HIT", 200, 1)
	allocs := testing.AllocsPerRun(200, func() {
		r.RecordRequest("HIT", 200, 5)
	})
	assert.Equal(t, float64(0), allocs)
}

// TestRouteRing_RecordRouteZeroAllocs asserts zero allocations for the
// steady-state path (route already known).
func TestRouteRing_RecordRouteZeroAllocs(t *testing.T) {
	r := &RouteRing{}
	r.RecordRoute("/api/v1", "HIT", 200, 10) // seed so LoadOrStore hits fast path
	allocs := testing.AllocsPerRun(200, func() {
		r.RecordRoute("/api/v1", "HIT", 200, 10)
	})
	assert.Equal(t, float64(0), allocs)
}

// BenchmarkRequestRing_RecordRequest measures hot-path throughput and
// is used by the CI bench gate (see Makefile bench target).
func BenchmarkRequestRing_RecordRequest(b *testing.B) {
	r := &RequestRing{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.RecordRequest("HIT", 200, 5)
	}
}

// BenchmarkRouteRing_RecordRoute measures steady-state route recording.
func BenchmarkRouteRing_RecordRoute(b *testing.B) {
	r := &RouteRing{}
	r.RecordRoute("/api", "HIT", 200, 10) // seed
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.RecordRoute("/api", "HIT", 200, 10)
	}
}

func TestLatencyHistogram_Percentile(t *testing.T) {
	var h LatencyHistogram
	got := h.Percentile(0.5)
	require.Equal(t, int64(0), got)
	// 100 requests all in the ≤10ms bucket (index 3, bound 10).
	h[3] = 100
	for _, p := range []float64{0.5, 0.9, 0.99} {
		got := h.Percentile(p)
		require.Equal(t, int64(10), got)
	}
	// Mixed: 90 fast (≤1ms idx0), 10 slow (overflow idx10).
	h = LatencyHistogram{}
	h[0] = 90
	h[latencyHistBuckets-1] = 10
	got = h.Percentile(0.5)
	require.Equal(t, int64(1), got)
	got = h.Percentile(0.99)
	require.Equal(t, LatencyBoundsMs[len(LatencyBoundsMs)-1], got)
}

func TestLatencyBucketIndex(t *testing.T) {
	cases := []struct {
		durMs int64
		want  int
	}{
		{0, 0}, {1, 0}, {2, 1}, {3, 2}, {10, 3}, {11, 4},
		{1000, 9}, {1001, latencyHistBuckets - 1}, {99999, latencyHistBuckets - 1},
	}
	for _, c := range cases {
		got := latencyBucketIndex(c.durMs)
		require.Equal(t, c.want, got)
	}
}

func TestRequestRing_LatencyHistogramFlush(t *testing.T) {
	r := &RequestRing{}
	r.RecordRequest("HIT", 200, 1)
	r.RecordRequest("HIT", 200, 8)
	r.RecordRequest("MISS", 200, 2000)
	r.Flush(time.Unix(100, 0))
	snap := r.Snapshot(1)
	b := snap[0]
	if b.LatHist[0] != 1 || b.LatHist[3] != 1 || b.LatHist[latencyHistBuckets-1] != 1 {
		t.Fatalf("unexpected latency histogram: %v", b.LatHist)
	}
}

func TestMergeSummaries_LatencyHistogram(t *testing.T) {
	t.Parallel()
	s1 := MetricsSummary{
		NodeName:    "n1",
		RequestSnap: make([]RequestBucket, requestBuckets),
	}
	s1.RequestSnap[0].LatHist[0] = 2
	s1.RequestSnap[0].LatHist[3] = 5
	s1.RequestSnap[0].LatHist[latencyHistBuckets-1] = 1
	last := requestBuckets - 1
	s1.RequestSnap[last].LatHist[1] = 3
	s1.RequestSnap[last].LatHist[5] = 4

	s2 := MetricsSummary{
		NodeName:    "n2",
		RequestSnap: make([]RequestBucket, requestBuckets),
	}
	s2.RequestSnap[0].LatHist[0] = 4
	s2.RequestSnap[0].LatHist[3] = 7
	s2.RequestSnap[0].LatHist[latencyHistBuckets-1] = 2
	s2.RequestSnap[last].LatHist[1] = 6
	s2.RequestSnap[last].LatHist[5] = 8

	merged := MergeSummaries([]MetricsSummary{s1, s2})

	b0 := merged.RequestSnap[0].LatHist
	if b0[0] != 6 || b0[3] != 12 || b0[latencyHistBuckets-1] != 3 {
		t.Fatalf("merged LatHist[0] not summed: got %v", b0)
	}

	bLast := merged.RequestSnap[last].LatHist
	if bLast[1] != 9 || bLast[5] != 12 {
		t.Fatalf("merged LatHist[%d] not summed: got %v", last, bLast)
	}

	for i := range requestBuckets {
		if i != 0 && i != last {
			h := merged.RequestSnap[i].LatHist
			for _, v := range h {
				require.Equal(t, int64(0), v)
			}
		}
	}
}
