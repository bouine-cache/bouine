package observability

import (
	"path/filepath"
	"testing"
	"time"
)

// TestRequestRing_RecordAndFlush verifies that RecordRequest accumulates
// counters atomically and Flush drains them into a ring bucket.
func TestRequestRing_RecordAndFlush(t *testing.T) {
	r := &RequestRing{}

	r.RecordRequest(true, false, false, 200, 5)
	r.RecordRequest(true, false, false, 200, 15)
	r.RecordRequest(false, true, false, 500, 3)

	r.Flush(time.Now())

	snap := r.Snapshot(1)
	if len(snap) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(snap))
	}
	b := snap[0]
	if b.Requests != 3 {
		t.Errorf("Requests: want 3, got %d", b.Requests)
	}
	if b.Hits != 2 {
		t.Errorf("Hits: want 2, got %d", b.Hits)
	}
	if b.Misses != 1 {
		t.Errorf("Misses: want 1, got %d", b.Misses)
	}
	if b.Errors != 1 {
		t.Errorf("Errors: want 1, got %d", b.Errors)
	}
	if b.P99MS != 15 {
		t.Errorf("P99MS: want 15, got %d", b.P99MS)
	}
	if r.liveRequests.Load() != 0 {
		t.Errorf("live counters must be zero after flush")
	}
}

// TestRequestRing_SnapshotOrder verifies oldest-first ordering.
func TestRequestRing_SnapshotOrder(t *testing.T) {
	r := &RequestRing{}
	base := time.Now().Truncate(10 * time.Second)

	for i := range 5 {
		r.RecordRequest(false, false, false, 200, int64(i+1))
		r.Flush(base.Add(time.Duration(i) * 10 * time.Second))
	}

	snap := r.Snapshot(5)
	if len(snap) != 5 {
		t.Fatalf("expected 5 buckets, got %d", len(snap))
	}
	for i := range 4 {
		if snap[i].Timestamp > snap[i+1].Timestamp {
			t.Errorf("buckets not in ascending order at index %d: %d > %d",
				i, snap[i].Timestamp, snap[i+1].Timestamp)
		}
	}
}

// TestRequestRing_SnapshotWraparound verifies the ring buffer wraps without
// losing data or panicking.
func TestRequestRing_SnapshotWraparound(t *testing.T) {
	r := &RequestRing{}
	now := time.Now()
	for i := range requestBuckets + 10 {
		r.RecordRequest(true, false, false, 200, 1)
		r.Flush(now.Add(time.Duration(i) * requestBucketSecs * time.Second))
	}
	snap := r.Snapshot(requestBuckets)
	if len(snap) != requestBuckets {
		t.Errorf("expected %d buckets, got %d", requestBuckets, len(snap))
	}
}

// TestRouteRing_RecordAndFlush verifies per-route accumulation and stat
// aggregation.
func TestRouteRing_RecordAndFlush(t *testing.T) {
	r := &RouteRing{}
	r.RecordRoute("/api/v1", true, false)
	r.RecordRoute("/api/v1", true, false)
	r.RecordRoute("/api/v1", false, true)
	r.RecordRoute("/static", true, false)
	r.Flush(time.Now())

	stats := r.RouteStats(1)
	byRoute := map[string]RouteStat{}
	for _, s := range stats {
		byRoute[s.Route] = s
	}

	api, ok := byRoute["/api/v1"]
	if !ok {
		t.Fatal("/api/v1 not found in route stats")
	}
	if api.Requests != 3 {
		t.Errorf("/api/v1 Requests: want 3, got %d", api.Requests)
	}
	if api.Hits != 2 {
		t.Errorf("/api/v1 Hits: want 2, got %d", api.Hits)
	}

	st, ok := byRoute["/static"]
	if !ok {
		t.Fatal("/static not found in route stats")
	}
	if st.HitPct < 99.9 {
		t.Errorf("/static HitPct: want ~100, got %.1f", st.HitPct)
	}
}

// TestRings_SaveLoad verifies gob round-trip through Save and Load.
func TestRings_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.snap")

	ri := NewRings("node-1")
	ri.Request.RecordRequest(true, false, false, 200, 10)
	ri.Request.Flush(time.Now())
	ri.Route.RecordRoute("/test", true, false)
	ri.Route.Flush(time.Now())

	if err := ri.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	ri2 := NewRings("node-1")
	if err := ri2.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	snap := ri2.Request.Snapshot(requestBuckets)
	if len(snap) != requestBuckets {
		t.Fatalf("expected %d buckets, got %d", requestBuckets, len(snap))
	}
	var found bool
	for _, b := range snap {
		if b.Requests > 0 {
			if b.Requests != 1 {
				t.Errorf("expected 1 request, got %d", b.Requests)
			}
			found = true
		}
	}
	if !found {
		t.Error("expected at least one non-zero bucket after load")
	}
}

// TestRings_LoadMissingFile verifies that a missing snapshot file is silently
// ignored (returns nil error).
func TestRings_LoadMissingFile(t *testing.T) {
	ri := NewRings("node-x")
	if err := ri.Load("/tmp/bouine-nonexistent-snap-12345.snap"); err != nil {
		t.Errorf("expected nil error for missing file, got %v", err)
	}
}

// TestMergeSummaries verifies counter addition and p99 max semantics.
func TestMergeSummaries(t *testing.T) {
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
	if merged.RequestSnap[0].Requests != 150 {
		t.Errorf("merged Requests: want 150, got %d", merged.RequestSnap[0].Requests)
	}
	if merged.RequestSnap[0].Hits != 110 {
		t.Errorf("merged Hits: want 110, got %d", merged.RequestSnap[0].Hits)
	}
	if merged.RequestSnap[0].P99MS != 50 {
		t.Errorf("merged P99MS: want 50 (max), got %d", merged.RequestSnap[0].P99MS)
	}
	if len(merged.RouteStats) != 1 {
		t.Fatalf("expected 1 route stat, got %d", len(merged.RouteStats))
	}
	if merged.RouteStats[0].Requests != 15 {
		t.Errorf("merged route Requests: want 15, got %d", merged.RouteStats[0].Requests)
	}
}

// TestMergeSummaries_Empty verifies empty input returns zero value.
func TestMergeSummaries_Empty(t *testing.T) {
	m := MergeSummaries(nil)
	if m.NodeName != "" {
		t.Errorf("expected empty NodeName, got %q", m.NodeName)
	}
}
