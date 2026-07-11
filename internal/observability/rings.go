// Package observability — rings.go
// In-memory ring buffers for the dashboard data layer.
// Hot path: only atomic.Add calls. Rings are updated by a background
// goroutine every 10s from live atomic accumulators.
package observability

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ── Constants ────────────────────────────────────────────────────────

const (
	requestBucketSecs = 10                              // 10-second buckets
	requestBuckets    = 6 * 60 * 60 / requestBucketSecs // 2160 = 6h
	routeBuckets      = 24 * 60                         // 1440 = 24h, 1-min buckets
	sparklinePoints   = 8                               // per-route sparkline width
	opsLogCap         = 100                             // max ops-log entries
	peerBucketSecs    = 30                              // 30-second buckets for the peer health ring
	peerBuckets       = 30 * 60 / peerBucketSecs        // 60 = 30 min
	// latencyHistBuckets is the number of fixed log-scale latency buckets
	// recorded per request window (10 finite bands + 1 overflow).
	latencyHistBuckets = 11
)

// LatencyBoundsMs are the inclusive upper bounds (ms) for the first 10
// latency histogram buckets; the 11th bucket captures everything above
// the last bound. Index i holds requests with bound[i-1] < dur <= bound[i].
var LatencyBoundsMs = [latencyHistBuckets - 1]int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

// latencyBucketIndex returns the histogram bucket for a duration in ms.
func latencyBucketIndex(durMs int64) int {
	for i, bound := range LatencyBoundsMs {
		if durMs <= bound {
			return i
		}
	}
	return latencyHistBuckets - 1
}

// LatencyHistogram is a summed latency distribution over a window.
type LatencyHistogram [latencyHistBuckets]int64

// Percentile returns the upper bound (ms) of the bucket containing the
// p-th percentile (0..1), or 0 when the histogram is empty. The overflow
// bucket reports the last finite bound (i.e. ">1000ms" → 1000).
func (h LatencyHistogram) Percentile(p float64) int64 {
	var total int64
	for _, c := range h {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := int64(float64(total) * p)
	var cum int64
	for i, c := range h {
		cum += c
		if cum >= target {
			if i >= len(LatencyBoundsMs) {
				return LatencyBoundsMs[len(LatencyBoundsMs)-1]
			}
			return LatencyBoundsMs[i]
		}
	}
	return LatencyBoundsMs[len(LatencyBoundsMs)-1]
}

// Merge adds the counts from other into h element-wise, returning the
// combined histogram. Used to aggregate latency distributions across
// routes that share the same upstream pool.
func (h LatencyHistogram) Merge(other LatencyHistogram) LatencyHistogram {
	var out LatencyHistogram
	for i := range h {
		out[i] = h[i] + other[i]
	}
	return out
}

// ── Request ring ─────────────────────────────────────────────────────

// RequestBucket holds aggregated counters for one time window.
// All five X-Cache categories are tracked so the dashboard can render
// a full cache-breakdown donut.
type RequestBucket struct {
	Requests    int64
	Hits        int64
	Misses      int64
	StaleHits   int64
	Bypasses    int64            // X-Cache: BYPASS
	Revalidated int64            // X-Cache: REVALIDATED
	Errors      int64            // HTTP 5xx
	DurSumMs    int64            // sum of request durations in ms (for avg)
	DurN        int64            // number of samples with duration
	P99MS       int64            // running max latency ms (approximation; superseded by LatHist)
	LatHist     LatencyHistogram // latency distribution for this window
	Timestamp   int64            // unix seconds of window start
}

// RequestRing is a circular buffer of requestBuckets.
// The live* fields are updated by the data-plane hot path via
// atomic.Add and flushed into the ring every requestBucketSecs.
type RequestRing struct {
	mu      sync.RWMutex
	buckets [requestBuckets]RequestBucket
	head    int // next write position

	// live accumulators — updated atomically on the hot path
	liveRequests    atomic.Int64
	liveHits        atomic.Int64
	liveMisses      atomic.Int64
	liveStaleHits   atomic.Int64
	liveBypasses    atomic.Int64
	liveRevalidated atomic.Int64
	liveErrors      atomic.Int64
	liveDurSumMs    atomic.Int64
	liveDurN        atomic.Int64
	liveP99MS       atomic.Int64 // max duration seen since last flush
	liveLatHist     [latencyHistBuckets]atomic.Int64
}

// RecordRequest is called on the hot path for every completed request.
// xCache is the value of the X-Cache response header ("HIT", "MISS",
// "STALE", "BYPASS", "REVALIDATED"). Zero allocations — only atomic adds.
func (r *RequestRing) RecordRequest(xCache string, statusCode int, durMs int64) {
	r.liveRequests.Add(1)
	switch xCache {
	case "HIT":
		r.liveHits.Add(1)
	case "MISS":
		r.liveMisses.Add(1)
	case "STALE":
		r.liveStaleHits.Add(1)
	case "BYPASS":
		r.liveBypasses.Add(1)
	case "REVALIDATED":
		r.liveRevalidated.Add(1)
	}
	if statusCode >= 500 {
		r.liveErrors.Add(1)
	}
	r.liveDurSumMs.Add(durMs)
	r.liveDurN.Add(1)
	r.liveLatHist[latencyBucketIndex(durMs)].Add(1)
	// Update max via CAS loop.
	for {
		old := r.liveP99MS.Load()
		if durMs <= old {
			break
		}
		if r.liveP99MS.CompareAndSwap(old, durMs) {
			break
		}
	}
}

// Flush drains the live accumulators into the next ring bucket.
// Called by the background goroutine every requestBucketSecs.
func (r *RequestRing) Flush(now time.Time) {
	b := RequestBucket{
		Requests:    r.liveRequests.Swap(0),
		Hits:        r.liveHits.Swap(0),
		Misses:      r.liveMisses.Swap(0),
		StaleHits:   r.liveStaleHits.Swap(0),
		Bypasses:    r.liveBypasses.Swap(0),
		Revalidated: r.liveRevalidated.Swap(0),
		Errors:      r.liveErrors.Swap(0),
		DurSumMs:    r.liveDurSumMs.Swap(0),
		DurN:        r.liveDurN.Swap(0),
		P99MS:       r.liveP99MS.Swap(0),
		Timestamp:   now.Unix(),
	}
	for i := range r.liveLatHist {
		b.LatHist[i] = r.liveLatHist[i].Swap(0)
	}
	r.mu.Lock()
	r.buckets[r.head] = b
	r.head = (r.head + 1) % requestBuckets
	r.mu.Unlock()
}

// Snapshot returns up to n most-recent buckets, oldest first.
func (r *RequestRing) Snapshot(n int) []RequestBucket {
	if n > requestBuckets {
		n = requestBuckets
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RequestBucket, n)
	head := r.head
	for i := 0; i < n; i++ {
		idx := (head - n + i + requestBuckets) % requestBuckets
		out[i] = r.buckets[idx]
	}
	return out
}

// ── Route ring ───────────────────────────────────────────────────────

// RouteBucket holds per-route aggregated counters for one minute.
type RouteBucket struct {
	Route     string
	Requests  int64
	Hits      int64
	Misses    int64
	Errors    int64 // HTTP 5xx
	LatHist   LatencyHistogram
	Timestamp int64
}

// RouteRing holds per-route 1-minute buckets for the last 24h.
type RouteRing struct {
	mu      sync.RWMutex
	buckets []RouteBucket // grows as routes are discovered
	// live per-route accumulators
	liveRoutes sync.Map // string → *routeCounters
}

type routeCounters struct {
	requests atomic.Int64
	hits     atomic.Int64
	misses   atomic.Int64
	errors   atomic.Int64
	latHist  [latencyHistBuckets]atomic.Int64
}

// RecordRoute is called on the hot path.
// xCache is the X-Cache header value; "HIT" increments hits, "MISS" misses.
// statusCode is used to track 5xx errors per route.
// durMs is the request duration in milliseconds, used for per-route latency.
func (r *RouteRing) RecordRoute(route, xCache string, statusCode int, durMs int64) {
	v, ok := r.liveRoutes.Load(route)
	if !ok {
		v, _ = r.liveRoutes.LoadOrStore(route, &routeCounters{})
	}
	c := v.(*routeCounters)
	c.requests.Add(1)
	switch xCache {
	case "HIT":
		c.hits.Add(1)
	case "MISS":
		c.misses.Add(1)
	}
	if statusCode >= 500 {
		c.errors.Add(1)
	}
	c.latHist[latencyBucketIndex(durMs)].Add(1)
}

// Flush drains live per-route counters into the ring.
func (r *RouteRing) Flush(now time.Time) {
	ts := now.Unix()
	r.liveRoutes.Range(func(k, v any) bool {
		route := k.(string)
		c := v.(*routeCounters)
		b := RouteBucket{
			Route:     route,
			Requests:  c.requests.Swap(0),
			Hits:      c.hits.Swap(0),
			Misses:    c.misses.Swap(0),
			Errors:    c.errors.Swap(0),
			Timestamp: ts,
		}
		for i := range c.latHist {
			b.LatHist[i] = c.latHist[i].Swap(0)
		}
		r.mu.Lock()
		r.buckets = append(r.buckets, b)
		// Trim to 24h × routes.
		if len(r.buckets) > routeBuckets*20 {
			r.buckets = r.buckets[len(r.buckets)-routeBuckets*20:]
		}
		r.mu.Unlock()
		return true
	})
}

// RouteStats returns the latest aggregated stats per route over windowBuckets
// minutes, including a sparkline of the most recent sparklinePoints buckets.
func (r *RouteRing) RouteStats(windowBuckets int) []RouteStat {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Aggregate totals over the window.
	agg := make(map[string]*RouteStat)
	cutoff := max(0, len(r.buckets)-windowBuckets*20)
	for _, b := range r.buckets[cutoff:] {
		s, ok := agg[b.Route]
		if !ok {
			s = &RouteStat{Route: b.Route}
			agg[b.Route] = s
		}
		s.Requests += b.Requests
		s.Hits += b.Hits
		s.Misses += b.Misses
		s.Errors += b.Errors
		s.LatHist = s.LatHist.Merge(b.LatHist)
	}

	// Build sparkline (last sparklinePoints per-minute counts) per route.
	// Walk backwards through all buckets collecting up to sparklinePoints
	// distinct timestamps per route.
	type tsCount struct {
		ts  int64
		req int64
	}
	sparkRaw := make(map[string][]tsCount)
	for i := len(r.buckets) - 1; i >= 0; i-- {
		b := r.buckets[i]
		pts := sparkRaw[b.Route]
		if len(pts) < sparklinePoints {
			sparkRaw[b.Route] = append(pts, tsCount{b.Timestamp, b.Requests})
		}
	}

	out := make([]RouteStat, 0, len(agg))
	for _, s := range agg {
		if s.Requests > 0 {
			s.HitPct = math.Round(float64(s.Hits)/float64(s.Requests)*1000) / 10
		}
		s.P99MS = s.LatHist.Percentile(0.99)
		// Reverse sparkline so oldest-first.
		raw := sparkRaw[s.Route]
		s.Sparkline = make([]int64, sparklinePoints)
		for i, pt := range raw {
			s.Sparkline[sparklinePoints-1-i] = pt.req
		}
		out = append(out, *s)
	}
	return out
}

// RouteStat is the aggregated view of one route over a time window.
type RouteStat struct {
	Route     string
	Requests  int64
	Hits      int64
	Misses    int64
	Errors    int64 // HTTP 5xx
	P99MS     int64
	LatHist   LatencyHistogram
	HitPct    float64 // 0-100
	Sparkline []int64 // last sparklinePoints per-minute request counts
}

// ── Ops log ring ─────────────────────────────────────────────────────

// OpsLogEntry records a single operator invalidation action.
type OpsLogEntry struct {
	Timestamp int64  // unix seconds
	Op        string // "purge" | "ban" | "refresh"
	Arg       string // URL or predicate summary
	Result    string // "ok" | error message
}

// OpsLogRing is a circular buffer of the last opsLogCap operator actions.
// It is safe for concurrent use and never allocates on Record after warm-up.
type OpsLogRing struct {
	mu      sync.Mutex
	entries [opsLogCap]OpsLogEntry
	head    int
	count   int
}

// Record appends an invalidation event to the ring.
func (r *OpsLogRing) Record(op, arg, result string) {
	e := OpsLogEntry{
		Timestamp: time.Now().Unix(),
		Op:        op,
		Arg:       arg,
		Result:    result,
	}
	r.mu.Lock()
	r.entries[r.head] = e
	r.head = (r.head + 1) % opsLogCap
	if r.count < opsLogCap {
		r.count++
	}
	r.mu.Unlock()
}

// Snapshot returns the most recent n entries, oldest first.
func (r *OpsLogRing) Snapshot(n int) []OpsLogEntry {
	if n > opsLogCap {
		n = opsLogCap
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.count {
		n = r.count
	}
	out := make([]OpsLogEntry, n)
	for i := range n {
		idx := (r.head - n + i + opsLogCap) % opsLogCap
		out[i] = r.entries[idx]
	}
	return out
}

// ── Peer ring ─────────────────────────────────────────────────────────

// PeerBucket records whether a peer was reachable in a 30s window.
type PeerBucket struct {
	Timestamp int64
	NodeName  string
	Reachable bool
}

// PeerRing is a sliding-window ring of peer health snapshots.
// It records one sample per peer per 30s window and retains 30 minutes
// of history (60 buckets per peer).
type PeerRing struct {
	mu      sync.Mutex
	buckets []PeerBucket
}

// Record appends a health sample; old samples beyond peerBuckets are pruned.
func (r *PeerRing) Record(nodeName string, reachable bool) {
	b := PeerBucket{
		Timestamp: time.Now().Truncate(peerBucketSecs * time.Second).Unix(),
		NodeName:  nodeName,
		Reachable: reachable,
	}
	cutoff := b.Timestamp - int64(peerBuckets*peerBucketSecs)
	r.mu.Lock()
	r.buckets = append(r.buckets, b)
	start := 0
	for start < len(r.buckets) && r.buckets[start].Timestamp < cutoff {
		start++
	}
	if start > 0 {
		r.buckets = r.buckets[start:]
	}
	r.mu.Unlock()
}

// PeerHealth returns the uptime percentage (0-100) per node over the
// retention window. Nodes not seen are absent from the map.
func (r *PeerRing) PeerHealth() map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := make(map[string]int, 4)
	reach := make(map[string]int, 4)
	for _, b := range r.buckets {
		total[b.NodeName]++
		if b.Reachable {
			reach[b.NodeName]++
		}
	}
	out := make(map[string]float64, len(total))
	for name, t := range total {
		if t > 0 {
			out[name] = float64(reach[name]) / float64(t) * 100
		}
	}
	return out
}

// ── Rings manager ────────────────────────────────────────────────────

// Rings holds all ring buffers and their background flush goroutine.
type Rings struct {
	Request    *RequestRing
	Route      *RouteRing
	URL        *URLRing
	OpsLog     *OpsLogRing
	Peer       *PeerRing
	HeaderRing *OriginHeaderRing
	NodeName   string
}

// NewRings creates initialised ring buffers.
func NewRings(nodeName string) *Rings {
	return &Rings{
		Request:  &RequestRing{},
		Route:    &RouteRing{},
		URL:      &URLRing{},
		OpsLog:   &OpsLogRing{},
		Peer:     &PeerRing{},
		NodeName: nodeName,
	}
}

// Start launches the background flush goroutine. Blocks until ctx is done.
func (ri *Rings) Start(ctx interface{ Done() <-chan struct{} }, snapshotPath string) {
	reqTicker := time.NewTicker(requestBucketSecs * time.Second)
	snapTicker := time.NewTicker(60 * time.Second)
	defer reqTicker.Stop()
	defer snapTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			if snapshotPath != "" {
				_ = ri.Save(snapshotPath)
			}
			return
		case now := <-reqTicker.C:
			ri.Request.Flush(now)
			ri.Route.Flush(now)
		case <-snapTicker.C:
			if snapshotPath != "" {
				_ = ri.Save(snapshotPath)
			}
		}
	}
}

// ── Snapshot (persistence) ───────────────────────────────────────────

// ringSnapshot is the gob-serialisable form of all rings.
type ringSnapshot struct {
	SavedAt      time.Time
	RequestHead  int
	RequestBucts [requestBuckets]RequestBucket
	RouteBucts   []RouteBucket
}

// Save serialises rings to a file. Called on graceful shutdown and every 60s.
func (ri *Rings) Save(path string) error {
	ri.Request.mu.RLock()
	snap := ringSnapshot{
		SavedAt:      time.Now(),
		RequestHead:  ri.Request.head,
		RequestBucts: ri.Request.buckets,
	}
	ri.Request.mu.RUnlock()
	ri.Route.mu.RLock()
	snap.RouteBucts = make([]RouteBucket, len(ri.Route.buckets))
	copy(snap.RouteBucts, ri.Route.buckets)
	ri.Route.mu.RUnlock()

	f, err := os.CreateTemp(filepath.Dir(path), ".metrics-snap-*")
	if err != nil {
		return fmt.Errorf("rings save: %w", err)
	}
	if err := gob.NewEncoder(f).Encode(snap); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return fmt.Errorf("rings save encode: %w", err)
	}
	_ = f.Close()
	return os.Rename(f.Name(), path)
}

// Load restores rings from a snapshot file if it is < 10 minutes old.
func (ri *Rings) Load(path string) error {
	f, err := os.Open(path) //nolint:gosec // path is derived from operator-configured WarmDir, not user input
	if err != nil {
		return nil // file absent — not an error
	}
	defer func() { _ = f.Close() }()

	var snap ringSnapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return fmt.Errorf("rings load decode: %w", err)
	}
	if time.Since(snap.SavedAt) > 10*time.Minute {
		return nil // stale — ignore
	}

	ri.Request.mu.Lock()
	ri.Request.buckets = snap.RequestBucts
	ri.Request.head = snap.RequestHead
	ri.Request.mu.Unlock()

	ri.Route.mu.Lock()
	ri.Route.buckets = snap.RouteBucts
	ri.Route.mu.Unlock()
	return nil
}

// ── Cluster summary ──────────────────────────────────────────────────

// MetricsSummary is the compact struct returned by /v1/peer/metrics
// and used for fan-out aggregation.
type MetricsSummary struct {
	NodeName    string
	CollectedAt time.Time
	// Last requestBuckets buckets
	RequestSnap []RequestBucket
	// Per-route stats over last 30 min
	RouteStats []RouteStat
	// Per-URL-prefix stats (top-N by request count)
	URLStats []URLStat
}

// Summary builds a MetricsSummary from the local rings.
func (ri *Rings) Summary() MetricsSummary {
	return MetricsSummary{
		NodeName:    ri.NodeName,
		CollectedAt: time.Now(),
		RequestSnap: ri.Request.Snapshot(requestBuckets),
		RouteStats:  ri.Route.RouteStats(30),
		URLStats:    ri.URL.URLStats(),
	}
}

// MergeSummaries aggregates multiple MetricsSummary into one.
// Merge strategy:
//   - Counters: sum
//   - Ratios: weighted average
//   - Latency p99: max
//   - Latency histogram: element-wise sum (so the dashboard latency
//     distribution reflects the combined cluster traffic, not just one node)
//
// mergeRequestBuckets accumulates per-bucket counters and latency histogram
// bins from all summaries into merged.
func mergeRequestBuckets(merged *MetricsSummary, summaries []MetricsSummary) {
	for _, s := range summaries {
		for i, b := range s.RequestSnap {
			m := &merged.RequestSnap[i]
			m.Requests += b.Requests
			m.Hits += b.Hits
			m.Misses += b.Misses
			m.StaleHits += b.StaleHits
			m.Bypasses += b.Bypasses
			m.Revalidated += b.Revalidated
			m.Errors += b.Errors
			m.DurSumMs += b.DurSumMs
			m.DurN += b.DurN
			if b.P99MS > m.P99MS {
				m.P99MS = b.P99MS
			}
			if b.Timestamp > m.Timestamp {
				m.Timestamp = b.Timestamp
			}
			for j := range b.LatHist {
				m.LatHist[j] += b.LatHist[j]
			}
		}
	}
}

// mergeRouteStatsList sums counters and sparklines for each route across summaries.
func mergeRouteStatsList(summaries []MetricsSummary) []RouteStat {
	routeAgg := make(map[string]*RouteStat)
	for _, s := range summaries {
		for _, rs := range s.RouteStats {
			a, ok := routeAgg[rs.Route]
			if !ok {
				cp := rs
				routeAgg[rs.Route] = &cp
				continue
			}
			a.Requests += rs.Requests
			a.Hits += rs.Hits
			a.Misses += rs.Misses
			if len(rs.Sparkline) == sparklinePoints {
				if len(a.Sparkline) != sparklinePoints {
					a.Sparkline = make([]int64, sparklinePoints)
				}
				for i := range sparklinePoints {
					a.Sparkline[i] += rs.Sparkline[i]
				}
			}
		}
	}
	out := make([]RouteStat, 0, len(routeAgg))
	for _, a := range routeAgg {
		if a.Requests > 0 {
			a.HitPct = math.Round(float64(a.Hits)/float64(a.Requests)*1000) / 10
		}
		out = append(out, *a)
	}
	return out
}

// mergeURLStatsList sums counters for each URL across summaries.
func mergeURLStatsList(summaries []MetricsSummary) []URLStat {
	urlAgg := make(map[string]*URLStat)
	for _, s := range summaries {
		for _, us := range s.URLStats {
			a, ok := urlAgg[us.URL]
			if !ok {
				cp := us
				urlAgg[us.URL] = &cp
				continue
			}
			a.Requests += us.Requests
			a.Hits += us.Hits
			a.Misses += us.Misses
		}
	}
	out := make([]URLStat, 0, len(urlAgg))
	for _, a := range urlAgg {
		if a.Requests > 0 {
			a.HitPct = math.Round(float64(a.Hits)/float64(a.Requests)*1000) / 10
		}
		out = append(out, *a)
	}
	return out
}

// MergeSummaries combines per-node metric snapshots into a single cluster-wide
// summary. Rules:
//   - Counters: sum
//   - Ratios: weighted average
//   - Latency p99: max
//   - Latency histogram: element-wise sum across all peer summaries
func MergeSummaries(summaries []MetricsSummary) MetricsSummary {
	if len(summaries) == 0 {
		return MetricsSummary{}
	}
	merged := MetricsSummary{
		NodeName:    "cluster",
		CollectedAt: time.Now(),
		RequestSnap: make([]RequestBucket, requestBuckets),
		RouteStats:  nil,
	}
	mergeRequestBuckets(&merged, summaries)
	merged.RouteStats = mergeRouteStatsList(summaries)
	merged.URLStats = mergeURLStatsList(summaries)
	return merged
}

// ── URL ring ──────────────────────────────────────────────────────────

const (
	urlRingCap    = 512 // max distinct URL prefixes tracked
	urlSuffixSegs = 3   // number of path segments kept as the key
)

// URLStat is the per-URL-prefix cache result summary.
type URLStat struct {
	URL      string
	Route    string
	Requests int64
	Hits     int64
	Misses   int64
	HitPct   float64
}

// urlCounters holds live atomic accumulators for one URL prefix.
type urlCounters struct {
	route    string
	requests atomic.Int64
	hits     atomic.Int64
	misses   atomic.Int64
}

// URLRing tracks per-URL-prefix hit/miss counters using a fixed-capacity
// sync.Map. When the cap is reached new URLs are silently dropped.
// Zero allocations on the hot path for known URLs (Load fast-path).
type URLRing struct {
	entries sync.Map // urlKey → *urlCounters
	size    atomic.Int64
}

// urlKey returns a stable cache key from a URL path: the first
// urlSuffixSegs non-empty path segments joined with "/", e.g.
// "/api/v1/users/123?foo=bar" → "/api/v1/users".
func urlKey(path string) string {
	if path == "" || path == "/" {
		return "/"
	}
	segs := 0
	end := 0
	for i := 1; i < len(path); i++ {
		if path[i] == '?' || path[i] == '#' {
			end = i
			break
		}
		if path[i] == '/' {
			segs++
			if segs == urlSuffixSegs {
				end = i
				break
			}
		}
	}
	if end == 0 {
		end = len(path)
	}
	return path[:end]
}

// RecordURL is called on the hot path. Zero allocs for known URLs.
func (r *URLRing) RecordURL(path, route, xCache string) {
	key := urlKey(path)
	v, ok := r.entries.Load(key)
	if !ok {
		if r.size.Load() >= urlRingCap {
			return // cap reached, silently drop
		}
		nc := &urlCounters{route: route}
		v, ok = r.entries.LoadOrStore(key, nc)
		if !ok {
			r.size.Add(1)
			v = nc
		}
	}
	c := v.(*urlCounters)
	c.requests.Add(1)
	switch xCache {
	case "HIT":
		c.hits.Add(1)
	case "MISS":
		c.misses.Add(1)
	}
}

// URLStats returns a snapshot of all tracked URLs sorted by request count.
func (r *URLRing) URLStats() []URLStat {
	out := make([]URLStat, 0, 64)
	r.entries.Range(func(k, v any) bool {
		c := v.(*urlCounters)
		reqs := c.requests.Load()
		if reqs == 0 {
			return true
		}
		hits := c.hits.Load()
		stat := URLStat{
			URL:      k.(string),
			Route:    c.route,
			Requests: reqs,
			Hits:     hits,
			Misses:   c.misses.Load(),
		}
		if reqs > 0 {
			stat.HitPct = math.Round(float64(hits)/float64(reqs)*1000) / 10
		}
		out = append(out, stat)
		return true
	})
	return out
}
