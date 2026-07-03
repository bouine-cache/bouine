package storage

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

// realisticObj builds an api.Object that mirrors production allocation patterns:
// ~4 KiB body, 10 response headers, ETag, Cache-Control, and surrogate keys.
// This is the shape that hot.Put allocates on every cache-fill (6 allocs/op,
// ~1.7 KiB + body per entry in the production profile).
func realisticObj(key api.Key, bodySize int) *api.Object { //nolint:unparam // test helper retains bodySize for callers that vary object size
	h := make(http.Header, 12)
	h[header.ContentType] = []string{"text/html; charset=utf-8"}
	h["Content-Encoding"] = []string{"gzip"}
	h["Cache-Control"] = []string{"public, max-age=3600, stale-while-revalidate=60"}
	h["ETag"] = []string{fmt.Sprintf(`"abc-%d"`, key)}
	h["Last-Modified"] = []string{"Wed, 01 Jan 2025 00:00:00 GMT"}
	h["X-Bouine-Host"] = []string{"api.backmarket.com"}
	h["X-Bouine-Path"] = []string{fmt.Sprintf("/v1/products/%d", key)}
	h["Vary"] = []string{"Accept-Encoding"}
	h["X-Content-Type-Options"] = []string{"nosniff"}
	h["X-Frame-Options"] = []string{"DENY"}
	h["Age"] = []string{"0"}
	h["Server"] = []string{"bouine"}

	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte(i % 256)
	}

	return &api.Object{
		Key:          key,
		StatusCode:   200,
		Header:       header.FromHTTP(h),
		Body:         body,
		BodySize:     int64(bodySize),
		StoredAt:     time.Now(),
		TTL:          10 * time.Minute,
		ETag:         fmt.Sprintf(`"abc-%d"`, key),
		CacheControl: "public, max-age=3600, stale-while-revalidate=60",
	}
}

// memStatsSnapshot captures a point-in-time view of the Go runtime memory state.
func memStatsSnapshot(label string) runtime.MemStats { //nolint:unparam // label retained for debugging context at call sites
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func logMemStats(t *testing.T, label string, m runtime.MemStats) {
	t.Helper()
	t.Logf("[%s] HeapAlloc=%.1f MiB  HeapInuse=%.1f MiB  HeapSys=%.1f MiB  "+
		"NumGC=%d  GCCPUFraction=%.4f  NextGC=%.1f MiB  Goroutines=%d  RSS(sys)=%.1f MiB",
		label,
		float64(m.HeapAlloc)/(1<<20),
		float64(m.HeapInuse)/(1<<20),
		float64(m.HeapSys)/(1<<20),
		m.NumGC,
		m.GCCPUFraction,
		float64(m.NextGC)/(1<<20),
		runtime.NumGoroutine(),
		float64(m.Sys)/(1<<20),
	)
}

// writeHeapProfile writes a runtime heap profile to a file in the test's
// output directory. The file can be analyzed with:
//
//	go tool pprof -http=:8080 <file>
func writeHeapProfile(t *testing.T, name string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create heap profile %s: %v", path, err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatalf("write heap profile %s: %v", path, err)
	}
	t.Logf("heap profile written: %s", path)
}

// writeCPUProfile starts a CPU profile that captures allocation behaviour.
// Returns a stop function that must be deferred.
func writeCPUProfile(t *testing.T, name string) func() {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create cpu profile %s: %v", path, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		t.Fatalf("start cpu profile %s: %v", path, err)
	}
	t.Logf("cpu profile started: %s", path)
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}

// ---------------------------------------------------------------------------
// Test 1: Warmup — simulates a pod restart filling the hot store from empty.
//
// This reproduces the startup heap spike observed in production: after a pod
// restart, bouine_hot_store_bytes grows linearly from 0 to multi-GB as the
// cache repopulates. Each Put allocates hotEntry + sieve.Entry + Object +
// Header.Clone + body []byte + VaryKey. Captures heap profiles at 25%, 50%,
// 75%, and 100% fill to show how the heap grows during warmup.
//
// Run:
//
//	go test -run TestHeapProfile_Warmup -v -timeout=300s ./internal/storage/
// ---------------------------------------------------------------------------

func TestHeapProfile_Warmup(t *testing.T) {
	if testing.Short() {
		t.Skip("heap profiling test skipped in short mode")
	}

	const (
		bodySize     = 4 << 10   // 4 KiB — median production response body
		maxBytes     = 512 << 20 // 512 MiB budget — large enough to observe growth
		numShards    = 16
		totalEntries = 100_000
	)
	checkpoints := []float64{0.25, 0.50, 0.75, 1.00}

	s := NewHotStore(HotConfig{
		MaxBytes:       maxBytes,
		NumShards:      numShards,
		ReaperInterval: -1, // disable background reaper for deterministic measurement
	})
	defer s.Close(context.Background())

	ctx := context.Background()

	runtime.GC()
	before := memStatsSnapshot("before warmup")
	logMemStats(t, "before warmup", before)

	var nextCheckpoint int
	for i := range totalEntries {
		k := api.Key(i)
		_ = s.Put(ctx, k, realisticObj(k, bodySize))

		pct := float64(i+1) / float64(totalEntries)
		if nextCheckpoint < len(checkpoints) && pct >= checkpoints[nextCheckpoint] {
			runtime.GC()
			ms := memStatsSnapshot(fmt.Sprintf("warmup %d%%", int(checkpoints[nextCheckpoint]*100)))
			logMemStats(t, fmt.Sprintf("warmup %d%%", int(checkpoints[nextCheckpoint]*100)), ms)

			stats := s.Stats()
			t.Logf("  hot store: entries=%d  bytes=%.1f MiB  evictions=%d  hits=%d  misses=%d",
				stats.HotEntries,
				float64(stats.HotBytes)/(1<<20),
				stats.Evictions,
				stats.Hits,
				stats.Misses,
			)

			writeHeapProfile(t, fmt.Sprintf("heap_warmup_%d.pb.gz", int(checkpoints[nextCheckpoint]*100)))
			nextCheckpoint++
		}
	}

	runtime.GC()
	after := memStatsSnapshot("after warmup")
	logMemStats(t, "after warmup", after)

	stats := s.Stats()
	t.Logf("final hot store: entries=%d  bytes=%.1f MiB  evictions=%d",
		stats.HotEntries,
		float64(stats.HotBytes)/(1<<20),
		stats.Evictions,
	)

	heapGrowth := float64(after.HeapAlloc-before.HeapAlloc) / (1 << 20)
	t.Logf("heap growth during warmup: %.1f MiB (%.1f MiB/entry avg)",
		heapGrowth,
		heapGrowth/float64(totalEntries)*1024, // KiB/entry
	)
}

// ---------------------------------------------------------------------------
// Test 2: Steady-state — simulates sustained mixed hit/miss traffic against
// a capped cache. Shows the sawtooth heap pattern (GC cycles) and confirms
// whether the cache stays bounded or leaks.
//
// In production we see:
//   - GC cycles/min increasing from ~1.25 early to ~8+ as heap grows
//   - Sawtooth amplitude growing from 2-3 GiB early to 5-8 GiB late
//   - p99 latency growing from 0.1s to 4-5s, correlated with GC frequency
//
// This test isolates the hot store to show the allocation/GC pattern without
// network or origin overhead.
//
// Run:
//
//	go test -run TestHeapProfile_SteadyState -v -timeout=300s ./internal/storage/
// ---------------------------------------------------------------------------

func TestHeapProfile_SteadyState(t *testing.T) {
	if testing.Short() {
		t.Skip("heap profiling test skipped in short mode")
	}

	const (
		bodySize        = 4 << 10   // 4 KiB
		maxBytes        = 256 << 20 // 256 MiB — tight budget to force eviction
		numShards       = 16
		workingSet      = 80_000  // keys in the working set
		iterations      = 500_000 // total operations
		checkpointEvery = 100_000 // capture profile every N ops
		hitRatio        = 0.80    // 80% reads, 20% writes
	)

	s := NewHotStore(HotConfig{
		MaxBytes:       maxBytes,
		NumShards:      numShards,
		ReaperInterval: -1,
	})
	defer s.Close(context.Background())

	ctx := context.Background()

	// Pre-fill with the working set.
	t.Log("pre-filling working set...")
	for i := range workingSet {
		k := api.Key(i)
		_ = s.Put(ctx, k, realisticObj(k, bodySize))
	}
	runtime.GC()
	logMemStats(t, "after prefill", memStatsSnapshot("after prefill"))

	stats := s.Stats()
	t.Logf("prefill: entries=%d  bytes=%.1f MiB  evictions=%d",
		stats.HotEntries, float64(stats.HotBytes)/(1<<20), stats.Evictions)

	stopCPU := writeCPUProfile(t, "cpu_steady_state.pb.gz")
	defer stopCPU()

	var gcBefore uint32
	runtime.ReadMemStats(&runtime.MemStats{}) // warm up
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	gcBefore = m.NumGC

	for i := range iterations {
		k := api.Key(i % (workingSet * 1.5)) // 1.5x overflow: some keys miss, some hit

		if float64(i%100)/100 < hitRatio {
			// Read
			_, _ = s.Get(ctx, k)
		} else {
			// Write
			_ = s.Put(ctx, k, realisticObj(k, bodySize))
		}

		if (i+1)%checkpointEvery == 0 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			gcCycles := ms.NumGC - gcBefore
			logMemStats(t, fmt.Sprintf("steady-state op %d/%d", i+1, iterations), ms)
			t.Logf("  GC cycles in last %d ops: %d  (%.1f/min equiv)",
				checkpointEvery, gcCycles,
				float64(gcCycles)/float64(checkpointEvery)*60*1e-6,
			)

			stats := s.Stats()
			t.Logf("  hot store: entries=%d  bytes=%.1f MiB  evictions=%d  hits=%d  misses=%d",
				stats.HotEntries,
				float64(stats.HotBytes)/(1<<20),
				stats.Evictions,
				stats.Hits,
				stats.Misses,
			)

			writeHeapProfile(t, fmt.Sprintf("heap_steady_%d.pb.gz", i+1))
			gcBefore = ms.NumGC
		}
	}

	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	logMemStats(t, "final", final)

	stats = s.Stats()
	t.Logf("final hot store: entries=%d  bytes=%.1f MiB  evictions=%d  hits=%d  misses=%d",
		stats.HotEntries,
		float64(stats.HotBytes)/(1<<20),
		stats.Evictions,
		stats.Hits,
		stats.Misses,
	)
}

// ---------------------------------------------------------------------------
// Test 3: Memory pressure — simulates the production degradation pattern
// where RAM fills to GOMEMLIMIT and GC cost spirals.
//
// In production:
//   - hot_store_bytes grows linearly to ~9.5 GiB without plateau
//   - GC cycles/min goes from 1.25 → 8+ as heap approaches 24 GiB
//   - p99 goes from 0.1s → 4-5s → 9.3s
//   - RSS stays 2-4 GiB above heap (Go doesn't return pages to OS)
//
// This test uses a small GOMEMLIMIT-like budget to trigger the same pattern
// quickly. It measures GC pause times at each fill level to show the
// O(live-set) scaling of GC cost.
//
// Run:
//
//	go test -run TestHeapProfile_MemoryPressure -v -timeout=300s ./internal/storage/
// ---------------------------------------------------------------------------

func TestHeapProfile_MemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("heap profiling test skipped in short mode")
	}

	const (
		bodySize   = 4 << 10 // 4 KiB
		maxBytes   = 1 << 30 // 1 GiB — simulates a large cache approaching GOMEMLIMIT
		numShards  = 16
		fillTarget = 900_000 // ~3.6 GiB of raw body data + overhead, will overshoot the 1 GiB budget
	)

	s := NewHotStore(HotConfig{
		MaxBytes:       maxBytes,
		NumShards:      numShards,
		ReaperInterval: -1,
	})
	defer s.Close(context.Background())

	ctx := context.Background()

	runtime.GC()
	logMemStats(t, "start", memStatsSnapshot("start"))

	// Fill the cache in phases, measuring GC cost at each level.
	// Each phase adds ~25% of fillTarget entries.
	phases := []struct {
		name    string
		fromIdx int
		toIdx   int
	}{
		{"phase1_25%", 0, fillTarget / 4},
		{"phase2_50%", fillTarget / 4, fillTarget / 2},
		{"phase3_75%", fillTarget / 2, fillTarget * 3 / 4},
		{"phase4_100%", fillTarget * 3 / 4, fillTarget},
	}

	var gcCountBefore uint32
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	gcCountBefore = m.NumGC
	gcPauseTotalBefore := m.PauseTotalNs

	for _, phase := range phases {
		phaseStart := time.Now()

		for i := phase.fromIdx; i < phase.toIdx; i++ {
			k := api.Key(i)
			_ = s.Put(ctx, k, realisticObj(k, bodySize))
		}

		runtime.GC()
		runtime.ReadMemStats(&m)

		gcCycles := m.NumGC - gcCountBefore
		gcPauseNs := m.PauseTotalNs - gcPauseTotalBefore
		phaseDur := time.Since(phaseStart)

		logMemStats(t, phase.name, m)
		t.Logf("  phase duration: %v", phaseDur)
		t.Logf("  GC cycles: %d  total GC pause: %.1f ms  avg pause: %.2f ms",
			gcCycles,
			float64(gcPauseNs)/1e6,
			float64(gcPauseNs)/float64(max(gcCycles, 1))/1e6,
		)

		stats := s.Stats()
		t.Logf("  hot store: entries=%d  bytes=%.1f MiB  evictions=%d",
			stats.HotEntries,
			float64(stats.HotBytes)/(1<<20),
			stats.Evictions,
		)
		t.Logf("  heap/cache ratio: %.2f (overhead beyond stored bytes)",
			float64(m.HeapAlloc)/float64(max(stats.HotBytes, 1)),
		)

		writeHeapProfile(t, fmt.Sprintf("heap_pressure_%s.pb.gz", phase.name))

		gcCountBefore = m.NumGC
		gcPauseTotalBefore = m.PauseTotalNs
	}

	// Now measure GC pause with a large live set by forcing multiple
	// consecutive GCs and timing each one.
	t.Log("--- measuring individual GC pause times with large live set ---")
	var pauses []time.Duration
	for range 10 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		runtime.GC()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		pauses = append(pauses, elapsed)
		t.Logf("  forced GC: pause=%v  heap before=%.1f MiB  heap after=%.1f MiB  freed=%.1f MiB",
			elapsed,
			float64(before.HeapAlloc)/(1<<20),
			float64(after.HeapAlloc)/(1<<20),
			float64(before.HeapAlloc-after.HeapAlloc)/(1<<20),
		)
	}

	var total time.Duration
	for _, p := range pauses {
		total += p
	}
	t.Logf("average forced GC pause: %v (over %d samples)", total/time.Duration(len(pauses)), len(pauses))
}

// ---------------------------------------------------------------------------
// Test 4: Eviction churn — simulates a cache at capacity with continuous
// inserts, triggering SIEVE eviction on every Put. Captures a CPU profile
// to show allocation hotspots during the eviction path.
//
// In production, when the cache is full, every new response triggers
// inline eviction (up to inlineEvictCap=4 per Put) plus background sweeper
// drains. This is the steady-state pattern when maxBytes is actually
// enforced (unlike the current production config where maxBytes appears
// to match GOMEMLIMIT, so eviction never triggers until the pod OOMs).
//
// Run:
//
//	go test -run TestHeapProfile_EvictionChurn -v -timeout=300s ./internal/storage/
// ---------------------------------------------------------------------------

func TestHeapProfile_EvictionChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("heap profiling test skipped in short mode")
	}

	const (
		bodySize        = 4 << 10  // 4 KiB
		maxBytes        = 64 << 20 // 64 MiB — small budget to force constant eviction
		numShards       = 16
		prefill         = 20_000  // fill to capacity first
		iterations      = 200_000 // then churn
		checkpointEvery = 50_000
	)

	s := NewHotStore(HotConfig{
		MaxBytes:       maxBytes,
		NumShards:      numShards,
		ReaperInterval: -1,
	})
	defer s.Close(context.Background())

	ctx := context.Background()

	// Pre-fill to capacity.
	t.Log("pre-filling to capacity...")
	for i := range prefill {
		k := api.Key(i)
		_ = s.Put(ctx, k, realisticObj(k, bodySize))
	}
	runtime.GC()
	logMemStats(t, "after prefill", memStatsSnapshot("after prefill"))
	stats := s.Stats()
	t.Logf("prefill: entries=%d  bytes=%.1f MiB  evictions=%d",
		stats.HotEntries, float64(stats.HotBytes)/(1<<20), stats.Evictions)

	stopCPU := writeCPUProfile(t, "cpu_eviction_churn.pb.gz")

	var gcBefore uint32
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	gcBefore = m.NumGC

	for i := range iterations {
		k := api.Key(i + prefill) // new keys, always miss + insert
		_ = s.Put(ctx, k, realisticObj(k, bodySize))

		// 50% reads from the working set to exercise the hit path under churn.
		readKey := api.Key(i % prefill)
		_, _ = s.Get(ctx, readKey)

		if (i+1)%checkpointEvery == 0 {
			runtime.ReadMemStats(&m)
			gcCycles := m.NumGC - gcBefore
			logMemStats(t, fmt.Sprintf("churn op %d/%d", i+1, iterations), m)
			t.Logf("  GC cycles in last %d ops: %d", checkpointEvery, gcCycles)

			stats := s.Stats()
			t.Logf("  hot store: entries=%d  bytes=%.1f MiB  evictions=%d  hits=%d  misses=%d",
				stats.HotEntries,
				float64(stats.HotBytes)/(1<<20),
				stats.Evictions,
				stats.Hits,
				stats.Misses,
			)

			writeHeapProfile(t, fmt.Sprintf("heap_churn_%d.pb.gz", i+1))
			gcBefore = m.NumGC
		}
	}

	stopCPU()
	runtime.GC()
	runtime.ReadMemStats(&m)
	logMemStats(t, "final", m)
	stats = s.Stats()
	t.Logf("final: entries=%d  bytes=%.1f MiB  evictions=%d  hits=%d  misses=%d",
		stats.HotEntries,
		float64(stats.HotBytes)/(1<<20),
		stats.Evictions,
		stats.Hits,
		stats.Misses,
	)
}

// ---------------------------------------------------------------------------
// Test 5: RSS vs heap — demonstrates the gap between Go's HeapAlloc and
// actual RSS (process_resident_memory_bytes). In production we see RSS
// consistently 2-4 GiB above heap, growing over time because the Go runtime
// mmaps heap memory but doesn't return pages to the OS after GC.
//
// This test fills the cache, forces GC, then measures the gap between
// HeapAlloc and HeapSys (total mmap'd heap) to show the RSS overhead.
//
// Run:
//
//	go test -run TestHeapProfile_RSSvsHeap -v -timeout=120s ./internal/storage/
// ---------------------------------------------------------------------------

func TestHeapProfile_RSSvsHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("heap profiling test skipped in short mode")
	}

	const (
		bodySize  = 4 << 10
		maxBytes  = 256 << 20
		numShards = 16
		entries   = 50_000
	)

	s := NewHotStore(HotConfig{
		MaxBytes:       maxBytes,
		NumShards:      numShards,
		ReaperInterval: -1,
	})
	defer s.Close(context.Background())

	ctx := context.Background()

	runtime.GC()
	logMemStats(t, "empty", memStatsSnapshot("empty"))

	// Fill the cache.
	for i := range entries {
		k := api.Key(i)
		_ = s.Put(ctx, k, realisticObj(k, bodySize))
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logMemStats(t, "full (no GC)", m)
	stats := s.Stats()
	t.Logf("hot store: entries=%d  bytes=%.1f MiB",
		stats.HotEntries, float64(stats.HotBytes)/(1<<20))
	t.Logf("heap/cache ratio: %.2f", float64(m.HeapAlloc)/float64(max(stats.HotBytes, 1)))

	// Force GC and measure what's reclaimable vs what's stuck in HeapSys.
	runtime.GC()
	runtime.ReadMemStats(&m)
	logMemStats(t, "after GC", m)

	t.Logf("--- RSS overhead analysis ---")
	t.Logf("HeapAlloc (live objects): %.1f MiB", float64(m.HeapAlloc)/(1<<20))
	t.Logf("HeapSys (mmap'd by runtime): %.1f MiB", float64(m.HeapSys)/(1<<20))
	t.Logf("HeapIdle (freed, not returned): %.1f MiB", float64(m.HeapIdle)/(1<<20))
	t.Logf("HeapReleased (returned to OS): %.1f MiB", float64(m.HeapReleased)/(1<<20))
	t.Logf("RSS overhead (HeapSys - HeapAlloc): %.1f MiB (%.1f%% of HeapAlloc)",
		float64(m.HeapSys-m.HeapAlloc)/(1<<20),
		float64(m.HeapSys-m.HeapAlloc)/float64(max(m.HeapAlloc, 1))*100,
	)

	writeHeapProfile(t, "heap_rss_analysis.pb.gz")
}
