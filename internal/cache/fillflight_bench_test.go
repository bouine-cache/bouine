package cache

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Cold-burst collapse benchmarks (ADR-0066). Run single-shot:
//
//	go test -bench 'BenchmarkSingle_CollapseColdBurst' -benchtime=1x -count=10 \
//	  -run '^$' ./internal/cache/
//
// BenchmarkSingle_CollapseColdBurst measures the dedup ratio and CPU cost
// of a simultaneous burst of identical anonymous cold misses on an
// include-free route (the ADR-0057 §3 refusal case): before fill flights
// every caller fetches its own copy; with fill flights one leader fills
// and every follower re-enters the pipeline and serves from the store.
//
// BenchmarkSingle_CollapseColdBurstLatency measures per-request latency
// percentiles under a simulated origin RTT with bounded fetch
// concurrency (the capacity constraint that collapsing lifts): before
// fill flights the burst serializes in fetch-semaphore waves; with fill
// flights one fetch serves the whole burst. The origin latency is a
// deliberate real sleep — it is the quantity being measured, not a
// flaky wait; single-shot mode runs it once per -benchtime=1x.

// benchmarkColdBurst runs one simultaneous burst of identical anonymous
// cold misses and returns the origin fetch count and per-request
// latencies.
func benchmarkColdBurst(b *testing.B, callers, fetchConcurrency int, originLatency time.Duration) (int64, []time.Duration) {
	b.Helper()

	var fetchCount atomic.Int64
	origin := func(ctx *fasthttp.RequestCtx) {
		fetchCount.Add(1)
		if originLatency > 0 {
			time.Sleep(originLatency)
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("burst")
	}
	h := NewHandler(HandlerConfig{
		Upstream:            origin,
		FastClient:          &testFastClient{handler: origin},
		Store:               storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		MaxFetchConcurrency: fetchConcurrency,
	})

	latencies := make([]time.Duration, callers)
	started := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/bench-burst")
			<-started // release the whole burst simultaneously
			t0 := time.Now()
			serveRequest(h, ctx)
			latencies[i] = time.Since(t0)
		}()
	}
	close(started)
	wg.Wait()
	return fetchCount.Load(), latencies
}

func burstPercentiles(lat []time.Duration) (p50, p90, p99 time.Duration) {
	sorted := append([]time.Duration(nil), lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(p float64) time.Duration {
		return sorted[int(p*float64(len(sorted)-1))]
	}
	return at(0.50), at(0.90), at(0.99)
}

func BenchmarkSingle_CollapseColdBurst(b *testing.B) {
	first := true
	b.ReportAllocs()
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false

		const callers = 32
		fetches, lat := benchmarkColdBurst(b, callers, 0, 0)
		b.ReportMetric(float64(fetches), "fetches/burst")
		p50, p90, p99 := burstPercentiles(lat)
		b.ReportMetric(float64(p50.Nanoseconds())/1000, "p50_µs")
		b.ReportMetric(float64(p90.Nanoseconds())/1000, "p90_µs")
		b.ReportMetric(float64(p99.Nanoseconds())/1000, "p99_µs")
	}
}

func BenchmarkSingle_CollapseColdBurstLatency(b *testing.B) {
	first := true
	b.ReportAllocs()
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false

		// 32 simultaneous cold misses, origin RTT 2 ms, only 4
		// concurrent origin fetches allowed — the burst must either
		// serialize in fetch-semaphore waves (before fill flights) or
		// collapse to one fetch (with fill flights).
		const (
			callers = 32
			slots   = 4
			rtt     = 2 * time.Millisecond
		)
		fetches, lat := benchmarkColdBurst(b, callers, slots, rtt)
		b.ReportMetric(float64(fetches), "fetches/burst")
		p50, p90, p99 := burstPercentiles(lat)
		b.ReportMetric(float64(p50.Nanoseconds())/1000, "p50_µs")
		b.ReportMetric(float64(p90.Nanoseconds())/1000, "p90_µs")
		b.ReportMetric(float64(p99.Nanoseconds())/1000, "p99_µs")
	}
}

// BenchmarkSingle_CollapseColdBurstCapacity sweeps the burst size under
// the same constrained origin (2 ms RTT, 4 fetch slots) — the
// capacity-vs-load curve: without collapsing the tail scales linearly
// with burst size (fetch-semaphore waves); with collapsing it stays at
// one origin latency.
func BenchmarkSingle_CollapseColdBurstCapacity(b *testing.B) {
	first := true
	b.ReportAllocs()
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false

		for _, callers := range []int{8, 32, 128} {
			b.Run(fmt.Sprintf("callers=%d", callers), func(b *testing.B) {
				fetches, lat := benchmarkColdBurst(b, callers, 4, 2*time.Millisecond)
				b.ReportMetric(float64(fetches), "fetches/burst")
				p50, p90, p99 := burstPercentiles(lat)
				b.ReportMetric(float64(p50.Nanoseconds())/1000, "p50_µs")
				b.ReportMetric(float64(p90.Nanoseconds())/1000, "p90_µs")
				b.ReportMetric(float64(p99.Nanoseconds())/1000, "p99_µs")
			})
		}
	}
}
