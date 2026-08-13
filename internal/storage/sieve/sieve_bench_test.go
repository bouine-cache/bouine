package sieve

import (
	"testing"
	"time"
)

// BenchmarkSingle_SIEVE_Evict_AllVisited_1M measures a single EvictBounded(128)
// call on a 1 M entry SIEVE list where every entry has visited=true. This
// is the O(N) worst case that the sweep cap fixes: the unbounded Evict()
// sweeps the entire list (~2.25 ms), while EvictBounded(128) caps at 128
// probes and returns false, deferring eviction to the next call.
//
// The list is built once (all-visited). This benchmark is designed for
// -benchtime=1x -count=N: each run performs a single EvictBounded and
// reports the evict-ns custom metric. It skips itself under time-driven
// benchtime because the list state evolves across iterations (visited
// bits are cleared, entries are eventually evicted), so the evict-ns
// metric would reflect a mix of states rather than the pure all-visited
// worst case.
//
// Usage: go test -bench=BenchmarkSingle_SIEVE_Evict_AllVisited_1M -benchtime=1x -count=10 -benchmem ./internal/storage/sieve/
func BenchmarkSingle_SIEVE_Evict_AllVisited_1M(b *testing.B) {
	const n = 1_000_000
	l := NewList[uint64]()
	m := make(map[uint64]*Entry[uint64], n)

	for i := range n {
		e, _ := l.Access(uint64(i), func(uint64) *Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range n {
		l.Access(uint64(i), func(uint64) *Entry[uint64] { return m[uint64(i)] })
	}

	b.ResetTimer()
	b.ReportAllocs()

	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		start := time.Now()
		l.EvictBounded(128)
		b.ReportMetric(float64(time.Since(start).Nanoseconds()), "evict-ns")
	}
}
