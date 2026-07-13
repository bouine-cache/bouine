package sieve

import (
	"testing"
	"time"
)

// BenchmarkSIEVE_Evict_AllVisited_1M measures a single EvictBounded(128)
// call on a 1 M entry SIEVE list where every entry has visited=true. This
// is the O(N) worst case that the sweep cap fixes: the unbounded Evict()
// sweeps the entire list (~2.25 ms), while EvictBounded(128) caps at 128
// probes and returns false, deferring eviction to the next call.
//
// The list is built once (all-visited). Only a single EvictBounded is
// measured per run — use -benchtime=1x -count=10 to get 10 samples.
//
// Usage: go test -bench=BenchmarkSIEVE_Evict_AllVisited_1M -benchtime=1x -count=10 -benchmem ./internal/storage/sieve/
func BenchmarkSIEVE_Evict_AllVisited_1M(b *testing.B) {
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

	for b.Loop() {
		start := time.Now()
		_, ok := l.EvictBounded(128)
		elapsed := time.Since(start)
		if ok {
			b.Fatal("EvictBounded(128) on 1M all-visited should return false")
		}
		b.ReportMetric(float64(elapsed.Nanoseconds()), "evict-ns")
	}
}
