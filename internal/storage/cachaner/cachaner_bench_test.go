package cachaner

import (
	"testing"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
)

// BenchmarkGate_Cachaner_EvictBounded measures the cachaner eviction
// path in isolation on a pre-populated list. Each iteration evicts one
// entry (returned to the pool) and inserts one entry (drawn from the
// pool), so the list size stays constant and allocs/op is 0.
//
// The eviction sweep reads ioBits (freq) and may decrement it before
// evicting — the extra work vs plain SIEVE (which reads only the
// visited bit). This benchmark quantifies that delta.
func BenchmarkGate_Cachaner_EvictBounded(b *testing.B) {
	const n = 10_000
	l := NewList[uint64]()
	nilLookup := func(uint64) *evictor.Entry[uint64] { return nil }
	for i := range n {
		l.Access(uint64(i), nilLookup)
	}

	var key uint64 = n
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		l.EvictBounded(128)
		l.Access(key, nilLookup)
		key++
	}
}

// BenchmarkGate_Cachaner_AccessSlowPath measures the slow-path Access
// (freq increment + MarkVisited) on an existing entry. This is the path
// taken when the visited bit is false (first access after a sweep).
// Alloc budget: 0.
func BenchmarkGate_Cachaner_AccessSlowPath(b *testing.B) {
	l := NewList[uint64]()
	e, _ := l.Access(1, func(uint64) *evictor.Entry[uint64] { return nil })
	existingLookup := func(uint64) *evictor.Entry[uint64] { return e }

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		// Each Access increments freq (until saturation at 7, then
		// no-op) and sets visited. We ClearVisited each iteration to
		// keep the slow path active.
		l.Access(1, existingLookup)
		e.ClearVisited()
	}
}
