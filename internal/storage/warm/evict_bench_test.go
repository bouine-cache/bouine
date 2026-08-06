package warm

import (
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// BenchmarkWarmEvict_AllVisited_1M measures a single evictOne() call on a
// warm store with 1 M entries where all entries have been accessed via Get
// (visited=true). With the sweep cap (maxSweepProbes=256), the SIEVE sweep
// is bounded at 256 probes and returns false instead of scanning 2M entries.
//
// The store is built once (all-visited). Only a single evictOne() is
// measured per run — use -benchtime=1x -count=10 for 10 benchstat samples.
//
// Usage: go test -bench=BenchmarkWarmEvict_AllVisited_1M -benchtime=1x -count=10 -benchmem ./internal/storage/warm/
func BenchmarkWarmEvict_AllVisited_1M(b *testing.B) {
	if testing.Short() {
		b.Skip("1 M entry benchmark, skipped in -short mode")
	}

	const n = 1_000_000
	dir := b.TempDir()

	// MaxBytes large enough to hold all entries without eviction during setup.
	// Each record: HeaderLen(16) + body(100) + FooterLen(4) = 120 bytes.
	s, err := NewStore(Config{Dir: dir, MaxBytes: int64(n) * 120, SegMax: 64 << 20})
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 100)
	for i := range n {
		if _, _, err := s.Put(testkey.From(uint64(i)), body); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Access all entries via Get to set visited=true on every SIEVE entry.
	for i := range n {
		if _, err := s.Get(testkey.From(uint64(i))); err != nil {
			b.Fatalf("Get(%d): %v", i, err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		start := time.Now()
		_, ok := s.evictOne()
		elapsed := time.Since(start)
		// With the sweep cap, evictOne returns false when all entries
		// are visited and the cap is hit. This is the expected behavior
		// — the caller (evictToFit) will return ErrOverBudget.
		_ = ok
		b.ReportMetric(float64(elapsed.Nanoseconds()), "evict-ns")
	}
}
