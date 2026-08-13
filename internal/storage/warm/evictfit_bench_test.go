package warm

import (
	"testing"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// BenchmarkSingle_WarmEvictToFit_MultiEvict measures evictToFit when a single Put
// requires 10 consecutive evictions. The plan predicts that each evictOne
// call acquires seg.mu.Lock + idxMu.Lock independently, so 10 evictions
// means 10 lock/unlock cycles with a pwritev syscall under each lock.
//
// Setup: 10 K warm entries at budget. Insert a record large enough to
// require 10 evictions. Measure the total evictToFit cost.
//
// This is a single-shot benchmark: run with -benchtime=1x -count=10 for
// 10 benchstat samples. It skips itself under time-driven benchtime
// because the tombstones written by each iteration accumulate in the
// segment file and would eventually fill it, and after the first
// iteration the budget has room so subsequent iterations measure the
// fast-path no-op return, not eviction.
//
// Usage: go test -bench=BenchmarkSingle_WarmEvictToFit_MultiEvict -benchtime=1x -count=10 -benchmem ./internal/storage/warm/
func BenchmarkSingle_WarmEvictToFit_MultiEvict(b *testing.B) {
	if testing.Short() {
		b.Skip("multi-eviction benchmark, skipped in -short mode")
	}

	const (
		seedEntries = 10_000
		seedBody    = 100 // 120 bytes per record
	)

	dir := b.TempDir()

	// Budget exactly fits the seed entries: 10 K * 120 = 1.2 MB.
	seedBudget := int64(seedEntries) * (HeaderLen + seedBody + FooterLen)
	s, err := NewStore(Config{Dir: dir, MaxBytes: seedBudget})
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, seedBody)
	for i := range seedEntries {
		if _, _, err := s.Put(testkey.Key(uint64(i)), body); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Large record requiring ~10 evictions (1200 bytes / 120 bytes per entry).
	largeRecSize := int64(10 * (HeaderLen + seedBody + FooterLen))

	b.ReportAllocs()

	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		s.mu.RLock()
		_ = s.evictToFitBatchLocked(largeRecSize)
		s.mu.RUnlock()
	}
}
