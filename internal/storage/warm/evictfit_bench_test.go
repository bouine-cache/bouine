package warm

import (
	"testing"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// BenchmarkWarmEvictToFit_MultiEvict measures evictToFit when a single Put
// requires 10 consecutive evictions. The plan predicts that each evictOne
// call acquires seg.mu.Lock + idxMu.Lock independently, so 10 evictions
// means 10 lock/unlock cycles with a pwritev syscall under each lock.
//
// Setup: 10 K warm entries at budget. Insert a record large enough to
// require 10 evictions. Measure the total evictToFit cost.
//
// Usage: go test -bench=BenchmarkWarmEvictToFit_MultiEvict -benchtime=1x -count=10 -benchmem ./internal/storage/warm/
func BenchmarkWarmEvictToFit_MultiEvict(b *testing.B) {
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
	s, err := NewStore(Config{Dir: dir, MaxBytes: seedBudget, SegMax: 1 << 20})
	if err != nil {
		b.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, seedBody)
	for i := range seedEntries {
		if _, _, err := s.Put(testkey.From(uint64(i)), body); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Large record requiring ~10 evictions (1200 bytes / 120 bytes per entry).
	largeRecSize := int64(10 * (HeaderLen + seedBody + FooterLen))

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Re-seed: put back entries evicted in the previous iteration.
		b.StopTimer()
		for i := range seedEntries {
			s.idxMu.RLock()
			_, exists := s.index[testkey.From(uint64(i))]
			s.idxMu.RUnlock()
			if !exists {
				_, _, _ = s.Put(testkey.From(uint64(i)), body)
			}
		}
		b.StartTimer()

		s.mu.RLock()
		_ = s.evictToFitBatchLocked(largeRecSize)
		s.mu.RUnlock()
	}
}
