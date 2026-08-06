package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
)

// BenchmarkWarmSyncCycle_1M measures a single runWarmSyncCycle call with
// 1 M hot-only entries (no warm entries). The plan predicts ~500 MB of
// transient allocation: hot.Keys() (~24 MB), warm.Keys() (~8 MB at 1 M),
// and the warmSet diff map (~40 MB at 1 M).
//
// The store is built once. Only a single runWarmSyncCycle is measured per
// run — use -benchtime=1x -count=10 for 10 benchstat samples.
//
// Usage: go test -bench=BenchmarkWarmSyncCycle_1M -benchtime=1x -count=10 -benchmem ./internal/storage/
func BenchmarkWarmSyncCycle_1M(b *testing.B) {
	if testing.Short() {
		b.Skip("1 M entry benchmark, skipped in -short mode")
	}

	const n = 1_000_000

	dir := b.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")

	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 30, NumShards: 64},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 10 << 30, SegMax: 64 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      5000,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	if err != nil {
		b.Fatalf("NewTieredStore: %v", err)
	}
	defer func() { _ = ts.Close(context.Background()) }()

	for i := range n {
		k := api.NewKeyFromUint64(uint64(i))
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		start := time.Now()
		ts.runWarmSyncCycle(context.Background())
		elapsed := time.Since(start)
		b.ReportMetric(float64(elapsed.Milliseconds()), "sync-ms")
	}
}

// BenchmarkBan_1M measures Ban() wall-clock time with 1 M hot entries
// across 64 shards and a non-matching regex. The plan predicts 1-3 s
// because Ban() locks each shard sequentially and iterates all entries.
//
// Ban with a non-matching regex does not modify state, so multiple
// iterations are safe. Use -benchtime=1x -count=10 for single-call
// samples, or default benchtime for amortized cost.
//
// Usage: go test -bench=BenchmarkBan_1M -benchtime=1x -count=10 -benchmem ./internal/storage/
func BenchmarkBan_1M(b *testing.B) {
	if testing.Short() {
		b.Skip("1 M entry benchmark, skipped in -short mode")
	}

	const n = 1_000_000

	s := NewHotStore(HotConfig{
		MaxBytes:       1 << 30,
		NumShards:      64,
		ReaperInterval: -1,
	})
	defer func() { _ = s.Close(context.Background()) }()

	for i := range n {
		k := api.NewKeyFromUint64(uint64(i))
		_ = s.Put(context.Background(), k, obj(k, 1024))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		start := time.Now()
		_, err := s.Ban(context.Background(), api.BanExpr{
			PathRegex: "^/never-matches-anything$",
			CreatedAt: time.Now(),
		})
		elapsed := time.Since(start)
		if err != nil {
			b.Fatalf("Ban: %v", err)
		}
		b.ReportMetric(float64(elapsed.Milliseconds()), "ban-ms")
	}
}
