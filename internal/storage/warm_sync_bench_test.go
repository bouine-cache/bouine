package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// BenchmarkWarmSync_Overhead measures the wall-clock cost of a single
// warm sync cycle to quantify the background I/O overhead.
//
// Usage: go test -bench=BenchmarkWarmSync_Overhead -count=5 ./internal/storage/
func BenchmarkWarmSync_Overhead(b *testing.B) {
	dir := b.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      5000,
		TombstoneDrainInterval: -1,
	})
	if err != nil {
		b.Fatalf("NewTieredStore: %v", err)
	}
	defer func() { _ = ts.Close(context.Background()) }()

	// Fill with 1000 small objects (hot-only).
	for i := range 1000 {
		k := testkey.Key(uint64(i))
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset warm tier to force a full sync each iteration.
		for _, k := range ts.warm.Keys() {
			_, _ = ts.warm.Delete(k)
		}
		ts.runWarmSyncCycle(context.Background())
	}
}

// BenchmarkWarmSync_StaleEntryCleanup measures how quickly stale warm
// entries (from Put-replace) are cleaned up by the sync loop.
//
// Usage: go test -bench=BenchmarkWarmSync_StaleEntryCleanup -count=5 ./internal/storage/
func BenchmarkWarmSync_StaleEntryCleanup(b *testing.B) {
	dir := b.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      5000,
		TombstoneDrainInterval: -1,
	})
	if err != nil {
		b.Fatalf("NewTieredStore: %v", err)
	}
	defer func() { _ = ts.Close(context.Background()) }()

	// Put 500 large objects (warm-backed), then replace all with small
	// objects (hot-only). Without notifyEvict, the stale warm copies
	// would linger forever.
	for i := range 500 {
		k := testkey.Key(uint64(i))
		_ = ts.Put(context.Background(), k, bigObj(k, 2000))
	}
	for i := range 500 {
		k := testkey.Key(uint64(i))
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	warmBefore := len(ts.warm.Keys())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear tombstones from previous iteration.
		ts.runWarmSyncCycle(context.Background())
	}

	b.StopTimer()
	warmAfter := len(ts.warm.Keys())
	b.ReportMetric(float64(warmBefore), "stale_before")
	b.ReportMetric(float64(warmAfter), "stale_after")
}
