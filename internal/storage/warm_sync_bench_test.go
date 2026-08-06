package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
)

// BenchmarkWarmSync_CacheSurvivalRate measures how many cache entries
// survive a restart with and without the warm sync loop. The "before"
// scenario (WarmSyncInterval=0) only persists objects above
// body_threshold to warm. The "after" scenario (WarmSyncInterval>0 +
// runWarmSyncCycle) syncs all hot-only entries to warm before restart.
//
// Usage: go test -bench=BenchmarkWarmSync_CacheSurvival -count=5 ./internal/storage/
func BenchmarkWarmSync_CacheSurvivalRate(b *testing.B) {
	smallObjCount := 1000
	largeObjCount := 100
	bodyThreshold := 1024

	cases := []struct {
		name       string
		warmSyncOn bool
	}{
		{"before_warmSyncOff", false},
		{"after_warmSyncOn", true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			warmDir := filepath.Join(dir, "warm")
			walPath := filepath.Join(dir, "index.wal")

			ts1, err := NewTieredStore(TieredConfig{
				Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
				Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
				WALDir:                 walPath,
				BodyThreshold:          int64(bodyThreshold),
				WarmSyncInterval:       -1, // always 0 — we call runWarmSyncCycle manually
				WarmSyncBatchSize:      5000,
				TombstoneDrainInterval: -1,
			})
			if err != nil {
				b.Fatalf("NewTieredStore: %v", err)
			}

			// Fill with small objects (below threshold — hot-only).
			for i := range smallObjCount {
				k := api.KeyFromPrimary(uint64(i))
				_ = ts1.Put(context.Background(), k, obj(k, 100))
			}
			// Fill with large objects (above threshold — written to warm on Put).
			for i := range largeObjCount {
				k := api.KeyFromPrimary(uint64(smallObjCount + i))
				_ = ts1.Put(context.Background(), k, bigObj(k, 2000))
			}

			// Simulate the warm sync cycle for the "after" case.
			if tc.warmSyncOn {
				ts1.runWarmSyncCycle(context.Background())
			}

			// Close to flush everything to disk.
			_ = ts1.Close(context.Background())

			// Reopen and measure how many entries survived in warm.
			ts2, err := NewTieredStore(TieredConfig{
				Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
				Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
				WALDir:                 walPath,
				BodyThreshold:          int64(bodyThreshold),
				WarmSyncInterval:       -1,
				WarmSyncBatchSize:      5000,
				TombstoneDrainInterval: -1,
			})
			if err != nil {
				b.Fatalf("reopen: %v", err)
			}
			defer func() { _ = ts2.Close(context.Background()) }()

			warmKeys := ts2.warm.Keys()
			totalKeys := smallObjCount + largeObjCount
			survived := len(warmKeys)
			survivalRate := float64(survived) / float64(totalKeys) * 100

			smallSurvived := 0
			largeSurvived := 0
			for _, k := range warmKeys {
				if int(k) < smallObjCount {
					smallSurvived++
				} else {
					largeSurvived++
				}
			}

			b.ReportMetric(float64(survived), "entries_survived")
			b.ReportMetric(survivalRate, "survival_pct")
			b.ReportMetric(float64(smallSurvived), "small_survived")
			b.ReportMetric(float64(largeSurvived), "large_survived")
			b.ReportMetric(float64(totalKeys), "total_entries")
		})
	}
}

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
		k := api.KeyFromPrimary(uint64(i))
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
		k := api.KeyFromPrimary(uint64(i))
		_ = ts.Put(context.Background(), k, bigObj(k, 2000))
	}
	for i := range 500 {
		k := api.KeyFromPrimary(uint64(i))
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
