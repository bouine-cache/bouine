package storage

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func BenchmarkHotStore_Get_Hit(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	k := KeyHash([]byte("bench-hit"))
	_ = s.Put(context.Background(), k, obj(k, 1024))

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = s.Get(context.Background(), k)
	}
}

func BenchmarkHotStore_Get_Miss(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = s.Get(context.Background(), testkey.Key(0xDEADBEEF))
	}
}

func BenchmarkHotStore_Put(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	hdr := header.FromHTTP(http.Header{header.ContentType: {"text/plain"}})

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		k := testkey.Key(uint64(i))
		_ = s.Put(context.Background(), k, &api.Object{
			Key:        k,
			StatusCode: 200,
			Header:     hdr,
			Body:       make([]byte, 1024),
			BodySize:   1024,
			StoredAt:   time.Now(),
			TTL:        time.Minute,
		})
	}
}

func BenchmarkHotStore_Put_Eviction(b *testing.B) {
	// Tight budget forces eviction on every put after warmup.
	s := NewHotStore(HotConfig{MaxBytes: 8192, NumShards: 4})
	// Fill up.
	for i := range 100 {
		k := testkey.Key(uint64(i))
		_ = s.Put(context.Background(), k, obj(k, 512))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		k := testkey.Key(uint64(i + 10000))
		_ = s.Put(context.Background(), k, obj(k, 512))
	}
}

func BenchmarkSIEVE_Access(b *testing.B) {
	// Benchmark the SIEVE access path in isolation.
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 1})
	k := KeyHash([]byte("sieve-bench"))
	_ = s.Put(context.Background(), k, obj(k, 64))

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = s.Get(context.Background(), k)
	}
}

// BenchmarkHotGet_NoBans_Parallel measures the concurrent hit path with
// no active bans. After the Phase 1 atomic banCount fast path, the global
// bansMu is never taken, so ns/op should stay roughly flat as GOMAXPROCS
// scales. Run with: go test -bench BenchmarkHotGet_NoBans_Parallel -cpu 1,2,4,8
func BenchmarkHotGet_NoBans_Parallel(b *testing.B) {
	const keys = 1024
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	ks := make([]api.Key, keys)
	for i := range ks {
		ks[i] = testkey.Key(uint64(i + 1))
		_ = s.Put(context.Background(), ks[i], obj(ks[i], 1024))
	}
	// Warm the visited bits so every Get takes the RLock fast path.
	for _, k := range ks {
		_, _, _ = s.Get(context.Background(), k)
		_, _, _ = s.Get(context.Background(), k)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		var i uint64
		for pb.Next() {
			k := ks[i%keys]
			i++
			_, _, _ = s.Get(ctx, k)
		}
	})
}

// BenchmarkHotGet_WithBan_Parallel is the contended baseline: one active
// ban forces matchesActiveBan past the atomic fast path onto the global
// bansMu. Comparing this against the NoBans variant quantifies the lock's
// cost and confirms the fast path removes it when no bans exist.
func BenchmarkHotGet_WithBan_Parallel(b *testing.B) {
	const keys = 1024
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	ks := make([]api.Key, keys)
	for i := range ks {
		ks[i] = testkey.Key(uint64(i + 1))
		_ = s.Put(context.Background(), ks[i], obj(ks[i], 1024))
	}
	for _, k := range ks {
		_, _, _ = s.Get(context.Background(), k)
		_, _, _ = s.Get(context.Background(), k)
	}
	// Register a ban that matches nothing currently cached (objects were
	// stored before this ban's CreatedAt is in the future), so every Get
	// still pays the global-lock cost without evicting the working set.
	_, _ = s.Ban(context.Background(), api.BanExpr{
		PathRegex: "^/never-matches-anything$",
		CreatedAt: time.Now().Add(time.Hour),
	})

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		var i uint64
		for pb.Next() {
			k := ks[i%keys]
			i++
			_, _, _ = s.Get(ctx, k)
		}
	})
}

// BenchmarkHotPut_Overflow drives a working set 1.5x larger than the
// budget so every Put after warmup triggers SIEVE eviction. Tracks the
// cost (and allocs) of the eviction-on-Put critical path targeted by
// Phase 2.
func BenchmarkHotPut_Overflow(b *testing.B) {
	const (
		bodySize    = 1024
		budgetBytes = 4 << 20 // 4 MiB
	)
	s := NewHotStore(HotConfig{MaxBytes: budgetBytes, NumShards: 16})
	// Pre-fill to the budget so we start in steady-state eviction.
	prefill := (budgetBytes / (bodySize + 256))
	for i := range prefill {
		k := testkey.Key(uint64(i))
		_ = s.Put(context.Background(), k, obj(k, bodySize))
	}

	b.ResetTimer()
	b.ReportAllocs()
	ctx := context.Background()
	for i := range b.N {
		// 1.5x working set: keep churning fresh keys past the budget.
		k := testkey.Key(uint64(i + prefill))
		_ = s.Put(ctx, k, obj(k, bodySize))
	}
}

// BenchmarkHotMixed_80_20 runs an 80% Get / 20% Put workload at ~1.4x
// overflow and reports a p99 latency metric per op. This is the closest
// micro-equivalent of the innerspace memory-pressure load test and is the
// primary gate for the HIT p99 target.
func BenchmarkHotMixed_80_20(b *testing.B) {
	const (
		bodySize    = 1024
		budgetBytes = 4 << 20
		working     = 6000 // ~1.4x the ~4100 objects the budget holds
	)
	s := NewHotStore(HotConfig{MaxBytes: budgetBytes, NumShards: 16})
	for i := range working {
		k := testkey.Key(uint64(i))
		_ = s.Put(context.Background(), k, obj(k, bodySize))
	}

	var (
		getLatencies = make([]time.Duration, 0, 1<<16)
		mu           atomicAppender
		ctr          atomic.Uint64
	)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		local := make([]time.Duration, 0, 4096)
		for pb.Next() {
			n := ctr.Add(1)
			k := testkey.Key(uint64(n % working))
			if n%5 == 0 {
				// 20% writes.
				_ = s.Put(ctx, k, obj(k, bodySize))
				continue
			}
			// 80% reads, timed for the p99 distribution.
			start := time.Now()
			_, _, _ = s.Get(ctx, k)
			local = append(local, time.Since(start))
		}
		mu.append(&getLatencies, local)
	})
	b.StopTimer()

	if len(getLatencies) > 0 {
		sort.Slice(getLatencies, func(i, j int) bool {
			return getLatencies[i] < getLatencies[j]
		})
		p := func(q float64) float64 {
			idx := int(float64(len(getLatencies)) * q)
			if idx >= len(getLatencies) {
				idx = len(getLatencies) - 1
			}
			return float64(getLatencies[idx].Nanoseconds())
		}
		b.ReportMetric(p(0.50), "get-p50-ns")
		b.ReportMetric(p(0.99), "get-p99-ns")
		b.ReportMetric(p(0.999), "get-p999-ns")
	}
}

// atomicAppender serialises the merge of per-goroutine latency slices.
type atomicAppender struct {
	mu sync.Mutex
}

func (a *atomicAppender) append(dst *[]time.Duration, src []time.Duration) {
	a.mu.Lock()
	*dst = append(*dst, src...)
	a.mu.Unlock()
}

// BenchmarkHotStore_Get_Parallel_64Shards measures the parallel hit path
// with 64 shards, one key per shard. This isolates shard-level scalability
// under SO_REUSEPORT-style multi-listener contention (each listener goroutine
// hammering a different shard). The high shard count ensures zero cross-shard
// lock contention, so ns/op should scale linearly with GOMAXPROCS.
func BenchmarkHotStore_Get_Parallel_64Shards(b *testing.B) {
	const shards = 64
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: shards})
	ks := make([]api.Key, shards)
	for i := range ks {
		ks[i] = testkey.Key(uint64(i + 1))
		_ = s.Put(context.Background(), ks[i], obj(ks[i], 1024))
	}
	for _, k := range ks {
		_, _, _ = s.Get(context.Background(), k)
		_, _, _ = s.Get(context.Background(), k)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		var i uint64
		for pb.Next() {
			k := ks[i%shards]
			i++
			_, _, _ = s.Get(ctx, k)
		}
	})
}
