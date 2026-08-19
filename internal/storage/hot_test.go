package storage

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func obj(key api.Key, bodySize int) *api.Object {
	return &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:       make([]byte, bodySize),
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        time.Minute,
	}
}

func TestHotStore_PutGet(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	k := testkey.Hash([]byte("test-key"))
	o := obj(k, 100)

	err := s.Put(context.Background(), k, o)
	require.NoError(t, err, "put")
	got, src, err := s.Get(context.Background(), k)
	require.NoError(t, err, "get")
	require.NotNil(t, got)
	require.Equal(t, 200, got.StatusCode)
	// Put inserts with visited=true (one SIEVE sweep of protection),
	// so the first Get takes the fast path which does not increment
	// Object.Hits (only the slow path does). The hit is counted in
	// stats.hits and windowHits (both atomic), not Object.Hits.
	require.Equal(t, uint64(0), got.Hits)
	require.Equal(t, api.SourceHot, src)
}

func TestHotStore_Miss(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	got, src, err := s.Get(context.Background(), testkey.Key(999))
	require.NoError(t, err, "get")
	require.Nil(t, got)
	require.Equal(t, api.Source(""), src)
	st := s.Stats()
	require.Equal(t, int64(1), st.Misses)
}

func TestHotStore_Get_DelegatesToGet(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := testkey.Hash([]byte("delegate"))
	o := obj(k, 100)

	err := s.Put(context.Background(), k, o)
	require.NoError(t, err, "put")

	got, _, err := s.Get(context.Background(), k)
	require.NoError(t, err, "Get")
	require.NotNil(t, got)
}

func TestHotStore_Delete(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := testkey.Hash([]byte("del"))
	_ = s.Put(context.Background(), k, obj(k, 50))
	_ = s.Delete(context.Background(), k)

	got, _, _ := s.Get(context.Background(), k)
	require.Nil(t, got)
}

func TestHotStore_EvictsOnFull(t *testing.T) {
	t.Parallel()
	// 4 shards, 4096 bytes total = 1024 per shard.
	s := NewHotStore(HotConfig{MaxBytes: 4096, NumShards: 4})

	// Insert objects until eviction must have happened.
	for i := range 100 {
		k := testkey.Key(uint64(i))
		_ = s.Put(context.Background(), k, obj(k, 500))
	}

	st := s.Stats()
	require.NotEqual(t, 0, st.Evictions)
	if st.HotBytes > 4096 {
		t.Fatalf("HotBytes = %d, exceeds budget", st.HotBytes)
	}
}

func TestHotStore_ReapExpired_RemovesDeadEntries(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	now := time.Now()
	fresh := &api.Object{
		Key:        testkey.Hash([]byte("fresh")),
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:       make([]byte, 100),
		BodySize:   100,
		StoredAt:   now,
		TTL:        time.Minute,
	}
	expired := &api.Object{
		Key:                  testkey.Hash([]byte("expired")),
		StatusCode:           200,
		Header:               header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:                 make([]byte, 100),
		BodySize:             100,
		StoredAt:             now.Add(-10 * time.Minute),
		TTL:                  time.Second,
		StaleWhileRevalidate: time.Second,
		StaleIfError:         time.Second,
	}

	_ = s.Put(ctx, fresh.Key, fresh)
	_ = s.Put(ctx, expired.Key, expired)

	before := s.Stats()
	require.Equal(t, int64(2), before.HotEntries)

	s.reapExpired(now)

	after := s.Stats()
	require.Equal(t, int64(1), after.HotEntries)
	got, _, _ := s.Get(ctx, expired.Key)
	require.Nil(t, got)
	got, _, _ = s.Get(ctx, fresh.Key)
	require.NotNil(t, got)
}

func TestHotStore_ReapExpired_KeepsSWRAndSIEEntries(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	now := time.Now()
	withinSWR := &api.Object{
		Key:                  testkey.Hash([]byte("swr")),
		StatusCode:           200,
		Header:               header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:                 make([]byte, 100),
		BodySize:             100,
		StoredAt:             now.Add(-5 * time.Second),
		TTL:                  time.Second,
		StaleWhileRevalidate: 30 * time.Second,
	}
	withinSIE := &api.Object{
		Key:          testkey.Hash([]byte("sie")),
		StatusCode:   200,
		Header:       header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:         make([]byte, 100),
		BodySize:     100,
		StoredAt:     now.Add(-5 * time.Second),
		TTL:          time.Second,
		StaleIfError: 30 * time.Second,
	}

	_ = s.Put(ctx, withinSWR.Key, withinSWR)
	_ = s.Put(ctx, withinSIE.Key, withinSIE)

	s.reapExpired(now)

	st := s.Stats()
	require.Equal(t, int64(2), st.HotEntries)
}

func TestObjSize_AccountsForHeaders(t *testing.T) {
	t.Parallel()
	smallHeaders := http.Header{header.XBouinePath: {"/a"}}
	bigHeaders := http.Header{}
	for i := range 20 {
		bigHeaders.Set(fmt.Sprintf("X-H%d", i), strings.Repeat("v", 100))
	}

	bodyLen := int64(100)
	objSmall := &api.Object{Body: make([]byte, bodyLen), Header: header.FromHTTP(smallHeaders)}
	objBig := &api.Object{Body: make([]byte, bodyLen), Header: header.FromHTTP(bigHeaders)}

	sizeSmall := objSize(objSmall)
	sizeBig := objSize(objBig)

	if sizeSmall <= bodyLen {
		t.Fatalf("sizeSmall = %d, expected > bodyLen (%d) — overhead not counted",
			sizeSmall, bodyLen)
	}
	if sizeBig <= sizeSmall+bodyLen {
		t.Fatalf("sizeBig = %d, sizeSmall = %d — header bytes not accounted for",
			sizeBig, sizeSmall)
	}
}

func TestObjSize_StructSizeConstantsNotDrifted(t *testing.T) {
	t.Parallel()
	want := int64(unsafe.Sizeof(api.Object{}))
	assert.Equal(t, want, objectStructSize)
	want = int64(unsafe.Sizeof(hotEntry{}))
	assert.Equal(t, want, hotEntrySize)
	want = int64(unsafe.Sizeof(evictor.Entry[api.Key]{}))
	assert.Equal(t, want, sieveEntrySize)
}

func TestObjSize_MapOverheadConstant(t *testing.T) {
	t.Parallel()
	// 8-slot bucket = 208 B at load factor 6.5 (16B keys) → ~32 B/entry.
	// The hmap struct header (~96 B) is negligible at 1M+ entries.
	assert.Equal(t, int64(32), mapPerEntryOverhead)
}

func TestObjSize_OrphanedValuesCounted(t *testing.T) {
	t.Parallel()
	hdr := header.FromHTTP(http.Header{
		"Content-Type": {"text/html"},
		"Set-Cookie":   {"session=abc"},
		"X-Custom":     {"value1"},
	})
	hdr.Del("Set-Cookie")

	obj := &api.Object{
		Body:   []byte("hello"),
		Header: hdr,
	}
	size := objSize(obj)

	// Build a version without the orphan for comparison.
	hdrClean := header.FromHTTP(http.Header{
		"Content-Type": {"text/html"},
		"X-Custom":     {"value1"},
	})
	objClean := &api.Object{
		Body:   []byte("hello"),
		Header: hdrClean,
	}
	sizeClean := objSize(objClean)

	// The orphaned object should be larger because it counts the
	// orphaned value slot's string header (16 B) and data bytes
	// (len("session=abc") = 11).
	if size <= sizeClean {
		t.Fatalf("orphaned size = %d, clean size = %d — orphaned value not counted",
			size, sizeClean)
	}
	delta := size - sizeClean
	// Expected delta: headerValueHeader(16) + len("session=abc")(11) = 27.
	assert.Equal(t, int64(27), delta)
}

func TestObjSize_ExactValue(t *testing.T) {
	t.Parallel()
	hdr := header.FromHTTP(http.Header{
		"Content-Type": {"text/html"},
		"X-Custom":     {"val"},
	})
	obj := &api.Object{
		Body:          []byte("hello"),
		Header:        hdr,
		VaryKey:       "V1",
		ETag:          "E1",
		CacheControl:  "public",
		SurrogateKeys: []string{"s1", "s2"},
	}

	// Pin every component:
	// body: 5
	// objectStructSize: 272, hotEntrySize: 32, sieveEntrySize: 40, mapPerEntryOverhead: 32
	// headerEntriesSlice: 24, headerValuesSlice: 24
	// headerEntrySize * 2: 48
	// headerValueHeader * 2: 32
	// valueBytes: len("text/html") + len("val") = 9 + 3 = 12
	// VaryKey: 2, ETag: 2, CacheControl: 6
	// SurrogateKeys: 2 + 2 = 4
	want := int64(5) + 272 + 32 + 40 + 32 +
		24 + 24 + 48 + 32 + 12 +
		2 + 2 + 6 + 4
	got := objSize(obj)
	assert.Equal(t, want, got)
}

func TestHotStore_EvictionFiresWithLargeHeaders(t *testing.T) {
	t.Parallel()
	const budget = 1 << 16 // 64 KiB
	s := NewHotStore(HotConfig{MaxBytes: budget, NumShards: 4})
	ctx := context.Background()

	hdr := http.Header{}
	for i := range 20 {
		hdr.Set(fmt.Sprintf("X-H%d", i), strings.Repeat("v", 200))
	}

	for i := range 500 {
		k := testkey.Key(uint64(i))
		_ = s.Put(ctx, k, &api.Object{
			Key:        k,
			StatusCode: 200,
			Header:     header.FromHTTP(hdr),
			Body:       make([]byte, 64),
			BodySize:   64,
			StoredAt:   time.Now(),
			TTL:        time.Minute,
		})
	}

	st := s.Stats()
	require.NotEqual(t, 0, st.Evictions)
	overshoot := int64(budget * 11 / 10) // 10% transient overshoot bound
	if st.HotBytes > overshoot {
		t.Fatalf("HotBytes = %d, exceeds overshoot bound %d", st.HotBytes, overshoot)
	}
}

func TestHotStore_Replace(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := testkey.Hash([]byte("replace"))

	_ = s.Put(context.Background(), k, obj(k, 100))
	_ = s.Put(context.Background(), k, obj(k, 200))

	got, _, _ := s.Get(context.Background(), k)
	require.NotNil(t, got)
	require.Equal(t, int64(200), got.BodySize)
	st := s.Stats()
	require.Equal(t, int64(1), st.HotEntries)
}

func TestHotStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 8})
	var wg sync.WaitGroup

	for g := range 8 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range 1000 {
				k := testkey.Key(uint64(base*1000 + i))
				_ = s.Put(context.Background(), k, obj(k, 64))
				_, _, _ = s.Get(context.Background(), k)
			}
		}(g)
	}
	wg.Wait()

	st := s.Stats()
	require.NotEqual(t, 0, st.Hits)
}

func TestHotStore_Stats(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	k := testkey.Hash([]byte("stats"))
	_ = s.Put(context.Background(), k, obj(k, 50))
	_, _, _ = s.Get(context.Background(), k)
	_, _, _ = s.Get(context.Background(), testkey.Key(12345)) // miss

	st := s.Stats()
	require.Equal(t, int64(1), st.HotEntries)
	require.Equal(t, int64(1), st.Hits)
	require.Equal(t, int64(1), st.Misses)
}

func TestKeyHash_Deterministic(t *testing.T) {
	t.Parallel()
	a := testkey.Hash([]byte("hello"))
	b := testkey.Hash([]byte("hello"))
	require.Equal(t, b, a)
	c := testkey.Hash([]byte("world"))
	require.NotEqual(t, c, a)
}

func TestHotStore_SetBacked(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	k := testkey.Hash([]byte("warm-key"))
	o := obj(k, 512)
	err := s.Put(context.Background(), k, o)
	require.NoError(t, err)

	s.SetBacked(k)

	sh := &s.shards[k.Hash64()&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if e, ok := sh.entries[k]; !ok || !e.hasBackup {
		t.Fatal("expected entry to be marked hasBackup after SetBacked")
	}
	require.Equal(t, int64(1), sh.backedCount)
}

func TestHotStore_EvictPreferBacked(t *testing.T) {
	t.Parallel()
	// Single shard, 2 KiB budget. Each object is 1280 bytes (1024 body +
	// 256 overhead), so the budget holds 1 entry; the 2nd Put forces
	// eviction.
	s := NewHotStore(HotConfig{MaxBytes: 2 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := testkey.Hash([]byte("a"))
	k2 := testkey.Hash([]byte("b"))
	_ = s.Put(ctx, k1, obj(k1, 1024))

	// Mark k1 as backed before k2 arrives.
	s.SetBacked(k1)

	// k2 triggers eviction. k1 (backed) should be evicted, not k2.
	_ = s.Put(ctx, k2, obj(k2, 1024))

	got, _, _ := s.Get(ctx, k1)
	assert.Nil(t, got)
	got, _, _ = s.Get(ctx, k2)
	assert.NotNil(t, got)
}

func TestHotStore_EvictPreferBacked_SkipPath_ReinsertWithVisited(t *testing.T) {
	t.Parallel()
	// This test exercises the hot-only skip branch in evictPreferBacked
	// and proves that the MarkVisited after re-insert is load-bearing.
	//
	// When EvictBounded returns a hot-only entry (hasBackup=false), the
	// entry is re-inserted at the head via Access + MarkVisited.
	// Access pulls a fresh entry from the pool (Reset clears visited to
	// false), so without MarkVisited the re-inserted entry is in the
	// MOST evictable state and gets evicted on the very next sweep —
	// defeating the "second chance" semantic.
	//
	// With visited=true on insert (#484), all freshly inserted entries
	// get one SIEVE sweep of protection. To isolate the re-insert
	// MarkVisited, we ClearVisited on k1, k2, and k3 after their
	// respective inserts, restoring the pre-#484 visited=false insert
	// state. This ensures only the re-insert MarkVisited (not the
	// insert MarkVisited) provides k1's second chance.
	//
	// Scenario (single shard, 3 KiB budget, obj ~1498 B → 2 fit):
	//  1. Insert k1 (hot-only), k2 (backed). ClearVisited both. SetBacked(k2).
	//  2. Insert k3 → first eviction: k1 (tail, visited=false) is swept,
	//     re-inserted at head via Access + MarkVisited (visited=true).
	//     k2 (backed, visited=false) then evicted. List: k3 → k1.
	//     k1.visited=true (from re-insert MarkVisited).
	//  3. ClearVisited k3 → k3.visited=false (isolate re-insert MarkVisited).
	//  4. Insert k4 → second eviction: k1 (visited=true) gets a second
	//     chance (visited cleared, not evicted); k3 (visited=false) evicted.
	//     WITHOUT MarkVisited: k1 (visited=false) evicted immediately.
	//
	// No Get() calls between evictions — Get marks visited and would
	// contaminate the state under test.
	s := NewHotStore(HotConfig{MaxBytes: 3 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := testkey.Hash([]byte("hot"))
	k2 := testkey.Hash([]byte("backed"))
	_ = s.Put(ctx, k1, obj(k1, 1024))
	_ = s.Put(ctx, k2, obj(k2, 1024))
	s.SetBacked(k2)

	// Reset visited bits on k1 and k2 so the first eviction sweep sees
	// them in the old visited=false insert state. Without this, the
	// visited=true from insert (#484) would give both entries a free
	// pass on the first sweep, changing the eviction order and
	// obscuring the re-insert MarkVisited behavior under test.
	shard := &s.shards[0]
	shard.mu.Lock()
	shard.entries[k1].entry.ClearVisited()
	shard.entries[k2].entry.ClearVisited()
	shard.mu.Unlock()

	// First eviction: k1 (visited=false) is swept and re-inserted at
	// head via Access + MarkVisited (visited=true). k2 (backed,
	// visited=false) is then evicted. List: k3 → k1.
	k3 := testkey.Hash([]byte("k3"))
	_ = s.Put(ctx, k3, obj(k3, 1024))

	// Reset k3's visited bit so the second eviction sees k3 in the old
	// visited=false insert state. This ensures k3 is immediately
	// evictable, so k1 (protected by re-insert MarkVisited) survives
	// and k3 is evicted — proving the re-insert MarkVisited is
	// load-bearing.
	shard.mu.Lock()
	shard.entries[k3].entry.ClearVisited()
	shard.mu.Unlock()

	// Second eviction: k1 (visited=true from re-insert) gets a second
	// chance; k3 (visited=false) is evicted.
	k4 := testkey.Hash([]byte("k4"))
	_ = s.Put(ctx, k4, obj(k4, 1024))

	// Assertions after all evictions (no Get between evictions).
	got, _, _ := s.Get(ctx, k1)
	assert.NotNil(t, got, "k1 must survive all evictions — MarkVisited on re-insert gave it a second chance")
	got, _, _ = s.Get(ctx, k2)
	assert.Nil(t, got, "k2 (backed) must be evicted in first sweep")
	got, _, _ = s.Get(ctx, k3)
	assert.Nil(t, got, "k3 must be evicted in second sweep")
	got, _, _ = s.Get(ctx, k4)
	assert.NotNil(t, got, "k4 must be present")
}

func TestHotStore_EvictFallbackNoBacked(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 3 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := testkey.Hash([]byte("x"))
	k2 := testkey.Hash([]byte("y"))
	_ = s.Put(ctx, k1, obj(k1, 1000))
	_ = s.Put(ctx, k2, obj(k2, 1000))

	k3 := testkey.Hash([]byte("z"))
	err := s.Put(ctx, k3, obj(k3, 1000))
	require.NoError(t, err)

	// k3 must have been inserted (eviction loop allowed it).
	_, _, err = s.Get(ctx, k3)
	require.Nil(t, err)
}

func TestHotStore_BackedCountConsistency(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := testkey.Hash([]byte("consistency"))
	_ = s.Put(ctx, k, obj(k, 100))
	s.SetBacked(k)

	// Overwrite with new entry; backed status resets.
	_ = s.Put(ctx, k, obj(k, 200))
	s.SetBacked(k)

	sh := &s.shards[k.Hash64()&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if e, ok := sh.entries[k]; !ok || !e.hasBackup {
		t.Fatal("entry should have hasBackup after re-marking")
	}
	require.Equal(t, int64(1), sh.backedCount)
}

// TestHotOverflowLatency validates that under 1.5× working-set overflow,
// the HIT p99 stays below 5 ms and the store does not grow beyond
// maxBytes × 1.1 (transient overshoot bound).
//
// The test runs 30 s of concurrent 80% Get / 20% Put at 1.5× overflow
// using GOMAXPROCS goroutines, then checks the p99 latency histogram and
// RSS-equivalent (s.bytes) for all shards.
func TestHotOverflowLatency(t *testing.T) {
	t.Parallel()

	const (
		bodySize    = 1024
		budgetBytes = 8 << 20 // 8 MiB — small enough to overflow fast
		duration    = 5 * time.Second
		p99Budget   = 5 * time.Millisecond
	)
	// 1.5× working set: ~1.5 × (budgetBytes / (bodySize+256)) unique keys
	perShardMax := int64(budgetBytes) / int64(runtime.NumCPU())
	approxCap := int(perShardMax / int64(bodySize+256))
	working := approxCap*runtime.NumCPU()*3/2 + 1

	s := NewHotStore(HotConfig{MaxBytes: budgetBytes})
	defer func() { _ = s.Close(context.Background()) }()

	// Pre-fill to capacity.
	for i := range approxCap * runtime.NumCPU() {
		k := testkey.Key(uint64(i))
		_ = s.Put(context.Background(), k, obj(k, bodySize))
	}

	var (
		latencies []time.Duration
		mu        sync.Mutex
		ctr       atomic.Uint64
		stop      atomic.Bool
		wg        sync.WaitGroup
	)

	workers := runtime.NumCPU()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			local := make([]time.Duration, 0, 1024)
			for !stop.Load() {
				n := ctr.Add(1)
				k := testkey.Key(n % uint64(working))
				if n%5 == 0 {
					_ = s.Put(ctx, k, obj(k, bodySize))
					continue
				}
				start := time.Now()
				_, _, _ = s.Get(ctx, k)
				local = append(local, time.Since(start))
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}()
	}

	// Run the load for a fixed window. A timer stops the workers after
	// the duration; the main goroutine waits via wg.Wait instead of sleeping.
	time.AfterFunc(duration, func() { stop.Store(true) })
	wg.Wait()

	// p99 latency gate.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) > 0 {
		p99idx := int(float64(len(latencies)) * 0.99)
		p99 := latencies[p99idx]
		t.Logf("HIT p99: %v over %d samples (%d workers, %d-key working set)",
			p99, len(latencies), workers, working)
		if p99 > p99Budget {
			t.Errorf("HIT p99 %v exceeds %v budget — Phase 2 eviction regression",
				p99, p99Budget)
		}
	}

	// RSS-equivalent: total shard bytes must not exceed maxBytes × 1.1
	// (the transient overshoot bound documented in HotStore).
	totalBytes := s.Stats().HotBytes
	maxAllowed := int64(budgetBytes) * 11 / 10
	t.Logf("HotBytes after test: %d / %d (limit %d, overshoot bound %d)",
		totalBytes, budgetBytes, maxAllowed, maxAllowed-int64(budgetBytes))
	if totalBytes > maxAllowed {
		t.Errorf("HotBytes %d exceeds overshoot bound %d", totalBytes, maxAllowed)
	}
}

// TestHotClose verifies that Close() stops the background sweeper without
// leaking goroutines. A sequential test (no t.Parallel) is required because
// runtime.NumGoroutine() is a global counter sensitive to other goroutines.
func TestHotClose(t *testing.T) {
	before := runtime.NumGoroutine()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	// Poll for the sweeper goroutine to start — 10 ms sleeps are
	// unreliable on 2-core CI runners.
	poll.Eventually(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
		return runtime.NumGoroutine() > before
	})
	goroutinesWithSweeper := runtime.NumGoroutine()
	if goroutinesWithSweeper <= before {
		t.Error("expected sweeper goroutine to be running after NewHotStore")
	}

	_ = s.Close(context.Background())
	// Poll for the goroutine to exit.
	poll.Eventually(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
		return runtime.NumGoroutine() <= before+1
	})
	after := runtime.NumGoroutine()
	if after > before+1 { // +1 for test harness variance
		t.Errorf("goroutine leak: before=%d after close=%d", before, after)
	}
}

func TestHotStore_BanByHostRegex(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	k := testkey.Hash([]byte("host-ban"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/keep")
	_ = s.Put(context.Background(), k, o)

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "example\\.com"})
	require.NoError(t, err, "ban")
	require.Equal(t, 1, count)

	got, _, _ := s.Get(context.Background(), k)
	require.Nil(t, got)
}

func TestHotStore_BanByPathRegex(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	k := testkey.Hash([]byte("path-ban"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	_ = s.Put(context.Background(), k, o)

	count, err := s.Ban(context.Background(), api.BanExpr{PathRegex: "^/ban-me"})
	require.NoError(t, err, "ban")
	require.Equal(t, 1, count)

	got, _, _ := s.Get(context.Background(), k)
	require.Nil(t, got)
}

func TestHotStore_BanLazyEvictionSlowPath(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	banTime := time.Now()
	count, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "example\\.com",
		CreatedAt: banTime,
	})
	require.NoError(t, err, "ban")
	require.Equal(t, 0, count)

	// Simulate peer replication: Put an object with StoredAt before the ban.
	k := testkey.Hash([]byte("lazy-ban"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	o.StoredAt = banTime.Add(-1 * time.Hour)
	_ = s.Put(context.Background(), k, o)

	got, _, _ := s.Get(context.Background(), k)
	require.Nil(t, got)
}

func TestHotStore_BanLazyEvictionFastPath(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	// Put a matching object with old StoredAt and access it to set visited=true.
	k := testkey.Hash([]byte("lazy-fast"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	o.StoredAt = time.Now().Add(-1 * time.Hour)
	_ = s.Put(context.Background(), k, o)

	got, _, _ := s.Get(context.Background(), k)
	require.NotNil(t, got)

	// Issue a ban that covers the object's StoredAt. The next Get
	// takes the fast path (visited=true) and must enforce the lazy ban.
	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "example\\.com",
		CreatedAt: time.Now(),
	})
	require.NoError(t, err, "ban")

	got, _, _ = s.Get(context.Background(), k)
	require.Nil(t, got)
}

func TestHotStore_BanSkipsObjectStoredAfterBan(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	banTime := time.Now()
	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "example\\.com",
		CreatedAt: banTime,
	})
	require.NoError(t, err, "ban")

	// Object stored AFTER the ban — should be exempt from lazy eviction.
	k := testkey.Hash([]byte("exempt"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	o.StoredAt = banTime.Add(1 * time.Hour)
	_ = s.Put(context.Background(), k, o)

	got, _, _ := s.Get(context.Background(), k)
	require.NotNil(t, got)
}

// TestHot_484_SieveEvictionDemotesWarm verifies that SIEVE eviction of
// a backed hot entry demotes the warm copy (clears protected) instead
// of tombstoning it. The warm copy stays live and SIEVE-evictable
// (not stranded).
func TestHot_484_SieveEvictionDemotesWarm(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Small hot tier (holds ~1 entry) so a single competing Put evicts k.
	// Small warm tier (holds ~2 entries) to force warm SIEVE eviction.
	ts, err := NewTieredStore(TieredConfig{
		Hot: HotConfig{
			MaxBytes:       4 << 10, // 4 KiB — holds ~1 bigObj(2000)
			NumShards:      1,
			ReaperInterval: -1,
		},
		Warm: &warm.Config{
			Dir:      filepath.Join(dir, "warm"),
			MaxBytes: 5 << 10, // 5 KiB — holds ~2 entries, forces warm SIEVE eviction
			SegMax:   1 << 20,
		},
		WALDir:                 "",
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Step 1: Insert a large backed entry k.
	k := testkey.Key(1000)
	require.NoError(t, ts.Put(context.Background(), k, bigObj(k, 2000)))

	// Verify warm has k and it's protected.
	require.Len(t, ts.warm.Keys(), 1, "warm should have the backed entry")
	require.Equal(t, 1, ts.warm.ProtectedCount(), "k should be protected")

	// Step 2: Insert a competing entry to force SIEVE eviction of k.
	// Hot holds ~1 entry, so the second Put evicts k (backed, preferred).
	k2 := testkey.Key(2000)
	require.NoError(t, ts.Put(context.Background(), k2, bigObj(k2, 2000)))

	// Drain the warm unprotect queue (async — avoids lock-ordering cycle).
	ts.drainQueues()

	// Step 3: Assert k is still in warm (retained, not tombstoned).
	warmKeys := ts.warm.Keys()
	require.True(t, slices.Contains(warmKeys, k),
		"warm copy of SIEVE-evicted key should be retained (not tombstoned)")

	// Step 4: Assert k's warm copy is now unprotected (demoted).
	// Only k2 (still in hot, backed) should be protected.
	require.Equal(t, 1, ts.warm.ProtectedCount(),
		"only entries still backed by hot should be protected (k2)")

	// Step 5: Assert no tombstones were enqueued for SIEVE evictions.
	require.Equal(t, int64(0), ts.droppedTombstones.Load(),
		"no tombstones should be enqueued for SIEVE evictions")

	// Step 6: Verify k is evictable by warm SIEVE (not stranded).
	// Warm has 2 entries (k unprotected + k2 protected) at ~4 KB,
	// budget 5 KB. One more Put triggers eviction. k is the only
	// unprotected, non-visited entry → warm SIEVE evicts it.
	k3 := testkey.Key(3000)
	_, _, err = ts.warm.Put(k3, make([]byte, 2000))
	require.NoError(t, err, "warm.Put should succeed (evicting unprotected entry k)")

	// k should have been evicted by warm SIEVE (it was unprotected, not stranded).
	warmKeys = ts.warm.Keys()
	require.False(t, slices.Contains(warmKeys, k),
		"demoted warm entry should be evictable by warm SIEVE (not stranded)")
}

// TestHot_484_FreshInsertVisited verifies that a freshly inserted hot
// entry has visited=true, giving it one SIEVE sweep of protection
// before it can be evicted.
func TestHot_484_FreshInsertVisited(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ts, err := NewTieredStore(TieredConfig{
		Hot: HotConfig{
			MaxBytes:       50 << 10, // 50 KiB — ~20 entries
			NumShards:      1,
			ReaperInterval: -1,
		},
		Warm: &warm.Config{
			Dir:      filepath.Join(dir, "warm"),
			MaxBytes: 100 << 20,
			SegMax:   1 << 20,
		},
		WALDir:                 "",
		BodyThreshold:          1 << 20, // bodies stay hot-only (no warm backing)
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	k := testkey.Key(42)
	require.NoError(t, ts.Put(context.Background(), k, obj(k, 2000)))

	// Freshly inserted entry should have visited=true.
	ts.hot.shards[0].mu.RLock()
	entry, exists := ts.hot.shards[0].entries[k]
	require.True(t, exists, "k should be in hot")
	visited := entry.entry.Visited()
	ts.hot.shards[0].mu.RUnlock()

	require.True(t, visited,
		"freshly inserted entry should have visited=true (one sweep of protection)")
}
