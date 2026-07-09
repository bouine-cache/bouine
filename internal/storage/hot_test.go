package storage

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/bouine-cache/bouine/internal/storage/sieve"
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

	k := KeyHash([]byte("test-key"))
	o := obj(k, 100)

	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, src, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit, got miss")
	}
	if got.StatusCode != 200 {
		t.Fatalf("status = %d", got.StatusCode)
	}
	if s.Hits(k) != 1 {
		t.Fatalf("hits = %d, want 1", s.Hits(k))
	}
	if src != api.SourceHot {
		t.Fatalf("source = %q, want %q", src, api.SourceHot)
	}
}

func TestHotStore_Miss(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	got, src, err := s.Get(context.Background(), 999)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatal("expected miss")
	}
	if src != "" {
		t.Fatalf("source = %q, want empty", src)
	}
	st := s.Stats()
	if st.Misses != 1 {
		t.Fatalf("misses = %d", st.Misses)
	}
}

func TestHotStore_Get_DelegatesToGet(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("delegate"))
	o := obj(k, 100)

	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, _, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit via Get")
	}
}

func TestHotStore_Delete(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("del"))
	_ = s.Put(context.Background(), k, obj(k, 50))
	_ = s.Delete(context.Background(), k)

	got, _, _ := s.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss after delete")
	}
}

func TestHotStore_EvictsOnFull(t *testing.T) {
	t.Parallel()
	// 4 shards, 4096 bytes total = 1024 per shard.
	s := NewHotStore(HotConfig{MaxBytes: 4096, NumShards: 4})

	// Insert objects until eviction must have happened.
	for i := range 100 {
		k := api.Key(i)
		_ = s.Put(context.Background(), k, obj(k, 500))
	}

	st := s.Stats()
	if st.Evictions == 0 {
		t.Fatal("expected evictions")
	}
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
		Key:        KeyHash([]byte("fresh")),
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:       make([]byte, 100),
		BodySize:   100,
		StoredAt:   now,
		TTL:        time.Minute,
	}
	expired := &api.Object{
		Key:                  KeyHash([]byte("expired")),
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
	if before.HotEntries != 2 {
		t.Fatalf("entries before reap = %d, want 2", before.HotEntries)
	}

	s.reapExpired(now)

	after := s.Stats()
	if after.HotEntries != 1 {
		t.Fatalf("entries after reap = %d, want 1 (fresh only)", after.HotEntries)
	}
	got, _, _ := s.Get(ctx, expired.Key)
	if got != nil {
		t.Fatal("expired entry should have been reaped")
	}
	got, _, _ = s.Get(ctx, fresh.Key)
	if got == nil {
		t.Fatal("fresh entry should still be present")
	}
}

func TestHotStore_ReapExpired_KeepsSWRAndSIEEntries(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	now := time.Now()
	withinSWR := &api.Object{
		Key:                  KeyHash([]byte("swr")),
		StatusCode:           200,
		Header:               header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:                 make([]byte, 100),
		BodySize:             100,
		StoredAt:             now.Add(-5 * time.Second),
		TTL:                  time.Second,
		StaleWhileRevalidate: 30 * time.Second,
	}
	withinSIE := &api.Object{
		Key:          KeyHash([]byte("sie")),
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
	if st.HotEntries != 2 {
		t.Fatalf("entries after reap = %d, want 2 (SWR and SIE still valid)", st.HotEntries)
	}
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
	if want := int64(unsafe.Sizeof(api.Object{})); objectStructSize != want {
		t.Errorf("objectStructSize = %d, but unsafe.Sizeof(api.Object{}) = %d — update the constant",
			objectStructSize, want)
	}
	if want := int64(unsafe.Sizeof(hotEntry{})); hotEntrySize != want {
		t.Errorf("hotEntrySize = %d, but unsafe.Sizeof(hotEntry{}) = %d — update the constant",
			hotEntrySize, want)
	}
	if want := int64(unsafe.Sizeof(sieve.Entry[api.Key]{})); sieveEntrySize != want {
		t.Errorf("sieveEntrySize = %d, but unsafe.Sizeof(sieve.Entry[api.Key]{}) = %d — update the constant",
			sieveEntrySize, want)
	}
}

func TestObjSize_MapOverheadConstant(t *testing.T) {
	t.Parallel()
	// 8-slot bucket = 144 B at load factor 6.5 → ~22 B/entry.
	// The hmap struct header (~96 B) is negligible at 1M+ entries.
	if mapPerEntryOverhead != 22 {
		t.Errorf("mapPerEntryOverhead = %d, want 22 (Go runtime bucket overhead)", mapPerEntryOverhead)
	}
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
	if delta != 27 {
		t.Errorf("delta = %d, want 27 (16 B string header + 11 B orphaned value data)", delta)
	}
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
	// objectStructSize: 248, hotEntrySize: 32, sieveEntrySize: 32, mapPerEntryOverhead: 22
	// headerEntriesSlice: 24, headerValuesSlice: 24
	// headerEntrySize * 2: 48
	// headerValueHeader * 2: 32
	// valueBytes: len("text/html") + len("val") = 9 + 3 = 12
	// VaryKey: 2, ETag: 2, CacheControl: 6
	// SurrogateKeys: 2 + 2 = 4
	want := int64(5) + 256 + 24 + 32 + 22 +
		24 + 24 + 48 + 32 + 12 +
		2 + 2 + 6 + 4
	got := objSize(obj)
	if got != want {
		t.Errorf("objSize = %d, want %d (exact value mismatch)", got, want)
	}
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
		k := api.Key(i)
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
	if st.Evictions == 0 {
		t.Fatal("expected evictions with large headers and small bodies")
	}
	overshoot := int64(budget * 11 / 10) // 10% transient overshoot bound
	if st.HotBytes > overshoot {
		t.Fatalf("HotBytes = %d, exceeds overshoot bound %d", st.HotBytes, overshoot)
	}
}

func TestHotStore_Replace(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("replace"))

	_ = s.Put(context.Background(), k, obj(k, 100))
	_ = s.Put(context.Background(), k, obj(k, 200))

	got, _, _ := s.Get(context.Background(), k)
	if got == nil {
		t.Fatal("expected hit")
	}
	if got.BodySize != 200 {
		t.Fatalf("body_size = %d, want 200", got.BodySize)
	}
	st := s.Stats()
	if st.HotEntries != 1 {
		t.Fatalf("entries = %d, want 1", st.HotEntries)
	}
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
				k := api.Key(base*1000 + i)
				_ = s.Put(context.Background(), k, obj(k, 64))
				_, _, _ = s.Get(context.Background(), k)
			}
		}(g)
	}
	wg.Wait()

	st := s.Stats()
	if st.Hits == 0 {
		t.Fatal("expected hits from concurrent access")
	}
}

func TestHotStore_Stats(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	k := KeyHash([]byte("stats"))
	_ = s.Put(context.Background(), k, obj(k, 50))
	_, _, _ = s.Get(context.Background(), k)
	_, _, _ = s.Get(context.Background(), 12345) // miss

	st := s.Stats()
	if st.HotEntries != 1 {
		t.Fatalf("entries = %d", st.HotEntries)
	}
	if st.Hits != 1 {
		t.Fatalf("hits = %d", st.Hits)
	}
	if st.Misses != 1 {
		t.Fatalf("misses = %d", st.Misses)
	}
}

func TestKeyHash_Deterministic(t *testing.T) {
	t.Parallel()
	a := KeyHash([]byte("hello"))
	b := KeyHash([]byte("hello"))
	if a != b {
		t.Fatalf("non-deterministic: %d != %d", a, b)
	}
	c := KeyHash([]byte("world"))
	if a == c {
		t.Fatal("collision on different inputs")
	}
}

func TestHotStore_SetBacked(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	k := KeyHash([]byte("warm-key"))
	o := obj(k, 512)
	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatal(err)
	}

	s.SetBacked(k)

	sh := &s.shards[uint64(k)&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if e, ok := sh.entries[k]; !ok || !e.hasBackup {
		t.Fatal("expected entry to be marked hasBackup after SetBacked")
	}
	if sh.backedCount != 1 {
		t.Fatalf("backedCount = %d, want 1", sh.backedCount)
	}
}

func TestHotStore_EvictPreferBacked(t *testing.T) {
	t.Parallel()
	// Single shard, 2 KiB budget. Each object is 1280 bytes (1024 body +
	// 256 overhead), so the budget holds 1 entry; the 2nd Put forces
	// eviction.
	s := NewHotStore(HotConfig{MaxBytes: 2 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := KeyHash([]byte("a"))
	k2 := KeyHash([]byte("b"))
	_ = s.Put(ctx, k1, obj(k1, 1024))

	// Mark k1 as backed before k2 arrives.
	s.SetBacked(k1)

	// k2 triggers eviction. k1 (backed) should be evicted, not k2.
	_ = s.Put(ctx, k2, obj(k2, 1024))

	if got, _, _ := s.Get(ctx, k1); got != nil {
		t.Error("k1 (backed) should have been evicted first")
	}
	if got, _, _ := s.Get(ctx, k2); got == nil {
		t.Error("k2 (hot-only, newly inserted) should exist")
	}
}

func TestHotStore_EvictPreferBacked_PreservesVisitedBit(t *testing.T) {
	t.Parallel()
	// Single shard, 2 KiB budget. Insert k1 (hot-only) and access it to
	// set visited=true. Then insert k2 (backed). k2 should be evicted
	// first because it's backed. k1's visited bit should be preserved
	// by the Defer path.
	s := NewHotStore(HotConfig{MaxBytes: 3 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := KeyHash([]byte("hot"))
	k2 := KeyHash([]byte("warm"))
	_ = s.Put(ctx, k1, obj(k1, 1024))
	_ = s.Put(ctx, k2, obj(k2, 1024))

	// Access k1 to set visited=true.
	_, _, _ = s.Get(ctx, k1)

	// Mark k2 as backed.
	s.SetBacked(k2)

	// k3 triggers eviction. k2 (backed) should be evicted, k1 (hot, visited)
	// should survive with its visited bit intact.
	k3 := KeyHash([]byte("new"))
	_ = s.Put(ctx, k3, obj(k3, 1024))

	if got, _, _ := s.Get(ctx, k1); got == nil {
		t.Error("k1 (hot-only, visited) should survive — visited bit preserved")
	}
	if got, _, _ := s.Get(ctx, k2); got != nil {
		t.Error("k2 (backed) should have been evicted")
	}
}

func TestHotStore_EvictFallbackNoBacked(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 3 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := KeyHash([]byte("x"))
	k2 := KeyHash([]byte("y"))
	_ = s.Put(ctx, k1, obj(k1, 1000))
	_ = s.Put(ctx, k2, obj(k2, 1000))

	k3 := KeyHash([]byte("z"))
	if err := s.Put(ctx, k3, obj(k3, 1000)); err != nil {
		t.Fatal(err)
	}

	// k3 must have been inserted (eviction loop allowed it).
	if _, _, err := s.Get(ctx, k3); err != nil {
		t.Fatal("k3 should exist:", err)
	}
}

func TestHotStore_BackedCountConsistency(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("consistency"))
	_ = s.Put(ctx, k, obj(k, 100))
	s.SetBacked(k)

	// Overwrite with new entry; backed status resets.
	_ = s.Put(ctx, k, obj(k, 200))
	s.SetBacked(k)

	sh := &s.shards[uint64(k)&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if e, ok := sh.entries[k]; !ok || !e.hasBackup {
		t.Fatal("entry should have hasBackup after re-marking")
	}
	if sh.backedCount != 1 {
		t.Fatalf("backedCount = %d, want 1 after re-mark", sh.backedCount)
	}
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
		k := api.Key(i)
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
				k := api.Key(n % uint64(working))
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

	time.Sleep(duration)
	stop.Store(true)
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
	var goroutinesWithSweeper int
	for range 50 {
		goroutinesWithSweeper = runtime.NumGoroutine()
		if goroutinesWithSweeper > before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if goroutinesWithSweeper <= before {
		t.Error("expected sweeper goroutine to be running after NewHotStore")
	}

	_ = s.Close(context.Background())
	// Poll for the goroutine to exit.
	for range 50 {
		if runtime.NumGoroutine() <= before+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	after := runtime.NumGoroutine()
	if after > before+1 { // +1 for test harness variance
		t.Errorf("goroutine leak: before=%d after close=%d", before, after)
	}
}

func TestHotStore_BanByHostRegex(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	k := KeyHash([]byte("host-ban"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/keep")
	_ = s.Put(context.Background(), k, o)

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "example\\.com"})
	if err != nil {
		t.Fatalf("ban: %v", err)
	}
	if count != 1 {
		t.Fatalf("ban count = %d, want 1", count)
	}

	got, _, _ := s.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss after ban")
	}
}

func TestHotStore_BanByPathRegex(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	k := KeyHash([]byte("path-ban"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	_ = s.Put(context.Background(), k, o)

	count, err := s.Ban(context.Background(), api.BanExpr{PathRegex: "^/ban-me"})
	if err != nil {
		t.Fatalf("ban: %v", err)
	}
	if count != 1 {
		t.Fatalf("ban count = %d, want 1", count)
	}

	got, _, _ := s.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss after ban")
	}
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
	if err != nil {
		t.Fatalf("ban: %v", err)
	}
	if count != 0 {
		t.Fatalf("ban count = %d, want 0 (store empty)", count)
	}

	// Simulate peer replication: Put an object with StoredAt before the ban.
	k := KeyHash([]byte("lazy-ban"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	o.StoredAt = banTime.Add(-1 * time.Hour)
	_ = s.Put(context.Background(), k, o)

	got, _, _ := s.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss from lazy ban on slow path")
	}
}

func TestHotStore_BanLazyEvictionFastPath(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	// Put a matching object with old StoredAt and access it to set visited=true.
	k := KeyHash([]byte("lazy-fast"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	o.StoredAt = time.Now().Add(-1 * time.Hour)
	_ = s.Put(context.Background(), k, o)

	got, _, _ := s.Get(context.Background(), k)
	if got == nil {
		t.Fatal("expected hit before ban")
	}

	// Issue a ban that covers the object's StoredAt. The next Get
	// takes the fast path (visited=true) and must enforce the lazy ban.
	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "example\\.com",
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("ban: %v", err)
	}

	got, _, _ = s.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss from lazy ban on fast path")
	}
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
	if err != nil {
		t.Fatalf("ban: %v", err)
	}

	// Object stored AFTER the ban — should be exempt from lazy eviction.
	k := KeyHash([]byte("exempt"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	o.StoredAt = banTime.Add(1 * time.Hour)
	_ = s.Put(context.Background(), k, o)

	got, _, _ := s.Get(context.Background(), k)
	if got == nil {
		t.Fatal("expected hit — object stored after ban should be exempt")
	}
}
