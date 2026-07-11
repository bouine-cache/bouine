package storage

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage/wal"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func tieredStore(t *testing.T, withWarm bool) *TieredStore {
	t.Helper()
	dir := t.TempDir()
	var warmCfg *warm.Config
	var walDir string
	if withWarm {
		warmCfg = &warm.Config{
			Dir:      filepath.Join(dir, "warm"),
			MaxBytes: 100 << 20,
			SegMax:   1 << 20,
		}
		walDir = filepath.Join(dir, "index.wal")
	}
	ts, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          warmCfg,
		WALDir:        walDir,
		BodyThreshold: 1024,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close(context.Background()) })
	return ts
}

func bigObj(key api.Key, bodySize int) *api.Object {
	return &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"application/octet-stream"}}),
		Body:       make([]byte, bodySize),
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        time.Minute,
	}
}

func TestTiered_HotOnly(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := KeyHash([]byte("hot-only"))
	o := bigObj(k, 100) // below threshold, hot only

	if err := ts.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, src, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit")
	}
	if src != api.SourceHot {
		t.Fatalf("source = %q, want %q", src, api.SourceHot)
	}
}

func TestTiered_LargeObjectWritesToWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := KeyHash([]byte("big-object"))
	o := bigObj(k, 8192) // above 1024 threshold

	if err := ts.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Should be in hot tier.
	got, src, err := ts.Get(context.Background(), k)
	if err != nil || got == nil {
		t.Fatalf("get: err=%v got=%v", err, got)
	}
	if src != api.SourceHot {
		t.Fatalf("source = %q, want %q", src, api.SourceHot)
	}

	// Should also be in warm tier.
	wEnt, wBytes := ts.warm.Stats()
	if wEnt != 1 || wBytes <= 0 {
		t.Fatalf("warm stats: entries=%d bytes=%d", wEnt, wBytes)
	}

	// Stats should reflect both tiers.
	st := ts.Stats()
	if st.WarmEntries != 1 {
		t.Fatalf("tiered stats warm entries = %d", st.WarmEntries)
	}
}

func TestTiered_LargeObjectReadPath(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := KeyHash([]byte("big-read"))
	o := bigObj(k, 8192) // above 1024 threshold

	if err := ts.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Evict from hot tier so the next Get falls through to warm.
	if err := ts.hot.Delete(context.Background(), k); err != nil {
		t.Fatalf("delete from hot: %v", err)
	}

	got, src, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected warm hit")
	}
	if src != api.SourceWarm {
		t.Fatalf("source = %q, want %q", src, api.SourceWarm)
	}

	// After warm hit, object is promoted to hot — second Get should
	// report SourceHot.
	got2, src2, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if got2 == nil {
		t.Fatal("expected hot hit after promotion")
	}
	if src2 != api.SourceHot {
		t.Fatalf("source after promotion = %q, want %q", src2, api.SourceHot)
	}
}

func TestTieredStore_Get_Miss(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := KeyHash([]byte("tiered-miss"))

	got, src, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil on miss")
	}
	if src != "" {
		t.Fatalf("source = %q, want empty", src)
	}
}

func TestTiered_Stats_WarmDiskAndMaxBytes(t *testing.T) {
	t.Parallel()
	const maxBytes = 100 << 20
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: maxBytes, SegMax: 1 << 20},
		WALDir:        filepath.Join(dir, "index.wal"),
		BodyThreshold: 1024,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Put a large object so the warm tier has on-disk bytes.
	k := KeyHash([]byte("disk-bytes"))
	if err := ts.Put(context.Background(), k, bigObj(k, 2048)); err != nil {
		t.Fatalf("put: %v", err)
	}

	st := ts.Stats()
	if st.WarmMaxBytes != maxBytes {
		t.Errorf("WarmMaxBytes = %d, want %d", st.WarmMaxBytes, maxBytes)
	}
	if st.WarmDiskBytes <= 0 {
		t.Errorf("WarmDiskBytes = %d, want > 0 after warm-tier write", st.WarmDiskBytes)
	}
	// disk_bytes must be >= live bytes (WarmBytes) because it includes
	// tombstones and segment overhead; never less.
	if st.WarmDiskBytes < st.WarmBytes {
		t.Errorf("WarmDiskBytes = %d < WarmBytes = %d", st.WarmDiskBytes, st.WarmBytes)
	}
}

func TestTiered_DeleteBothTiers(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := KeyHash([]byte("del-both"))
	o := bigObj(k, 2048) // above threshold

	_ = ts.Put(context.Background(), k, o)
	_ = ts.Delete(context.Background(), k)

	got, _, _ := ts.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss after delete")
	}
}

func TestTiered_WALReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "index.wal")

	// Write WAL entries manually.
	l, err := wal.Open(walPath)
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	_ = l.Append(wal.PutEntry(42, 0, 0))
	_ = l.Append(wal.PutEntry(43, 0, 100))
	_ = l.Append(wal.DeleteEntry(42))
	_ = l.Close()

	// Replay and verify.
	var entries []wal.Entry
	err = wal.Replay(walPath, func(e wal.Entry) error {
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("replayed %d entries, want 3", len(entries))
	}
	if !entries[0].IsPut() || entries[0].Key != 42 {
		t.Fatalf("entry 0: %+v", entries[0])
	}
	if !entries[2].IsDelete() || entries[2].Key != 42 {
		t.Fatalf("entry 2: %+v", entries[2])
	}
}

func TestTiered_EphemeralMode(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := KeyHash([]byte("ephemeral"))
	_ = ts.Put(context.Background(), k, bigObj(k, 2048))

	st := ts.Stats()
	if st.WarmEntries != 0 {
		t.Fatalf("ephemeral mode should have 0 warm entries, got %d", st.WarmEntries)
	}
}

// TestTiered_WarmGet verifies that an object written to the warm tier
// is readable after it has been evicted from the hot tier, and that
// reopening the store with WAL replay restores the index so a Get
// succeeds without a hot-tier entry.
func TestTiered_WarmGet(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	newStore := func() *TieredStore {
		ts, err := NewTieredStore(TieredConfig{
			Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:        walPath,
			BodyThreshold: 512, // large objects (>512 B) go to warm tier
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	// Write a large object that crosses the threshold.
	ts1 := newStore()
	k := KeyHash([]byte("warm-get-key"))
	obj := bigObj(k, 1024)
	if err := ts1.Put(ctx, k, obj); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Warm entry must be present.
	if st := ts1.Stats(); st.WarmEntries == 0 {
		t.Fatal("expected warm entry after Put")
	}
	// Delete from hot tier so next Get must fall through to warm.
	if err := ts1.hot.Delete(ctx, k); err != nil {
		t.Fatalf("hot delete: %v", err)
	}
	got, _, err := ts1.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get after hot eviction: %v", err)
	}
	if got == nil || got.StatusCode != 200 {
		t.Fatalf("expected object from warm tier, got %v", got)
	}
	_ = ts1.Close(ctx)

	// Reopen: WAL replay must restore the index so warm Get still works.
	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })
	// Hot tier is empty after reopen.
	got2, _, err := ts2.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got2 == nil || got2.StatusCode != 200 {
		t.Fatalf("expected object from warm tier after reopen, got %v", got2)
	}
}

// TestTiered_WarmStatsRestoredAfterReopen verifies that warm-tier stats
// (entries, bytes) are correctly restored after a restart via WAL replay
// + RecomputeStats, not left at zero.
func TestTiered_WarmStatsRestoredAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	newStore := func() *TieredStore {
		ts, err := NewTieredStore(TieredConfig{
			Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:        walPath,
			BodyThreshold: 512,
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	ts1 := newStore()
	for i := range 5 {
		k := KeyHash([]byte(fmt.Sprintf("stats-key-%d", i)))
		if err := ts1.Put(ctx, k, bigObj(k, 1024)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	st1 := ts1.Stats()
	if st1.WarmEntries != 5 {
		t.Fatalf("before close: warm entries = %d, want 5", st1.WarmEntries)
	}
	if st1.WarmBytes <= 0 {
		t.Fatalf("before close: warm bytes = %d", st1.WarmBytes)
	}
	_ = ts1.Close(ctx)

	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })
	st2 := ts2.Stats()
	if st2.WarmEntries != 5 {
		t.Fatalf("after reopen: warm entries = %d, want 5", st2.WarmEntries)
	}
	if st2.WarmBytes != st1.WarmBytes {
		t.Fatalf("after reopen: warm bytes = %d, want %d", st2.WarmBytes, st1.WarmBytes)
	}
}

// TestTieredStore_CloseStopsCompaction verifies that Close stops the
// background compaction goroutine without leaking. Sequential (no
// t.Parallel) because runtime.NumGoroutine is a global counter.
func TestTieredStore_CloseStopsCompaction(t *testing.T) {
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:  HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm: &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 64 << 20, SegMax: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}

	before := runtime.NumGoroutine()
	if err := ts.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Poll for the goroutine to exit, matching the pattern in TestHotClose.
	for range 50 {
		if runtime.NumGoroutine() < before {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	after := runtime.NumGoroutine()

	if after >= before {
		t.Errorf("goroutine leak: before Close=%d, after Close=%d", before, after)
	}
}

// TestTieredStore_KeysReturnsHotWarmUnion reproduces issue #175: when a
// warm-backed key is evicted from the hot tier, Keys() must still report
// it because the node still owns the object in the warm tier. Returning
// hot-only keys caused warm→hot promotion to re-fill evicted
// keys, re-overfilling the hot tier in a self-sustaining loop.
func TestTieredStore_KeysReturnsHotWarmUnion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	// Tiny hot tier so a single large object triggers eviction.
	ts, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 2048, NumShards: 2},
		Warm:          &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:        filepath.Join(dir, "index.wal"),
		BodyThreshold: 512,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Two large objects that exceed the hot budget; both go to warm.
	k1 := KeyHash([]byte("union-key-1"))
	k2 := KeyHash([]byte("union-key-2"))
	if err := ts.Put(ctx, k1, bigObj(k1, 1024)); err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	if err := ts.Put(ctx, k2, bigObj(k2, 1024)); err != nil {
		t.Fatalf("Put k2: %v", err)
	}

	// Evict k1 from the hot tier only; it remains in warm.
	if err := ts.hot.Delete(ctx, k1); err != nil {
		t.Fatalf("hot delete k1: %v", err)
	}

	got := ts.Keys()
	gotSet := make(map[api.Key]bool, len(got))
	for _, k := range got {
		gotSet[k] = true
	}
	if !gotSet[k1] {
		t.Errorf("Keys() missing warm-only key %d (evicted from hot but still owned)", k1)
	}
	if !gotSet[k2] {
		t.Errorf("Keys() missing hot key %d", k2)
	}
}

// TestTieredStore_OverBudget verifies the OverBudget contract: it must
// report false when the hot tier is within its byte budget and true when
// it exceeds it. TieredStore uses this to skip warm→hot promotion under
// memory pressure, preventing the eviction ↔ promotion feedback loop (#175).
func TestTieredStore_OverBudget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const maxBytes = 1024
	ts, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: maxBytes, NumShards: 1},
		BodyThreshold: 64 << 10, // objects stay hot-only
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}

	// Empty store is not over budget.
	if ts.OverBudget() {
		t.Fatal("OverBudget = true on empty store")
	}

	// Under-budget store is not over budget.
	k := KeyHash([]byte("small"))
	if err := ts.Put(ctx, k, bigObj(k, 256)); err != nil {
		t.Fatalf("Put small: %v", err)
	}
	if ts.OverBudget() {
		t.Fatalf("OverBudget = true with %d bytes under %d max", 256, maxBytes)
	}

	// Stop the sweeper so the overshoot from an oversized object is
	// deterministic. The sweeper would otherwise evict the oversized
	// object before we can observe OverBudget. Closing done stops both
	// the sweeper and reaper goroutines; we skip Close in cleanup to
	// avoid a double-close on done.
	close(ts.hot.done)

	overK := KeyHash([]byte("oversized"))
	if err := ts.Put(ctx, overK, bigObj(overK, maxBytes*2)); err != nil {
		t.Fatalf("Put oversized: %v", err)
	}
	if !ts.OverBudget() {
		t.Fatalf("OverBudget = false after putting %d bytes with %d max", maxBytes*2, maxBytes)
	}
}

func TestTieredStore_ImplementsKeyLister(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ts := tieredStore(t, true)
	keys := []api.Key{
		KeyHash([]byte("k1")),
		KeyHash([]byte("k2")),
		KeyHash([]byte("k3")),
	}
	for _, k := range keys {
		if err := ts.Put(ctx, k, bigObj(k, 100)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	kl, ok := any(ts).(KeyLister)
	if !ok {
		t.Fatal("TieredStore does not implement KeyLister")
	}
	got := kl.Keys()
	if len(got) != len(keys) {
		t.Fatalf("Keys() returned %d keys, want %d", len(got), len(keys))
	}
	want := make(map[api.Key]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	for _, k := range got {
		if !want[k] {
			t.Errorf("unexpected key %d in Keys()", k)
		}
	}
}

// TestTiered_EvictsLegacyCodecBlobOnGet reproduces issue #171: a warm-tier
// blob written by codec v1 (≤ v0.1.17) is unreadable after the v0.1.18
// codec bump. Get must return a clean miss (nil, nil), evict the blob
// durably (warm tombstone + WAL delete), and the heal must survive
// restart. A subsequent Put of a v2 object for the same key must be
// readable.
func TestTiered_EvictsLegacyCodecBlobOnGet(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	newStore := func() *TieredStore {
		ts, err := NewTieredStore(TieredConfig{
			Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:        walPath,
			BodyThreshold: 512,
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	ts1 := newStore()
	k := KeyHash([]byte("legacy-codec-key"))
	// Inject a blob whose version byte is 1 (legacy). warm.Put writes
	// the record and sets the in-memory index; we also append a WAL
	// PutEntry so the durability of the eviction can be tested after
	// reopen.
	legacyBlob := []byte{0x01, 0x02, 0x03, 0x04}
	segID, offset, err := ts1.warm.Put(uint64(k), legacyBlob)
	if err != nil {
		t.Fatalf("warm.Put: %v", err)
	}
	if err := ts1.wal.Append(wal.PutEntry(uint64(k), int32(segID), offset)); err != nil { //nolint:gosec // test
		t.Fatalf("wal.Append: %v", err)
	}

	// Get must treat the undecodable blob as a miss, not an error.
	got, _, err := ts1.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get: expected nil error for legacy blob, got %v", err)
	}
	if got != nil {
		t.Fatalf("Get: expected nil (miss) for legacy blob, got object")
	}

	// The warm-tier index must no longer contain the key: warm.Get
	// returns nil after the tombstone + index removal.
	if body, _ := ts1.warm.Get(uint64(k)); body != nil {
		t.Fatalf("expected warm.Get to return nil after eviction, got %d bytes", len(body))
	}

	// A fresh Put of a v2 object for the same key must be readable
	// from the warm tier after hot eviction.
	fresh := bigObj(k, 1024)
	if err := ts1.Put(ctx, k, fresh); err != nil {
		t.Fatalf("Put fresh: %v", err)
	}
	if err := ts1.hot.Delete(ctx, k); err != nil {
		t.Fatalf("hot.Delete: %v", err)
	}
	gotFresh, _, err := ts1.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get fresh from warm: %v", err)
	}
	if gotFresh == nil || gotFresh.StatusCode != 200 {
		t.Fatalf("expected fresh object from warm tier, got %v", gotFresh)
	}

	// The heal must survive restart: WAL replay processes the Put
	// (legacy), the Delete (eviction), then the Put (fresh). The key
	// must resolve to the fresh v2 blob, not the legacy one.
	if err := ts1.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })
	// Evict from hot so Get falls through to warm.
	_ = ts2.hot.Delete(ctx, k)
	got2, _, err := ts2.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get after reopen: expected nil error, got %v", err)
	}
	if got2 == nil || got2.StatusCode != 200 {
		t.Fatalf("expected fresh object from warm tier after reopen, got %v", got2)
	}
}

// TestTiered_EvictsCorruptBlobOnGet verifies that a warm-tier blob whose
// record frame is valid but whose encoded content is malformed (errCorrupt
// from decodeObject) is also evicted on Get, not propagated as an error.
func TestTiered_EvictsCorruptBlobOnGet(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	ctx := context.Background()
	k := KeyHash([]byte("corrupt-codec-key"))

	// A blob that starts with the current version byte but is truncated
	// mid-metadata: decodeObject will set errCorrupt.
	corruptBlob := encodeObject(&api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{"A": {"b"}}),
		Body:       []byte("xx"),
	})[:4]
	if _, _, err := ts.warm.Put(uint64(k), corruptBlob); err != nil {
		t.Fatalf("warm.Put: %v", err)
	}

	got, _, err := ts.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get: expected nil error for corrupt blob, got %v", err)
	}
	if got != nil {
		t.Fatalf("Get: expected nil (miss) for corrupt blob, got object")
	}
	if body, _ := ts.warm.Get(uint64(k)); body != nil {
		t.Fatalf("expected warm.Get to return nil after corrupt eviction, got %d bytes", len(body))
	}
}

// TestTiered_EvictsLegacyBlobAfterReopen is the production scenario from
// issue #171: v0.1.17 writes a codec-v1 blob, the process restarts into
// v0.1.18, and the first Get must evict the undecodable blob. This tests
// the reopen path (segment file offset is restored from stat, not from
// the write cursor) to catch the warm.Put seek bug.
func TestTiered_EvictsLegacyBlobAfterReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	cfg := TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:        walPath,
		BodyThreshold: 512,
	}

	// Phase 1: write a legitimate large object so the segment has real
	// data at a non-zero offset, then also inject a legacy codec-v1
	// blob. Close.
	ts1, err := NewTieredStore(cfg)
	if err != nil {
		t.Fatalf("ts1: %v", err)
	}
	goodKey := KeyHash([]byte("good-object"))
	if err := ts1.Put(ctx, goodKey, bigObj(goodKey, 1024)); err != nil {
		t.Fatalf("Put good: %v", err)
	}
	legacyKey := KeyHash([]byte("legacy-after-reopen"))
	legacyBlob := []byte{0x01, 0x02, 0x03, 0x04}
	segID, offset, err := ts1.warm.Put(uint64(legacyKey), legacyBlob)
	if err != nil {
		t.Fatalf("warm.Put legacy: %v", err)
	}
	if err := ts1.wal.Append(wal.PutEntry(uint64(legacyKey), int32(segID), offset)); err != nil { //nolint:gosec // test
		t.Fatalf("wal.Append: %v", err)
	}
	if err := ts1.Close(ctx); err != nil {
		t.Fatalf("ts1.Close: %v", err)
	}

	// Phase 2: reopen. WAL replay re-indexes both keys. Get on the
	// legacy key must evict it (clean miss). Get on the good key must
	// still return the valid object (the O_APPEND fix prevents the
	// tombstone write from corrupting it).
	ts2, err := NewTieredStore(cfg)
	if err != nil {
		t.Fatalf("ts2: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	// Evict good key from hot so Get falls through to warm for both.
	_ = ts2.hot.Delete(ctx, goodKey)
	_ = ts2.hot.Delete(ctx, legacyKey)

	gotLegacy, _, err := ts2.Get(ctx, legacyKey)
	if err != nil {
		t.Fatalf("Get legacy after reopen: expected nil error, got %v", err)
	}
	if gotLegacy != nil {
		t.Fatalf("expected miss for legacy blob after reopen, got object")
	}

	gotGood, _, err := ts2.Get(ctx, goodKey)
	if err != nil {
		t.Fatalf("Get good after reopen: %v", err)
	}
	if gotGood == nil || gotGood.StatusCode != 200 {
		t.Fatalf("expected good object to survive legacy eviction, got %v", gotGood)
	}
}

// TestTiered_TornWriteReplayReturnsMiss reproduces the bug from issue #157:
// the WAL entry is fsynced but the segment data is not. On restart, WAL
// replay rebuilds the index pointing at a torn offset. Get must return a
// clean miss (nil, nil), not an error, and drop the stale index entry.
func TestTiered_TornWriteReplayReturnsMiss(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	ts1, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:        walPath,
		BodyThreshold: 512,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	k := KeyHash([]byte("torn-write-key"))
	if err := ts1.Put(ctx, k, bigObj(k, 1024)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ts1.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	truncateLastSegmentRecord(t, warmDir)

	ts2, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:        walPath,
		BodyThreshold: 512,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	got, _, err := ts2.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get after torn write replay: expected nil error, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected miss (nil) after torn write replay, got object")
	}
}

// TestTiered_PutCloseReopenRoundTrip verifies the happy path: after
// Put + Close + reopen, the warm-tier object is intact and servable.
// This is a round-trip smoke test, not a durability-ordering proof —
// the ordering invariant is exercised by TestTiered_TornWriteReplayReturnsMiss.
func TestTiered_PutCloseReopenRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	ts1, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:        walPath,
		BodyThreshold: 512,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	k := KeyHash([]byte("durable-key"))
	if err := ts1.Put(ctx, k, bigObj(k, 1024)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ts1.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts2, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:        walPath,
		BodyThreshold: 512,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	_ = ts2.hot.Delete(ctx, k)
	got, _, err := ts2.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit from warm tier after reopen (data should be durable)")
	}
}

// truncateLastSegmentRecord finds the last (highest-ID) .seg file in
// warmDir, finds the offset of the last record by scanning, and truncates
// the file mid-body to simulate a torn write where the WAL entry
// persisted but the segment data did not.
func truncateLastSegmentRecord(t *testing.T, warmDir string) {
	t.Helper()
	entries, err := os.ReadDir(warmDir)
	if err != nil {
		t.Fatalf("readdir %s: %v", warmDir, err)
	}
	var segFile string
	var maxID = -1
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".seg" {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(e.Name(), "%d.seg", &id); err != nil {
			continue
		}
		if id > maxID {
			maxID = id
			segFile = filepath.Join(warmDir, e.Name())
		}
	}
	if segFile == "" {
		t.Fatal("no segment file found")
	}

	scan, err := warm.NewStore(warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("open for scan: %v", err)
	}
	var lastOff int64
	if err := scan.Scan(func(r warm.Record) error {
		lastOff = r.Offset
		return nil
	}); err != nil {
		_ = scan.Close()
		t.Fatalf("scan for last offset: %v", err)
	}
	_ = scan.Close()

	cutAt := lastOff + 20
	if err := os.Truncate(segFile, cutAt); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// TestTiered_WALReplayRestoresIndex verifies that after a close/reopen
// cycle, WAL replay correctly populates the warm-tier index so all
// entries are servable via Get. This is the real startup path: Put
// appends to the WAL, Close flushes the warm tier, and reopen replays
// the WAL to restore the index. The fix was in initWAL checking
// IndexLen (actual map size) instead of Stats (atomic counters that
// stay zero after replay) to decide whether a segment scan is needed.
func TestTiered_WALReplayRestoresIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	newStore := func() *TieredStore {
		ts, err := NewTieredStore(TieredConfig{
			Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:          &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:        walPath,
			BodyThreshold: 512,
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	// Write objects to the warm tier.
	ts1 := newStore()
	keys := make([]api.Key, 10)
	for i := range 10 {
		k := KeyHash([]byte(fmt.Sprintf("replay-key-%d", i)))
		keys[i] = k
		if err := ts1.Put(ctx, k, bigObj(k, 1024)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := ts1.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: WAL replay should restore all 10 entries.
	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	st2 := ts2.Stats()
	if st2.WarmEntries != 10 {
		t.Fatalf("after reopen: warm entries = %d, want 10", st2.WarmEntries)
	}

	// Verify every entry is servable from warm tier (hot tier is empty on reopen).
	for i, k := range keys {
		obj, src, err := ts2.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if obj == nil {
			t.Fatalf("Get %d: nil object", i)
		}
		if src != api.SourceWarm {
			t.Fatalf("Get %d: source = %s, want %s", i, src, api.SourceWarm)
		}
	}
}
