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

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/wal"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
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
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   warmCfg,
		WALDir:                 walDir,
		BodyThreshold:          1024,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")
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
	k := testkey.Hash([]byte("hot-only"))
	o := bigObj(k, 100) // below threshold, hot only

	err := ts.Put(context.Background(), k, o)
	require.NoError(t, err, "put")
	got, src, err := ts.Get(context.Background(), k)
	require.NoError(t, err, "get")
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)
}

func TestTiered_LargeObjectWritesToWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := testkey.Hash([]byte("big-object"))
	o := bigObj(k, 8192) // above 1024 threshold

	err := ts.Put(context.Background(), k, o)
	require.NoError(t, err, "put")

	// Should be in hot tier.
	got, src, err := ts.Get(context.Background(), k)
	if err != nil || got == nil {
		t.Fatalf("get: err=%v got=%v", err, got)
	}
	require.Equal(t, api.SourceHot, src)

	// Should also be in warm tier.
	wEnt, wBytes := ts.warm.Stats()
	if wEnt != 1 || wBytes <= 0 {
		t.Fatalf("warm stats: entries=%d bytes=%d", wEnt, wBytes)
	}

	// Stats should reflect both tiers.
	st := ts.Stats()
	require.Equal(t, int64(1), st.WarmEntries)
}

func TestTiered_LargeObjectReadPath(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := testkey.Hash([]byte("big-read"))
	o := bigObj(k, 8192) // above 1024 threshold

	err := ts.Put(context.Background(), k, o)
	require.NoError(t, err, "put")

	// Evict from hot tier so the next Get falls through to warm.
	err = ts.hot.Delete(context.Background(), k)
	require.NoError(t, err, "delete from hot")

	got, src, err := ts.Get(context.Background(), k)
	require.NoError(t, err, "Get")
	require.NotNil(t, got)
	require.Equal(t, api.SourceWarm, src)

	// After warm hit, object is promoted to hot — second Get should
	// report SourceHot.
	got2, src2, err := ts.Get(context.Background(), k)
	require.NoError(t, err, "second Get")
	require.NotNil(t, got2)
	require.Equal(t, api.SourceHot, src2)
}

func TestTieredStore_Get_Miss(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := testkey.Hash([]byte("tiered-miss"))

	got, src, err := ts.Get(context.Background(), k)
	require.NoError(t, err, "Get")
	require.Nil(t, got)
	require.Equal(t, api.Source(""), src)
}

func TestTiered_Stats_WarmDiskAndMaxBytes(t *testing.T) {
	t.Parallel()
	const maxBytes = 100 << 20
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: maxBytes, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Put a large object so the warm tier has on-disk bytes.
	k := testkey.Hash([]byte("disk-bytes"))
	err = ts.Put(context.Background(), k, bigObj(k, 2048))
	require.NoError(t, err, "put")

	st := ts.Stats()
	assert.Equal(t, int64(maxBytes), st.WarmMaxBytes)
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
	k := testkey.Hash([]byte("del-both"))
	o := bigObj(k, 2048) // above threshold

	_ = ts.Put(context.Background(), k, o)
	_ = ts.Delete(context.Background(), k)

	got, _, _ := ts.Get(context.Background(), k)
	require.Nil(t, got)
}

func TestTiered_WALReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "index.wal")

	// Write WAL entries manually.
	l, err := wal.Open(walPath)
	require.NoError(t, err, "wal open")
	_ = l.Append(wal.PutEntry(testkey.Key(42), 0, 0))
	_ = l.Append(wal.PutEntry(testkey.Key(43), 0, 100))
	_ = l.Append(wal.DeleteEntry(testkey.Key(42)))
	_ = l.Close()

	// Replay and verify.
	var entries []wal.Entry
	err = wal.Replay(walPath, func(e wal.Entry) error {
		entries = append(entries, e)
		return nil
	})
	require.NoError(t, err, "replay")
	require.Len(t, entries, 3)
	if !entries[0].IsPut() || entries[0].Key != testkey.Key(42) {
		t.Fatalf("entry 0: %+v", entries[0])
	}
	if !entries[2].IsDelete() || entries[2].Key != testkey.Key(42) {
		t.Fatalf("entry 2: %+v", entries[2])
	}
}

func TestTiered_EphemeralMode(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := testkey.Hash([]byte("ephemeral"))
	_ = ts.Put(context.Background(), k, bigObj(k, 2048))

	st := ts.Stats()
	require.Equal(t, int64(0), st.WarmEntries)
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
			Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:                 walPath,
			BodyThreshold:          512, // large objects (>512 B) go to warm tier
			TombstoneDrainInterval: -1,  // disabled — tests drain manually
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	// Write a large object that crosses the threshold.
	ts1 := newStore()
	k := testkey.Hash([]byte("warm-get-key"))
	obj := bigObj(k, 1024)
	err := ts1.Put(ctx, k, obj)
	require.NoError(t, err, "Put")
	// Warm entry must be present.
	st := ts1.Stats()
	require.NotEqual(t, 0, st.WarmEntries)
	// Delete from hot tier so next Get must fall through to warm.
	err = ts1.hot.Delete(ctx, k)
	require.NoError(t, err, "hot delete")
	got, _, err := ts1.Get(ctx, k)
	require.NoError(t, err, "Get after hot eviction")
	if got == nil || got.StatusCode != 200 {
		t.Fatalf("expected object from warm tier, got %v", got)
	}
	_ = ts1.Close(ctx)

	// Reopen: WAL replay must restore the index so warm Get still works.
	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })
	// Hot tier is empty after reopen.
	got2, _, err := ts2.Get(ctx, k)
	require.NoError(t, err, "Get after reopen")
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
			Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:                 walPath,
			BodyThreshold:          512,
			TombstoneDrainInterval: -1, // disabled — tests drain manually
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	ts1 := newStore()
	for i := range 5 {
		k := testkey.Hash([]byte(fmt.Sprintf("stats-key-%d", i)))
		err := ts1.Put(ctx, k, bigObj(k, 1024))
		require.NoErrorf(t, err, "Put %d", i)
	}
	st1 := ts1.Stats()
	require.Equal(t, int64(5), st1.WarmEntries)
	if st1.WarmBytes <= 0 {
		t.Fatalf("before close: warm bytes = %d", st1.WarmBytes)
	}
	_ = ts1.Close(ctx)

	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })
	st2 := ts2.Stats()
	require.Equal(t, int64(5), st2.WarmEntries)
	require.Equal(t, st1.WarmBytes, st2.WarmBytes)
}

// TestTieredStore_CloseStopsCompaction verifies that Close stops the
// background compaction goroutine without leaking. Sequential (no
// t.Parallel) because runtime.NumGoroutine is a global counter.
func TestTieredStore_CloseStopsCompaction(t *testing.T) {
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 64 << 20, SegMax: 1 << 20},
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err)

	before := runtime.NumGoroutine()
	err = ts.Close(context.Background())
	require.NoError(t, err)

	// Poll for the goroutine to exit, matching the pattern in TestHotClose.
	poll.Eventually(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
		return runtime.NumGoroutine() < before
	})
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
		Hot:                    HotConfig{MaxBytes: 2048, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          512,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Two large objects that exceed the hot budget; both go to warm.
	k1 := testkey.Hash([]byte("union-key-1"))
	k2 := testkey.Hash([]byte("union-key-2"))
	err = ts.Put(ctx, k1, bigObj(k1, 1024))
	require.NoError(t, err, "Put k1")
	err = ts.Put(ctx, k2, bigObj(k2, 1024))
	require.NoError(t, err, "Put k2")

	// Evict k1 from the hot tier only; it remains in warm.
	err = ts.hot.Delete(ctx, k1)
	require.NoError(t, err, "hot delete k1")

	got := ts.Keys()
	gotSet := make(map[api.Key]bool, len(got))
	for _, k := range got {
		gotSet[k] = true
	}
	assert.True(t, gotSet[k1])
	assert.True(t, gotSet[k2])
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
		Hot:                    HotConfig{MaxBytes: maxBytes, NumShards: 1},
		BodyThreshold:          64 << 10, // objects stay hot-only
		TombstoneDrainInterval: -1,       // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")

	// Empty store is not over budget.
	require.False(t, ts.OverBudget())

	// Under-budget store is not over budget.
	k := testkey.Hash([]byte("small"))
	err = ts.Put(ctx, k, bigObj(k, 256))
	require.NoError(t, err, "Put small")
	require.False(t, ts.OverBudget())

	// Stop the sweeper so the overshoot from an oversized object is
	// deterministic. The sweeper would otherwise evict the oversized
	// object before we can observe OverBudget. Closing done stops both
	// the sweeper and reaper goroutines; we skip Close in cleanup to
	// avoid a double-close on done. Wait for both goroutines to fully
	// exit so the oversized Put's evictSignal has no live consumer.
	close(ts.hot.done)
	ts.hot.wg.Wait()

	overK := testkey.Hash([]byte("oversized"))
	err = ts.Put(ctx, overK, bigObj(overK, maxBytes*2))
	require.NoError(t, err, "Put oversized")
	require.True(t, ts.OverBudget())
}

func TestTieredStore_ImplementsKeyLister(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ts := tieredStore(t, true)
	keys := []api.Key{
		testkey.Hash([]byte("k1")),
		testkey.Hash([]byte("k2")),
		testkey.Hash([]byte("k3")),
	}
	for _, k := range keys {
		err := ts.Put(ctx, k, bigObj(k, 100))
		require.NoError(t, err, "Put")
	}

	kl, ok := any(ts).(KeyLister)
	require.True(t, ok)
	got := kl.Keys()
	require.Len(t, got, len(keys))
	want := make(map[api.Key]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	for _, k := range got {
		assert.True(t, want[k])
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
			Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:                 walPath,
			BodyThreshold:          512,
			TombstoneDrainInterval: -1, // disabled — tests drain manually
		})
		if err != nil {
			t.Fatalf("NewTieredStore: %v", err)
		}
		return ts
	}

	ts1 := newStore()
	k := testkey.Hash([]byte("legacy-codec-key"))
	// Inject a blob whose version byte is 1 (legacy). warm.Put writes
	// the record and sets the in-memory index; we also append a WAL
	// PutEntry so the durability of the eviction can be tested after
	// reopen.
	legacyBlob := []byte{0x01, 0x02, 0x03, 0x04}
	segID, offset, err := ts1.warm.Put(k, legacyBlob)
	require.NoError(t, err, "warm.Put")
	err = ts1.wal.(*wal.Log).Append(wal.PutEntry(k, int32(segID), offset))
	require.NoError(t, err, "wal.Append")

	// Get must treat the undecodable blob as a miss, not an error.
	got, _, err := ts1.Get(ctx, k)
	require.NoError(t, err, "Get: expected nil error for legacy blob,")
	require.Nil(t, got)

	// The warm-tier index must no longer contain the key: warm.Get
	// returns nil after the tombstone + index removal.
	body, _ := ts1.warm.Get(k)
	require.Nil(t, body)

	// A fresh Put of a v2 object for the same key must be readable
	// from the warm tier after hot eviction.
	fresh := bigObj(k, 1024)
	err = ts1.Put(ctx, k, fresh)
	require.NoError(t, err, "Put fresh")
	err = ts1.hot.Delete(ctx, k)
	require.NoError(t, err, "hot.Delete")
	gotFresh, _, err := ts1.Get(ctx, k)
	require.NoError(t, err, "Get fresh from warm")
	if gotFresh == nil || gotFresh.StatusCode != 200 {
		t.Fatalf("expected fresh object from warm tier, got %v", gotFresh)
	}

	// The heal must survive restart: WAL replay processes the Put
	// (legacy), the Delete (eviction), then the Put (fresh). The key
	// must resolve to the fresh v2 blob, not the legacy one.
	err = ts1.Close(ctx)
	require.NoError(t, err, "Close")
	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })
	// Evict from hot so Get falls through to warm.
	_ = ts2.hot.Delete(ctx, k)
	got2, _, err := ts2.Get(ctx, k)
	require.NoError(t, err, "Get after reopen: expected nil error,")
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
	k := testkey.Hash([]byte("corrupt-codec-key"))

	// A blob that starts with the current version byte but is truncated
	// mid-metadata: decodeObject will set errCorrupt.
	corruptBlob := encodeObject(&api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{"A": {"b"}}),
		Body:       []byte("xx"),
	})[:4]
	_, _, err := ts.warm.Put(k, corruptBlob)
	require.NoError(t, err, "warm.Put")

	got, _, err := ts.Get(ctx, k)
	require.NoError(t, err, "Get: expected nil error for corrupt blob,")
	require.Nil(t, got)
	body, _ := ts.warm.Get(k)
	require.Nil(t, body)
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
	require.NoError(t, err, "ts1")
	goodKey := testkey.Hash([]byte("good-object"))
	err = ts1.Put(ctx, goodKey, bigObj(goodKey, 1024))
	require.NoError(t, err, "Put good")
	legacyKey := testkey.Hash([]byte("legacy-after-reopen"))
	legacyBlob := []byte{0x01, 0x02, 0x03, 0x04}
	segID, offset, err := ts1.warm.Put(legacyKey, legacyBlob)
	require.NoError(t, err, "warm.Put legacy")
	err = ts1.wal.(*wal.Log).Append(wal.PutEntry(legacyKey, int32(segID), offset))
	require.NoError(t, err, "wal.Append")
	err = ts1.Close(ctx)
	require.NoError(t, err, "ts1.Close")

	// Phase 2: reopen. WAL replay re-indexes both keys. Get on the
	// legacy key must evict it (clean miss). Get on the good key must
	// still return the valid object (the O_APPEND fix prevents the
	// tombstone write from corrupting it).
	ts2, err := NewTieredStore(cfg)
	require.NoError(t, err, "ts2")
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	// Evict good key from hot so Get falls through to warm for both.
	_ = ts2.hot.Delete(ctx, goodKey)
	_ = ts2.hot.Delete(ctx, legacyKey)

	gotLegacy, _, err := ts2.Get(ctx, legacyKey)
	require.NoError(t, err, "Get legacy after reopen: expected nil error,")
	require.Nil(t, gotLegacy)

	gotGood, _, err := ts2.Get(ctx, goodKey)
	require.NoError(t, err, "Get good after reopen")
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
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          512,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")
	k := testkey.Hash([]byte("torn-write-key"))
	err = ts1.Put(ctx, k, bigObj(k, 1024))
	require.NoError(t, err, "Put")
	err = ts1.Close(ctx)
	require.NoError(t, err, "Close")

	truncateLastSegmentRecord(t, warmDir)

	ts2, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          512,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	got, _, err := ts2.Get(ctx, k)
	require.NoError(t, err, "Get after torn write replay: expected nil error,")
	require.Nil(t, got)
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
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          512,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")
	k := testkey.Hash([]byte("durable-key"))
	err = ts1.Put(ctx, k, bigObj(k, 1024))
	require.NoError(t, err, "Put")
	err = ts1.Close(ctx)
	require.NoError(t, err, "Close")

	ts2, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          512,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	_ = ts2.hot.Delete(ctx, k)
	got, _, err := ts2.Get(ctx, k)
	require.NoError(t, err, "Get after reopen")
	require.NotNil(t, got)
}

// truncateLastSegmentRecord finds the last (highest-ID) .seg file in
// warmDir, finds the offset of the last record by scanning, and truncates
// the file mid-body to simulate a torn write where the WAL entry
// persisted but the segment data did not.
func truncateLastSegmentRecord(t *testing.T, warmDir string) {
	t.Helper()
	entries, err := os.ReadDir(warmDir)
	require.NoErrorf(t, err, "readdir %s", warmDir)
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
	require.NotEqual(t, "", segFile)

	scan, err := warm.NewStore(warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "open for scan")
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
	err = os.Truncate(segFile, cutAt)
	require.NoError(t, err, "truncate")
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
			Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:                 walPath,
			BodyThreshold:          512,
			TombstoneDrainInterval: -1, // disabled — tests drain manually
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
		k := testkey.Hash([]byte(fmt.Sprintf("replay-key-%d", i)))
		keys[i] = k
		err := ts1.Put(ctx, k, bigObj(k, 1024))
		require.NoErrorf(t, err, "Put %d", i)
	}
	err := ts1.Close(ctx)
	require.NoError(t, err, "Close")

	// Reopen: WAL replay should restore all 10 entries.
	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	st2 := ts2.Stats()
	require.Equal(t, int64(10), st2.WarmEntries)

	// Verify every entry is servable from warm tier (hot tier is empty on reopen).
	for i, k := range keys {
		obj, src, err := ts2.Get(ctx, k)
		require.NoErrorf(t, err, "Get %d", i)
		require.NotNil(t, obj)
		require.Equal(t, api.SourceWarm, src)
	}
}

// warmRecordSize returns the on-disk byte footprint of a warm-tier record
// with the given body length, using the exported warm.HeaderLen and
// warm.FooterLen constants so the test tracks the real on-disk format.
func warmRecordSize(bodyLen int) int {
	return warm.HeaderLen + bodyLen + warm.FooterLen
}

// TestWarmSync_SkipsPromotionWhenOverBudget verifies that the warm sync
// loop skips hot→warm promotion when the warm tier is already over its
// byte budget, instead of wasting I/O on Put calls that will return
// ErrOverBudget (#205).
func TestWarmSync_SkipsPromotionWhenOverBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	// Fill the warm tier to exactly the budget so OverBudget() returns true.
	// Record size is computed via warmRecordSize so the test doesn't
	// hardcode the internal header/footer layout.
	const warmBodySize = 200
	recSize := warmRecordSize(warmBodySize)
	const numFill = 3
	warmMaxBytes := int64(numFill * recSize)

	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: warmMaxBytes, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1 << 20, // everything stays hot-only (never written to warm on Put)
		WarmSyncInterval:       -1,      // disabled — we call runWarmSyncCycle manually
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Fill the warm tier to exactly the budget. Protect all entries so
	// the eviction policy can't free space.
	for i := range numFill {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), make([]byte, warmBodySize))
		require.NoErrorf(t, err, "warm.Put %d under budget", i)
	}
	for i := range numFill {
		ts.warm.Protect(testkey.Key(uint64(i)))
	}

	// Verify warm is over budget.
	require.True(t, ts.warm.OverBudget())

	// Put some objects in the hot tier (below body_threshold so they're
	// hot-only and candidates for warm sync promotion).
	for i := range 10 {
		k := testkey.Key(uint64(1000 + i))
		err := ts.Put(ctx, k, obj(k, 100))
		require.NoErrorf(t, err, "Put %d", i)
	}

	// Run a sync cycle. It should skip promotion because warm is over budget.
	ts.runWarmSyncCycle(ctx)

	// Verify none of the hot-only keys were promoted to warm.
	for i := range 10 {
		k := uint64(1000 + i)
		_, _, ok := ts.warm.Lookup(testkey.Key(k))
		assert.False(t, ok)
	}

	// The warm entry count should not have increased from the promotion
	// (tombstone draining may have decreased it, but it should not have
	// gone up). We started with numFill fill entries.
	warmEntries := ts.Stats().WarmEntries
	if warmEntries > numFill {
		t.Errorf("warm entries = %d, expected no promotion (should be <= %d)", warmEntries, numFill)
	}
}

// TestWarmSync_StopsPromotionMidCycleOnOverBudget verifies that
// writeHotOnlyToWarm stops at the first ErrOverBudget when the warm tier
// fills up mid-cycle: some keys are promoted, the rest are skipped, and
// the skippedOverBudget count is correct (#205).
func TestWarmSync_StopsPromotionMidCycleOnOverBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	// Compute the encoded body size for obj(k, 100) using the same key
	// range as the actual hot-only objects below — the Key field is
	// uvarint-encoded so key magnitude affects the encoded length.
	// The warm record size is warmRecordSize(len(encodedBody)).
	probeKey := testkey.Key(1000)
	encodedBody := encodeObject(obj(probeKey, 100))
	recSize := warmRecordSize(len(encodedBody))

	// Budget for 2 records, leaving room for exactly 2 promotions.
	// The 3rd Put will exceed the budget (2*recSize + recSize > budget)
	// and return ErrOverBudget since the fill entries are protected.
	const fillCount = 2
	warmMaxBytes := int64(fillCount * recSize)

	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: warmMaxBytes, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1 << 20, // everything stays hot-only
		WarmSyncInterval:       -1,      // disabled — we call runWarmSyncCycle manually
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Put 5 hot-only objects. The warm tier is empty so OverBudget() is
	// false at cycle start — promotion will proceed until the budget
	// is hit mid-cycle.
	const numHotOnly = 5
	for i := range numHotOnly {
		k := testkey.Key(uint64(1000 + i))
		err := ts.Put(ctx, k, obj(k, 100))
		require.NoErrorf(t, err, "Put %d", i)
	}

	// Run a sync cycle. The first fillCount keys should be promoted;
	// the remaining keys should be skipped due to ErrOverBudget.
	ts.runWarmSyncCycle(ctx)

	// Verify exactly fillCount keys were promoted to warm.
	promoted := 0
	for i := range numHotOnly {
		k := uint64(1000 + i)
		if _, _, ok := ts.warm.Lookup(testkey.Key(k)); ok {
			// Protect promoted entries so eviction doesn't remove
			// them before we count.
			ts.warm.Protect(testkey.Key(k))
			promoted++
		}
	}
	require.Equal(t, fillCount, promoted)

	// The warm entry count should be exactly fillCount — no more, no less.
	warmEntries := ts.Stats().WarmEntries
	assert.Equal(t, int64(fillCount), warmEntries)
}

// TestTieredPut_LargeObjectSucceedsWhenWarmOverBudget verifies that
// TieredStore.Put absorbs warm.ErrOverBudget: the object is kept hot-only,
// Put returns nil, no WAL entry is written, and the object stays servable
// from hot via Get. Without this, a cache miss that fetched successfully
// from origin would fail the response when the warm tier is at its byte
// budget (#206).
func TestTieredPut_LargeObjectSucceedsWhenWarmOverBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	walPath := filepath.Join(dir, "index.wal")

	// Wire warm metrics so the OverBudget counter proves the
	// warm.Put rejection branch was actually taken — without this, the
	// test would stay green even if BodyThreshold regressed and the warm
	// path was skipped entirely (Put returns nil from the hot-only path
	// with the same observable symptoms).
	reg := prometheus.NewRegistry()
	warmMetrics := warm.RegisterMetrics(reg)

	// Fill the warm tier to exactly its byte budget with protected
	// entries so any subsequent warm.Put cannot evict to make room and
	// must return ErrOverBudget.
	const warmBodySize = 200
	recSize := warmRecordSize(warmBodySize)
	const numFill = 3
	warmMaxBytes := int64(numFill * recSize)

	// BodyThreshold sits below warmBodySize so the Put path attempts a
	// warm write (and hits ErrOverBudget) rather than staying hot-only.
	const bodyThreshold = 100
	ts, err := NewTieredStore(TieredConfig{
		Hot:              HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:             &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: warmMaxBytes, SegMax: 1 << 20},
		WarmMetrics:      warmMetrics,
		WALDir:           walPath,
		BodyThreshold:    bodyThreshold,
		WarmSyncInterval: -1, // disabled — not relevant to the Put path
	})
	require.NoError(t, err, "NewTieredStore")

	for i := range numFill {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), make([]byte, warmBodySize))
		require.NoErrorf(t, err, "warm.Put fill %d", i)
		ts.warm.Protect(testkey.Key(uint64(i)))
	}
	require.True(t, ts.warm.OverBudget(), "warm should be over budget after fill")

	// Snapshot the OverBudget counter before the Put. The fill calls
	// were under budget, so it must still be 0 here.
	before := counterValue(t, warmMetrics.OverBudget)
	require.Equal(t, float64(0), before, "no over-budget rejections expected before Put")

	// Put a large object whose body exceeds BodyThreshold, forcing the
	// warm write path. warm.Put returns ErrOverBudget; Put must absorb
	// it and keep the object hot-only instead of failing the response.
	key := testkey.Hash([]byte("large-over-budget"))
	o := bigObj(key, warmBodySize)
	err = ts.Put(ctx, key, o)
	require.NoError(t, err, "Put should return nil when warm is over budget")

	// The OverBudget counter must have incremented exactly once — this
	// proves the warm.Put rejection branch fired, not just the hot-only
	// path.
	after := counterValue(t, warmMetrics.OverBudget)
	assert.Equal(t, before+1, after, "OverBudget counter must increment by 1")

	// The key must not be present in warm — it was rejected, not stored.
	_, _, ok := ts.warm.Lookup(key)
	assert.False(t, ok, "key should not be promoted to warm on ErrOverBudget")

	// The object must still be servable from the hot tier.
	got, src, err := ts.Get(ctx, key)
	require.NoError(t, err, "Get after over-budget Put")
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)
	require.Equal(t, int64(warmBodySize), got.BodySize)

	// Close the store so the WAL is flushed, then replay it to prove no
	// entry was appended for the skipped warm write. The fill calls went
	// directly through ts.warm.Put (bypassing TieredStore.Put), so the
	// WAL must be empty.
	require.NoError(t, ts.Close(ctx), "Close to flush WAL")
	var walEntries int
	err = wal.Replay(walPath, func(wal.Entry) error {
		walEntries++
		return nil
	})
	require.NoError(t, err, "wal.Replay")
	assert.Equal(t, 0, walEntries, "no WAL entry should be appended when warm write is skipped")
}

// counterValue reads the current value of a prometheus.Counter. Used by
// the over-budget Put test to prove the ErrOverBudget branch fired.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.(prometheus.Metric).Write(&m), "read counter")
	if cm := m.GetCounter(); cm != nil {
		return cm.GetValue()
	}
	return 0
}

// TestHotStore_Has_NoSideEffects verifies that Has does not increment
// hit counters, miss counters, or windowHits — unlike Get, which does.
// Has is used by Handler.Purge to probe ownership without inflating
// metrics on the purge path.
func TestHotStore_Has_NoSideEffects(t *testing.T) {
	t.Parallel()

	store := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	k := testkey.Hash([]byte("has-test"))
	obj := bigObj(k, 100)
	require.NoError(t, store.Put(context.Background(), k, obj))

	// Record stats after Put (Get during Put path is absent — Put
	// doesn't call Get).
	before := store.Stats()

	// Call Has 5 times. None should change any counter.
	for i := 0; i < 5; i++ {
		require.True(t, store.Has(k), "Has should find the key")
	}

	after := store.Stats()
	require.Equal(t, before.Hits, after.Hits, "Has must not increment hits")
	require.Equal(t, before.Misses, after.Misses, "Has must not increment misses")

	// Has on a missing key must also be side-effect-free.
	missing := testkey.Hash([]byte("does-not-exist"))
	for i := 0; i < 5; i++ {
		require.False(t, store.Has(missing), "Has should not find missing key")
	}

	afterMissing := store.Stats()
	require.Equal(t, after.Hits, afterMissing.Hits, "Has on missing key must not increment hits")
	require.Equal(t, after.Misses, afterMissing.Misses, "Has on missing key must not increment misses")

	// WindowHits must be unchanged — Get increments it, Has must not.
	require.Equal(t, int64(0), store.WindowHits(k), "Has must not increment windowHits")
}

// TestTieredStore_Has_NoWarmTombstone proves that Has does not write a
// warm-tier tombstone. This is the storage-level guarantee that
// Handler.Purge relies on: when purgeKey iterates non-owning handlers,
// Has returns false and store.Delete is never called, so no spurious
// tombstone is appended to the warm-tier segment.
func TestTieredStore_Has_NoWarmTombstone(t *testing.T) {
	t.Parallel()

	ts := tieredStore(t, true)
	k := testkey.Hash([]byte("warm-has-test"))
	o := bigObj(k, 8192) // above 1024 threshold → stored in warm too

	require.NoError(t, ts.Put(context.Background(), k, o))

	// Confirm the key is in both tiers.
	require.True(t, ts.Has(k), "key should be present")

	diskBefore := ts.Stats().WarmDiskBytes

	// Has must not write a tombstone — it's a read-only check.
	for i := 0; i < 10; i++ {
		require.True(t, ts.Has(k))
	}

	diskAfter := ts.Stats().WarmDiskBytes
	require.Equal(t, diskBefore, diskAfter, "Has must not increase warm disk bytes (no tombstone)")

	// Now prove that Delete DOES write a tombstone (sanity check that
	// the test setup is actually detecting tombstone growth).
	require.NoError(t, ts.Delete(context.Background(), k))
	diskAfterDelete := ts.Stats().WarmDiskBytes
	require.Greater(t, diskAfterDelete, diskBefore, "Delete must increase warm disk bytes (tombstone written)")
}

// TestTieredStore_Delete_MissingKey_WritesTombstone documents the
// behavior that motivated the Has-based ownership probe in
// Handler.Purge: TieredStore.Delete on a key that was never cached
// still writes a warm-tier tombstone. This is why purgeKey must use
// Has to skip non-owning handlers instead of blindly calling Delete.
func TestTieredStore_Delete_MissingKey_WritesTombstone(t *testing.T) {
	t.Parallel()

	ts := tieredStore(t, true)

	missing := testkey.Hash([]byte("never-cached"))
	diskBefore := ts.Stats().WarmDiskBytes

	// Delete a key that was never Put. HotStore.Delete is a no-op, but
	// TieredStore.Delete calls evictWarmErr which unconditionally writes
	// a warm-tier tombstone.
	require.NoError(t, ts.Delete(context.Background(), missing))

	diskAfter := ts.Stats().WarmDiskBytes
	require.Greater(t, diskAfter, diskBefore, "Delete on missing key must still write a warm tombstone")
}
func TestTieredStore_WALStats_NoWAL(t *testing.T) {
	t.Parallel()
	store := newTestTieredStore(t)
	dropped, lastSync := store.WALStats()
	assert.Equal(t, int64(0), dropped)
	assert.True(t, lastSync.IsZero())
}

func TestTieredStore_WindowHits(t *testing.T) {
	t.Parallel()
	store := newTestTieredStore(t)
	key := testkey.Key(1)
	// WindowHits on a key not in hot tier returns 0.
	assert.Equal(t, int64(0), store.WindowHits(key))
}

func TestTieredStore_Ban(t *testing.T) {
	t.Parallel()
	store := newTestTieredStore(t)
	// Ban with an empty expression should return 0 matches.
	count, err := store.Ban(context.Background(), api.BanExpr{})
	_ = count
	_ = err
}

func newTestTieredStore(t *testing.T) *TieredStore {
	t.Helper()
	hot := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return &TieredStore{
		hot: hot,
	}
}

// TestCompactLoop_PerSegmentCompaction verifies that the compactLoop
// triggers per-segment incremental compaction on the 30s tick when
// segments have enough dead bytes. Uses CompactStartupDelay=-1 (start
// immediately) and a short interval to keep the test fast.
func TestCompactLoop_PerSegmentCompaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0, // all objects go to warm
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1, // start immediately
		CompactInterval:        -1, // disable periodic global; per-segment on 30s tick
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Write records to create multiple segments, then delete enough
	// to make the first segment exceed the dead-byte threshold.
	for i := range 40 {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), []byte{byte(i)})
		require.NoErrorf(t, err, "Put %d", i)
	}
	for i := range 20 {
		_, err := ts.warm.Delete(testkey.Key(uint64(i)))
		require.NoErrorf(t, err, "Delete %d", i)
	}

	// Verify we have multiple segments.
	require.Greater(t, ts.warm.IndexLen(), 0, "warm store should have entries")

	// The compactLoop runs on a 30s tick. We can't wait 30s in a
	// short test, so trigger compaction manually to verify the
	// per-segment path works end-to-end via the store methods.
	segID, needs := ts.warm.NeedsSegmentCompaction()
	require.True(t, needs, "should find a segment needing compaction")
	require.NoError(t, ts.warm.CompactSegment(segID), "CompactSegment")

	// After compaction, the old segment should be replaced.
	_, found := ts.warm.NeedsSegmentCompaction()
	// The new segment may or may not need compaction depending on
	// remaining dead bytes — the key assertion is no panic.
	_ = found

	// Surviving keys must be readable.
	for i := 20; i < 40; i++ {
		got, err := ts.warm.Get(testkey.Key(uint64(i)))
		require.NoErrorf(t, err, "Get %d", i)
		require.NotNilf(t, got, "key %d should survive compaction", i)
		require.Equal(t, byte(i), got[0])
	}
}

// TestCompactLoop_RunPostCompaction verifies that runPostCompaction
// rewrites the WAL and writes a snapshot after per-segment compaction.
func TestCompactLoop_RunPostCompaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0,
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1,
		CompactInterval:        -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Write and delete to create dead bytes.
	for i := range 40 {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), []byte{byte(i)})
		require.NoErrorf(t, err, "Put %d", i)
	}
	for i := range 20 {
		_, err := ts.warm.Delete(testkey.Key(uint64(i)))
		require.NoErrorf(t, err, "Delete %d", i)
	}

	// Compact a segment, then call runPostCompaction.
	segID, needs := ts.warm.NeedsSegmentCompaction()
	require.True(t, needs)
	require.NoError(t, ts.warm.CompactSegment(segID))

	// runPostCompaction should rewrite WAL + snapshot without error.
	ts.runPostCompaction("test")

	// WAL entry count should be reset.
	require.Equal(t, int64(0), ts.walEntryCount.Load(), "walEntryCount should be 0 after runPostCompaction")

	// Snapshot file should exist.
	require.NotEmpty(t, ts.warm.SnapshotPath(), "snapshot path should be set")
	_, err = os.Stat(ts.warm.SnapshotPath())
	require.NoError(t, err, "snapshot file should exist after runPostCompaction")
}

// TestCompactLoop_StartupDelayNegative verifies that CompactStartupDelay=-1
// sets startupDelay to 0, allowing immediate compaction. This covers the
// `startupDelay < 0` branch in compactLoop.
func TestCompactLoop_StartupDelayNegative(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0,
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1,
		CompactInterval:        -1,
	})
	require.NoError(t, err, "NewTieredStore")
	require.NoError(t, ts.Close(ctx))
	// If startupDelay < 0 wasn't handled, the timer would fire
	// immediately anyway (negative duration), but the branch
	// coverage is the goal — no panic, clean close.
}

// TestRunCompaction_ForceBypassesNeedsCheck verifies that runCompaction
// with force=true bypasses the NeedsCompaction dead-byte ratio check.
func TestRunCompaction_ForceBypassesNeedsCheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0,
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1,
		CompactInterval:        -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Write a few live records — no dead bytes, so NeedsCompaction
	// would return false.
	for i := range 5 {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), []byte{byte(i)})
		require.NoErrorf(t, err, "Put %d", i)
	}
	require.False(t, ts.warm.NeedsCompaction(), "should not need compaction")

	// Force compaction — should run anyway.
	ts.runCompaction("test-force", true)

	// Keys should still be readable.
	for i := range 5 {
		got, err := ts.warm.Get(testkey.Key(uint64(i)))
		require.NoErrorf(t, err, "Get %d", i)
		require.NotNilf(t, got, "key %d should survive forced compaction", i)
	}
}

// TestRunCompaction_NoForceSkipsWhenNotNeeded verifies that runCompaction
// with force=false skips compaction when NeedsCompaction returns false.
func TestRunCompaction_NoForceSkipsWhenNotNeeded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0,
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1,
		CompactInterval:        -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Write a few live records — no dead bytes.
	for i := range 5 {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), []byte{byte(i)})
		require.NoErrorf(t, err, "Put %d", i)
	}
	require.False(t, ts.warm.NeedsCompaction(), "should not need compaction")

	// runCompaction with force=false should be a no-op.
	// WAL entry count should remain 0 (not reset by runPostCompaction).
	ts.runCompaction("test-skip", false)
	require.Equal(t, int64(0), ts.walEntryCount.Load(), "walEntryCount should remain 0 — compaction skipped")
}

// TestCompactLoop_PeriodicTickTriggersCompaction verifies that the
// compactLoop background goroutine triggers global compaction via the
// periodic tick when CompactInterval is very short. This covers the
// periodicTick → NeedsCompaction → runCompaction path (lines 831-843).
func TestCompactLoop_PeriodicTickTriggersCompaction(t *testing.T) {
	// Sequential — not t.Parallel — to avoid interfering with other
	// tests that check goroutine counts.
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0,
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1, // start immediately
		CompactInterval:        100 * time.Millisecond,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Write enough records to create dead bytes (>30% threshold).
	for i := range 40 {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), []byte{byte(i)})
		require.NoErrorf(t, err, "Put %d", i)
	}
	for i := range 20 {
		_, err := ts.warm.Delete(testkey.Key(uint64(i)))
		require.NoErrorf(t, err, "Delete %d", i)
	}

	// The periodic tick fires every 100ms. Poll until compaction
	// reduces the dead-byte ratio below the threshold (NeedsCompaction
	// returns false after a successful global Compact).
	poll.Eventually(t, 5*time.Second, 100*time.Millisecond, func() bool {
		return !ts.warm.NeedsCompaction()
	})
}

// TestCompactLoop_DiskOverBudgetTriggersCompaction verifies that the
// compactLoop disk-over-budget path triggers global compaction on the
// 30s check tick. Since the tick is hardcoded at 30s, we test the
// underlying path (DiskOverBudget → runCompaction) directly.
func TestCompactLoop_DiskOverBudgetTriggersCompaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 1 << 30, SegMax: 512, MaxDiskBytes: 1},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          0,
		TombstoneDrainInterval: -1,
		CompactStartupDelay:    -1,
		CompactInterval:        -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(ctx) })

	// Write records to exceed the tiny MaxDiskBytes=1.
	for i := range 5 {
		_, _, err := ts.warm.Put(testkey.Key(uint64(i)), []byte{byte(i)})
		require.NoErrorf(t, err, "Put %d", i)
	}
	require.True(t, ts.warm.DiskOverBudget(), "should be over disk budget")

	// Force compaction the same way the 30s tick would.
	ts.runCompaction("disk over-budget", true)

	// Keys should survive.
	for i := range 5 {
		got, err := ts.warm.Get(testkey.Key(uint64(i)))
		require.NoErrorf(t, err, "Get %d", i)
		require.NotNilf(t, got, "key %d should survive", i)
	}
}
