package storage

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/storage/wal"
	"github.com/thylong/bouine/internal/storage/warm"
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
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
		Header:     http.Header{header.ContentType: {"application/octet-stream"}},
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
	got, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit")
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
	got, err := ts.Get(context.Background(), k)
	if err != nil || got == nil {
		t.Fatalf("get: err=%v got=%v", err, got)
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

func TestTiered_DeleteBothTiers(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := KeyHash([]byte("del-both"))
	o := bigObj(k, 2048) // above threshold

	_ = ts.Put(context.Background(), k, o)
	_ = ts.Delete(context.Background(), k)

	got, _ := ts.Get(context.Background(), k)
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
	got, err := ts1.Get(ctx, k)
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
	got2, err := ts2.Get(ctx, k)
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
