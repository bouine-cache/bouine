package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/wal"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func newTieredStoreWithDir(t *testing.T, dir string) *TieredStore {
	t.Helper()
	warmCfg := &warm.Config{
		Dir:      filepath.Join(dir, "warm"),
		MaxBytes: 100 << 20,
		SegMax:   1 << 20,
	}
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   warmCfg,
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		TombstoneDrainInterval: -1, // disabled — tests drain manually
	})
	require.NoError(t, err, "NewTieredStore")
	return ts
}

func TestCheckpointAndSnapshotRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 20 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")

	require.Equal(t, int64(0), ts.walEntryCount.Load())

	snapPath := ts.warm.SnapshotPath()
	require.NotEqual(t, "", snapPath)
	_, err = os.Stat(snapPath)
	require.NoError(t, err, "snapshot file not created by checkpoint")

	err = ts.Close(context.Background())
	require.NoError(t, err, "close")

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 20 {
		k := testkey.Key(uint64(i + 1))
		obj, src, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d after restart", i)
		require.NotNil(t, obj)
		require.Equal(t, api.SourceWarm, src)
	}
}

func TestSnapshotFallbackOnMissingSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 10 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	err := ts.Close(context.Background())
	require.NoError(t, err, "close")

	snapPath := filepath.Join(dir, "warm", "index.snap")
	_ = os.Remove(snapPath)

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 10 {
		k := testkey.Key(uint64(i + 1))
		obj, _, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d", i)
		require.NotNil(t, obj)
	}
}

func TestSnapshotWithWALDelta(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 10 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")

	for i := 10; i < 20; i++ {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put delta %d", i)
	}

	err = ts.Delete(context.Background(), testkey.Key(1))
	require.NoError(t, err, "delete")

	err = ts.Close(context.Background())
	require.NoError(t, err, "close")

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	obj, _, err := ts2.Get(context.Background(), testkey.Key(1))
	require.NoError(t, err, "get deleted key")
	require.Nil(t, obj)

	for i := 1; i < 20; i++ {
		k := testkey.Key(uint64(i + 1))
		obj, _, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d", i)
		require.NotNil(t, obj)
	}
}

func TestCheckpointTruncatesWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts.Close(context.Background()) }()

	for i := range 5 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	require.Equal(t, int64(5), ts.walEntryCount.Load())

	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")

	require.Equal(t, int64(0), ts.walEntryCount.Load())

	for i := 5; i < 10; i++ {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	require.Equal(t, int64(5), ts.walEntryCount.Load())
}

// TestClose_FinalSnapshotAndTruncate verifies that Close performs a
// crash-safe final checkpoint when there are uncheckpointed WAL entries:
// the index snapshot is written and the WAL is truncated, so the next
// startup loads the snapshot directly without WAL replay or a segment
// scan. Without the truncate, the WAL would grow across restarts and the
// next startup would still replay the now-redundant WAL on top of the
// snapshot — the exact regression PR #478 introduced and this test pins.
func TestClose_FinalSnapshotAndTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 5 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	// No explicit checkpoint — the periodic loop defaults to 5m and the
	// threshold is 100k entries, so neither fires during this test.
	require.Equal(t, int64(5), ts.walEntryCount.Load(),
		"precondition: WAL has uncheckpointed entries")

	walPath := filepath.Join(dir, "index.wal")
	snapPath := ts.warm.SnapshotPath()

	err := ts.Close(context.Background())
	require.NoError(t, err, "close")

	// Snapshot must exist and be non-empty.
	fi, err := os.Stat(snapPath)
	require.NoError(t, err, "snapshot file written on close")
	require.Greater(t, fi.Size(), int64(0), "snapshot is not empty")

	// WAL must be truncated to zero bytes — this is the regression guard.
	// If a future change drops the Truncate call, the WAL retains all
	// entries and the next startup replays them redundantly.
	wfi, err := os.Stat(walPath)
	require.NoError(t, err, "wal file still exists after close")
	require.Equal(t, int64(0), wfi.Size(),
		"wal truncated to zero after final checkpoint on close")

	// Reopen and verify the snapshot-only fast path serves every key
	// without relying on WAL replay or segment scan.
	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 5 {
		k := testkey.Key(uint64(i + 1))
		obj, src, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d after reopen", i)
		require.NotNil(t, obj, "key %d present after reopen", i)
		require.Equal(t, api.SourceWarm, src, "key %d served from warm", i)
	}

	// After reopen via snapshot, the WAL should still be empty (no new
	// writes), proving the snapshot carried the full index state.
	wfi2, err := os.Stat(walPath)
	require.NoError(t, err, "wal file exists after reopen")
	require.Equal(t, int64(0), wfi2.Size(),
		"wal remains empty after snapshot-only reopen")
}

// TestSnapshotCorruptFallbackToWALReplay verifies that a corrupt snapshot
// file triggers the Warn-level fallback to WAL replay (not the Info-level
// "no snapshot" branch). This covers the os.IsNotExist false / else branch.
func TestSnapshotCorruptFallbackToWALReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 5 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")

	err = ts.Close(context.Background())
	require.NoError(t, err, "close")

	// Corrupt the snapshot file — write garbage that will fail the magic
	// check in LoadSnapshot, returning ErrSnapshotInvalid (not os.ErrNotExist).
	snapPath := filepath.Join(dir, "warm", "index.snap")
	require.NoError(t, os.WriteFile(snapPath, []byte("corrupt-snapshot-garbage"), 0o600))

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	// WAL was truncated by checkpoint, so WAL replay produces an empty
	// index. The segment scan rebuilds from the warm-tier segment files.
	// All 5 keys should be recoverable despite the corrupt snapshot.
	for i := range 5 {
		k := testkey.Key(uint64(i + 1))
		obj, _, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d after corrupt snapshot", i)
		require.NotNil(t, obj, "key %d recovered via segment scan", i)
	}
}

// TestInitWAL_FreshStartSegmentScan covers the walEntries == 0 Info
// branch in initWAL: a warm dir with segment files from a previous run
// that was checkpointed (WAL truncated) but whose snapshot was removed.
// On reopen, there's no snapshot, no WAL entries, and the index is empty
// — so initWAL logs "warm index is empty; running segment scan" and
// rebuilds from segments.
func TestInitWAL_FreshStartSegmentScan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 3 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	// Checkpoint to truncate the WAL and write the snapshot.
	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")
	err = ts.Close(context.Background())
	require.NoError(t, err, "close")

	// Remove the snapshot so reopen takes the "no snapshot, no WAL
	// entries" path and falls back to segment scan.
	snapPath := filepath.Join(dir, "warm", "index.snap")
	require.NoError(t, os.Remove(snapPath))

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	// Segment scan should recover all keys.
	for i := range 3 {
		k := testkey.Key(uint64(i + 1))
		obj, _, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d after fresh-start segment scan", i)
		require.NotNil(t, obj, "key %d recovered via segment scan", i)
	}
}

// TestClose_FinalSnapshotWriteFailure covers the WriteSnapshot error
// branch on Close: if the warm dir is removed after entries are written,
// WriteSnapshot fails to create the tmp file and logs a Warn. Close
// must still complete without panicking, and the WAL is not truncated.
func TestClose_FinalSnapshotWriteFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 3 {
		k := testkey.Key(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	require.Equal(t, int64(3), ts.walEntryCount.Load())

	// Remove the warm directory so WriteSnapshot's os.OpenFile on the
	// tmp path fails (parent dir does not exist).
	warmDir := filepath.Join(dir, "warm")
	require.NoError(t, os.RemoveAll(warmDir))

	// Close must not panic even though the snapshot write fails.
	err := ts.Close(context.Background())
	require.NoError(t, err, "close despite snapshot write failure")
}

// TestInitWAL_WALReplayEmptyIndex covers the walEntries > 0 else branch
// in initWAL: a WAL with delete entries but no snapshot. WAL replay
// processes the deletes but the index stays empty (the keys were never
// put). needRebuild is true, walEntries > 0, so initWAL logs "wal
// replay produced empty index; rebuilding from segment scan" and runs
// the segment scan (which is a no-op since no segments exist).
func TestInitWAL_WALReplayEmptyIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "index.wal")

	// Write a delete entry to the WAL directly. No snapshot, no
	// segments — just a WAL with a delete for a non-existent key.
	w, err := wal.Open(walPath)
	require.NoError(t, err, "open wal")
	require.NoError(t, w.Enqueue(wal.DeleteEntry(testkey.Key(42))), "enqueue delete")
	require.NoError(t, w.Sync(), "sync wal")
	require.NoError(t, w.Close(), "close wal")

	// Reopen with a TieredStore pointing at the same dir. initWAL
	// finds no snapshot, replays the WAL (1 delete entry), the index
	// stays empty, walEntries > 0 → takes the "wal replay produced
	// empty index" else branch.
	ts := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts.Close(context.Background()) }()

	// No keys should be present (the delete was for a non-existent key
	// and there are no segments to scan).
	obj, _, err := ts.Get(context.Background(), testkey.Key(42))
	require.NoError(t, err, "get non-existent key")
	require.Nil(t, obj, "key not present after empty WAL replay")
}
