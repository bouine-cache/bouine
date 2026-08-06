package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/warm"
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
		k := api.KeyFromPrimary(uint64(i + 1))
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
		k := api.KeyFromPrimary(uint64(i + 1))
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
		k := api.KeyFromPrimary(uint64(i + 1))
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
		k := api.KeyFromPrimary(uint64(i + 1))
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
		k := api.KeyFromPrimary(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")

	for i := 10; i < 20; i++ {
		k := api.KeyFromPrimary(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put delta %d", i)
	}

	err = ts.Delete(context.Background(), api.KeyFromPrimary(1))
	require.NoError(t, err, "delete")

	err = ts.Close(context.Background())
	require.NoError(t, err, "close")

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	obj, _, err := ts2.Get(context.Background(), api.KeyFromPrimary(1))
	require.NoError(t, err, "get deleted key")
	require.Nil(t, obj)

	for i := 1; i < 20; i++ {
		k := api.KeyFromPrimary(uint64(i + 1))
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
		k := api.KeyFromPrimary(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	require.Equal(t, int64(5), ts.walEntryCount.Load())

	err := ts.checkpoint()
	require.NoError(t, err, "checkpoint")

	require.Equal(t, int64(0), ts.walEntryCount.Load())

	for i := 5; i < 10; i++ {
		k := api.KeyFromPrimary(uint64(i + 1))
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d", i)
	}

	require.Equal(t, int64(5), ts.walEntryCount.Load())
}
