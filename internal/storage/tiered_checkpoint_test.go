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
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:          warmCfg,
		WALDir:        filepath.Join(dir, "index.wal"),
		BodyThreshold: 1024,
	})
	require.NoErrorf(t, err, "NewTieredStore: %v", err)
	return ts
}

func TestCheckpointAndSnapshotRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 20 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d: %v", i, err)
	}

	err := ts.checkpoint()
	require.NoErrorf(t, err, "checkpoint: %v", err)

	require.Equal(t, int64(0), ts.walEntryCount.Load())

	snapPath := ts.warm.SnapshotPath()
	require.NotEqual(t, "", snapPath)
	_, err = os.Stat(snapPath)
	require.NoErrorf(t, err, "snapshot file not created by checkpoint: %v", err)

	err = ts.Close(context.Background())
	require.NoErrorf(t, err, "close: %v", err)

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 20 {
		k := api.Key(i + 1)
		obj, src, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d after restart: %v", i, err)
		require.NotNil(t, obj)
		require.Equal(t, api.SourceWarm, src)
	}
}

func TestSnapshotFallbackOnMissingSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 10 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d: %v", i, err)
	}

	err := ts.Close(context.Background())
	require.NoErrorf(t, err, "close: %v", err)

	snapPath := filepath.Join(dir, "warm", "index.snap")
	_ = os.Remove(snapPath)

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 10 {
		k := api.Key(i + 1)
		obj, _, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d: %v", i, err)
		require.NotNil(t, obj)
	}
}

func TestSnapshotWithWALDelta(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 10 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d: %v", i, err)
	}

	err := ts.checkpoint()
	require.NoErrorf(t, err, "checkpoint: %v", err)

	for i := 10; i < 20; i++ {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put delta %d: %v", i, err)
	}

	err = ts.Delete(context.Background(), api.Key(1))
	require.NoErrorf(t, err, "delete: %v", err)

	err = ts.Close(context.Background())
	require.NoErrorf(t, err, "close: %v", err)

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	obj, _, err := ts2.Get(context.Background(), api.Key(1))
	require.NoErrorf(t, err, "get deleted key: %v", err)
	require.Nil(t, obj)

	for i := 1; i < 20; i++ {
		k := api.Key(i + 1)
		obj, _, err := ts2.Get(context.Background(), k)
		require.NoErrorf(t, err, "get %d: %v", i, err)
		require.NotNil(t, obj)
	}
}

func TestCheckpointTruncatesWAL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts.Close(context.Background()) }()

	for i := range 5 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d: %v", i, err)
	}

	require.Equal(t, int64(5), ts.walEntryCount.Load())

	err := ts.checkpoint()
	require.NoErrorf(t, err, "checkpoint: %v", err)

	require.Equal(t, int64(0), ts.walEntryCount.Load())

	for i := 5; i < 10; i++ {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		err := ts.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d: %v", i, err)
	}

	require.Equal(t, int64(5), ts.walEntryCount.Load())
}
