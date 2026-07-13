package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	return ts
}

func TestCheckpointAndSnapshotRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 20 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		if err := ts.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if err := ts.checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if ts.walEntryCount.Load() != 0 {
		t.Fatalf("walEntryCount = %d after checkpoint, want 0", ts.walEntryCount.Load())
	}

	snapPath := ts.warm.SnapshotPath()
	if snapPath == "" {
		t.Fatal("empty snapshot path")
	}
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("snapshot file not created by checkpoint: %v", err)
	}

	if err := ts.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 20 {
		k := api.Key(i + 1)
		obj, src, err := ts2.Get(context.Background(), k)
		if err != nil {
			t.Fatalf("get %d after restart: %v", i, err)
		}
		if obj == nil {
			t.Fatalf("key %d missing after checkpoint+restart", i+1)
		}
		if src != api.SourceWarm {
			t.Fatalf("key %d source = %q, want %q", i+1, src, api.SourceWarm)
		}
	}
}

func TestSnapshotFallbackOnMissingSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 10 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		if err := ts.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if err := ts.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	snapPath := filepath.Join(dir, "warm", "index.snap")
	_ = os.Remove(snapPath)

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	for i := range 10 {
		k := api.Key(i + 1)
		obj, _, err := ts2.Get(context.Background(), k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if obj == nil {
			t.Fatalf("key %d missing after restart without snapshot", i+1)
		}
	}
}

func TestSnapshotWithWALDelta(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts := newTieredStoreWithDir(t, dir)

	for i := range 10 {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		if err := ts.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if err := ts.checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	for i := 10; i < 20; i++ {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		if err := ts.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put delta %d: %v", i, err)
		}
	}

	if err := ts.Delete(context.Background(), api.Key(1)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := ts.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts2 := newTieredStoreWithDir(t, dir)
	defer func() { _ = ts2.Close(context.Background()) }()

	obj, _, err := ts2.Get(context.Background(), api.Key(1))
	if err != nil {
		t.Fatalf("get deleted key: %v", err)
	}
	if obj != nil {
		t.Fatal("key 1 should be deleted")
	}

	for i := 1; i < 20; i++ {
		k := api.Key(i + 1)
		obj, _, err := ts2.Get(context.Background(), k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if obj == nil {
			t.Fatalf("key %d missing after snapshot+WAL delta restart", i+1)
		}
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
		if err := ts.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if ts.walEntryCount.Load() != 5 {
		t.Fatalf("walEntryCount = %d, want 5", ts.walEntryCount.Load())
	}

	if err := ts.checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	if ts.walEntryCount.Load() != 0 {
		t.Fatalf("walEntryCount = %d after checkpoint, want 0", ts.walEntryCount.Load())
	}

	for i := 5; i < 10; i++ {
		k := api.Key(i + 1)
		o := bigObj(k, 2048)
		if err := ts.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if ts.walEntryCount.Load() != 5 {
		t.Fatalf("walEntryCount = %d after post-checkpoint writes, want 5", ts.walEntryCount.Load())
	}
}
