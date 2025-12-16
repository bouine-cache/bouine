package storage

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/storage/wal"
	"github.com/thylong/bouine/internal/storage/warm"
	"github.com/thylong/bouine/pkg/api"
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
		Header:     http.Header{"Content-Type": {"application/octet-stream"}},
		Body:       make([]byte, bodySize),
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        time.Minute,
	}
}

func TestTiered_HotOnly(t *testing.T) {
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
	ts := tieredStore(t, false)
	k := KeyHash([]byte("ephemeral"))
	_ = ts.Put(context.Background(), k, bigObj(k, 2048))

	st := ts.Stats()
	if st.WarmEntries != 0 {
		t.Fatalf("ephemeral mode should have 0 warm entries, got %d", st.WarmEntries)
	}
}
