package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// muWriter is a thread-safe bytes.Buffer wrapper for slog output.
// Required because the sweeper goroutine writes logs concurrently
// with the test reading the buffer after Close.
type muWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *muWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func newCaptureLogger(mu *sync.Mutex, buf *bytes.Buffer) observability.Logger {
	return observability.NewSampledLogger(
		slog.New(slog.NewJSONHandler(&muWriter{mu: mu, buf: buf}, &slog.HandlerOptions{Level: slog.LevelDebug})),
		0,
	)
}

func parseLogRecords(t *testing.T, mu *sync.Mutex, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	var records []map[string]any
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("unmarshal log line: %v\nline: %s", err, line)
		}
		records = append(records, rec)
	}
	return records
}

func TestEvictionLogging_WarmBacked(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var buf bytes.Buffer
	// Each object with 100-byte body costs ~470 bytes (struct overhead).
	// Budget fits 2 objects; the 3rd triggers inline eviction.
	h := NewHotStore(HotConfig{
		MaxBytes:       1000,
		NumShards:      1,
		ReaperInterval: -1,
		Logger:         newCaptureLogger(&mu, &buf),
	})

	ctx := context.Background()
	_ = h.Put(ctx, api.Key(1), &api.Object{
		Key: 1, Body: make([]byte, 100),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	_ = h.Put(ctx, api.Key(2), &api.Object{
		Key: 2, Body: make([]byte, 100),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	h.SetWarm(api.Key(1))
	_ = h.Put(ctx, api.Key(3), &api.Object{
		Key: 3, Body: make([]byte, 100),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	h.Close(ctx)

	records := parseLogRecords(t, &mu, &buf)
	found := false
	for _, rec := range records {
		if rec["msg"] == "evicted from hot store" && rec["had_warm_backup"] == true {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a warm-backed eviction log at Info level")
	}
}

func TestEvictionLogging_NoWarmBackup(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var buf bytes.Buffer
	h := NewHotStore(HotConfig{
		MaxBytes:       1000,
		NumShards:      1,
		ReaperInterval: -1,
		Logger:         newCaptureLogger(&mu, &buf),
	})

	ctx := context.Background()
	_ = h.Put(ctx, api.Key(1), &api.Object{
		Key: 1, Body: make([]byte, 100),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	_ = h.Put(ctx, api.Key(2), &api.Object{
		Key: 2, Body: make([]byte, 100),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	_ = h.Put(ctx, api.Key(3), &api.Object{
		Key: 3, Body: make([]byte, 100),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	h.Close(ctx)

	records := parseLogRecords(t, &mu, &buf)
	found := false
	for _, rec := range records {
		if rec["msg"] == "evicted from hot store" && rec["had_warm_backup"] == false {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a no-warm-backup eviction log at Warn level")
	}
}

func TestEvictionLogging_Expired(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var buf bytes.Buffer
	h := NewHotStore(HotConfig{
		MaxBytes:       1 << 20,
		NumShards:      1,
		ReaperInterval: -1,
		Logger:         newCaptureLogger(&mu, &buf),
	})

	ctx := context.Background()
	_ = h.Put(ctx, api.Key(1), &api.Object{
		Key: 1, Body: make([]byte, 10),
		StoredAt: time.Now().Add(-2 * time.Hour),
		TTL:      time.Second,
	})
	h.reapShard(0, time.Now())
	h.Close(ctx)

	records := parseLogRecords(t, &mu, &buf)
	found := false
	for _, rec := range records {
		if rec["msg"] == "evicted from hot store" && rec["reason"] == "expired" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected an expired eviction log")
	}
}

func TestEvictionLogging_SweeperOvershoot(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var buf bytes.Buffer
	// Budget fits ~2 objects (~470 bytes each with overhead).
	// Inserting 8 objects: each Put does up to 4 inline evictions.
	// After the 3rd Put, the shard is over budget with 1 entry but
	// inline eviction already removed 4. The 4th+ Put sends the
	// sweeper signal because after inline eviction of 4 + insert,
	// the shard is still over budget (only 2 fit, but 4 were
	// evicted leaving room). Actually, the sweeper fires when
	// stillOver=true after inline cap. This happens when even after
	// evicting inlineEvictCap (4) entries, the shard + new entry
	// still exceeds perShardMax. With budget 1000 and ~470 bytes per
	// object, 2 objects fit (~940). After 3rd Put, 4 are evicted
	// leaving 0 + new = ~470, which is under 1000. So stillOver=false.
	// Need a scenario where even after evicting 4, it's still over.
	// Use a large object that fills the shard past capacity even
	// after 4 evictions. Budget 500, object body 400 (total ~870).
	// 1st Put: fits (870 > 500? yes → inline evict but nothing to
	// evict → stillOver=true → sweeper signal). This triggers the
	// sweeper path on the very first oversized Put.
	h := NewHotStore(HotConfig{
		MaxBytes:       500,
		NumShards:      1,
		ReaperInterval: -1,
		Logger:         newCaptureLogger(&mu, &buf),
	})

	ctx := context.Background()
	// First object: fits (shard empty, 870 > 500 but no entries to
	// evict → stillOver=true, sweeper signaled but has nothing to do).
	_ = h.Put(ctx, api.Key(0), &api.Object{
		Key: 0, Body: make([]byte, 400),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	// Second object: shard already over (870 > 500). Inline evicts
	// up to 4 (only 1 entry), inserts new (870+870=1740 >> 500).
	// stillOver=true → sweeper signal. Sweeper evicts to get under 500.
	_ = h.Put(ctx, api.Key(1), &api.Object{
		Key: 1, Body: make([]byte, 400),
		StoredAt: time.Now(), TTL: time.Hour,
	})
	time.Sleep(100 * time.Millisecond)
	h.Close(ctx)

	records := parseLogRecords(t, &mu, &buf)
	found := false
	for _, rec := range records {
		if rec["msg"] == "evicted from hot store" && rec["reason"] == "sweeper_overshoot" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a sweeper_overshoot eviction log")
	}
}

func TestEvictionLogging_ConcurrentSafe(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var buf bytes.Buffer
	h := NewHotStore(HotConfig{
		MaxBytes:       500,
		NumShards:      2,
		ReaperInterval: -1,
		Logger:         newCaptureLogger(&mu, &buf),
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = h.Put(ctx, api.Key(idx), &api.Object{
				Key: api.Key(idx), Body: make([]byte, 50),
				StoredAt: time.Now(), TTL: time.Hour,
			})
		}(i)
	}
	wg.Wait()
	h.Close(ctx)
}
