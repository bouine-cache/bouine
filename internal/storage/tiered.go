package storage

import (
	"context"
	"sync"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/storage/wal"
	"github.com/thylong/bouine/internal/storage/warm"
	"github.com/thylong/bouine/pkg/api"
)

// TieredStore wraps a hot tier (RAM) and an optional warm tier (disk)
// into a single Store. Objects smaller than a threshold live in the
// hot tier only; larger objects are demoted to the warm tier. The WAL
// ensures the index can be rebuilt after a crash.
//
// When warm is nil the store is ephemeral (equivalent to --no-persist).
//
// Stable.
type TieredStore struct {
	hot    *HotStore
	warm   *warm.Store
	wal    *wal.Log
	logger observability.Logger

	// bodyThreshold: objects with Body <= this stay hot-only.
	bodyThreshold int64

	// done is closed by Close to stop the background compaction goroutine.
	done chan struct{}
	// compactWg tracks the compaction goroutine for join-on-close.
	compactWg sync.WaitGroup
}

// TieredConfig configures a TieredStore.
type TieredConfig struct {
	Hot    HotConfig
	Warm   *warm.Config // nil = no warm tier (ephemeral mode)
	WALDir string       // empty = no WAL
	Logger observability.Logger

	// BodyThreshold controls the hot/warm admission boundary. Objects
	// with BodySize <= this value stay in the hot tier only. Objects
	// above this value are also written to the warm tier so they can
	// be evicted from RAM without data loss.
	// Default: 64 KiB.
	BodyThreshold int64
}

// NewTieredStore creates a tiered store. If WALDir is non-empty, the
// WAL is replayed on open to rebuild the warm-tier index.
func NewTieredStore(cfg TieredConfig) (*TieredStore, error) {
	if cfg.Logger == nil {
		cfg.Logger = observability.NoopLogger{}
	}
	if cfg.BodyThreshold <= 0 {
		cfg.BodyThreshold = 64 << 10
	}

	hot := NewHotStore(cfg.Hot)

	ts := &TieredStore{
		hot:           hot,
		logger:        cfg.Logger,
		bodyThreshold: cfg.BodyThreshold,
		done:          make(chan struct{}),
	}

	if cfg.Warm != nil {
		w, err := warm.NewStore(*cfg.Warm)
		if err != nil {
			return nil, err
		}
		ts.warm = w
	}

	// Background compaction: check every 30 minutes and compact if the
	// warm tier has accumulated significant tombstone/dead byte waste.
	if ts.warm != nil {
		ts.compactWg.Add(1)
		go ts.compactLoop()
	}

	if cfg.WALDir != "" {
		l, err := wal.Open(cfg.WALDir)
		if err != nil {
			return nil, err
		}
		ts.wal = l

		// Replay WAL to rebuild the warm-tier in-memory index so
		// Get() can locate objects on disk without a full segment scan.
		if ts.warm != nil {
			if rErr := wal.Replay(cfg.WALDir, func(e wal.Entry) error {
				switch {
				case e.IsPut():
					ts.warm.SetIndex(e.Key, int(e.SegID), e.Offset)
				case e.IsDelete():
					ts.warm.DelIndex(e.Key)
				}
				return nil
			}); rErr != nil {
				// Non-fatal: an empty or absent WAL is fine (fresh start).
				ts.logger.Warn("wal replay failed; warm-tier index may be incomplete",
					"error", rErr)
			}
			ts.warm.RecomputeStats()
		}
	}

	return ts, nil
}

// Get looks up the hot tier first. On a miss, the warm tier is
// consulted and the object is promoted back into the hot tier so
// the next hit is served from RAM.
func (t *TieredStore) Get(ctx context.Context, key api.Key) (*api.Object, error) {
	obj, err := t.hot.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if obj != nil {
		return obj, nil
	}
	if t.warm == nil {
		return nil, nil
	}
	body, wErr := t.warm.Get(uint64(key))
	if wErr != nil {
		return nil, wErr
	}
	if body == nil {
		return nil, nil
	}
	loaded, decErr := decodeObject(body)
	if decErr != nil {
		return nil, decErr
	}
	// Re-derive transient fields not serialised to disk (tagged json:"-").
	// These fields exist purely for hit-path performance; recalculating them
	// once on warm-tier load restores the fast path for subsequent hits.
	if cc := loaded.Header.Get("Cache-Control"); cc != "" {
		loaded.CacheControl = cc
	}
	if age := loaded.Header.Get("Age"); age != "" {
		var secs int64
		for _, b := range []byte(age) {
			if b < '0' || b > '9' {
				break
			}
			secs = secs*10 + int64(b-'0')
		}
		loaded.OriginAge = time.Duration(secs) * time.Second
	}
	// Promote to hot tier (best-effort: ignore error).
	_ = t.hot.Put(ctx, key, loaded)
	return loaded, nil
}

// Put stores an object in the hot tier and, for large objects, also
// in the warm tier (with a WAL record).
func (t *TieredStore) Put(ctx context.Context, key api.Key, obj *api.Object) error {
	if err := t.hot.Put(ctx, key, obj); err != nil {
		return err
	}

	if t.warm != nil && obj.BodySize > t.bodyThreshold {
		body := encodeObject(obj)
		segID, offset, err := t.warm.Put(uint64(key), body) //nolint:gosec // segID fits int32
		if err != nil {
			return err
		}
		// Mark the hot entry as having a warm backup so eviction
		// can prefer it under memory pressure.
		t.hot.SetWarm(key)
		if t.wal != nil {
			return t.wal.Append(wal.PutEntry(uint64(key), int32(segID), offset)) //nolint:gosec // segID bounded
		}
	}
	return nil
}

// Delete removes from both tiers.
func (t *TieredStore) Delete(ctx context.Context, key api.Key) error {
	if err := t.hot.Delete(ctx, key); err != nil {
		return err
	}
	if t.warm != nil {
		if err := t.warm.Delete(uint64(key)); err != nil {
			return err
		}
		if t.wal != nil {
			return t.wal.Append(wal.DeleteEntry(uint64(key)))
		}
	}
	return nil
}

// Ban delegates to the hot tier. Warm-tier lazy bans are deferred.
func (t *TieredStore) Ban(ctx context.Context, expr api.BanExpr) (int, error) {
	return t.hot.Ban(ctx, expr)
}

// Stats merges hot + warm stats.
func (t *TieredStore) Stats() api.Stats {
	st := t.hot.Stats()
	if t.warm != nil {
		wEnt, wBytes := t.warm.Stats()
		st.WarmEntries = wEnt
		st.WarmBytes = wBytes
	}
	return st
}

// compactLoop runs the periodic warm-tier compaction until done is closed.
func (t *TieredStore) compactLoop() {
	defer t.compactWg.Done()
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			if t.warm.NeedsCompaction() {
				if err := t.warm.Compact(); err != nil {
					t.logger.Warn("warm tier compaction failed", "error", err)
				} else {
					t.logger.Info("warm tier compaction complete")
				}
			}
		}
	}
}

// Close shuts down the WAL, warm tier, and hot tier in order.
// The compaction goroutine is stopped and joined before the warm
// store is closed, preventing use-after-close on file handles.
func (t *TieredStore) Close(ctx context.Context) error {
	close(t.done)
	t.compactWg.Wait()

	if t.wal != nil {
		if err := t.wal.Close(); err != nil {
			t.logger.Warn("wal close error", "error", err)
		}
	}
	if t.warm != nil {
		if err := t.warm.Close(); err != nil {
			t.logger.Warn("warm close error", "error", err)
		}
	}
	return t.hot.Close(ctx)
}
