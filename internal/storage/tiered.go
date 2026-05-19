package storage

import (
	"context"
	"encoding/json"
	"log/slog"

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
	logger *slog.Logger

	// bodyThreshold: objects with Body <= this stay hot-only.
	bodyThreshold int64
}

// TieredConfig configures a TieredStore.
type TieredConfig struct {
	Hot    HotConfig
	Warm   *warm.Config // nil = no warm tier (ephemeral mode)
	WALDir string       // empty = no WAL
	Logger *slog.Logger

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
		cfg.Logger = slog.Default()
	}
	if cfg.BodyThreshold <= 0 {
		cfg.BodyThreshold = 64 << 10
	}

	hot := NewHotStore(cfg.Hot)

	ts := &TieredStore{
		hot:           hot,
		logger:        cfg.Logger,
		bodyThreshold: cfg.BodyThreshold,
	}

	if cfg.Warm != nil {
		w, err := warm.NewStore(*cfg.Warm)
		if err != nil {
			return nil, err
		}
		ts.warm = w
	}

	if cfg.WALDir != "" {
		l, err := wal.Open(cfg.WALDir)
		if err != nil {
			return nil, err
		}
		ts.wal = l
	}

	return ts, nil
}

// Get looks up the hot tier first. If the object is not in the hot
// tier but the warm tier is available, it is not fetched from disk
// on a cold read path in phase 2. Warm-tier lookups will be added
// in phase 3 when the cache engine drives revalidation.
func (t *TieredStore) Get(ctx context.Context, key api.Key) (*api.Object, error) {
	return t.hot.Get(ctx, key)
}

// Put stores an object in the hot tier and, for large objects, also
// in the warm tier (with a WAL record).
func (t *TieredStore) Put(ctx context.Context, key api.Key, obj *api.Object) error {
	if err := t.hot.Put(ctx, key, obj); err != nil {
		return err
	}

	if t.warm != nil && obj.BodySize > t.bodyThreshold {
		body, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		segID, offset, err := t.warm.Put(uint64(key), body) //nolint:gosec // segID fits int32
		if err != nil {
			return err
		}
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

// Ban delegates to the hot tier. Warm-tier lazy bans land in phase 4.
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

// Close shuts down the WAL, warm tier, and hot tier in order.
func (t *TieredStore) Close(ctx context.Context) error {
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
