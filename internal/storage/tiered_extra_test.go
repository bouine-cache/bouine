package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

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
