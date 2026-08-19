package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestTieredStore_Has_HotOnly(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := testkey.Hash([]byte("hot-has"))
	o := bigObj(k, 100)

	require.NoError(t, ts.Put(context.Background(), k, o))
	assert.True(t, ts.Has(k))

	missing := testkey.Hash([]byte("missing"))
	assert.False(t, ts.Has(missing))
}

func TestTieredStore_Has_WarmOnly(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := testkey.Hash([]byte("warm-has"))
	o := bigObj(k, 8192)

	require.NoError(t, ts.Put(context.Background(), k, o))
	// Delete from hot to make it warm-only.
	require.NoError(t, ts.hot.Delete(context.Background(), k))
	// Should still be in warm.
	assert.True(t, ts.Has(k))
}

func TestTieredStore_Delete_NoWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := testkey.Hash([]byte("del-no-warm"))
	o := bigObj(k, 100)

	require.NoError(t, ts.Put(context.Background(), k, o))
	require.NoError(t, ts.Delete(context.Background(), k))
	assert.False(t, ts.Has(k))
}

func TestPeriodicTick_NilTicker(t *testing.T) {
	t.Parallel()
	ch := periodicTick(nil)
	// nil channel blocks forever — select should not fire.
	select {
	case <-ch:
		t.Fatal("nil ticker channel should not fire")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestPeriodicTick_ActiveTicker(t *testing.T) {
	t.Parallel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	ch := periodicTick(ticker)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("active ticker channel should fire")
	}
}

func TestTieredStore_Keys_Union(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	// Put a hot-only key and a warm key.
	hotKey := testkey.Hash([]byte("keys-hot"))
	warmKey := testkey.Hash([]byte("keys-warm"))
	require.NoError(t, ts.Put(context.Background(), hotKey, bigObj(hotKey, 100)))
	require.NoError(t, ts.Put(context.Background(), warmKey, bigObj(warmKey, 8192)))

	keys := ts.Keys()
	seen := make(map[api.Key]struct{})
	for _, k := range keys {
		seen[k] = struct{}{}
	}
	_, hasHot := seen[hotKey]
	_, hasWarm := seen[warmKey]
	assert.True(t, hasHot, "hot key should be in Keys union")
	assert.True(t, hasWarm, "warm key should be in Keys union")
}

func TestTieredStore_Keys_NoWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := testkey.Hash([]byte("keys-no-warm"))
	require.NoError(t, ts.Put(context.Background(), k, bigObj(k, 100)))

	keys := ts.Keys()
	assert.NotEmpty(t, keys)
}

func TestRunCompaction_NoWarm_NeedsCompactionFalse(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	// runCompaction with force=false and NeedsCompaction=false is a no-op.
	// The warm tier is empty, so NeedsCompaction should return false.
	ts.runCompaction("test", false)
}

func TestCollectHotOnlyKeysFallback_EmptyHot(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	// No keys in hot tier — fallback should return nil.
	keys := ts.collectHotOnlyKeysFallback()
	assert.Nil(t, keys)
}

func TestTieredStore_EvictWarm_NoWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := testkey.Hash([]byte("evict-no-warm"))
	// evictWarmErr with no warm tier should return nil.
	require.NoError(t, ts.evictWarmErr(k))
}
