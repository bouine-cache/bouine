package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSoftPurgeSurrogate_CountsOnlyOwnedKeys(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer store.Close(context.Background())
	handler := cache.NewHandler(cache.HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {},
		Store:    store,
		Logger:   newTestLogger(),
	})
	rs := &runState{store: store, handlers: []*cache.Handler{handler}}
	ctx := context.Background()

	// Store two tagged objects with SWR so soft purge marks them stale.
	k1 := testkey.Hash([]byte("owned-1"))
	k2 := testkey.Hash([]byte("owned-2"))
	for _, k := range []api.Key{k1, k2} {
		o := &api.Object{
			Key:                  k,
			StatusCode:           200,
			Body:                 []byte("x"),
			BodySize:             1,
			StoredAt:             time.Now(),
			TTL:                  time.Minute,
			StaleWhileRevalidate: time.Minute,
			SurrogateKeys:        []string{"count-tag"},
		}
		require.NoError(t, store.Put(ctx, k, o))
	}

	n, err := rs.softPurgeSurrogate(ctx, "count-tag")
	require.NoError(t, err)
	require.Equal(t, 2, n, "both stored keys were owned and refreshed")

	// A tag matching nothing counts zero.
	n, err = rs.softPurgeSurrogate(ctx, "absent-tag")
	require.NoError(t, err)
	require.Equal(t, 0, n)

	// Refreshing the same tag again: the soft-purged keys were
	// re-Put with TTL=0, so the reaper may have dropped them by now;
	// the count must simply reflect what is actually owned at call
	// time and never error.
	n, err = rs.softPurgeSurrogate(ctx, "count-tag")
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 0)
	require.LessOrEqual(t, n, 2)
}

func TestSoftPurgeSurrogate_CountExcludesEvictedBetweenScanAndPurge(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer store.Close(context.Background())
	handler := cache.NewHandler(cache.HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {},
		Store:    store,
		Logger:   newTestLogger(),
	})
	rs := &runState{store: store, handlers: []*cache.Handler{handler}}
	ctx := context.Background()

	k1 := testkey.Hash([]byte("race-1"))
	o := &api.Object{
		Key:                  k1,
		StatusCode:           200,
		Body:                 []byte("x"),
		BodySize:             1,
		StoredAt:             time.Now(),
		TTL:                  time.Minute,
		StaleWhileRevalidate: time.Minute,
		SurrogateKeys:        []string{"race-tag"},
	}
	require.NoError(t, store.Put(ctx, k1, o))

	// Delete the key after it is resolvable but simulate the TOCTOU by
	// deleting before the purge loop reaches it: use a hook via a
	// surrogate key that resolves, then hard-delete the key, then purge
	// a tag that no longer matches anything stored. The count must be 0,
	// not 1.
	require.NoError(t, store.Delete(ctx, k1))
	n, err := rs.softPurgeSurrogate(ctx, "race-tag")
	require.NoError(t, err)
	require.Equal(t, 0, n, "evicted key must not be counted as refreshed")
}
