package cmd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// explainTestState builds the routeless runState the decision tests
// run against: a real hot store with the TTL reaper disabled (an
// expired entry's presence must be deterministic, not a race against
// the reaper), no router, no handlers — the default key policy
// applies, the same surface cachecheck's tests exercise.
func explainTestState(t *testing.T) *runState {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:       1 << 20,
		NumShards:      2,
		ReaperInterval: -1, // disabled: expired entries stay until evicted
	})
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return &runState{store: store}
}

// explainObject builds a stored object at key with the given
// lifecycle: the ban headers, status, and freshness windows.
func explainObject(key api.Key, path string, storedAgo, ttl, swr time.Duration) *api.Object {
	m := header.NewMap(2)
	m.Set(header.XBouineHost, "example.com")
	m.Set(header.XBouinePath, path)
	return &api.Object{
		Key:                  key,
		Header:               m,
		StatusCode:           200,
		StoredAt:             time.Now().Add(-storedAgo),
		TTL:                  ttl,
		StaleWhileRevalidate: swr,
		CacheControl:         fmt.Sprintf("max-age=%d", int(ttl.Seconds())),
	}
}

func TestExplain_FreshHit(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/hit"
	key := cache.BuildKeyFromURL(url, nil)
	obj := explainObject(key, "/hit", 10*time.Minute, time.Hour, 0)
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))

	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "hit", result.Verdict)
	assert.Equal(t, "HIT", result.CacheResult)
	assert.Equal(t, "fresh", result.TTL.Verdict)
	assert.True(t, result.Stored.Present)
	assert.Equal(t, "hot", result.Stored.Tier)
	assert.Empty(t, result.Reason)
	assert.Empty(t, result.Bans)
	freshFor, err := time.ParseDuration(result.TTL.FreshFor)
	require.NoError(t, err)
	assert.InDelta(t, 50*time.Minute, freshFor, float64(2*time.Second),
		"one hour minus the ten elapsed minutes, modulo explain's own runtime")
}

func TestExplain_StaleHitWithinSWR(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/stale"
	key := cache.BuildKeyFromURL(url, nil)
	obj := explainObject(key, "/stale", 2*time.Minute, time.Minute, 10*time.Minute)
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))

	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "stale-hit", result.Verdict)
	assert.Equal(t, "STALE", result.CacheResult)
	assert.Equal(t, "stale-swr", result.TTL.Verdict)
}

func TestExplain_MissByTTL(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/expired"
	key := cache.BuildKeyFromURL(url, nil)
	obj := explainObject(key, "/expired", 2*time.Minute, time.Minute, 0)
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))

	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "miss", result.Verdict)
	assert.Equal(t, "MISS", result.CacheResult)
	assert.Equal(t, "expired", result.Reason)
	assert.Equal(t, "dead", result.TTL.Verdict)
	assert.True(t, result.Stored.Present, "the expired entry is still on disk")
}

// TestExplain_MissByBan pins the primary varnishlog use case: the
// eager ban scan deleted the entry, so only the lazy registry can
// say why — through the URL-shaped candidate.
func TestExplain_MissByBan(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/banned"
	key := cache.BuildKeyFromURL(url, nil)
	obj := explainObject(key, "/banned", time.Minute, time.Hour, 0)
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))

	n, err := rs.store.Ban(context.Background(), api.BanExpr{PathRegex: "^/banned"})
	require.NoError(t, err)
	require.Equal(t, 1, n, "the eager scan deleted the entry")

	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "miss", result.Verdict)
	assert.Equal(t, "banned", result.Reason)
	require.Len(t, result.Bans, 1)
	assert.Equal(t, "^/banned", result.Bans[0].PathRegex)
	assert.NotEmpty(t, result.Bans[0].CreatedAt)
	assert.NotEmpty(t, result.Bans[0].ExpiresAt)
	assert.False(t, result.Stored.Present)
}

// TestExplain_BannedButStillPresent pins the contrast case the issue
// calls the explanation: the object survived (coalesced scan) and is
// fresh, yet the verdict is MISS · banned — freshness was not the
// problem. The TTL data still reports the object's life.
func TestExplain_BannedButStillPresent(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/banned-fresh"
	key := cache.BuildKeyFromURL(url, nil)
	obj := explainObject(key, "/banned-fresh", time.Minute, time.Hour, 0)
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))
	// Open the coalesce window, then register the real ban lazily.
	_, err := rs.store.Ban(context.Background(), api.BanExpr{HostRegex: "unrelated.example"})
	require.NoError(t, err)
	n, err := rs.store.Ban(context.Background(), api.BanExpr{HostRegex: "example.com"})
	require.NoError(t, err)
	require.Zero(t, n, "coalesced ban scans nothing; the entry survives")

	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "miss", result.Verdict)
	assert.Equal(t, "banned", result.Reason)
	assert.True(t, result.Stored.Present, "Peek reported the entry the data plane will evict")
	assert.Equal(t, "fresh", result.TTL.Verdict, "the bar shows a full life under a banned headline")
	require.Len(t, result.Bans, 1)
	assert.Equal(t, "example.com", result.Bans[0].HostRegex)
}

func TestExplain_NotStored(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/never"
	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "miss", result.Verdict)
	assert.Equal(t, "not-stored", result.Reason)
	assert.False(t, result.Stored.Present)
	assert.Empty(t, result.TTL.Verdict)
	assert.NotEmpty(t, result.Key.Hex)
}

// TestExplain_VariantAbsent pins the vary/encoding bucket selection:
// the stored primary declares Vary: Accept-Encoding, a gzip variant
// exists, and explain's nominal request (browser dialect) selects br
// — the bucket the data plane would key, and the one that is not
// stored.
func TestExplain_VariantAbsent(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/vary"
	primary := cache.BuildKeyFromURL(url, nil)

	gzipHeader := header.NewMap(1)
	gzipHeader.Set(header.AcceptEncoding, "gzip")
	gzipKey := cache.VariantKey(primary, header.AcceptEncoding, gzipHeader, nil)

	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		VaryValue:  header.AcceptEncoding,
		StoredAt:   time.Now().Add(-time.Hour),
		TTL:        7 * 24 * time.Hour,
	}
	require.NoError(t, rs.store.Put(context.Background(), primary, primaryObj.CloneForStorage(nil)))
	variantObj := &api.Object{
		Key:        gzipKey,
		StatusCode: 200,
		VaryValue:  header.AcceptEncoding,
		VaryKey:    cache.BuildVaryKey(header.AcceptEncoding, gzipHeader, nil),
		StoredAt:   time.Now().Add(-time.Hour),
		TTL:        7 * 24 * time.Hour,
	}
	require.NoError(t, rs.store.Put(context.Background(), gzipKey, variantObj.CloneForStorage(nil)))

	result := explainURL(context.Background(), url, rs)
	assert.Equal(t, "miss", result.Verdict)
	assert.Equal(t, "variant-absent", result.Reason)
	assert.Equal(t, header.AcceptEncoding, result.Variant.Vary)
	assert.Equal(t, "br", result.Variant.RequestBucket,
		"the nominal browser dialect selects the br bucket")
	assert.NotEmpty(t, result.Variant.RequestKeyHex)
	assert.False(t, result.Variant.Matched)
	assert.False(t, result.Stored.Present, "no object for this request")
}

// TestExplain_SideEffectFree pins the issue's read-only acceptance
// criterion end to end: an explain call leaves hit counters, misses,
// and windowHits exactly as they were.
func TestExplain_SideEffectFree(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	url := "http://example.com/side-effect"
	key := cache.BuildKeyFromURL(url, nil)
	obj := explainObject(key, "/side-effect", 0, time.Hour, 0)
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))

	before := rs.store.Stats()
	hits := rs.store.WindowHits(key)

	for range 3 {
		result := explainURL(context.Background(), url, rs)
		require.Equal(t, "hit", result.Verdict)
	}

	assert.Equal(t, before, rs.store.Stats(), "explain must not move hits or misses")
	assert.Equal(t, hits, rs.store.WindowHits(key), "explain must not touch the refresh gate's windowHits")
}

// TestProbeVariantBucket pins the label probe: a variant's stored
// assertion key resolves back to its bucket name through the same
// BuildVaryKey that stamped it.
func TestProbeVariantBucket(t *testing.T) {
	t.Parallel()
	gzipHeader := header.NewMap(1)
	gzipHeader.Set(header.AcceptEncoding, "gzip")
	gzipKey := cache.BuildVaryKey(header.AcceptEncoding, gzipHeader, nil)
	assert.Equal(t, "gzip", probeVariantBucket(header.AcceptEncoding, gzipKey, nil),
		"the assertion key resolves back to its bucket label")

	// Non-encoding Vary carries no derivable label.
	langHeader := header.NewMap(1)
	langHeader.Set("Accept-Language", "fr-FR")
	langKey := cache.BuildVaryKey("Accept-Language", langHeader, nil)
	assert.Empty(t, probeVariantBucket("Accept-Language", langKey, nil))
}
