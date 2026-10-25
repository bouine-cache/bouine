package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// testStore builds a hot store sized for these tests and closes it on
// cleanup.
func testStore(t *testing.T) *HotStore {
	t.Helper()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// storedObject builds a minimal object the way buildObject would:
// the ban headers the lazy list matches against, a TTL, and a body.
func storedObject(key api.Key, host, path string) *api.Object {
	m := header.NewMap(2)
	m.Set(header.XBouineHost, host)
	m.Set(header.XBouinePath, path)
	return &api.Object{
		Key:        key,
		Header:     m,
		StatusCode: 200,
		StoredAt:   time.Now(),
		TTL:        time.Hour,
		Body:       []byte("body"),
		BodySize:   4,
	}
}

func TestProbe_SideEffectFree(t *testing.T) {
	t.Parallel()
	s := testStore(t)
	key := api.Key{1, 2, 3, 4}
	obj := storedObject(key, "example.com", "/page")
	// Put stores the cache-owned clone, same as the data plane.
	require.NoError(t, s.Put(context.Background(), key, obj.CloneForStorage(nil)))

	// One Get warms the fast path so counters have a nonzero baseline.
	got, src, err := s.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)
	before := s.Stats()
	windowHits := s.WindowHits(key)

	st, ok := s.Probe(key)
	require.True(t, ok)
	require.Equal(t, "hot", st.Tier)
	require.True(t, st.Present)
	assert.Equal(t, int64(4), st.SizeBytes, "Probe is metadata-only: the body is never materialized")

	after := s.Stats()
	assert.Equal(t, before, after, "Probe must not touch hits, misses, or eviction counters")
	assert.Equal(t, windowHits, s.WindowHits(key), "Probe must not touch windowHits (it feeds the refresh gate)")
	assert.True(t, s.Has(key), "Probe must not evict")
}

// TestProbe_FreshnessWindows pins the fields the explain plane's
// lifecycle gauge reads (issue #814): the stamped TTL, swr and sie
// windows, and the merged Cache-Control the codec carries.
func TestProbe_FreshnessWindows(t *testing.T) {
	t.Parallel()
	s := testStore(t)
	key := api.Key{1, 2, 3}
	obj := storedObject(key, "example.com", "/page")
	obj.TTL = time.Hour
	obj.StaleWhileRevalidate = 5 * time.Minute
	obj.StaleIfError = 30 * time.Second
	obj.CacheControl = "public, max-age=3600"
	require.NoError(t, s.Put(context.Background(), key, obj.CloneForStorage(nil)))

	st, ok := s.Probe(key)
	require.True(t, ok)
	assert.Equal(t, int64(3600), st.TTLSeconds)
	assert.Equal(t, int64(300), st.SWRSeconds)
	assert.Equal(t, int64(30), st.SIESeconds)
	assert.Equal(t, "public, max-age=3600", st.CacheControl)
	assert.NotEmpty(t, st.StoredAt, "the stored-at timestamp feeds the age math")
}

func TestProbe_AbsentIsNotAnError(t *testing.T) {
	t.Parallel()
	s := testStore(t)
	st, ok := s.Probe(api.Key{9, 9})
	assert.False(t, ok)
	assert.Equal(t, api.KeyState{}, st)
}

// TestProbe_ReportsBannedWithoutEviction pins the read-only contract
// against the lazy ban list: Get evicts a matching object on lookup,
// Probe reports Banned and leaves the entry for the data plane. The
// object survives because the ban's eager scan was coalesced away
// (the same lazy-only path TestBan_ScanCoalescing_SkipsEagerWithinWindow
// pins).
func TestProbe_ReportsBannedWithoutEviction(t *testing.T) {
	t.Parallel()
	s := testStore(t)
	key := api.Key{5, 5}
	obj := storedObject(key, "banned.com", "/x")
	require.NoError(t, s.Put(context.Background(), key, obj.CloneForStorage(nil)))

	// First ban runs the eager scan (no matches) and opens the
	// coalesce window; the second ban skips the scan and registers
	// lazily only.
	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "unrelated.com"})
	require.NoError(t, err)
	n, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "banned.com"})
	require.NoError(t, err)
	require.Equal(t, 0, n, "coalesced ban scans nothing")

	st, ok := s.Probe(key)
	require.True(t, ok, "Probe reports the lazily banned entry, it does not evict it")
	assert.True(t, st.Present, "a banned object physically sits in the tier until the next lookup")
	assert.True(t, st.Banned, "the match is a question, and it answers")
	require.Len(t, s.MatchingBans(obj), 1)
	assert.True(t, s.Has(key), "the entry stays for the data plane to evict")

	// Get, by contrast, applies the ban (the data plane's contract).
	got, _, _ := s.Get(context.Background(), key)
	assert.Nil(t, got)
	assert.False(t, s.Has(key))
}

// TestMatchingBans_Descriptors pins the descriptor walk: pattern
// fields, registration order, expiry from the configured TTL, and
// no mutation of the list.
func TestMatchingBans_Descriptors(t *testing.T) {
	t.Parallel()
	s := testStore(t)
	banAt := time.Now().Add(-time.Minute)
	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "a.com", CreatedAt: banAt})
	require.NoError(t, err)
	_, err = s.Ban(context.Background(), api.BanExpr{PathRegex: `^/p/`, CreatedAt: banAt})
	require.NoError(t, err)
	_, err = s.Ban(context.Background(), api.BanExpr{HostRegex: "other.com", CreatedAt: banAt})
	require.NoError(t, err)

	obj := storedObject(api.Key{7}, "a.com", "/p/x")
	obj.StoredAt = banAt.Add(-time.Second)
	matches := s.MatchingBans(obj)
	require.Len(t, matches, 2, "host literal + path prefix both match")
	assert.Equal(t, "a.com", matches[0].HostRegex, "oldest-first registration order")
	assert.Equal(t, `^/p/`, matches[1].PathRegex)
	assert.False(t, matches[0].CreatedAt.IsZero(), "created_at carries the registration instant")
	assert.False(t, matches[0].ExpiresAt.IsZero(), "expires_at is created_at plus the ban TTL")
	assert.True(t, matches[0].ExpiresAt.After(matches[0].CreatedAt))

	// Non-matching object reports nothing; nil never matches.
	nomatch := storedObject(api.Key{8}, "zzz.com", "/q")
	nomatch.StoredAt = banAt.Add(-time.Second)
	assert.Empty(t, s.MatchingBans(nomatch))
	assert.Nil(t, s.MatchingBans(nil))

	// Read-only: the ban list is unchanged by the query.
	assert.Equal(t, 3, s.bans.len())
}

// TestMatchingBans_ExemptAfterRegistration pins RFC 9111 §4.4: an
// object stored after the ban's CreatedAt is not subject to it.
func TestMatchingBans_ExemptAfterRegistration(t *testing.T) {
	t.Parallel()
	s := testStore(t)
	banAt := time.Now()
	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "ex.com", CreatedAt: banAt})
	require.NoError(t, err)

	fresh := storedObject(api.Key{3}, "ex.com", "/y")
	fresh.StoredAt = banAt.Add(time.Minute)
	assert.Empty(t, s.MatchingBans(fresh), "post-ban fills are exempt")
}

// TestTieredProbe_WarmWithoutPromotion pins the tiered Probe
// contract: warm hits do not promote into the hot tier (Get does),
// and the warm path reports the re-derived Cache-Control.
func TestTieredProbe_WarmWithoutPromotion(t *testing.T) {
	t.Parallel()
	ts, err := NewTieredStore(TieredConfig{
		Hot:           HotConfig{MaxBytes: 1 << 20, NumShards: 2},
		Warm:          &warm.Config{Dir: t.TempDir(), MaxBytes: 1 << 30, SegMax: 64 << 20},
		BodyThreshold: 1, // every object warm-backed
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	key := api.Key{4, 2}
	obj := storedObject(key, "warm.com", "/z")
	obj.Body = make([]byte, 4096)
	obj.BodySize = 4096
	obj.CacheControl = "max-age=3600"
	require.NoError(t, ts.Put(context.Background(), key, obj.CloneForStorage(nil)))
	// Same-package access: drop the hot copy so the key is warm-only.
	require.NoError(t, ts.hot.Delete(context.Background(), key))
	require.True(t, ts.Has(key), "the warm copy holds the key")

	st, ok := ts.Probe(key)
	require.True(t, ok)
	assert.Equal(t, "warm", st.Tier)
	assert.Equal(t, "max-age=3600", st.CacheControl, "the warm path re-derives the merged Cache-Control")
	assert.False(t, ts.hot.Has(key), "Probe must not promote warm hits into the hot tier")

	// Get promotes (the data plane's contract).
	got, gsrc, _ := ts.Get(context.Background(), key)
	require.NotNil(t, got)
	assert.Equal(t, api.SourceWarm, gsrc)
	assert.True(t, ts.hot.Has(key), "Get promotes on warm hits")
}
