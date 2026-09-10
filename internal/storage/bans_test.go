package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// banTestObj returns a subject object: stored before now-offset so it
// predates any ban registered after it.
func banTestObj(host, path string, storedAgo time.Duration) *api.Object {
	o := obj(testkey.Hash([]byte(host+path)), 64)
	o.Header.Set(header.XBouineHost, host)
	o.Header.Set(header.XBouinePath, path)
	o.StoredAt = time.Now().Add(-storedAgo)
	return o
}

// TestBanSnapshot_LiteralHostHit verifies a literal host ban matches an
// object with that host via the O(1) set check.
func TestBanSnapshot_LiteralHostHit(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "banned.example.com"})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("banned.example.com", "/any", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("other.example.com", "/any", time.Hour)))
}

// TestBanSnapshot_LiteralPathHit verifies literal path bans.
func TestBanSnapshot_LiteralPathHit(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{PathRegex: "/api/v1"})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("any.example.com", "/api/v1", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("any.example.com", "/api/v2", time.Hour)))
}

// TestBanSnapshot_PrefixPathHit verifies anchored-prefix bans match by
// HasPrefix and reject non-matching subjects cheaply.
func TestBanSnapshot_PrefixPathHit(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{PathRegex: `^/blog/`})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("any.example.com", "/blog/post-1", time.Hour)))
	require.True(t, s.MatchesActiveBan(banTestObj("any.example.com", "/blog/", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("any.example.com", "/about/blog", time.Hour)),
		"prefix ban must not match mid-path")
	assert.False(t, s.MatchesActiveBan(banTestObj("any.example.com", "/api", time.Hour)))
}

// TestBanSnapshot_PrefixHostHit verifies anchored-prefix host bans.
func TestBanSnapshot_PrefixHostHit(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: `^api\.`})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("api.example.com", "/any", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("example-api.com", "/any", time.Hour)))
}

// TestBanSnapshot_SurrogateHit verifies surrogate-key bans through the
// surrogates set.
func TestBanSnapshot_SurrogateHit(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "product-42"})
	require.NoError(t, err)

	tagged := banTestObj("any.example.com", "/p/42", time.Hour)
	tagged.SurrogateKeys = []string{"product-42", "other"}
	require.True(t, s.MatchesActiveBan(tagged))

	untagged := banTestObj("any.example.com", "/p/42", time.Hour)
	untagged.SurrogateKeys = []string{"product-43"}
	assert.False(t, s.MatchesActiveBan(untagged))
}

// TestBanSnapshot_MultiConditionNotFalsePositive pins the soundness
// rule: a ban with host AND path conditions must NOT match an object
// whose host matches but path does not. The set checks alone would
// wrongly claim a match, so multi-condition bans must take the
// predicate path.
func TestBanSnapshot_MultiConditionNotFalsePositive(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "shop.example.com",
		PathRegex: `/^/checkout`, // deliberate non-matching shape for the second condition
	})
	require.NoError(t, err)

	// Host matches, path does not: no ban may match.
	assert.False(t, s.MatchesActiveBan(banTestObj("shop.example.com", "/browse", time.Hour)),
		"multi-condition ban must require ALL conditions")
	// Neither matches.
	assert.False(t, s.MatchesActiveBan(banTestObj("other.example.com", "/browse", time.Hour)))
}

// TestBanSnapshot_MultiConditionAllMatch verifies a multi-condition
// ban matches when every condition holds.
func TestBanSnapshot_MultiConditionAllMatch(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "shop.example.com",
		PathRegex: `^/checkout`,
	})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("shop.example.com", "/checkout/step-1", time.Hour)))
}

// TestBanSnapshot_RegexBansStillWalk verifies regex bans that are not
// pure anchored literals evaluate with full regexp semantics.
func TestBanSnapshot_RegexBansStillWalk(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{PathRegex: `^/(a|b)$`})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("any.example.com", "/a", time.Hour)))
	require.True(t, s.MatchesActiveBan(banTestObj("any.example.com", "/b", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("any.example.com", "/c", time.Hour)))
}

// TestBanSnapshot_ExemptObjectSkipped verifies the RFC 9111 §4.4
// exemption on the fast path: an object stored AFTER the ban's
// CreatedAt is not subject to it, even when its host is in the
// banned-hosts set.
func TestBanSnapshot_ExemptObjectSkipped(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	// Ban with an explicit CreatedAt 1h ago.
	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: "banned.example.com",
		CreatedAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)

	// Object stored 2h ago (before the ban): subject → evicted on Get.
	require.True(t, s.MatchesActiveBan(banTestObj("banned.example.com", "/x", 2*time.Hour)))
	// Object stored 30m ago (after the ban): exempt.
	assert.False(t, s.MatchesActiveBan(banTestObj("banned.example.com", "/x", 30*time.Minute)))
}

// TestBanReaper_PrunesExpiredBans verifies fix 2: the TTL reaper
// prunes expired bans so a quiet period after a storm stops taxing
// hits within one reaper interval, not after the full 24 h banTTL.
func TestBanReaper_PrunesExpiredBans(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1}) // reaper loop disabled; call reapExpired manually
	defer func() { _ = s.Close(context.Background()) }()

	// Register a live ban and an expired one by aging its created time.
	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "live.example.com"})
	require.NoError(t, err)
	_, err = s.Ban(context.Background(), api.BanExpr{HostRegex: "dead.example.com"})
	require.NoError(t, err)
	// Age the dead ban past banTTL directly in the list.
	s.bans.mu.Lock()
	for i := range s.bans.list {
		if s.bans.list[i].pattern.hostRegex == "dead.example.com" {
			s.bans.list[i].created = time.Now().Add(-banTTL - time.Minute)
		}
	}
	s.bans.mu.Unlock()

	// Simulate a reaper tick.
	s.reapExpired(time.Now())

	assert.Equal(t, 1, s.bans.len(), "expired ban must be pruned by the reaper")
	// The live ban still matches.
	require.True(t, s.MatchesActiveBan(banTestObj("live.example.com", "/x", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("dead.example.com", "/x", time.Hour)),
		"pruned ban must no longer match")
}

// TestBanSnapshot_RebuildOnRefresh verifies re-issuing an identical
// ban refreshes its exemption window in the compiled snapshot, not
// just the list. Uses the anchored-exact form (the literal-classified
// shape that populates the hosts set).
func TestBanSnapshot_RebuildOnRefresh(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	host := `^banned\.example\.com$`
	// First issuance: exemptAfter one hour ago.
	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: host,
		CreatedAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	// Object stored 30m ago is exempt under the old exemption time.
	assert.False(t, s.MatchesActiveBan(banTestObj("banned.example.com", "/x", 30*time.Minute)))

	// Re-issue the same pattern with a fresh CreatedAt (now): the
	// refreshed exemption time makes the 30m-old object subject.
	_, err = s.Ban(context.Background(), api.BanExpr{
		HostRegex: host,
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)
	assert.True(t, s.MatchesActiveBan(banTestObj("banned.example.com", "/x", 30*time.Minute)))
}

// TestBanSnapshot_AnchoredExactHost verifies the escaped-dot
// anchored-exact form (^www\.example\.com$) classifies into the hosts
// set and matches by equality.
func TestBanSnapshot_AnchoredExactHost(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: `^www\.shop\.com$`})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("www.shop.com", "/any", time.Hour)))
	// The regex would ALSO match "wwwxshop.com" (any-char dot); the
	// snapshot is conservative in the match direction too — equality
	// only — which is a deliberate, documented narrowing: anchored
	// escaped-dot patterns are issued with exact-match intent.
	assert.False(t, s.MatchesActiveBan(banTestObj("wwwxshop.com", "/any", time.Hour)))
}

// TestBanSnapshot_EscapedPrefixHost verifies the ^api\. form (the
// common subdomain ban) takes the HasPrefix path.
func TestBanSnapshot_EscapedPrefixHost(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: `^api\.`})
	require.NoError(t, err)

	require.True(t, s.MatchesActiveBan(banTestObj("api.example.com", "/any", time.Hour)))
	assert.False(t, s.MatchesActiveBan(banTestObj("apid.example.com", "/any", time.Hour)))
}

// TestBanSnapshot_UnanchoredDottedStaysOpaque pins why unanchored
// dotted patterns ("example.com") cannot join the literal sets: as a
// regex, the dot is any-char, so set equality would MISS matches
// ("examplexcom") — unsound rejection. They evaluate as regex.
func TestBanSnapshot_UnanchoredDottedStaysOpaque(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "example.com"})
	require.NoError(t, err)

	// The any-char regex semantics must be preserved.
	require.True(t, s.MatchesActiveBan(banTestObj("examplexcom", "/any", time.Hour)))
	require.True(t, s.MatchesActiveBan(banTestObj("example.com", "/any", time.Hour)))
}

// TestBanGet_EvictsLiteralSubjectOnHotPath verifies end-to-end that a
// Get on an object subject to a literal ban (via the fast set check)
// evicts it and returns a miss.
func TestBanGet_EvictsLiteralSubjectOnHotPath(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	k := testkey.Hash([]byte("lit-subject"))
	o := obj(k, 64)
	o.Header.Set(header.XBouineHost, "banned.example.com")
	o.Header.Set(header.XBouinePath, "/x")
	o.StoredAt = time.Now().Add(-time.Hour)
	require.NoError(t, s.Put(context.Background(), k, o))

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "banned.example.com"})
	require.NoError(t, err)

	got, _, err := s.Get(context.Background(), k)
	require.NoError(t, err)
	assert.Nil(t, got, "subject object must be lazily evicted on Get via the snapshot check")
	// The eviction must have removed it from the shard.
	assert.False(t, s.Has(k))
}

// TestBanSurrogate_SkipsEagerScan verifies Option B: a pure
// surrogate-key ban performs no eager store walk (count 0, no scan
// recorded) yet the ban list is populated and enforcement is lazy.
func TestBanSurrogate_SkipsEagerScan(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	k := testkey.Hash([]byte("surrogate-lazy"))
	o := obj(k, 64)
	o.SurrogateKeys = []string{"tag-1"}
	o.StoredAt = time.Now().Add(-time.Hour)
	require.NoError(t, s.Put(context.Background(), k, o))

	count, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-1"})
	require.NoError(t, err)
	require.Zero(t, count, "surrogate-only ban must skip the eager scan")
	require.Equal(t, 1, s.bans.len(), "ban list must hold the lazy entry")

	// Enforcement is lazy: the tagged object is evicted on Get.
	got, _, err := s.Get(context.Background(), k)
	require.NoError(t, err)
	assert.Nil(t, got, "banned object must be evicted lazily on lookup")
	assert.False(t, s.Has(k), "evicted lazily; entry removed from shard")
}

// TestBanSurrogate_ReaperReclaims verifies the memory-reclaim half of
// Option B: entries matching a surrogate ban that are never looked up
// are collected by the TTL reaper instead of the eager scan.
func TestBanSurrogate_ReaperReclaims(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	// Entry with a 1-minute TTL, subject to the ban, never accessed.
	k := testkey.Hash([]byte("surrogate-reaper"))
	o := obj(k, 64)
	o.SurrogateKeys = []string{"tag-1"}
	o.StoredAt = time.Now().Add(-2 * time.Hour)
	o.TTL = time.Minute
	require.NoError(t, s.Put(context.Background(), k, o))

	_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-1"})
	require.NoError(t, err)

	// Simulate the reaper tick after the entry's full TTL elapsed.
	s.reapExpired(time.Now().Add(2 * time.Hour))

	assert.False(t, s.Has(k), "banned entry must be reclaimed by the reaper")
}

// TestBanSurrogate_ExemptObjectsNotEvicted verifies the RFC 9111 §4.4
// exemption holds on the lazy path: objects stored after the ban's
// CreatedAt are served normally.
func TestBanSurrogate_ExemptObjectsNotEvicted(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	oldK := testkey.Hash([]byte("surrogate-old"))
	oldObj := obj(oldK, 64)
	oldObj.SurrogateKeys = []string{"tag-1"}
	oldObj.StoredAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, s.Put(context.Background(), oldK, oldObj))

	_, err := s.Ban(context.Background(), api.BanExpr{
		SurrogateKey: "tag-1",
		CreatedAt:    time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)

	// Object stored after the ban: exempt, still served.
	newK := testkey.Hash([]byte("surrogate-new"))
	newObj := obj(newK, 64)
	newObj.SurrogateKeys = []string{"tag-1"}
	newObj.StoredAt = time.Now().Add(-30 * time.Minute)
	require.NoError(t, s.Put(context.Background(), newK, newObj))

	got, _, err := s.Get(context.Background(), newK)
	require.NoError(t, err)
	assert.NotNil(t, got, "object stored after the ban is exempt")

	// Object stored before the ban: evicted lazily.
	got, _, err = s.Get(context.Background(), oldK)
	require.NoError(t, err)
	assert.Nil(t, got, "object stored before the ban is subject to it")
}

// TestBanSurrogate_MultiConditionStillScans verifies bans carrying a
// surrogate key PLUS host/path patterns keep the eager scan: they
// classify opaque and need immediate reclaim of matching entries.
func TestBanSurrogate_MultiConditionStillScans(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	k := testkey.Hash([]byte("multi-cond"))
	o := obj(k, 64)
	o.Header.Set(header.XBouineHost, "shop.example.com")
	o.SurrogateKeys = []string{"tag-1"}
	o.StoredAt = time.Now().Add(-time.Hour)
	require.NoError(t, s.Put(context.Background(), k, o))

	count, err := s.Ban(context.Background(), api.BanExpr{
		SurrogateKey: "tag-1",
		HostRegex:    "shop.example.com",
	})
	require.NoError(t, err)
	require.Equal(t, 1, count, "multi-condition ban must run the eager scan")
	assert.False(t, s.Has(k))
}

// TestBanSurrogate_RepeatBanIsCheap verifies re-issuing the same
// surrogate ban (the dominant production pattern: tags re-invalidate
// ~26x/day) costs no scan and no ban-list growth.
func TestBanSurrogate_RepeatBanIsCheap(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	for range 100 {
		count, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-1"})
		require.NoError(t, err)
		require.Zero(t, count, "every repeat skip the scan")
	}
	assert.Equal(t, 1, s.bans.len(), "identical bans dedup to one list entry")
}
