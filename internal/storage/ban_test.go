package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestBan_ParallelEvictsAllMatching verifies that Ban with parallel
// shard scanning evicts all matching entries across all shards and
// returns the correct total count.
func TestBan_ParallelEvictsAllMatching(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 1000
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "example.com")
		o.Header.Set(header.XBouinePath, fmt.Sprintf("/ban-%d", i))
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "example\\.com"})
	require.NoError(t, err, "Ban")
	require.Equal(t, n, count)

	for i := range n {
		got, _, _ := s.Get(context.Background(), testkey.Key(uint64(i)))
		assert.Nil(t, got)
	}
}

// TestBan_ParallelNonMatchingRegexReturnsZero verifies that a
// non-matching regex returns 0 and leaves all entries intact.
func TestBan_ParallelNonMatchingRegexReturnsZero(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 500
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "keep.example.com")
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "^/never-matches$"})
	require.NoError(t, err, "Ban")
	require.Equal(t, 0, count)

	for i := range n {
		got, _, _ := s.Get(context.Background(), testkey.Key(uint64(i)))
		assert.NotNil(t, got)
	}
}

// TestBan_ParallelPartialMatch verifies that Ban only evicts matching
// entries and leaves non-matching ones in the store.
func TestBan_ParallelPartialMatch(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 400
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		if i%2 == 0 {
			o.Header.Set(header.XBouinePath, "/ban-me")
		} else {
			o.Header.Set(header.XBouinePath, "/keep")
		}
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{PathRegex: "^/ban-me$"})
	require.NoError(t, err, "Ban")
	require.Equal(t, n/2, count)

	for i := range n {
		got, _, _ := s.Get(context.Background(), testkey.Key(uint64(i)))
		if i%2 == 0 {
			assert.Nil(t, got)
		} else {
			assert.NotNil(t, got)
		}
	}
}

// TestBan_ParallelEvictionCount verifies that the eviction counter
// matches the returned count after parallel ban.
func TestBan_ParallelEvictionCount(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 800
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "evict.example.com")
		_ = s.Put(context.Background(), k, o)
	}

	before := s.stats.evictions.Load()
	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "evict\\.example\\.com"})
	require.NoError(t, err, "Ban")
	after := s.stats.evictions.Load()

	require.Equal(t, n, count)
	require.Equal(t, int64(n), after-before)
}

// TestBan_ParallelSurrogateKey verifies that parallel Ban correctly
// matches by surrogate key across shards.
func TestBan_ParallelSurrogateKey(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 600
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		if i%3 == 0 {
			o.SurrogateKeys = []string{"target"}
		} else {
			o.SurrogateKeys = []string{"other"}
		}
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "target"})
	require.NoError(t, err, "Ban")
	want := n / 3
	require.Equal(t, want, count)

	for i := range n {
		got, _, _ := s.Get(context.Background(), testkey.Key(uint64(i)))
		if i%3 == 0 {
			assert.Nil(t, got)
		} else {
			assert.NotNil(t, got)
		}
	}
}

// TestBan_ParallelConcurrentBans verifies that two concurrent Ban
// calls don't race or corrupt state. Run with -race.
func TestBan_ParallelConcurrentBans(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 32})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 1000
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		if i%2 == 0 {
			o.Header.Set(header.XBouineHost, "a.example.com")
		} else {
			o.Header.Set(header.XBouineHost, "b.example.com")
		}
		_ = s.Put(context.Background(), k, o)
	}

	var total atomic.Int64
	var banErr atomic.Value // error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "a\\.example\\.com"})
		if err != nil {
			banErr.Store(err)
			return
		}
		total.Add(int64(count))
	}()
	go func() {
		defer wg.Done()
		count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "b\\.example\\.com"})
		if err != nil {
			banErr.Store(err)
			return
		}
		total.Add(int64(count))
	}()
	wg.Wait()

	if err, ok := banErr.Load().(error); ok && err != nil {
		t.Fatalf("concurrent Ban failed: %v", err)
	}

	// Both bans match disjoint host patterns, so every entry is
	// evicted by exactly one ban. total must equal n.
	got := total.Load()
	require.Equal(t, int64(n), got)
	for i := range n {
		got, _, _ := s.Get(context.Background(), testkey.Key(uint64(i)))
		assert.Nil(t, got)
	}
}

// TestBan_ScanCoalescing_SkipsEagerWithinWindow verifies that a second
// Ban arriving within banScanCoalesceWindow of a completed scan skips
// the eager pass (count 0) but still registers in the lazy list, so
// objects that would have matched the second ban are evicted on lookup.
func TestBan_ScanCoalescing_SkipsEagerWithinWindow(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	const n = 100
	for i := range n {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		o.Header.Set(header.XBouineHost, "storm.example.com")
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: `storm\.example\.com`})
	require.NoError(t, err, "first Ban")
	require.Equal(t, n, count)

	// A matching entry stored BEFORE the coalesced ban (StoredAt <
	// CreatedAt, so it is subject to it per RFC 9111 §4.4 semantics)
	// is caught lazily on next lookup — the lazy list does not skip.
	k := testkey.Key(uint64(n + 1))
	o := obj(k, 64)
	o.Header.Set(header.XBouineHost, "storm.example.com")
	_ = s.Put(context.Background(), k, o)

	// Second ban within the coalesce window: eager scan skipped, count 0.
	count, err = s.Ban(context.Background(), api.BanExpr{HostRegex: `storm\.example\.com`})
	require.NoError(t, err, "coalesced Ban")
	require.Equal(t, 0, count)

	got, _, _ := s.Get(context.Background(), k)
	assert.Nil(t, got, "lazy ban must evict matching entry on lookup")
}

// TestBan_LiteralHostAndPath verifies that metacharacter-free host and
// path patterns match by exact string equality: literal patterns match
// identical values and reject non-identical ones (previously a regex
// like "example.com" also matched "examplexcom").
func TestBan_LiteralHostAndPath(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	hosts := []string{"literal.example.com", "other.example.com"}
	paths := []string{"/api/v1", "/api/v2"}
	for i, host := range hosts {
		for j, path := range paths {
			k := testkey.Key(uint64(i*len(paths) + j))
			o := obj(k, 64)
			o.Header.Set(header.XBouineHost, host)
			o.Header.Set(header.XBouinePath, path)
			_ = s.Put(context.Background(), k, o)
		}
	}

	count, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "literal.example.com", PathRegex: "/api/v1"})
	require.NoError(t, err, "Ban")
	require.Equal(t, 1, count)

	// The literal host pattern must NOT regex-match "literalxexample.com"
	// (a plain regex would treat "." as any-char). It is registered in the
	// lazy list; verify via MatchesActiveBan on a synthetic object.
	pred := api.Object{Header: header.Map{}}
	pred.Header.Set(header.XBouineHost, "literalxexample.com")
	assert.False(t, s.MatchesActiveBan(&pred), "literal pattern must not substring/regex-match")
}

// TestBan_AnchoredRegexStillCompiles verifies that patterns containing
// metacharacters still take the regexp path and evaluate with regex
// semantics (anchors, alternation).
func TestBan_AnchoredRegexStillCompiles(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	for i, path := range []string{"/a", "/b", "/c"} {
		k := testkey.Key(uint64(i))
		o := obj(k, 64)
		o.Header.Set(header.XBouinePath, path)
		_ = s.Put(context.Background(), k, o)
	}

	count, err := s.Ban(context.Background(), api.BanExpr{PathRegex: `^/(a|b)$`})
	require.NoError(t, err, "Ban")
	require.Equal(t, 2, count)
}

// TestBan_InvalidRegexStillRejected verifies the literal fast-path does
// not bypass regexp validation of malformed patterns.
func TestBan_InvalidRegexStillRejected(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 30, NumShards: 8})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{PathRegex: "^/unclosed["})
	require.Error(t, err, "malformed regexp must be rejected")
}

// TestBan_IdenticalReissuedDedups verifies that re-issuing the same ban
// pattern refreshes the existing lazy-list entry instead of appending a
// duplicate — a storm of identical bans must not grow the list that
// every subsequent cache hit walks.
func TestBan_IdenticalReissuedDedups(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	for range 1000 {
		_, _ = s.Ban(context.Background(), api.BanExpr{HostRegex: "same.example.com"})
	}
	cur := s.bans.Load()
	require.NotNil(t, cur)
	assert.Len(t, *cur, 1, "identical re-issued bans must dedup to one list entry")

	// A different pattern still appends.
	_, _ = s.Ban(context.Background(), api.BanExpr{HostRegex: "other.example.com"})
	cur = s.bans.Load()
	assert.Len(t, *cur, 2)
}

// TestBan_ListCapBoundsGrowth verifies that distinct bans stop growing
// the lazy list past banListCap and that the newest ban remains active.
func TestBan_ListCapBoundsGrowth(t *testing.T) {
	t.Parallel()

	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	for i := range banListCap + 50 {
		_, _ = s.Ban(context.Background(), api.BanExpr{HostRegex: fmt.Sprintf("host-%d.example.com", i)})
	}
	cur := s.bans.Load()
	require.NotNil(t, cur)
	require.Len(t, *cur, banListCap, "ban list must be capped")

	// The newest ban must still be active. Store a matching object
	// BEFORE re-issuing the ban so it is subject to it (objects
	// stored after a ban's CreatedAt are exempt per RFC 9111 §4.4).
	o := obj(testkey.Key(9999), 64)
	o.Header.Set(header.XBouineHost, fmt.Sprintf("host-%d.example.com", banListCap+49))
	_ = s.Put(context.Background(), testkey.Key(9999), o)
	_, _ = s.Ban(context.Background(), api.BanExpr{HostRegex: fmt.Sprintf("host-%d.example.com", banListCap+49)})
	got, _, _ := s.Get(context.Background(), testkey.Key(9999))
	assert.Nil(t, got, "newest ban must remain active at the cap")
}
