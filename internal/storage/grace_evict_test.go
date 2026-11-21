package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// putEvictObj stores a small object with the given KeepGrace flag.
func putEvictObj(t *testing.T, s *HotStore, name string, keepGrace bool) api.Key {
	t.Helper()
	key := testkey.Hash([]byte(name))
	obj := graceObj(name, keepGrace, time.Now())
	obj.Key = key
	require.NoError(t, s.Put(context.Background(), key, obj))
	return key
}

// TestEvictPreferNonGraced_Ordering pins issue #947's core behavior:
// on a hot-only shard under capacity pressure, non-graced entries are
// evicted before graced (stayin_alive) ones. Budget sized to hold 4
// small entries but not the 4 KiB pressure object; the inline cap plus
// the background sweeper then evict down to budget.
func TestEvictPreferNonGraced_Ordering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 12, NumShards: 1, ReaperInterval: -1})
	defer s.Close(ctx)

	mk := func(name string, grace bool) (api.Key, *api.Object) {
		obj := graceObj(name, grace, time.Now())
		obj.Key = testkey.Hash([]byte(name))
		obj.Body = make([]byte, 512)
		obj.BodySize = 512
		return obj.Key, obj
	}
	graced, gracedObj := mk("graced-0", true)
	normal1, normalObj1 := mk("normal-0", false)
	normal2, normalObj2 := mk("normal-1", false)
	normal3, normalObj3 := mk("normal-2", false)
	require.NoError(t, s.Put(ctx, graced, gracedObj))
	require.NoError(t, s.Put(ctx, normal1, normalObj1))
	require.NoError(t, s.Put(ctx, normal2, normalObj2))
	require.NoError(t, s.Put(ctx, normal3, normalObj3))
	time.Sleep(50 * time.Millisecond) // let any sweeper signal drain

	// Force overshoot eviction with a 2 KiB object.
	big := graceObj("big", false, time.Now())
	big.Key = testkey.Hash([]byte("big"))
	big.Body = make([]byte, 2<<10)
	big.BodySize = 2 << 10
	_ = s.Put(ctx, big.Key, big)
	time.Sleep(300 * time.Millisecond) // let the background sweeper finish

	objG, _, _ := s.Get(ctx, graced)
	obj1, _, _ := s.Get(ctx, normal1)
	obj2, _, _ := s.Get(ctx, normal2)
	obj3, _, _ := s.Get(ctx, normal3)
	assert.NotNil(t, objG, "graced entry must survive the pressure eviction")
	assert.Nil(t, obj1, "normal entry should have been evicted first")
	assert.Nil(t, obj2, "normal entry should have been evicted first")
	assert.Nil(t, obj3, "normal entry should have been evicted first")
	assert.Positive(t, s.Stats().GraceEvictSkips, "skip counter must record the graced skips")
}

// TestEvictPreferNonGraced_Fallback pins the bounded-resources-win
// invariant: when EVERY entry is graced, capacity eviction still
// evicts (the skip budget exhausts and plain SIEVE runs).
func TestEvictPreferNonGraced_Fallback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 10, NumShards: 1, ReaperInterval: -1})
	defer s.Close(ctx)

	k0 := putEvictObj(t, s, "g0", true)
	k1 := putEvictObj(t, s, "g1", true)
	putEvictObj(t, s, "g2", true)

	big := graceObj("big", true, time.Now())
	big.Key = testkey.Hash([]byte("big"))
	big.Body = make([]byte, 4<<10)
	big.BodySize = 4 << 10
	_ = s.Put(ctx, big.Key, big)

	st := s.Stats()
	assert.Less(t, st.HotEntries, int64(4), "all-graced shard under pressure must still evict")
	require.Positive(t, st.GraceEvictSkips)
	// At least one of the original graced entries is gone.
	oa, _, _ := s.Get(ctx, k0)
	ob, _, _ := s.Get(ctx, k1)
	assert.False(t, oa != nil && ob != nil, fmt.Sprintf("expected at least one graced eviction, both present (entries=%d)", st.HotEntries))
}

// TestEvictPreferNonGraced_NoGracedIsPlainSIEVE pins the zero-cost case:
// with no graced entries the first probe evicts immediately (no skips
// recorded).
func TestEvictPreferNonGraced_NoGracedIsPlainSIEVE(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 10, NumShards: 1, ReaperInterval: -1})
	defer s.Close(ctx)

	putEvictObj(t, s, "n0", false)
	putEvictObj(t, s, "n1", false)
	big := graceObj("big", false, time.Now())
	big.Key = testkey.Hash([]byte("big"))
	big.Body = make([]byte, 4<<10)
	big.BodySize = 4 << 10
	_ = s.Put(ctx, big.Key, big)

	assert.Zero(t, s.Stats().GraceEvictSkips, "no graced entries means no skips recorded")
	assert.Positive(t, s.Stats().Evictions)
}
