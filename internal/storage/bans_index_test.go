package storage

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// surrogateSetLen counts the bans published across every shard.
func surrogateSetLen(s *surrogateSet) int {
	t := s.table.Load()
	if t == nil {
		return 0
	}
	n := 0
	for i := range t.shards {
		if m := t.shards[i].Load(); m != nil {
			n += len(*m)
		}
	}
	return n
}

// checkBanInvariants asserts the views of b agree with its ordered
// list: one index entry per listed ban, every surrogate-only ban (and
// nothing else) published in the surrogate set under its key, and rest
// equal to the non-surrogate subsequence of the list.
func checkBanInvariants(t *testing.T, b *banListState) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	require.LessOrEqual(t, len(b.list), b.capOrDefault(), "list exceeds its cap")
	require.Len(t, b.index, len(b.list), "index and list disagree on size")
	var wantRest []*activeBan
	surrogates := 0
	for _, ban := range b.list {
		require.Same(t, ban, b.index[ban.pattern], "index must map each listed pattern to its ban")
		require.Equal(t, classifyBan(ban.pattern) == banClassLiteralSurrogate, ban.surrogate)
		if !ban.surrogate {
			wantRest = append(wantRest, ban)
			continue
		}
		surrogates++
		sh := b.surr.table.Load().load(ban.pattern.surrogateKey)
		require.Same(t, ban, sh.find(ban.pattern.surrogateKey), "surrogate ban missing from its shard")
		require.True(t, slices.IsSortedFunc(sh, func(a, c surrogateEntry) int { return strings.Compare(a.key, c.key) }),
			"shard must stay sorted")
	}
	require.Equal(t, surrogates, surrogateSetLen(&b.surr), "surrogate set holds bans the list does not")
	require.Len(t, b.rest, len(wantRest), "rest must be the non-surrogate subset of list")
	for i := range wantRest {
		require.Same(t, wantRest[i], b.rest[i], "rest must keep list order")
	}
}

// bruteForceMatch is the reference semantics: obj is banned iff some
// listed ban's predicate matches and obj is subject to it.
func bruteForceMatch(b *banListState, obj *api.Object) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ban := range b.list {
		if subjectTo(obj, ban.exemptAfter()) && ban.pred(obj) {
			return true
		}
	}
	return false
}

// TestBanList_RandomizedMatchesReference drives register/refresh/
// evict/prune sequences over a small cap and a mixed pattern universe
// (surrogate, literal host/path, prefix, regex, multi-condition), and
// checks after every step that the views stay consistent with the list
// and that the lock-free match agrees with a brute-force walk of it.
func TestBanList_RandomizedMatchesReference(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	exprs := make([]api.BanExpr, 0, 64)
	for i := range 40 {
		exprs = append(exprs, api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i)})
	}
	for i := range 6 {
		exprs = append(exprs,
			api.BanExpr{HostRegex: fmt.Sprintf("host-%d", i)},
			api.BanExpr{PathRegex: fmt.Sprintf("^/p/%d/", i)},
			api.BanExpr{HostRegex: fmt.Sprintf("h.st-%d", i)},
			api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i), HostRegex: fmt.Sprintf("host-%d", i)},
		)
	}
	preds := make([]banPredicate, len(exprs))
	for i, e := range exprs {
		p, err := compileBanPredicate(e)
		require.NoError(t, err)
		preds[i] = p
	}

	b := &banListState{capacity: 16}
	now := time.Now()
	for step := range 5000 {
		if rng.IntN(20) == 0 {
			b.pruneExpired(time.Now())
		} else {
			i := rng.IntN(len(exprs))
			created := now.Add(-time.Duration(rng.IntN(3600)) * time.Second)
			b.register(exprs[i], preds[i], created)
		}
		checkBanInvariants(t, b)

		o := banTestObj(
			fmt.Sprintf("host-%d", rng.IntN(8)),
			fmt.Sprintf("/p/%d/x", rng.IntN(8)),
			time.Duration(rng.IntN(7200))*time.Second,
		)
		o.SurrogateKeys = []string{fmt.Sprintf("tag-%d", rng.IntN(48)), fmt.Sprintf("tag-%d", rng.IntN(48))}
		require.Equal(t, bruteForceMatch(b, o), b.matches(o), "step %d: lock-free match disagrees with the list", step)
	}
}

// TestBanList_SurrogateBanEnforcedWithoutRebuild pins the property the
// surrogate set exists for: a tag ban is visible to the very next read
// without dirtying or recompiling the snapshot.
func TestBanList_SurrogateBanEnforcedWithoutRebuild(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()
	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "seed.example.com"})
	require.NoError(t, err)
	require.True(t, s.MatchesActiveBan(banTestObj("seed.example.com", "/", time.Hour))) // compile
	before := snapPtr(&s.bans)

	for i := range 100 {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i)})
		require.NoError(t, err)
		require.False(t, s.bans.dirty.Load(), "a surrogate ban must not dirty the snapshot")
	}
	assert.True(t, s.MatchesActiveBan(surrogateObj("tag-99")), "newest tag ban enforced on the next read")
	assert.True(t, s.MatchesActiveBan(surrogateObj("tag-0")))
	assert.Same(t, before, snapPtr(&s.bans), "tag bans must never recompile the snapshot")
}

// TestBanList_SurrogateRefreshMovesExemption verifies a re-issued tag
// ban rewrites the shared exemption instant in place: objects stored
// between the two issuances become subject, without a new list entry.
func TestBanList_SurrogateRefreshMovesExemption(t *testing.T) {
	t.Parallel()
	b := &banListState{}
	pred, err := compileBanPredicate(api.BanExpr{SurrogateKey: "tag"})
	require.NoError(t, err)
	first := time.Now().Add(-time.Hour)
	b.register(api.BanExpr{SurrogateKey: "tag"}, pred, first)

	o := banTestObj("h", "/", 30*time.Minute) // stored after the first issuance
	o.SurrogateKeys = []string{"tag"}
	require.False(t, b.matches(o), "object stored after the ban is exempt")

	b.register(api.BanExpr{SurrogateKey: "tag"}, pred, time.Now())
	assert.True(t, b.matches(o), "the refresh must re-anchor the exemption")
	assert.Equal(t, 1, b.len(), "a refresh must not append")
	checkBanInvariants(t, b)
}

// TestBanList_EvictionDropsOldestFromEveryView fills a mixed list past
// its cap and checks the evicted bans stop matching in both read views.
func TestBanList_EvictionDropsOldestFromEveryView(t *testing.T) {
	t.Parallel()
	b := &banListState{capacity: 8}
	register := func(e api.BanExpr) {
		p, err := compileBanPredicate(e)
		require.NoError(t, err)
		b.register(e, p, time.Now())
	}
	register(api.BanExpr{HostRegex: "old.example.com"})
	register(api.BanExpr{SurrogateKey: "old-tag"})
	for i := range 8 {
		register(api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i)})
	}
	checkBanInvariants(t, b)
	assert.Equal(t, 8, b.len())
	assert.False(t, b.matches(banTestObj("old.example.com", "/", time.Hour)), "evicted host ban must leave the snapshot")
	assert.False(t, b.matches(surrogateObj("old-tag")), "evicted tag ban must leave the surrogate set")
	assert.True(t, b.matches(surrogateObj("tag-0")))
	assert.True(t, b.matches(surrogateObj("tag-7")))
}

// TestBanList_RegisterPopsExpiredHead verifies registration pops
// expired bans from the head before appending, for both views.
func TestBanList_RegisterPopsExpiredHead(t *testing.T) {
	t.Parallel()
	b := &banListState{ttl: time.Minute}
	for _, e := range []api.BanExpr{{SurrogateKey: "stale-tag"}, {HostRegex: "stale.example.com"}} {
		p, err := compileBanPredicate(e)
		require.NoError(t, err)
		b.register(e, p, time.Now().Add(-2*time.Minute))
	}
	p, err := compileBanPredicate(api.BanExpr{SurrogateKey: "live"})
	require.NoError(t, err)
	b.register(api.BanExpr{SurrogateKey: "live"}, p, time.Now())

	assert.Equal(t, 1, b.len(), "expired head bans must be popped by the next registration")
	checkBanInvariants(t, b)
	assert.False(t, b.matches(surrogateObj("stale-tag")))
	assert.False(t, b.matches(banTestObj("stale.example.com", "/", time.Hour)))
}

// TestBanList_PruneSweepsExpiredFromMiddle covers the case the head pop
// cannot: an expired ban behind a refreshed (live) head is removed by
// the reaper sweep, from every view.
func TestBanList_PruneSweepsExpiredFromMiddle(t *testing.T) {
	t.Parallel()
	b := &banListState{ttl: time.Minute}
	reg := func(e api.BanExpr, at time.Time) {
		p, err := compileBanPredicate(e)
		require.NoError(t, err)
		b.register(e, p, at)
	}
	reg(api.BanExpr{SurrogateKey: "head"}, time.Now())
	reg(api.BanExpr{SurrogateKey: "mid-tag"}, time.Now())
	reg(api.BanExpr{HostRegex: "mid.example.com"}, time.Now())
	reg(api.BanExpr{SurrogateKey: "tail"}, time.Now())
	b.mu.Lock()
	for _, ban := range b.list[1:3] {
		ban.created = time.Now().Add(-2 * time.Minute)
	}
	b.mu.Unlock()

	b.pruneExpired(time.Now())
	assert.Equal(t, 2, b.len())
	checkBanInvariants(t, b)
	assert.False(t, b.matches(surrogateObj("mid-tag")))
	assert.False(t, b.matches(banTestObj("mid.example.com", "/", time.Hour)))
	assert.True(t, b.matches(surrogateObj("head")))
	assert.True(t, b.matches(surrogateObj("tail")))
}

// TestBanList_SubjectToNanosMatchesSubjectTo pins the hit-path
// exemption encoding against the reference rule, including the zero
// exemption and a zero StoredAt.
func TestBanList_SubjectToNanosMatchesSubjectTo(t *testing.T) {
	t.Parallel()
	now := time.Now()
	for _, exempt := range []time.Time{{}, now, now.Add(-time.Hour), time.Unix(0, 0)} {
		for _, stored := range []time.Time{{}, time.Unix(0, 0), time.Unix(0, 1), now.Add(-2 * time.Hour), now, now.Add(time.Nanosecond), now.Add(time.Hour)} {
			ban := &activeBan{}
			ban.setExemptAfter(exempt)
			o := &api.Object{StoredAt: stored}
			assert.Equal(t, subjectTo(o, exempt), subjectToNanos(o, ban.exemptNanos.Load()),
				"exempt=%v stored=%v", exempt, stored)
		}
	}
}

// TestBanList_ConcurrentSurrogateStorm hammers lock-free surrogate
// reads while tag registrations churn the list past its cap (shard
// copies on every put/remove), refreshes rewrite exemption instants,
// and reaper sweeps run. Under -race this pins that the shard
// publication and the atomic exemption are race-free; the views must
// still agree with the list afterwards.
func TestBanList_ConcurrentSurrogateStorm(t *testing.T) {
	t.Parallel()
	b := &banListState{capacity: 64}
	pred, err := compileBanPredicate(api.BanExpr{SurrogateKey: "x"})
	require.NoError(t, err)
	pinned := api.BanExpr{SurrogateKey: "pinned"}
	b.register(pinned, pred, time.Now())

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			b.register(api.BanExpr{SurrogateKey: fmt.Sprintf("storm-%d", i)}, pred, time.Now())
			if i%32 == 0 {
				// Refresh the pinned ban: readers race the atomic
				// exemption rewrite, and — once the pinned ban reaches
				// the head and is evicted — its shard removal and the
				// re-publish.
				b.register(pinned, pred, time.Now())
			}
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				b.pruneExpired(time.Now())
			}
		}
	})
	pinnedObj := surrogateObj("pinned")
	for range 6 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_ = b.matches(surrogateObj("storm-1"))
					_ = b.matches(pinnedObj)
				}
			}
		})
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	checkBanInvariants(t, b)
}

// TestBanList_RegisterAllocsBoundedAtLargeCap pins the cost model: a
// distinct registration on a saturated list allocates the ban, one
// copied surrogate-set shard (the table keeps ~2 bans per shard at any
// cap), and amortized list growth — nothing proportional to the list.
func TestBanList_RegisterAllocsBoundedAtLargeCap(t *testing.T) {
	// Not parallel: testing.AllocsPerRun forbids parallel tests.
	pred, err := compileBanPredicate(api.BanExpr{SurrogateKey: "x"})
	require.NoError(t, err)
	measure := func(capacity int) float64 {
		b := &banListState{capacity: capacity}
		for i := range capacity {
			b.register(api.BanExpr{SurrogateKey: fmt.Sprintf("fill-%d", i)}, pred, time.Now())
		}
		keys := make([]string, 1000)
		for i := range keys {
			keys[i] = fmt.Sprintf("new-%d", i)
		}
		next := 0
		return testing.AllocsPerRun(500, func() {
			b.register(api.BanExpr{SurrogateKey: keys[next%len(keys)]}, pred, time.Now())
			next++
		})
	}
	small, large := measure(1024), measure(65536)
	t.Logf("allocs per distinct registration: cap=1024 %.2f, cap=65536 %.2f", small, large)
	assert.LessOrEqual(t, small, float64(5), "registration at the default cap")
	assert.LessOrEqual(t, large, small+1, "registration allocations must not scale with the cap")
}

// TestBanList_MixedHitObject sanity-checks a hit object carrying both
// a matching host and tags through the combined read path.
func TestBanList_MixedHitObject(t *testing.T) {
	t.Parallel()
	b := &banListState{}
	for _, e := range []api.BanExpr{{HostRegex: "a.example.com"}, {SurrogateKey: "t1"}} {
		p, err := compileBanPredicate(e)
		require.NoError(t, err)
		b.register(e, p, time.Now())
	}
	o := banTestObj("b.example.com", "/", time.Hour)
	o.Header.Set(header.XBouineHost, "b.example.com")
	o.SurrogateKeys = []string{"t0", "t1"}
	assert.True(t, b.matches(o), "a tag hit must match even when the host does not")
	o.SurrogateKeys = slices.Delete(o.SurrogateKeys, 1, 2)
	assert.False(t, b.matches(o))
}
