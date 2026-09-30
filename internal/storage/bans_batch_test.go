package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// snapPtr returns the identity of the currently published snapshot so
// tests can observe whether a registration rebuilt it eagerly.
func snapPtr(b *banListState) *banSnapshot {
	return b.snap.Load()
}

// TestBanRegister_DoesNotRebuildSnapshotEagerly pins the amortization
// this change exists for: a registration must NOT compile a new
// snapshot of the whole list (the O(list) allocation burst that, at
// production ban-storm rates over a list saturated at banListCap, put
// ~7 GB of transient garbage on the heap per storm). A burst arriving
// within banFlushDelay of the last rebuild defers publication to the
// coalescing timer — here pinned deterministically by disabling the
// timer and flushing by hand.
func TestBanRegister_DoesNotRebuildSnapshotEagerly(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()
	s.bans.flushDelay = time.Hour

	_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-1"})
	require.NoError(t, err)
	// Sparse registration: flushed synchronously, enforced immediately.
	require.True(t, s.MatchesActiveBan(surrogateObj("tag-1")))
	before := snapPtr(&s.bans)
	require.NotNil(t, before)

	_, err = s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-2"})
	require.NoError(t, err)

	assert.Same(t, before, snapPtr(&s.bans),
		"a burst registration must not rebuild the snapshot eagerly")

	// The deferred flush publishes the batch: the newest ban is
	// enforced without any lookup having paid a compile.
	s.bans.flushSnapshot()
	obj := banTestObj("any.example.com", "/p/1", time.Hour)
	obj.SurrogateKeys = []string{"tag-2"}
	assert.True(t, s.MatchesActiveBan(obj), "deferred flush must enforce the newest ban")
	assert.NotSame(t, before, snapPtr(&s.bans), "the flush must have published a fresh snapshot")
}

// TestBanRegister_OneRebuildPerBatch verifies N distinct registrations
// between two reads cost exactly one snapshot compile: only the newest
// snapshot pointer exists after the read, and every ban in the batch is
// enforced by it.
func TestBanRegister_OneRebuildPerBatch(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "seed"})
	require.NoError(t, err)
	require.True(t, s.MatchesActiveBan(surrogateObj("seed")), "seed ban enforced")
	rebuildsAfterSeed := s.bans.rebuilds.Load()

	const batch = 100
	for i := range batch {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i)})
		require.NoError(t, err)
	}

	// The burst (all inside banFlushDelay of the seed's synchronous
	// flush) defers to the coalescing timer; one flush publishes the
	// whole batch — first and last members alike — at the cost of a
	// single compile.
	s.bans.flushSnapshot()
	assert.Equal(t, rebuildsAfterSeed+1, s.bans.rebuilds.Load(),
		"the whole batch must cost exactly one rebuild")
	assert.True(t, s.MatchesActiveBan(surrogateObj("tag-0")))
	assert.True(t, s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", batch-1))))
}

// TestBanRegister_AllocBudget asserts the steady-state allocation cost
// of registration against a list pre-filled to half the cap. Registrations
// never compile the snapshot (publication is synchronous for sparse
// bans, coalesced on the timer for bursts), so a refresh or an
// in-place append allocates at most the amortized append growth.
func TestBanRegister_AllocBudget(t *testing.T) {
	// Not parallel: testing.AllocsPerRun forbids parallel tests.

	b := &banListState{flushDelay: time.Hour} // no timer rebuilds mid-measurement
	pred, err := compileBanPredicate(api.BanExpr{SurrogateKey: "tag"})
	require.NoError(t, err)

	for i := range banListCap / 2 {
		b.register(api.BanExpr{SurrogateKey: fmt.Sprintf("warm-%d", i)}, pred, time.Now())
	}
	b.flushSnapshot() // publish once so only register is measured below

	refreshAllocs := testing.AllocsPerRun(200, func() {
		b.register(api.BanExpr{SurrogateKey: "warm-0"}, pred, time.Now())
	})
	assert.LessOrEqual(t, refreshAllocs, float64(1),
		"re-issuing a known ban must not allocate (in-place refresh)")

	// Pre-built distinct keys so the measured closure allocates nothing
	// of its own.
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("new-%d", i)
	}
	next := 0
	appendAllocs := testing.AllocsPerRun(200, func() {
		b.register(api.BanExpr{SurrogateKey: keys[next]}, pred, time.Now())
		next++
	})
	assert.LessOrEqual(t, appendAllocs, float64(2),
		"appending a distinct ban must cost only amortized slice growth")
}

// TestBanRegister_StormEnforcedWithinFlushDelay pins the visibility
// bound end-to-end with the production timer: a burst of registrations
// defers publication, and the newest ban must be enforced by the lazy
// lookup path within the flush window. Bounded polling (no sleeps).
func TestBanRegister_StormEnforcedWithinFlushDelay(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	for i := range 50 {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("storm-%d", i)})
		require.NoError(t, err)
	}

	obj := surrogateObj("storm-49")
	require.Eventually(t, func() bool { return s.MatchesActiveBan(obj) },
		10*banFlushDelay, banFlushDelay/10,
		"a registered ban must be enforced within the coalescing window")
}

// TestBanRegister_StatsExposeRebuildAmortization verifies the Stats()
// ban fields report the storm-health signal: registrations count every
// ban, rebuilds stay amortized (one flush per burst), and the list
// size and last-rebuild duration are populated.
func TestBanRegister_StatsExposeRebuildAmortization(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	const burst = 10
	for i := range burst {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("stat-%d", i)})
		require.NoError(t, err)
	}
	s.bans.flushSnapshot()

	stats := s.Stats()
	assert.Equal(t, int64(burst), stats.BanRegistrations)
	assert.Equal(t, int64(2), stats.BanSnapshotRebuilds,
		"one burst must cost the initial synchronous flush plus one deferred flush, not one rebuild per ban")
	assert.Equal(t, int64(burst), stats.BanListEntries)
	assert.Positive(t, stats.BanLastRebuildNanos)
}

// TestBanRegister_CapEvictionInPlace verifies the list stays capped at
// banListCap with the oldest entries dropped, without per-registration
// list copies, and that eviction drops the oldest distinct ban.
func TestBanRegister_CapEvictionInPlace(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	for i := range banListCap + 10 {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i)})
		require.NoError(t, err)
	}
	s.bans.flushSnapshot() // burst: publication deferred to the coalescing flush

	assert.Equal(t, banListCap, s.bans.len(), "list must stay capped")

	// The earliest tags were evicted (oldest first); the latest survive.
	assert.False(t, s.MatchesActiveBan(surrogateObj("tag-0")), "oldest ban must be evicted")
	assert.True(t, s.MatchesActiveBan(surrogateObj("tag-10")), "post-eviction bans must survive")
	assert.True(t, s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", banListCap+9))), "newest ban must survive")
}

// surrogateObj returns a subject object carrying the given surrogate key.
func surrogateObj(key string) *api.Object {
	obj := banTestObj("any.example.com", "/p", time.Hour)
	obj.SurrogateKeys = []string{key}
	return obj
}

// TestBanRegister_ConcurrentRegisterAndRead hammers registrations and
// snapshot reads concurrently: registrations mutate the list in place
// under the mutex while reads lazily compile and publish immutable
// snapshots. Run under -race (the suite default).
//
// Deterministic by construction (AGENTS.md §8 forbids clock-driven
// assertions): every writer registers a fixed, disjoint set of tags, so
// the full 256-tag space is covered regardless of scheduling — the
// original time-driven form failed on a loaded CI runner where writer
// goroutines were starved and never reached the tail of the tag space
// before the deadline. The reader loop is bounded by registration
// progress (it stops after the writers finish), not by wall clock,
// and the final read must enforce every registered tag — the 256
// distinct keys sit far below banListCap, so none can be evicted.
func TestBanRegister_ConcurrentRegisterAndRead(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	const writers = 4
	const tags = 256
	const perWriter = tags / writers
	// Each writer owns a disjoint slice of the tag space, so every tag
	// is registered exactly once no matter how the scheduler interleaves
	// the writers.
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				_, _ = s.Ban(context.Background(), api.BanExpr{
					SurrogateKey: fmt.Sprintf("tag-%d", w*perWriter+i),
				})
			}
		})
	}

	// Interleave reads with the registrations: each read either
	// compiles a fresh snapshot or reuses the published one. The loop
	// ends when the writers are done, so no tag can be asserted before
	// its registration has happened; a read can observe a snapshot
	// compiled before some writers finish — which is fine, the final
	// assertion below re-reads the settled state.
	readerStop := make(chan struct{})
	readsDone := make(chan struct{})
	go func() {
		defer close(readsDone)
		for {
			select {
			case <-readerStop:
				return
			default:
				_ = s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", tags/2)))
			}
		}
	}()
	wg.Wait()
	close(readerStop)
	<-readsDone

	// Publish the settled list (the writers burst, so publication was
	// deferred to the coalescing flush); every tag is enforced.
	s.bans.flushSnapshot()
	for k := range tags {
		assert.True(t, s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", k))),
			"final snapshot must enforce every registered tag")
	}
}
