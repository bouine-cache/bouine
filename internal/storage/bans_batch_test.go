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

// snapPtr returns the identity of the currently compiled snapshot so
// tests can observe whether a registration rebuilt it eagerly.
func snapPtr(b *banListState) *banSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snap
}

// TestBanRegister_DoesNotRebuildSnapshotEagerly pins the amortization
// this change exists for: a registration must NOT compile a new
// snapshot of the whole list (the O(list) allocation burst that, at
// production ban-storm rates over a list saturated at banListCap, put
// ~7 GB of transient garbage on the heap per storm). The rebuild is
// deferred to the next snapshot() read, so a batch of N registrations
// costs one rebuild, not N.
func TestBanRegister_DoesNotRebuildSnapshotEagerly(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-1"})
	require.NoError(t, err)
	// Force the initial compile with a read, so the pointer captured
	// below is the published snapshot.
	require.True(t, s.MatchesActiveBan(surrogateObj("tag-1")))
	before := snapPtr(&s.bans)
	require.NotNil(t, before)

	_, err = s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-2"})
	require.NoError(t, err)

	assert.Same(t, before, snapPtr(&s.bans),
		"register must not rebuild the snapshot eagerly")

	// The next read compiles the batch: the newest ban is enforced
	// immediately (no staleness window — the first lookup after the
	// batch sees it).
	obj := banTestObj("any.example.com", "/p/1", time.Hour)
	obj.SurrogateKeys = []string{"tag-2"}
	assert.True(t, s.MatchesActiveBan(obj), "lazy rebuild must enforce the newest ban")
	assert.NotSame(t, before, snapPtr(&s.bans), "the read must have compiled a fresh snapshot")
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

	const batch = 100
	for i := range batch {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("tag-%d", i)})
		require.NoError(t, err)
	}

	// One read enforces the whole batch — first and last members alike.
	assert.True(t, s.MatchesActiveBan(surrogateObj("tag-0")))
	assert.True(t, s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", batch-1))))
}

// TestBanRegister_AllocBudget asserts the steady-state allocation cost
// of registration against a list pre-filled to half the cap. Before the
// deferred rebuild, each register copied the whole list and recompiled
// the snapshot (~150 KB per ban at the cap); now a refresh or an
// in-place append allocates at most the amortized append growth.
func TestBanRegister_AllocBudget(t *testing.T) {
	// Not parallel: testing.AllocsPerRun forbids parallel tests.

	b := &banListState{}
	pred, err := compileBanPredicate(api.BanExpr{SurrogateKey: "tag"})
	require.NoError(t, err)

	for i := range banListCap / 2 {
		b.register(api.BanExpr{SurrogateKey: fmt.Sprintf("warm-%d", i)}, pred, time.Now())
	}
	_ = b.snapshot() // compile once so only register is measured below

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
// snapshots. Run under -race (the suite default). After the writers
// stop, the final read must enforce every registered tag — the 256
// distinct keys sit far below banListCap, so none can be evicted.
func TestBanRegister_ConcurrentRegisterAndRead(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	const writers = 4
	const tags = 256
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = s.Ban(context.Background(), api.BanExpr{
					SurrogateKey: fmt.Sprintf("tag-%d", i%tags),
				})
				i++
			}
		}()
	}

	// Interleave reads with the registrations: each read either
	// compiles a fresh snapshot or reuses the published one.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		_ = s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", tags/2)))
	}
	close(stop)
	wg.Wait()

	// The final read compiles the settled list; every tag is enforced.
	for k := range tags {
		assert.True(t, s.MatchesActiveBan(surrogateObj(fmt.Sprintf("tag-%d", k))),
			"final snapshot must enforce every registered tag")
	}
}
