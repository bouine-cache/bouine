package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestBanSnapshot_ConcurrentReadsDuringRegistrations hammers the
// lock-free snapshot read from many goroutines while registrations and
// reaper prunes mutate the list — run under -race this pins that the
// atomic publication is race-free and that every read returns a
// well-formed snapshot (issue #757).
func TestBanSnapshot_ConcurrentReadsDuringRegistrations(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()
	// The seed ban matches the seed object; live registrations do not.
	seed := banTestObj("seed.example.com", "/seed", time.Hour)
	_, err := s.Ban(context.Background(), api.BanExpr{
		HostRegex: `^seed\.example\.com$`,
	})
	require.NoError(t, err)
	require.True(t, s.MatchesActiveBan(seed))
	for i := range 32 {
		_, err := s.Ban(context.Background(), api.BanExpr{
			HostRegex: fmt.Sprintf(`^live-seed-%d\.example\.com$`, i),
		})
		require.NoError(t, err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Registrations + refreshes (dirty transitions).
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = s.Ban(context.Background(), api.BanExpr{
					HostRegex: fmt.Sprintf(`^live-%d-%d\.example\.com$`, time.Now().UnixNano(), i),
				})
				i++
			}
		}
	}()
	// Reaper prunes (rebuild from the writer path).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.bans.pruneExpired(time.Now())
			}
		}
	}()
	// Parallel readers.
	for r := range 6 {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					o := banTestObj(fmt.Sprintf("r-%d.example.com", r), "/p", time.Hour)
					s.MatchesActiveBan(o)
					// The seeded ban must be enforced by every snapshot
					// the reader ever observes, old or new: a stale
					// snapshot during a batch only defers NEW bans, it
					// never drops existing ones (the list is
					// append-only between prunes, and prunes only
					// remove expired entries).
					if !s.MatchesActiveBan(seed) {
						// Seed host never expires within the test; a
						// miss means a snapshot lost the seed ban —
						// corruption, not staleness.
						panic("snapshot lost an existing ban")
					}
				}
			}
		}(r)
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestBanSnapshot_ZeroValueReturnsEmptyView pins the zero-value
// fallback: a banListState that never registered and never compiled
// must serve matches() as a clean empty view, not nil.
func TestBanSnapshot_ZeroValueReturnsEmptyView(t *testing.T) {
	t.Parallel()
	b := &banListState{}
	snap := b.snapshot()
	require.NotNil(t, snap)
	assert.False(t, snap.matches(banTestObj("any.example.com", "/p", time.Hour)),
		"zero-value snapshot must reject (no bans active)")
	// Repeat after a dirty mark without any rebuild: still never nil.
	b.dirty.Store(true)
	snap = b.snapshot()
	require.NotNil(t, snap)
}

// TestBanSnapshot_DirtyBatchAmortizesToSingleRebuild pins the
// amortization contract under the lock-free design: a batch of N
// registrations costs exactly one rebuild on the next read, and the
// rebuilt snapshot enforces the newest ban (no unbounded staleness).
func TestBanSnapshot_DirtyBatchAmortizesToSingleRebuild(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: "tag-1"})
	require.NoError(t, err)
	require.True(t, s.MatchesActiveBan(surrogateObj("tag-1"))) // initial compile
	before := snapPtr(&s.bans)

	for i := range 8 {
		_, err := s.Ban(context.Background(), api.BanExpr{SurrogateKey: fmt.Sprintf("batch-%d", i)})
		require.NoError(t, err)
	}
	assert.Same(t, before, snapPtr(&s.bans),
		"a registration batch must not rebuild eagerly")

	// The first read after the batch compiles once and enforces every
	// new ban immediately.
	obj := banTestObj("any.example.com", "/p", time.Hour)
	obj.SurrogateKeys = []string{"batch-7"}
	assert.True(t, s.MatchesActiveBan(obj),
		"the post-batch read must enforce the newest ban")
	assert.NotSame(t, before, snapPtr(&s.bans))
}

// TestBanSnapshot_FirstReadAfterRegistrationEnforcesNewBan pins the
// enforcement freshness contract end to end through the exported
// surface: immediately after Ban returns, the very next hit-path
// matchesActiveBan must see the ban (the TryLock winner rebuilds, so
// there is no read that observes a dirty-but-stale state twice).
func TestBanSnapshot_FirstReadAfterRegistrationEnforcesNewBan(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: `^fresh\.example\.com$`})
	require.NoError(t, err)
	require.True(t, s.MatchesActiveBan(banTestObj("fresh.example.com", "/p", time.Hour)),
		"first read after registration must enforce the ban")
	assert.False(t, s.MatchesActiveBan(banTestObj("other.example.com", "/p", time.Hour)))
}

// TestBanSnapshot_TryLockLoserServesPreviousSnapshot pins the
// staleness semantics of the TryLock race: while a rebuild is in
// flight (mutex held), concurrent readers serve the previous snapshot
// rather than parking — and once the rebuild completes, new bans are
// enforced.
func TestBanSnapshot_TryLockLoserServesPreviousSnapshot(t *testing.T) {
	t.Parallel()
	b := &banListState{ttl: time.Hour}
	pred, err := compileBanPredicate(api.BanExpr{HostRegex: `^base\.example\.com$`})
	require.NoError(t, err)
	b.register(api.BanExpr{HostRegex: `^base\.example\.com$`}, pred, time.Now())
	require.NotNil(t, b.snapshot())

	// Hold the mutex the way a rebuild would; readers must not park.
	b.mu.Lock()
	served := make(chan *banSnapshot, 8)
	for range 8 {
		go func() { served <- b.snapshot() }()
	}
	for range 8 {
		select {
		case s := <-served:
			require.NotNil(t, s, "reader must serve the previous snapshot, never nil")
			assert.True(t, s.matches(banTestObj("base.example.com", "/p", time.Hour)),
				"served snapshot must remain enforceable")
		case <-time.After(2 * time.Second):
			t.Fatal("snapshot() parked on the mutex — readers must not block")
		}
	}
	b.mu.Unlock()
}

// TestBanSnapshot_ReaperPrunePublishesNewSnapshot pins that the
// reaper's rebuild publishes through the atomic pointer: expired bans
// disappear from the next read, unexpired ones stay enforced.
func TestBanSnapshot_ReaperPrunePublishesNewSnapshot(t *testing.T) {
	t.Parallel()
	// Reaper loop disabled; reapExpired is called manually so the test
	// controls the rebuild timing.
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	oldObj := banTestObj("old.example.com", "/p", time.Hour)
	liveObj := banTestObj("live.example.com", "/p", time.Hour)
	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: `^old\.example\.com$`})
	require.NoError(t, err)
	_, err = s.Ban(context.Background(), api.BanExpr{HostRegex: `^live\.example\.com$`})
	require.NoError(t, err)
	require.True(t, s.MatchesActiveBan(oldObj))

	// Age the old ban past the default TTL directly in the list (same
	// pattern as TestBanReaper_PrunesExpiredBans) — registering the
	// live ban already pruned expired entries, so the age is applied
	// after both registrations.
	s.bans.mu.Lock()
	for i := range s.bans.list {
		if s.bans.list[i].pattern.hostRegex == `^old\.example\.com$` {
			s.bans.list[i].created = time.Now().Add(-defaultBanTTL - time.Minute)
		}
	}
	s.bans.mu.Unlock()
	s.bans.dirty.Store(true)
	s.reapExpired(time.Now())

	assert.False(t, s.MatchesActiveBan(oldObj), "expired ban must not match after prune")
	assert.True(t, s.MatchesActiveBan(liveObj), "unexpired ban must stay enforced")
	assert.Equal(t, 1, s.bans.len(), "only the unexpired ban survives")
}

// TestBanSnapshot_HitPathServesStoredBansDuringBatch proves the
// production behavior the fix exists for at the store level: during a
// registration batch, previously-stored hits keep serving (no parked
// readers) and the batch's bans become enforced on the next read.
func TestBanSnapshot_HitPathServesStoredBansDuringBatch(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()
	k := testkey.Hash([]byte("during-batch"))
	obj := obj(k, 1024)
	require.NoError(t, s.Put(context.Background(), k, obj))
	first, _, err := s.Get(context.Background(), k) // visited bit set
	require.NoError(t, err)
	require.NotNil(t, first)

	_, err = s.Ban(context.Background(), api.BanExpr{HostRegex: `^other\.example\.com$`})
	require.NoError(t, err)

	// Second hit must serve while the state is dirty (batch pending).
	got, _, err := s.Get(context.Background(), k)
	require.NoError(t, err)
	require.NotNil(t, got, "hit during a dirty batch must not stall or miss")
}

// BenchmarkGate entries for the ban-snapshot read path (issue #757).
// Budgets are enforced in bench/run.sh's BUDGETS map.

func BenchmarkGate_HotStore_Get_Hit_BanSnapshotRead(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	defer func() { _ = s.Close(context.Background()) }()
	k := testkey.Hash([]byte("bench-ban-read"))
	_ = s.Put(context.Background(), k, obj(k, 1024))
	_, _ = s.Ban(context.Background(), api.BanExpr{HostRegex: `^banned\.example\.com$`})
	o := banTestObj("clean.example.com", "/p", time.Hour)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		s.MatchesActiveBan(o)
	}
}
