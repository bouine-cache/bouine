package warm

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestEvictToFitBatch_MultiEvict verifies that evictToFitBatch evicts
// the correct number of entries in a single lock acquisition cycle and
// fires OnEvict for each.
func TestEvictToFitBatch_MultiEvict(t *testing.T) {
	t.Parallel()

	const (
		seedEntries = 100
		seedBody    = 100 // 120 bytes per record
	)

	dir := t.TempDir()
	seedBudget := int64(seedEntries) * (HeaderLen + seedBody + FooterLen)
	s, err := NewStore(Config{Dir: dir, MaxBytes: seedBudget, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, seedBody)
	for i := range seedEntries {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	var evicted atomic.Int64
	s.OnEvict = func(key api.Key) {
		evicted.Add(1)
	}

	// Require 10 evictions: 10 * 120 = 1200 bytes.
	largeRecSize := int64(10 * (HeaderLen + seedBody + FooterLen))
	beforeEntries := s.stats.entries.Load()

	s.mu.RLock()
	if err := s.evictToFitBatchLocked(largeRecSize); err != nil {
		s.mu.RUnlock()
		t.Fatalf("evictToFitBatchLocked: %v", err)
	}
	s.mu.RUnlock()

	afterEntries := s.stats.entries.Load()
	afterBytes := s.stats.bytes.Load()

	evictedCount := beforeEntries - afterEntries
	if evictedCount < 10 {
		t.Errorf("evicted %d entries, want >= 10", evictedCount)
	}
	assert.Equal(t, evictedCount, evicted.Load())
	// After eviction, bytes + largeRecSize should fit.
	if afterBytes+largeRecSize > seedBudget {
		t.Errorf("afterBytes=%d + recSize=%d > budget=%d", afterBytes, largeRecSize, seedBudget)
	}
}

// TestEvictToFitBatch_NoEvictionNeeded verifies the fast path: when
// the budget already accommodates recSize, no eviction occurs.
func TestEvictToFitBatch_NoEvictionNeeded(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	var evicted atomic.Int64
	s.OnEvict = func(key api.Key) {
		evicted.Add(1)
	}

	_, _, err = s.Put(api.NewKeyFromUint64(1), make([]byte, 100))
	require.NoError(t, err, "Put")

	// Small record, plenty of budget left.
	s.mu.RLock()
	if err := s.evictToFitBatchLocked(int64(HeaderLen + 100 + FooterLen)); err != nil {
		s.mu.RUnlock()
		t.Fatalf("evictToFitBatchLocked: %v", err)
	}
	s.mu.RUnlock()
	assert.Equal(t, int64(0), evicted.Load())
}

// TestEvictToFitBatch_OverBudget verifies that a record larger than the
// entire budget is rejected without evicting anything.
func TestEvictToFitBatch_OverBudget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	budget := int64(1000)
	s, err := NewStore(Config{Dir: dir, MaxBytes: budget, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	var evicted atomic.Int64
	s.OnEvict = func(key api.Key) {
		evicted.Add(1)
	}

	_, _, err = s.Put(api.NewKeyFromUint64(1), make([]byte, 100))
	require.NoError(t, err, "Put")

	// Record larger than entire budget.
	hugeRec := budget + 1
	s.mu.RLock()
	if err := s.evictToFitBatchLocked(hugeRec); err == nil {
		s.mu.RUnlock()
		t.Fatal("expected ErrOverBudget, got nil")
	}
	s.mu.RUnlock()
	assert.Equal(t, int64(0), evicted.Load())
}

// TestEvictToFitBatch_PutIntegration verifies that Put triggers batch
// eviction when over budget and succeeds.
func TestEvictToFitBatch_PutIntegration(t *testing.T) {
	t.Parallel()

	const (
		seedEntries = 50
		seedBody    = 100
	)

	dir := t.TempDir()
	seedBudget := int64(seedEntries) * (HeaderLen + seedBody + FooterLen)
	s, err := NewStore(Config{Dir: dir, MaxBytes: seedBudget, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, seedBody)
	for i := range seedEntries {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	var evicted atomic.Int64
	s.OnEvict = func(key api.Key) {
		evicted.Add(1)
	}

	// Put a large record that forces multiple evictions.
	largeBody := make([]byte, 10*(HeaderLen+seedBody+FooterLen))
	_, _, err = s.Put(api.NewKeyFromUint64(999), largeBody)
	require.NoError(t, err, "Put with batch eviction")

	if evicted.Load() < 10 {
		t.Errorf("OnEvict fired %d times, want >= 10", evicted.Load())
	}

	// Verify the large key is retrievable.
	_, _, ok := s.Lookup(api.NewKeyFromUint64(999))
	require.True(t, ok)
}

// TestEvictToFitBatch_EmptyStore verifies behavior on an empty warm tier.
func TestEvictToFitBatch_EmptyStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1000, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	// Small record fits in empty budget.
	s.mu.RLock()
	if err := s.evictToFitBatchLocked(int64(HeaderLen + 100 + FooterLen)); err != nil {
		s.mu.RUnlock()
		t.Fatalf("evictToFitBatchLocked on empty store: %v", err)
	}
	s.mu.RUnlock()
}

// TestEvictToFitBatch_NoVictimsAvailable verifies that when all entries
// are protected and no victim can be found, ErrOverBudget is returned.
func TestEvictToFitBatch_NoVictimsAvailable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	budget := int64(HeaderLen + 100 + FooterLen)
	s, err := NewStore(Config{Dir: dir, MaxBytes: budget, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	// Insert one entry that fills the budget.
	_, _, err = s.Put(api.NewKeyFromUint64(1), make([]byte, 100))
	require.NoError(t, err, "Put")

	// Mark it protected so pickEvictVictim skips it.
	s.idxMu.Lock()
	if loc, ok := s.index[api.NewKeyFromUint64(1)]; ok {
		loc.protected = true
		s.index[api.NewKeyFromUint64(1)] = loc
	}
	s.idxMu.Unlock()

	// Try to insert a record that requires eviction — should fail.
	s.mu.RLock()
	if err := s.evictToFitBatchLocked(int64(HeaderLen + 100 + FooterLen)); err == nil {
		s.mu.RUnlock()
		t.Fatal("expected ErrOverBudget, got nil")
	}
	s.mu.RUnlock()
}

// TestEvictToFitBatch_MidBatchFailure verifies that if a tombstone write
// fails mid-batch (segment file I/O error), already-evicted entries are
// gone from the index while un-evicted entries remain. The partial
// eviction state must be consistent.
func TestEvictToFitBatch_MidBatchFailure(t *testing.T) {
	t.Parallel()

	const (
		seedEntries = 20
		seedBody    = 100
	)

	dir := t.TempDir()
	seedBudget := int64(seedEntries) * (HeaderLen + seedBody + FooterLen)
	s, err := NewStore(Config{Dir: dir, MaxBytes: seedBudget, SegMax: 1 << 20, SegmentCacheSize: -1})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, seedBody)
	for i := range seedEntries {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	var evictedCount atomic.Int64
	s.OnEvict = func(key api.Key) {
		evictedCount.Add(1)
	}

	// Close the active segment's underlying OS file descriptor but leave
	// seg.f non-nil so openLocked is not triggered. writeRecordAt will
	// get a "bad file descriptor" error from the OS, simulating an I/O
	// failure on the first tombstone write in the batch.
	s.mu.RLock()
	seg, err := s.activeSegRLocked()
	if err != nil {
		s.mu.RUnlock()
		t.Fatalf("activeSegRLocked: %v", err)
	}
	seg.mu.Lock()
	if seg.f != nil {
		// Close the raw fd without setting seg.f = nil, so the
		// seg.f == nil guard in evictToFitBatchLocked does not reopen it.
		_ = seg.f.Close()
	}
	seg.mu.Unlock()

	// Require 5 evictions. The first writeTombstoneLocked will fail
	// because the OS file descriptor is closed. evictToFitBatchLocked
	// should return ErrOverBudget with zero evictions.
	recSize := int64(5 * (HeaderLen + seedBody + FooterLen))
	err = s.evictToFitBatchLocked(recSize)
	s.mu.RUnlock()
	require.Error(t, err)

	// No evictions should have completed because the first tombstone
	// write fails and writeTombstoneLocked restores the SIEVE entry.
	assert.Equal(t, int64(0), evictedCount.Load())

	// All seed entries should still be in the index.
	s.idxMu.RLock()
	indexLen := len(s.index)
	s.idxMu.RUnlock()
	assert.Equal(t, seedEntries, indexLen)
}

// TestOverBudget_EntryCap verifies OverBudget returns true when the
// entry count exceeds maxEntries even if byte budget is not exceeded.
func TestOverBudget_EntryCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1 << 20, MaxEntries: 5, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 5 {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	require.True(t, s.OverBudget())
}

// TestOverBudget_EntryCapNotReached verifies OverBudget returns false
// when entries are under the cap.
func TestOverBudget_EntryCapNotReached(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1 << 20, MaxEntries: 10, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 5 {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	require.False(t, s.OverBudget())
}

// TestPut_EntryCapRejects verifies that Put rejects when the entry cap
// is reached and eviction cannot free space (all entries protected).
func TestPut_EntryCapRejects(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// No byte budget, only entry cap. 3 entries max.
	s, err := NewStore(Config{Dir: dir, MaxEntries: 3, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 3 {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	// Mark all entries protected so eviction cannot find a victim.
	s.idxMu.Lock()
	for k := range s.index {
		loc := s.index[k]
		loc.protected = true
		s.index[k] = loc
	}
	s.idxMu.Unlock()

	// 4th Put should be rejected — entry cap reached, no evictable victim.
	_, _, err = s.Put(api.NewKeyFromUint64(99), body)
	require.Error(t, err)
}

// TestPut_EntryCapEvictsAndSucceeds verifies that Put triggers eviction
// when entry cap is reached and succeeds when a non-protected victim exists.
func TestPut_EntryCapEvictsAndSucceeds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// No byte budget, only entry cap. 3 entries max.
	s, err := NewStore(Config{Dir: dir, MaxEntries: 3, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 3 {
		_, _, err := s.Put(api.NewKeyFromUint64(uint64(i)), body)
		require.NoErrorf(t, err, "Put(%d)", i)
	}

	// 4th Put should evict one entry and succeed.
	_, _, err = s.Put(api.NewKeyFromUint64(99), body)
	require.NoError(t, err, "Put with entry cap eviction")

	assert.Equal(t, int64(3), s.stats.entries.Load())
}
