package warm

import (
	"sync/atomic"
	"testing"
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, seedBody)
	for i := range seedEntries {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var evicted atomic.Int64
	s.OnEvict = func(key uint64) {
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
	if evicted.Load() != evictedCount {
		t.Errorf("OnEvict fired %d times, want %d", evicted.Load(), evictedCount)
	}
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var evicted atomic.Int64
	s.OnEvict = func(key uint64) {
		evicted.Add(1)
	}

	if _, _, err := s.Put(1, make([]byte, 100)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Small record, plenty of budget left.
	s.mu.RLock()
	if err := s.evictToFitBatchLocked(int64(HeaderLen + 100 + FooterLen)); err != nil {
		s.mu.RUnlock()
		t.Fatalf("evictToFitBatchLocked: %v", err)
	}
	s.mu.RUnlock()
	if evicted.Load() != 0 {
		t.Errorf("OnEvict fired %d times, want 0", evicted.Load())
	}
}

// TestEvictToFitBatch_OverBudget verifies that a record larger than the
// entire budget is rejected without evicting anything.
func TestEvictToFitBatch_OverBudget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	budget := int64(1000)
	s, err := NewStore(Config{Dir: dir, MaxBytes: budget, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var evicted atomic.Int64
	s.OnEvict = func(key uint64) {
		evicted.Add(1)
	}

	if _, _, err := s.Put(1, make([]byte, 100)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Record larger than entire budget.
	hugeRec := budget + 1
	s.mu.RLock()
	if err := s.evictToFitBatchLocked(hugeRec); err == nil {
		s.mu.RUnlock()
		t.Fatal("expected ErrOverBudget, got nil")
	}
	s.mu.RUnlock()
	if evicted.Load() != 0 {
		t.Errorf("OnEvict fired %d times, want 0 (no eviction for oversized record)", evicted.Load())
	}
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, seedBody)
	for i := range seedEntries {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var evicted atomic.Int64
	s.OnEvict = func(key uint64) {
		evicted.Add(1)
	}

	// Put a large record that forces multiple evictions.
	largeBody := make([]byte, 10*(HeaderLen+seedBody+FooterLen))
	if _, _, err := s.Put(999, largeBody); err != nil {
		t.Fatalf("Put with batch eviction: %v", err)
	}

	if evicted.Load() < 10 {
		t.Errorf("OnEvict fired %d times, want >= 10", evicted.Load())
	}

	// Verify the large key is retrievable.
	if _, _, ok := s.Lookup(999); !ok {
		t.Fatal("key 999 not found after batch eviction Put")
	}
}

// TestEvictToFitBatch_EmptyStore verifies behavior on an empty warm tier.
func TestEvictToFitBatch_EmptyStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1000, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Insert one entry that fills the budget.
	if _, _, err := s.Put(1, make([]byte, 100)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Mark it protected so pickEvictVictim skips it.
	s.idxMu.Lock()
	if loc, ok := s.index[1]; ok {
		loc.protected = true
		s.index[1] = loc
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, seedBody)
	for i := range seedEntries {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var evictedCount atomic.Int64
	s.OnEvict = func(key uint64) {
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
	if err == nil {
		t.Fatal("expected error from evictToFitBatchLocked with closed file descriptor, got nil")
	}

	// No evictions should have completed because the first tombstone
	// write fails and writeTombstoneLocked restores the SIEVE entry.
	if evictedCount.Load() != 0 {
		t.Errorf("OnEvict fired %d times, want 0 (first write should fail)", evictedCount.Load())
	}

	// All seed entries should still be in the index.
	s.idxMu.RLock()
	indexLen := len(s.index)
	s.idxMu.RUnlock()
	if indexLen != seedEntries {
		t.Errorf("index has %d entries after failed batch, want %d", indexLen, seedEntries)
	}
}

// TestOverBudget_EntryCap verifies OverBudget returns true when the
// entry count exceeds maxEntries even if byte budget is not exceeded.
func TestOverBudget_EntryCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1 << 20, MaxEntries: 5, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 5 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	if !s.OverBudget() {
		t.Fatal("OverBudget should be true with 5 entries and maxEntries=5")
	}
}

// TestOverBudget_EntryCapNotReached verifies OverBudget returns false
// when entries are under the cap.
func TestOverBudget_EntryCapNotReached(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1 << 20, MaxEntries: 10, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 5 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	if s.OverBudget() {
		t.Fatal("OverBudget should be false with 5 entries and maxEntries=10")
	}
}

// TestPut_EntryCapRejects verifies that Put rejects when the entry cap
// is reached and eviction cannot free space (all entries protected).
func TestPut_EntryCapRejects(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// No byte budget, only entry cap. 3 entries max.
	s, err := NewStore(Config{Dir: dir, MaxEntries: 3, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 3 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
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
	_, _, err = s.Put(99, body)
	if err == nil {
		t.Fatal("expected ErrOverBudget, got nil")
	}
}

// TestPut_EntryCapEvictsAndSucceeds verifies that Put triggers eviction
// when entry cap is reached and succeeds when a non-protected victim exists.
func TestPut_EntryCapEvictsAndSucceeds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// No byte budget, only entry cap. 3 entries max.
	s, err := NewStore(Config{Dir: dir, MaxEntries: 3, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 3 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// 4th Put should evict one entry and succeed.
	if _, _, err := s.Put(99, body); err != nil {
		t.Fatalf("Put with entry cap eviction: %v", err)
	}

	if s.stats.entries.Load() != 3 {
		t.Errorf("entries = %d, want 3 (cap)", s.stats.entries.Load())
	}
}
