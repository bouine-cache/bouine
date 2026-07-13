package sieve

import "testing"

func TestEvictBounded_AllVisited_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert 10 entries, all visited.
	for i := range 10 {
		e, _ := l.Access(uint64(i), func(uint64) *Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 10 {
		l.Access(uint64(i), func(uint64) *Entry[uint64] { return m[uint64(i)] })
	}

	// With maxProbes < len, all visited: should return false.
	// The hand clears visited bits as it advances, but 5 probes
	// cannot clear all 10 and find an unvisited one.
	_, ok := l.EvictBounded(5)
	if ok {
		t.Fatal("EvictBounded(5) on 10 all-visited entries should return false")
	}
	if l.Len() != 10 {
		t.Fatalf("len = %d, want 10 (no eviction should occur)", l.Len())
	}
}

func TestEvictBounded_AllVisited_FindsAfterClearing(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert 4 entries, all visited.
	for i := range 4 {
		e, _ := l.Access(uint64(i), func(uint64) *Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 4 {
		l.Access(uint64(i), func(uint64) *Entry[uint64] { return m[uint64(i)] })
	}

	// With maxProbes >= len: one sweep clears all visited bits (4 probes),
	// then the next probe finds an unvisited entry. So maxProbes=5
	// (4 clears + 1 find) should succeed.
	key, ok := l.EvictBounded(5)
	if !ok {
		t.Fatal("EvictBounded(5) on 4 all-visited entries should succeed")
	}
	if l.Len() != 3 {
		t.Fatalf("len = %d, want 3 after one eviction", l.Len())
	}
	// The evicted key should be the one the hand lands on after
	// clearing all 4 visited bits and wrapping around.
	delete(m, key)
}

func TestEvictBounded_PreservesUnvisitedEntries(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert 100 entries, all visited.
	for i := range 100 {
		e, _ := l.Access(uint64(i), func(uint64) *Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 100 {
		l.Access(uint64(i), func(uint64) *Entry[uint64] { return m[uint64(i)] })
	}

	// EvictBounded(10) should return false (all visited, 10 probes
	// only clears 10 bits, no unvisited entry found).
	_, ok := l.EvictBounded(10)
	if ok {
		t.Fatal("EvictBounded(10) on 100 all-visited entries should return false")
	}

	// Entries beyond the 10 probed should still have visited=true.
	// The hand started at tail and advanced 10 positions. Entries
	// near the head (not yet probed) should still be visited.
	// Check a few entries near the head.
	visitedCount := 0
	for i := range 100 {
		if e, exists := m[uint64(i)]; exists && e.Visited() {
			visitedCount++
		}
	}
	// After 10 probes clearing 10 bits, 90 should still be visited.
	if visitedCount != 90 {
		t.Fatalf("expected 90 visited entries, got %d", visitedCount)
	}
}

func TestEvictBounded_Zero_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	// Insert one entry.
	_, _ = l.Access(1, func(uint64) *Entry[uint64] { return nil })

	_, ok := l.EvictBounded(0)
	if ok {
		t.Fatal("EvictBounded(0) should return false immediately")
	}
	if l.Len() != 1 {
		t.Fatalf("len = %d, want 1 (no eviction)", l.Len())
	}
}

func TestEvictBounded_Negative_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	_, _ = l.Access(1, func(uint64) *Entry[uint64] { return nil })

	_, ok := l.EvictBounded(-1)
	if ok {
		t.Fatal("EvictBounded(-1) should return false")
	}
}

func TestEvictBounded_EmptyList_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	_, ok := l.EvictBounded(128)
	if ok {
		t.Fatal("EvictBounded on empty list should return false")
	}
}

func TestEvictBounded_UnvisitedEntryEvictedImmediately(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	// Insert one entry (not visited).
	_, _ = l.Access(42, func(uint64) *Entry[uint64] { return nil })

	key, ok := l.EvictBounded(128)
	if !ok {
		t.Fatal("EvictBounded should evict unvisited entry")
	}
	if key != 42 {
		t.Fatalf("evicted key = %d, want 42", key)
	}
	if l.Len() != 0 {
		t.Fatalf("len = %d, want 0", l.Len())
	}
}

func TestEvictBounded_ProgressAcrossCalls(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert 200 entries, all visited.
	for i := range 200 {
		e, _ := l.Access(uint64(i), func(uint64) *Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 200 {
		l.Access(uint64(i), func(uint64) *Entry[uint64] { return m[uint64(i)] })
	}

	// First call: 128 probes clear 128 visited bits, no eviction.
	_, ok := l.EvictBounded(128)
	if ok {
		t.Fatal("first EvictBounded(128) should return false (all visited, only 128 cleared)")
	}

	// Second call: the hand resumes from the advanced position. The
	// next 72 entries still have visited=true (cleared in this call),
	// then the hand reaches entries cleared in the first call
	// (visited=false). So within 73 probes it should find an
	// evictable entry.
	key, ok := l.EvictBounded(128)
	if !ok {
		t.Fatal("second EvictBounded(128) should find an entry cleared by the first call")
	}
	delete(m, key)
	if l.Len() != 199 {
		t.Fatalf("len = %d, want 199", l.Len())
	}
}

func TestEvict_EquivalentToEvictBoundedLen2(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(uint64) *Entry[uint64] { return nil })
		m[k] = e
	}

	// Evict() should behave the same as EvictBounded(l.len * 2) = EvictBounded(6).
	key, ok := l.Evict()
	if !ok {
		t.Fatal("Evict should succeed")
	}
	if l.Len() != 2 {
		t.Fatalf("len = %d, want 2", l.Len())
	}
	delete(m, key)

	// EvictBounded(l.len * 2) on the remaining should also work.
	key2, ok2 := l.EvictBounded(l.Len() * 2)
	if !ok2 {
		t.Fatal("EvictBounded(len*2) should succeed")
	}
	if l.Len() != 1 {
		t.Fatalf("len = %d, want 1", l.Len())
	}
	delete(m, key2)
}
