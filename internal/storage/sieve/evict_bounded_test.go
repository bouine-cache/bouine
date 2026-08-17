package sieve

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
)

func TestEvictBounded_AllVisited_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	// Insert 10 entries, all visited.
	for i := range 10 {
		e, _ := l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 10 {
		l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return m[uint64(i)] })
	}

	// With maxProbes < len, all visited: should return false.
	// The hand clears visited bits as it advances, but 5 probes
	// cannot clear all 10 and find an unvisited one.
	_, ok := l.EvictBounded(5)
	require.False(t, ok)
	require.Equal(t, 10, l.Len())
}

func TestEvictBounded_AllVisited_FindsAfterClearing(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	// Insert 4 entries, all visited.
	for i := range 4 {
		e, _ := l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 4 {
		l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return m[uint64(i)] })
	}

	// With maxProbes >= len: one sweep clears all visited bits (4 probes),
	// then the next probe finds an unvisited entry. So maxProbes=5
	// (4 clears + 1 find) should succeed.
	key, ok := l.EvictBounded(5)
	require.True(t, ok)
	require.Equal(t, 3, l.Len())
	// The evicted key should be the one the hand lands on after
	// clearing all 4 visited bits and wrapping around.
	delete(m, key)
}

func TestEvictBounded_PreservesUnvisitedEntries(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	// Insert 100 entries, all visited.
	for i := range 100 {
		e, _ := l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 100 {
		l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return m[uint64(i)] })
	}

	// EvictBounded(10) should return false (all visited, 10 probes
	// only clears 10 bits, no unvisited entry found).
	_, ok := l.EvictBounded(10)
	require.False(t, ok)

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
	require.Equal(t, 90, visitedCount)
}

func TestEvictBounded_Zero_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	// Insert one entry.
	_, _ = l.Access(1, func(uint64) *evictor.Entry[uint64] { return nil })

	_, ok := l.EvictBounded(0)
	require.False(t, ok)
	require.Equal(t, 1, l.Len())
}

func TestEvictBounded_Negative_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	_, _ = l.Access(1, func(uint64) *evictor.Entry[uint64] { return nil })

	_, ok := l.EvictBounded(-1)
	require.False(t, ok)
}

func TestEvictBounded_EmptyList_ReturnsFalse(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	_, ok := l.EvictBounded(128)
	require.False(t, ok)
}

func TestEvictBounded_UnvisitedEntryEvictedImmediately(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()

	// Insert one entry (not visited).
	_, _ = l.Access(42, func(uint64) *evictor.Entry[uint64] { return nil })

	key, ok := l.EvictBounded(128)
	require.True(t, ok)
	require.Equal(t, uint64(42), key)
	require.Equal(t, 0, l.Len())
}

func TestEvictBounded_ProgressAcrossCalls(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	// Insert 200 entries, all visited.
	for i := range 200 {
		e, _ := l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return nil })
		m[uint64(i)] = e
	}
	for i := range 200 {
		l.Access(uint64(i), func(uint64) *evictor.Entry[uint64] { return m[uint64(i)] })
	}

	// First call: 128 probes clear 128 visited bits, no eviction.
	_, ok := l.EvictBounded(128)
	require.False(t, ok)

	// Second call: the hand resumes from the advanced position. The
	// next 72 entries still have visited=true (cleared in this call),
	// then the hand reaches entries cleared in the first call
	// (visited=false). So within 73 probes it should find an
	// evictable entry.
	key, ok := l.EvictBounded(128)
	require.True(t, ok)
	delete(m, key)
	require.Equal(t, 199, l.Len())
}
