package sieve

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSieve_InsertAndAccess(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	e1, ins := l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	require.True(t, ins)
	m[1] = e1

	e2, ins := l.Access(2, func(k uint64) *Entry[uint64] { return m[k] })
	require.True(t, ins)
	m[2] = e2

	require.Equal(t, 2, l.Len())

	// Re-access key 1 — should not insert, should mark visited.
	_, ins = l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	require.False(t, ins)
	require.Equal(t, 2, l.Len())
}

func TestSieve_EvictsLeastRecent(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert 1, 2, 3.
	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Access 1 and 3 so they get visited=true.
	l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	l.Access(3, func(k uint64) *Entry[uint64] { return m[k] })

	// Evict — should evict 2 (not visited, at tail after 1 was visited).
	key, ok := l.Evict()
	require.True(t, ok)
	delete(m, key)

	require.Equal(t, uint64(2), key)
	require.Equal(t, 2, l.Len())
}

func TestSieve_EvictAll(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	for _, k := range []uint64{10, 20, 30} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	evicted := map[uint64]bool{}
	for range 3 {
		k, ok := l.Evict()
		require.True(t, ok)
		evicted[k] = true
		delete(m, k)
	}

	require.Len(t, evicted, 3)
	require.Equal(t, 0, l.Len())

	_, ok := l.Evict()
	require.False(t, ok)
}

func TestSieve_Remove(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	l.Remove(m[2])
	delete(m, 2)

	require.Equal(t, 2, l.Len())

	// Evict remaining — should get 1 and 3 in some order.
	var keys []uint64
	for range 2 {
		k, ok := l.Evict()
		require.True(t, ok)
		keys = append(keys, k)
	}
	require.Len(t, keys, 2)
}

func TestSieve_SecondChance(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	// Insert A, B.
	for _, k := range []uint64{1, 2} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Visit both.
	l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	l.Access(2, func(k uint64) *Entry[uint64] { return m[k] })

	// First evict: both visited, so both get second chance; then one
	// is evicted (the one the hand lands on after clearing visited).
	k1, ok := l.Evict()
	require.True(t, ok)
	delete(m, k1)
	require.Equal(t, 1, l.Len())
}

func TestSieve_Defer_PreservesVisitedBit(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*Entry[uint64]{}

	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Visit key 1 (visited=true).
	l.Access(1, func(k uint64) *Entry[uint64] { return m[k] })
	require.True(t, m[1].Visited())

	// Defer key 1 — move to head, preserve visited bit.
	l.Defer(m[1])

	require.True(t, m[1].Visited())
	require.Equal(t, 3, l.Len())

	// Key 1 should now get a second chance during eviction.
	// Evict twice — key 1 (visited) should survive the first evict.
	evicted := []uint64{}
	for range 2 {
		k, ok := l.Evict()
		require.True(t, ok)
		evicted = append(evicted, k)
		delete(m, k)
	}
	for _, k := range evicted {
		require.NotEqual(t, 1, k)
	}
}
