package sieve

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSieve_InsertAndAccess(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	e1, ins := l.Access(1, func(k uint64) *evictor.Entry[uint64] { return m[k] })
	require.True(t, ins)
	m[1] = e1

	e2, ins := l.Access(2, func(k uint64) *evictor.Entry[uint64] { return m[k] })
	require.True(t, ins)
	m[2] = e2

	require.Equal(t, 2, l.Len())

	// Re-access key 1 — should not insert, should mark visited.
	_, ins = l.Access(1, func(k uint64) *evictor.Entry[uint64] { return m[k] })
	require.False(t, ins)
	require.Equal(t, 2, l.Len())
}

func TestSieve_EvictsLeastRecent(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	// Insert 1, 2, 3.
	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *evictor.Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Access 1 and 3 so they get visited=true.
	l.Access(1, func(k uint64) *evictor.Entry[uint64] { return m[k] })
	l.Access(3, func(k uint64) *evictor.Entry[uint64] { return m[k] })

	// Evict — should evict 2 (not visited, at tail after 1 was visited).
	key, ok := l.EvictBounded(l.Len() * 2)
	require.True(t, ok)
	delete(m, key)

	require.Equal(t, uint64(2), key)
	require.Equal(t, 2, l.Len())
}

func TestSieve_EvictAll(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	for _, k := range []uint64{10, 20, 30} {
		e, _ := l.Access(k, func(k uint64) *evictor.Entry[uint64] { return m[k] })
		m[k] = e
	}

	evicted := map[uint64]bool{}
	for range 3 {
		k, ok := l.EvictBounded(l.Len() * 2)
		require.True(t, ok)
		evicted[k] = true
		delete(m, k)
	}

	require.Len(t, evicted, 3)
	require.Equal(t, 0, l.Len())

	_, ok := l.EvictBounded(l.Len() * 2)
	require.False(t, ok)
}

func TestSieve_Remove(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	for _, k := range []uint64{1, 2, 3} {
		e, _ := l.Access(k, func(k uint64) *evictor.Entry[uint64] { return m[k] })
		m[k] = e
	}

	l.Remove(m[2])
	delete(m, 2)

	require.Equal(t, 2, l.Len())

	// Evict remaining — should get 1 and 3 in some order.
	var keys []uint64
	for range 2 {
		k, ok := l.EvictBounded(l.Len() * 2)
		require.True(t, ok)
		keys = append(keys, k)
	}
	require.Len(t, keys, 2)
}

func TestSieve_SecondChance(t *testing.T) {
	t.Parallel()
	l := NewList[uint64]()
	m := map[uint64]*evictor.Entry[uint64]{}

	// Insert A, B.
	for _, k := range []uint64{1, 2} {
		e, _ := l.Access(k, func(k uint64) *evictor.Entry[uint64] { return m[k] })
		m[k] = e
	}

	// Visit both.
	l.Access(1, func(k uint64) *evictor.Entry[uint64] { return m[k] })
	l.Access(2, func(k uint64) *evictor.Entry[uint64] { return m[k] })

	// First evict: both visited, so both get second chance; then one
	// is evicted (the one the hand lands on after clearing visited).
	k1, ok := l.EvictBounded(l.Len() * 2)
	require.True(t, ok)
	delete(m, k1)
	require.Equal(t, 1, l.Len())
}

func TestEntry_MarkVisited(t *testing.T) {
	t.Parallel()
	e := &evictor.Entry[int]{Key: 42}
	assert.False(t, e.Visited())
	e.MarkVisited()
	assert.True(t, e.Visited())
}

func TestList_Clear(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	e1, _ := l.Access(1, func(k int) *evictor.Entry[int] { return nil })
	e2, _ := l.Access(2, func(k int) *evictor.Entry[int] { return nil })
	require.Equal(t, 2, l.Len())
	assert.NotNil(t, e1)
	assert.NotNil(t, e2)
	l.Clear()
	assert.Equal(t, 0, l.Len())
	assert.Nil(t, l.head)
	assert.Nil(t, l.tail)
	assert.Nil(t, l.hand)
}

func TestList_ClearAndReuse(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	for i := range 10 {
		l.Access(i, func(k int) *evictor.Entry[int] { return nil })
	}
	l.Clear()
	assert.Equal(t, 0, l.Len())
	// Reuse after clear.
	e, isNew := l.Access(100, func(k int) *evictor.Entry[int] { return nil })
	assert.True(t, isNew)
	assert.NotNil(t, e)
	assert.Equal(t, 1, l.Len())
}

func TestList_ClearEmpty(t *testing.T) {
	t.Parallel()
	l := NewList[string]()
	l.Clear() // must not panic on empty list
	assert.Equal(t, 0, l.Len())
}

func TestEntry_VisitedWithGenericKey(t *testing.T) {
	t.Parallel()
	type k string
	e := &evictor.Entry[k]{Key: "test"}
	e.MarkVisited()
	assert.True(t, e.Visited())
}

func TestList_AccessExistingKey(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	e1, isNew1 := l.Access(1, func(k int) *evictor.Entry[int] { return nil })
	require.True(t, isNew1)
	// Access same key again — should find existing entry and mark visited.
	e2, isNew2 := l.Access(1, func(k int) *evictor.Entry[int] {
		if k == 1 {
			return e1
		}
		return nil
	})
	assert.False(t, isNew2)
	assert.Same(t, e1, e2)
	assert.True(t, e2.Visited())
}

func TestList_AccessWithTestKey(t *testing.T) {
	t.Parallel()
	l := NewList[api.Key]()
	k := testkey.Key(42)
	e, isNew := l.Access(k, func(key api.Key) *evictor.Entry[api.Key] { return nil })
	require.True(t, isNew)
	assert.Equal(t, k, e.Key)
}
