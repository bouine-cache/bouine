package sieve

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestEntry_MarkVisited(t *testing.T) {
	t.Parallel()
	e := &Entry[int]{Key: 42}
	assert.False(t, e.Visited())
	e.MarkVisited()
	assert.True(t, e.Visited())
}

func TestList_Clear(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	e1, _ := l.Access(1, func(k int) *Entry[int] { return nil })
	e2, _ := l.Access(2, func(k int) *Entry[int] { return nil })
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
		l.Access(i, func(k int) *Entry[int] { return nil })
	}
	l.Clear()
	assert.Equal(t, 0, l.Len())
	// Reuse after clear.
	e, isNew := l.Access(100, func(k int) *Entry[int] { return nil })
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
	e := &Entry[k]{Key: "test"}
	e.MarkVisited()
	assert.True(t, e.Visited())
}

func TestList_AccessExistingKey(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	e1, isNew1 := l.Access(1, func(k int) *Entry[int] { return nil })
	require.True(t, isNew1)
	// Access same key again — should find existing entry and mark visited.
	e2, isNew2 := l.Access(1, func(k int) *Entry[int] {
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
	e, isNew := l.Access(k, func(key api.Key) *Entry[api.Key] { return nil })
	require.True(t, isNew)
	assert.Equal(t, k, e.Key)
}
