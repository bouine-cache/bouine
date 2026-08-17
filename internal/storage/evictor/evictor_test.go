package evictor

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntry_Visited_Lifecycle(t *testing.T) {
	t.Parallel()
	e := &Entry[uint64]{Key: 42}
	assert.False(t, e.Visited(), "fresh entry should have visited=false")

	e.MarkVisited()
	assert.True(t, e.Visited(), "after MarkVisited, visited should be true")

	e.ClearVisited()
	assert.False(t, e.Visited(), "after ClearVisited, visited should be false")
}

func TestEntry_MarkVisited_Idempotent(t *testing.T) {
	t.Parallel()
	e := &Entry[int]{}
	e.MarkVisited()
	e.MarkVisited()
	assert.True(t, e.Visited())
}

func TestEntry_PrevNext_Accessors(t *testing.T) {
	t.Parallel()
	a := &Entry[int]{Key: 1}
	b := &Entry[int]{Key: 2}
	c := &Entry[int]{Key: 3}

	// Link a <-> b <-> c.
	b.SetPrev(a)
	b.SetNext(c)
	a.SetNext(b)
	c.SetPrev(b)

	assert.Equal(t, a, b.Prev(), "b.Prev should be a")
	assert.Equal(t, c, b.Next(), "b.Next should be c")
	assert.Nil(t, a.Prev(), "a is head, Prev should be nil")
	assert.Nil(t, c.Next(), "c is tail, Next should be nil")

	// Unlink b.
	b.SetPrev(nil)
	b.SetNext(nil)
	assert.Nil(t, b.Prev())
	assert.Nil(t, b.Next())
}

func TestEntry_Reset_ClearsAllFields(t *testing.T) {
	t.Parallel()
	other := &Entry[string]{Key: "other"}
	e := &Entry[string]{Key: "stale"}
	e.MarkVisited()
	e.SetPrev(other)
	e.SetNext(other)

	e.Reset()

	assert.Equal(t, "", e.Key, "Reset should zero the key")
	assert.False(t, e.Visited(), "Reset should clear visited")
	assert.Nil(t, e.Prev(), "Reset should nil prev")
	assert.Nil(t, e.Next(), "Reset should nil next")
}

func TestEntry_Reset_ZeroesGenericKey(t *testing.T) {
	t.Parallel()
	type k struct{ a, b int }
	e := &Entry[k]{Key: k{a: 1, b: 2}}
	e.Reset()
	assert.Equal(t, k{}, e.Key, "Reset should zero a struct key")
}

func TestEntryPool_GetReturnsResetEntry(t *testing.T) {
	t.Parallel()
	p := NewEntryPool[uint64]()

	// Pollute an entry and put it back.
	dirty := p.Get()
	dirty.Key = 999
	dirty.MarkVisited()
	other := &Entry[uint64]{Key: 42}
	dirty.SetPrev(other)
	dirty.SetNext(other)
	p.Put(dirty)

	// Get should return a reset entry. sync.Pool may return the dirty
	// one or a fresh one; either way it must be reset.
	got := p.Get()
	assert.Equal(t, uint64(0), got.Key, "Get should return a zeroed key")
	assert.False(t, got.Visited(), "Get should return a non-visited entry")
	assert.Nil(t, got.Prev(), "Get should return an entry with nil prev")
	assert.Nil(t, got.Next(), "Get should return an entry with nil next")
}

func TestEntryPool_GetAllocatesFreshWhenEmpty(t *testing.T) {
	t.Parallel()
	p := NewEntryPool[int]()
	e := p.Get()
	require.NotNil(t, e)
	assert.Equal(t, 0, e.Key)
	assert.False(t, e.Visited())
}

func TestEntryPool_ReuseCycle(t *testing.T) {
	t.Parallel()
	p := NewEntryPool[string]()
	// Simulate insert/evict cycles: Get, populate, Put, Get again.
	for i := range 10 {
		e := p.Get()
		e.Key = "key"
		p.Put(e)
		_ = i
	}
	// Final get must be clean.
	e := p.Get()
	assert.Equal(t, "", e.Key)
	assert.False(t, e.Visited())
}

func TestEntryPool_Concurrent_Use(t *testing.T) {
	// sync.Pool is concurrency-safe; this is a smoke test under -race.
	t.Parallel()
	p := NewEntryPool[uint64]()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				e := p.Get()
				e.Key = 1
				e.MarkVisited()
				p.Put(e)
			}
		}()
	}
	wg.Wait()
}

func TestEntry_IOBits_RoundTrip(t *testing.T) {
	t.Parallel()
	e := &Entry[uint64]{Key: 1}
	assert.Equal(t, uint32(0), e.IOBits(), "fresh entry ioBits=0")

	e.SetIOBits(7)
	assert.Equal(t, uint32(7), e.IOBits())

	e.SetIOBits(0)
	assert.Equal(t, uint32(0), e.IOBits())
}

func TestEntry_Size_40Bytes_64Bit(t *testing.T) {
	// Pins the struct layout contract referenced by
	// hot.sieveEntrySize and warm.EstimatedWarmLocHeapBytes.
	// 16B key + 4B atomic.Bool + 4B pad + 8B prev + 8B next = 40B.
	// Uses a 16-byte key type to match api.Key's inline size without
	// importing pkg/api (keeps evictor a leaf package).
	if unsafe.Sizeof(uint64(0)) != 8 {
		t.Skip("test is only meaningful on 64-bit")
	}
	type key16 [16]byte
	got := unsafe.Sizeof(Entry[key16]{})
	assert.Equal(t, uintptr(40), got, "Entry with 16B key must stay 40B; see ADR-0031")
}
