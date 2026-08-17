package cachaner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
)

func TestPackUnpack(t *testing.T) {
	t.Parallel()
	for freq := range uint8(8) {
		// freq=0 is the insert default.
		if freq == 0 {
			continue
		}
		l := NewList[int]()
		e, _ := l.Access(1, func(int) *evictor.Entry[int] { return nil })
		for range freq {
			l.Access(1, func(int) *evictor.Entry[int] { return e })
		}
		require.Equal(t, freq, unpackFreq(e.IOBits()), "freq=%d", freq)
	}
}

func TestList_AccessIncrementsFreq(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }

	e, isNew := l.Access(1, nilLookup)
	require.True(t, isNew)
	require.Equal(t, uint8(0), unpackFreq(e.IOBits()), "new entry freq=0")
	require.False(t, e.Visited(), "new entry visited=false after insert")

	// Simulate slow-path accesses for existing key.
	existingLookup := func(k int) *evictor.Entry[int] { return e }

	l.Access(1, existingLookup)
	require.Equal(t, uint8(1), unpackFreq(e.IOBits()), "freq after 1st access")
	require.True(t, e.Visited())

	l.Access(1, existingLookup)
	require.Equal(t, uint8(2), unpackFreq(e.IOBits()), "freq after 2nd access")

	// Saturate at maxFreq=7.
	for range 10 {
		l.Access(1, existingLookup)
	}
	require.Equal(t, uint8(7), unpackFreq(e.IOBits()), "freq saturates at 7")
}

func TestList_EvictFirstColdEntry(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }

	// Insert two entries, neither accessed (freq=0, visited=false).
	l.Access(1, nilLookup)
	l.Access(2, nilLookup)
	require.Equal(t, 2, l.Len())

	// Eviction should evict the first cold entry found (key=1 at tail).
	key, ok := l.EvictBounded(10)
	require.True(t, ok)
	require.Equal(t, 1, key, "first cold entry (key=1) should be evicted")
	require.Equal(t, 1, l.Len())

	// Second eviction gets the remaining cold entry.
	key, ok = l.EvictBounded(10)
	require.True(t, ok)
	require.Equal(t, 2, key)
	require.Equal(t, 0, l.Len())
}

func TestList_FreqGivesSecondChance(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }

	// Insert entry and give it freq=3 via slow-path accesses.
	e, _ := l.Access(1, nilLookup)
	existingLookup := func(int) *evictor.Entry[int] { return e }
	l.Access(1, existingLookup)
	l.Access(1, existingLookup)
	l.Access(1, existingLookup)
	require.Equal(t, uint8(3), unpackFreq(e.IOBits()))

	// Clear visited so the sweep tests the freq path directly.
	e.ClearVisited()

	// With only 1 probe, the sweep decrements freq (3→2) and skips.
	// No victim found → returns false.
	_, ok := l.EvictBounded(1)
	require.False(t, ok, "freq>0 entry should survive a 1-probe sweep")
	require.Equal(t, uint8(2), unpackFreq(e.IOBits()), "freq should decrement")
	require.Equal(t, 1, l.Len(), "entry should still be in list")

	// With enough probes (4), freq decrements to 0 and the entry is
	// evicted. Pass 1: freq 2→1, skip. Pass 2: freq 1→0, skip. Pass 3:
	// freq=0, evict.
	key, ok := l.EvictBounded(10)
	require.True(t, ok, "entry should be evicted once freq reaches 0")
	require.Equal(t, 1, key)
	require.Equal(t, 0, l.Len())
}

func TestList_EvictBudgetAccountsForMaxFreq(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }

	// Insert one entry and saturate its freq to maxFreq=7.
	e, _ := l.Access(1, nilLookup)
	existingLookup := func(int) *evictor.Entry[int] { return e }
	for range maxFreq {
		l.Access(1, existingLookup)
	}
	require.Equal(t, maxFreq, unpackFreq(e.IOBits()))

	// Clear visited so the sweep must exhaust freq before evicting.
	e.ClearVisited()

	// Evict() uses len*(maxFreq+2) = 1*9 = 9 probes: 7 freq decrements
	// + 1 evict = 8 probes needed, within budget.
	key, ok := l.Evict()
	require.True(t, ok, "Evict() budget must be large enough to exhaust maxFreq")
	require.Equal(t, 1, key)
	require.Equal(t, 0, l.Len())
}

func TestList_EvictVisitedClearsThenEvicts(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }

	// Insert entry, mark visited (but freq=0).
	e, _ := l.Access(1, nilLookup)
	e.MarkVisited()

	// First sweep: visited=true → clear visited, skip (1 probe).
	// Second sweep: visited=false, freq=0 → evict (1 probe).
	key, ok := l.EvictBounded(10)
	require.True(t, ok)
	require.Equal(t, 1, key)
	require.Equal(t, 0, l.Len())
}

func TestList_Clear(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	l.Access(1, nilLookup)
	l.Access(2, nilLookup)
	require.Equal(t, 2, l.Len())

	l.Clear()
	require.Equal(t, 0, l.Len())
}

func TestList_Remove(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	e, _ := l.Access(1, nilLookup)
	require.Equal(t, 1, l.Len())

	l.Remove(e)
	require.Equal(t, 0, l.Len())
}

func TestList_Defer(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	e, _ := l.Access(1, nilLookup)

	l.Defer(e)
	require.Equal(t, 1, l.Len(), "Defer should keep entry in list")
	require.Equal(t, l.head, e, "Defer should move entry to head")
}

func TestList_DeferNil(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	l.Defer(nil)
	require.Equal(t, 0, l.Len())
}

func TestList_DeferHandPointingAtEntry(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	e1, _ := l.Access(1, nilLookup)
	e2, _ := l.Access(2, nilLookup)

	// List order after pushHead: head=e2 → e1=tail.
	// Set hand to e1 (the tail), then Defer e1 — hand should move to e2.
	l.hand = e1
	l.Defer(e1)
	require.Equal(t, e2, l.hand, "hand should move to prev when deferring the hand entry")
}

func TestList_RemoveNil(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	l.Remove(nil)
	require.Equal(t, 0, l.Len())
}

func TestList_RemoveHandPointingAtEntry(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	e1, _ := l.Access(1, nilLookup)
	e2, _ := l.Access(2, nilLookup)

	// List order: head=e2 → e1=tail.
	// Set hand to e1 (the tail), then Remove e1 — hand should move to e2.
	l.hand = e1
	l.Remove(e1)
	require.Equal(t, e2, l.hand, "hand should move to prev when removing the hand entry")
	require.Equal(t, 1, l.Len())
}

func TestList_RemoveTail(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	l.Access(1, nilLookup)
	e2, _ := l.Access(2, nilLookup)

	// List order: head=e2 → e1=tail. Remove the head (e2), tail stays e1.
	l.Remove(e2)
	require.Equal(t, 1, l.Len())
	require.Equal(t, l.head, l.tail, "head and tail should match after removing the only other entry")
}

func TestList_EvictBoundedEmptyList(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	_, ok := l.EvictBounded(10)
	require.False(t, ok, "empty list should not evict")
}

func TestList_EvictBoundedZeroMaxProbes(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	l.Access(1, nilLookup)
	_, ok := l.EvictBounded(0)
	require.False(t, ok, "maxProbes<=0 should not evict")
}

func TestList_EvictBoundedHandWrapsToTail(t *testing.T) {
	t.Parallel()
	l := NewList[int]()
	nilLookup := func(k int) *evictor.Entry[int] { return nil }
	e1, _ := l.Access(1, nilLookup)

	// List: head=e1=tail. Mark visited so the sweep clears it and sets
	// hand = e1.Prev() = nil. On the next probe, cur=nil → wrap to tail
	// = e1. Now freq=0, visited=false → evict.
	e1.MarkVisited()
	key, ok := l.EvictBounded(3)
	require.True(t, ok, "should evict after hand wraps to tail")
	require.Equal(t, 1, key)
	require.Equal(t, 0, l.Len())
}

func TestList_EvictBoundedHandNilAndTailNil(t *testing.T) {
	t.Parallel()
	l := NewList[int]()

	// Simulate a corrupted state: len > 0 (bypasses the initial guard)
	// but hand and tail are both nil. The sweep should detect cur==nil
	// after wrap and return false.
	l.len = 1
	l.hand = nil
	_, ok := l.EvictBounded(10)
	require.False(t, ok, "should not evict when hand wraps and tail is nil")
}
