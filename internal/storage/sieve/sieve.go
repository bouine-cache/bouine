// Package sieve implements the SIEVE cache eviction policy.
//
// SIEVE is a simple, near-LRU-K algorithm with O(1) amortized per
// operation and O(maxSweepProbes) worst case. It uses a doubly-linked
// list of entries with a single "visited" bit. A hand pointer sweeps
// the list; entries with visited=true get a second chance (visited
// cleared), entries with visited=false are evicted.
//
// This package implements ONLY the SIEVE policy. Both hot and warm tiers
// use it via the evictor.List interface. See ADR-0031 for the pluggable
// eviction framework.
//
// Reference: "SIEVE is Simpler than LRU: an Efficient Turn-Key
// Eviction Algorithm for Web Caches" (Zhang et al., NSDI 2024).
package sieve

import (
	"github.com/bouine-cache/bouine/internal/storage/evictor"
)

// List is a SIEVE eviction list. It is NOT goroutine-safe; the caller
// (the per-shard hot tier or the warm tier) must hold the appropriate
// lock for all mutations. Reads of the visited bit via Entry.Visited()
// are safe under the shard read lock.
type List[K comparable] struct {
	head *evictor.Entry[K]
	tail *evictor.Entry[K]
	hand *evictor.Entry[K]
	pool *evictor.EntryPool[K]
	len  int
}

// NewList creates an empty SIEVE list.
func NewList[K comparable]() *List[K] {
	return &List[K]{pool: evictor.NewEntryPool[K]()}
}

// Clear removes all entries from the list, returning them to the pool.
// The hand, head, and tail are reset to nil and len to 0. Use this to
// rebuild a list in-place (e.g. during compaction) without allocating a
// fresh List and losing the pooled entries.
func (l *List[K]) Clear() {
	for l.head != nil {
		e := l.head
		l.head = e.Next()
		l.pool.Put(e)
	}
	l.head = nil
	l.tail = nil
	l.hand = nil
	l.len = 0
}

// Len returns the number of entries.
func (l *List[K]) Len() int { return l.len }

// Access records an access to key. If the key already exists in the
// list its visited bit is set. If not, a new entry is inserted at the
// head and returned.
//
// Returns the entry and whether it was newly inserted.
func (l *List[K]) Access(key K, lookup func(K) *evictor.Entry[K]) (*evictor.Entry[K], bool) {
	if e := lookup(key); e != nil {
		e.MarkVisited()
		return e, false
	}
	e := l.pool.Get()
	e.Key = key
	l.pushHead(e)
	return e, true
}

// EvictBounded removes and returns the key of the evicted entry,
// scanning at most maxProbes entries. The caller must hold the write
// lock as for Evict.
//
// When maxProbes is smaller than the list length, the sweep may return
// (zero, false) even if evictable entries exist elsewhere in the list.
// The caller handles this by treating it as a temporary budget
// overshoot (hot tier) or an ErrOverBudget rejection (warm tier). The
// hand advances and clears visited bits during the capped sweep, so
// the next call resumes from the advanced position and typically finds
// an evictable entry within a few probes.
//
// Returns the evicted key and true, or the zero value and false if
// the list is empty or no evictable entry is found within maxProbes.
func (l *List[K]) EvictBounded(maxProbes int) (K, bool) {
	if l.len == 0 || maxProbes <= 0 {
		var zero K
		return zero, false
	}

	if l.hand == nil {
		l.hand = l.tail
	}

	for range maxProbes {
		cur := l.hand
		if cur == nil {
			l.hand = l.tail
			cur = l.hand
			if cur == nil {
				var zero K
				return zero, false
			}
		}

		if !cur.Visited() {
			l.hand = cur.Prev()
			l.remove(cur)
			key := cur.Key
			l.pool.Put(cur)
			return key, true
		}

		cur.ClearVisited()
		l.hand = cur.Prev()
		if l.hand == nil {
			l.hand = l.tail
		}
	}

	var zero K
	return zero, false
}

// Remove explicitly removes an entry from the list (for Delete
// operations, not eviction).
func (l *List[K]) Remove(e *evictor.Entry[K]) {
	if e == nil {
		return
	}
	if l.hand == e {
		l.hand = e.Prev()
	}
	l.remove(e)
	l.pool.Put(e)
}

func (l *List[K]) pushHead(e *evictor.Entry[K]) {
	e.SetPrev(nil)
	e.SetNext(l.head)
	if l.head != nil {
		l.head.SetPrev(e)
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
	l.len++
}

func (l *List[K]) remove(e *evictor.Entry[K]) {
	if e.Prev() != nil {
		e.Prev().SetNext(e.Next())
	} else {
		l.head = e.Next()
	}
	if e.Next() != nil {
		e.Next().SetPrev(e.Prev())
	} else {
		l.tail = e.Prev()
	}
	e.SetPrev(nil)
	e.SetNext(nil)
	l.len--
}
