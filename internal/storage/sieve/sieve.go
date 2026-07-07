// Package sieve implements the SIEVE cache eviction algorithm.
//
// SIEVE is a simple, near-LRU-K algorithm with O(1) per operation.
// It uses a doubly-linked list of entries with a single "visited" bit.
// A hand pointer sweeps the list; entries with visited=true get a
// second chance (visited cleared), entries with visited=false are
// evicted.
//
// Reference: "SIEVE is Simpler than LRU: an Efficient Turn-Key
// Eviction Algorithm for Web Caches" (Zhang et al., NSDI 2024).
package sieve

import (
	"sync"
	"sync/atomic"
)

// Entry is a node in the SIEVE eviction list. Exported so the hot
// tier can embed it alongside the cached object.
//
// The visited field uses atomic.Bool so the hot store can read it safely
// under a read lock (RLock fast path) without a data race against the
// eviction path that writes under a write lock.
type Entry[K comparable] struct {
	Key     K
	visited atomic.Bool // safe to read under RLock; written under WLock
	prev    *Entry[K]
	next    *Entry[K]
}

// Visited returns the current value of the visited bit.
// Safe to call while holding only the shard read lock.
func (e *Entry[K]) Visited() bool { return e.visited.Load() }

// MarkVisited sets the visited bit to true. Safe to call under a read
// lock — the underlying store is atomic.Bool. Used by warm-tier Get to
// record access without upgrading to a write lock.
func (e *Entry[K]) MarkVisited() { e.visited.Store(true) }

// List is a SIEVE eviction list. It is NOT goroutine-safe; the caller
// (the per-shard hot tier) must hold the shard write lock for all
// mutations. Reads of the visited bit via Entry.Visited() are safe
// under the shard read lock.
type List[K comparable] struct {
	head *Entry[K]
	tail *Entry[K]
	hand *Entry[K]
	len  int
	pool sync.Pool
}

// NewList creates an empty SIEVE list.
func NewList[K comparable]() *List[K] {
	l := &List[K]{}
	l.pool = sync.Pool{
		New: func() any { return new(Entry[K]) },
	}
	return l
}

// Clear removes all entries from the list, returning them to the pool.
// The hand, head, and tail are reset to nil and len to 0. Use this to
// rebuild a list in-place (e.g. during compaction) without allocating a
// fresh List and losing the pooled entries.
func (l *List[K]) Clear() {
	for l.head != nil {
		e := l.head
		l.head = e.next
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
func (l *List[K]) Access(key K, lookup func(K) *Entry[K]) (*Entry[K], bool) {
	if e := lookup(key); e != nil {
		e.visited.Store(true)
		return e, false
	}
	e := l.pool.Get().(*Entry[K])
	// Reset fields individually: atomic.Bool must not be copied via struct
	// assignment (the zero value of atomic.Bool has a noCopy sentinel).
	e.Key = key
	e.visited.Store(false)
	e.prev = nil
	e.next = nil
	l.pushHead(e)
	return e, true
}

// Evict removes and returns the key of the evicted entry. The caller
// must delete the corresponding data from the shard map.
//
// Returns the evicted key and true, or the zero value and false if
// the list is empty.
func (l *List[K]) Evict() (K, bool) {
	if l.len == 0 {
		var zero K
		return zero, false
	}

	// Start from the hand (or tail if hand is nil).
	if l.hand == nil {
		l.hand = l.tail
	}

	for {
		cur := l.hand
		if cur == nil {
			// Wrapped around with no evictable entry (shouldn't happen
			// unless every entry was just visited). Reset hand to tail.
			l.hand = l.tail
			cur = l.hand
			if cur == nil {
				var zero K
				return zero, false
			}
		}

		if !cur.visited.Load() {
			// Evict this entry.
			l.hand = cur.prev
			l.remove(cur)
			key := cur.Key
			l.pool.Put(cur)
			return key, true
		}

		// Give a second chance.
		cur.visited.Store(false)
		l.hand = cur.prev
		if l.hand == nil {
			l.hand = l.tail
		}
	}
}

// Remove explicitly removes an entry from the list (for Delete
// operations, not eviction).
func (l *List[K]) Remove(e *Entry[K]) {
	if e == nil {
		return
	}
	if l.hand == e {
		l.hand = e.prev
	}
	l.remove(e)
	l.pool.Put(e)
}

// Defer moves an entry to the head of the list without changing its
// visited bit and without returning it to the pool. This is used by
// eviction policies that want to skip an entry and give it another
// chance without losing its access history.
func (l *List[K]) Defer(e *Entry[K]) {
	if e == nil {
		return
	}
	if l.hand == e {
		l.hand = e.prev
	}
	l.remove(e)
	l.pushHead(e)
}

func (l *List[K]) pushHead(e *Entry[K]) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
	l.len++
}

func (l *List[K]) remove(e *Entry[K]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
	l.len--
}
