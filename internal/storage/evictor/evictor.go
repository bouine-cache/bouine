// Package evictor defines the shared data structures and interface for
// per-tier cache eviction policies.
//
// Entry is the linked-list node embedded by the hot and warm tiers alongside
// their per-key metadata. The visited field is an atomic.Bool so the hot
// store can read it safely under a shard read lock (the hit-path fast path)
// without a data race against the eviction path that writes under the shard
// write lock.
//
// List is the interface that concrete eviction policies (sieve.List,
// freqcost.List) implement. Tiers hold a List value and dispatch through it
// for insertion, access recording, and eviction. The hit-path fast path does
// NOT dispatch through this interface: it reads Entry.Visited() directly
// under the shard read lock. Interface dispatch happens only on the slow
// path (Access) and the eviction path (EvictBounded), neither of which is the
// 0-alloc hit path.
//
// Unstable.
package evictor

import (
	"sync"
	"sync/atomic"
)

// Entry is a node in an eviction list. Exported so the hot and warm tiers
// can embed it alongside their per-key metadata.
//
// The visited field uses atomic.Bool so the hot store can read it safely
// under a read lock (RLock fast path) without a data race against the
// eviction path that writes under a write lock.
//
// Struct size on 64-bit: 16B key + 4B atomic.Bool + 4B pad + 8B prev + 8B
// next = 40B. Policies that need extra per-entry state (e.g. a frequency
// counter) should add it in a way that fills the existing 4B padding slot so
// SIEVE users see no memory regression.
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
// lock — the underlying store is atomic.Bool. Callers that hold only a
// read lock can record access without upgrading to a write lock.
func (e *Entry[K]) MarkVisited() { e.visited.Store(true) }

// ClearVisited stores false to the visited bit. Called by the SIEVE
// sweep when giving an entry a second chance. The caller MUST hold the
// shard write lock.
func (e *Entry[K]) ClearVisited() { e.visited.Store(false) }

// Reset clears all fields of e so it can be reused from a sync.Pool
// without contaminating a fresh insertion with stale state from a
// previous incarnation. Callers MUST hold the shard write lock.
func (e *Entry[K]) Reset() {
	e.Key = *new(K)
	e.visited.Store(false)
	e.prev = nil
	e.next = nil
}

// Prev returns the previous entry in the list (nil at the head).
func (e *Entry[K]) Prev() *Entry[K] { return e.prev }

// Next returns the next entry in the list (nil at the tail).
func (e *Entry[K]) Next() *Entry[K] { return e.next }

// SetPrev sets the previous-entry pointer. Called by List
// implementations during linked-list manipulation under the write lock.
func (e *Entry[K]) SetPrev(p *Entry[K]) { e.prev = p }

// SetNext sets the next-entry pointer. Called by List implementations
// during linked-list manipulation under the write lock.
func (e *Entry[K]) SetNext(n *Entry[K]) { e.next = n }

// List is the eviction-policy interface implemented by concrete policies
// (sieve.List, freqcost.List). Tiers hold a List value and dispatch
// insertion, access recording, and eviction through it.
//
// The hit-path fast path does NOT call through this interface — it reads
// Entry.Visited() directly. Interface dispatch occurs only on the slow
// path and eviction path.
type List[K comparable] interface {
	// Access records an access to key. If the key already exists its
	// visited bit is set (and freq incremented for freq-aware policies).
	// If not, a new entry is inserted at the head and returned. Returns
	// the entry and whether it was newly inserted.
	Access(key K, lookup func(K) *Entry[K]) (*Entry[K], bool)

	// EvictBounded removes and returns the key of the evicted entry,
	// scanning at most maxProbes entries. Returns the zero key and false
	// if the list is empty or no evictable entry is found within the
	// probe budget.
	EvictBounded(maxProbes int) (K, bool)

	// Remove explicitly removes an entry from the list (for Delete /
	// reaper / ban operations, not eviction).
	Remove(e *Entry[K])

	// Len returns the number of entries.
	Len() int

	// Clear removes all entries, returning them to the pool.
	Clear()
}

// NewEntryPool returns a sync.Pool that allocates fresh Entry[K] values.
// Policies use this to recycle Entry structs across insertion/removal
// cycles, avoiding per-Access allocation.
func NewEntryPool[K comparable]() *EntryPool[K] {
	return &EntryPool[K]{
		pool: sync.Pool{
			New: func() any { return new(Entry[K]) },
		},
	}
}

// EntryPool is a typed wrapper around sync.Pool for Entry[K] values.
type EntryPool[K comparable] struct {
	pool sync.Pool
}

// Get returns a reset Entry from the pool.
func (p *EntryPool[K]) Get() *Entry[K] {
	e := p.pool.Get().(*Entry[K])
	e.Reset()
	return e
}

// Put returns e to the pool. Callers MUST NOT reference e after Put.
func (p *EntryPool[K]) Put(e *Entry[K]) {
	p.pool.Put(e)
}
