// Package cachaner implements the cachaner eviction policy — SIEVE
// with a 3-bit saturating frequency counter.
//
// This extends SIEVE's 1-bit visited field with a 3-bit frequency
// counter packed into the evictor.Entry's ioBits field (unused under
// plain SIEVE). The counter gives hot objects up to 7 second chances
// across sweep passes, vs SIEVE's 1.
//
// freq (bits 0-2): 0-7, incremented on each slow-path Access (first
// access after a sweep pass clears the visited bit). Saturates at
// maxFreq (7).
//
// Eviction sweep (EvictBounded):
//   - visited=true: clear visited, skip (same as SIEVE — one free pass).
//   - visited=false, freq>0: freq--, skip (extra second chance from freq).
//   - visited=false, freq=0: evict (cold entry).
//
// The fast path (Get under RLock) checks Entry.Visited() exactly as
// SIEVE does — zero allocations, no ioBits read. freq is only touched
// on the slow path (write lock) and during eviction, so no atomic CAS
// is needed; ioBits is a plain uint32 accessed only under the shard
// write lock.
//
// See ADR-0031 for the pluggable eviction framework.
package cachaner

import (
	"github.com/bouine-cache/bouine/internal/storage/evictor"
)

// ioBits layout: bits 0-2 = freq (0-7). Bits 3-31 are always zero.
// Because the upper bits are zero, ioBits *is* freq as a raw uint32 —
// incrementFreq and the eviction decrement use plain arithmetic (+1 /
// -1) instead of unpack-mask-pack, avoiding the mask, truncate, and
// OR instructions on every slow-path access and eviction probe.
const (
	freqMask uint32 = 0x7
	maxFreq  uint8  = 7
)

// List is a cachaner eviction list. It is NOT goroutine-safe; the
// caller (per-shard hot tier) must hold the appropriate lock.
type List[K comparable] struct {
	head *evictor.Entry[K]
	tail *evictor.Entry[K]
	hand *evictor.Entry[K]
	len  int
	pool *evictor.EntryPool[K]
}

// NewList creates an empty cachaner list.
func NewList[K comparable]() *List[K] {
	return &List[K]{
		pool: evictor.NewEntryPool[K](),
	}
}

// Clear removes all entries from the list, returning them to the pool.
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

// Access records an access to key. If the key already exists, its freq
// is incremented (saturating at 7) and visited is set. If not, a new
// entry is inserted at the head with freq=0.
func (l *List[K]) Access(key K, lookup func(K) *evictor.Entry[K]) (*evictor.Entry[K], bool) {
	if e := lookup(key); e != nil {
		l.incrementFreq(e)
		e.MarkVisited()
		return e, false
	}
	e := l.pool.Get()
	e.Key = key
	l.pushHead(e)
	return e, true
}

// Evict removes and returns the key of the evicted entry. The probe
// budget is l.len * (maxFreq + 2): one sweep to clear visited bits,
// plus up to maxFreq sweeps to exhaust freq second chances, plus one
// to find a cold entry. This bounds the worst case at O(maxFreq * len)
// rather than SIEVE's O(2 * len), accounting for the extra second
// chances the freq counter provides.
func (l *List[K]) Evict() (K, bool) {
	return l.EvictBounded(l.len * (int(maxFreq) + 2))
}

// EvictBounded removes and returns the key of the evicted entry,
// scanning at most maxProbes entries. The caller must hold the write
// lock.
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

		if cur.Visited() {
			// Accessed since last sweep — give a free pass (same as SIEVE).
			cur.ClearVisited()
			l.hand = cur.Prev()
			if l.hand == nil {
				l.hand = l.tail
			}
			continue
		}

		bits := cur.IOBits()
		if bits > 0 {
			// Extra second chance from freq counter. bits is freq (upper
			// bits are zero), so a plain decrement is safe — no underflow
			// because bits > 0 here.
			cur.SetIOBits(bits - 1)
			l.hand = cur.Prev()
			if l.hand == nil {
				l.hand = l.tail
			}
			continue
		}

		// Cold entry (freq=0, not visited) — evict it.
		l.hand = cur.Prev()
		l.remove(cur)
		key := cur.Key
		l.pool.Put(cur)
		return key, true
	}

	var zero K
	return zero, false
}

// Remove explicitly removes an entry from the list (for Delete / reaper
// / ban operations, not eviction).
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

// Defer moves an entry to the head without changing its visited bit or
// freq counter, and without returning it to the pool. Used by eviction
// policies that want to skip an entry and give it another chance.
func (l *List[K]) Defer(e *evictor.Entry[K]) {
	if e == nil {
		return
	}
	if l.hand == e {
		l.hand = e.Prev()
	}
	l.remove(e)
	l.pushHead(e)
}

// incrementFreq increments the freq portion of ioBits, saturating at
// maxFreq. Called under the write lock; ioBits is a plain uint32 so no
// atomic operation is needed.
//
// Because ioBits holds only freq (upper bits are zero), we can use
// plain arithmetic: bits+1 is safe as long as bits < 7 (no carry into
// upper bits), and the early return guarantees that.
func (l *List[K]) incrementFreq(e *evictor.Entry[K]) {
	bits := e.IOBits()
	if bits >= uint32(maxFreq) {
		return
	}
	e.SetIOBits(bits + 1)
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

// unpackFreq extracts the 3-bit freq value from ioBits. Used by tests
// to verify freq state without reaching into the raw field.
func unpackFreq(bits uint32) uint8 {
	return uint8(bits & freqMask) //nolint:gosec // G115: 3-bit value, max 7
}
