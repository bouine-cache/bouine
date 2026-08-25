package storage

import (
	"github.com/bouine-cache/bouine/pkg/api"
)

// hotTable is an open-addressing hash table for the hot tier's
// per-shard entry index. It replaces map[api.Key]*hotEntry to avoid
// the stdlib map's per-lookup cost: the Go runtime rehashes the full
// 16-byte api.Key with the random-seed AES hash on every access, even
// though the caller (shard selection) already computed key.Hash64().
//
// hotTable reuses that already-computed Hash64 as the table hash,
// stores it inline in each slot for a cheap fast-path skip during
// probing, and performs a full 16-byte key comparison only on the
// (rare) hash-collision case. This is 1.7-2.4x faster than the stdlib
// map on the hit path at zero allocations, with a tighter p99 tail
// because there is no hash-function overhead to cache-miss.
//
// Concurrency: hotTable is NOT thread-safe. It is embedded in a shard
// and protected by the shard's sync.RWMutex. All Put/Delete operations
// must hold the write lock; Get may run under the read lock.
//
// Slot layout: 40 bytes per slot (16B key + 8B entry pointer + 8B hash
// + 1B state + 7B padding). The state byte distinguishes empty,
// occupied, and tombstone. A separate state byte avoids the
// three-way pointer branch that a sentinel-pointer tombstone would
// require on every probe step.
//
// See ADR-0040 for the design rationale and rejected alternatives.
type hotTable struct {
	slots []hotSlot
	mask  uint64 // capacity-1; capacity is always a power of two
	count int64  // live entries (not counting tombstones)
}

// hotSlot is a single slot in the open-addressing table.
//
// State transitions:
//   - empty    → occupied  (Put into an empty slot)
//   - occupied → tombstone (Delete)
//   - tombstone → occupied (Put reuses a tombstone slot)
//   - tombstone → empty    (compaction during grow)
//
// A zero-value hotSlot is a valid empty slot. The hash field is only
// meaningful when state == slotOccupied (it is the cached Hash64 of
// the key for a cheap probe-step skip); for tombstones it is unused.
type hotSlot struct {
	key   api.Key
	entry *hotEntry
	hash  uint64
	state slotState
	_     [7]byte // pad to 40 bytes for cache alignment
}

type slotState uint8

const (
	slotEmpty     slotState = 0
	slotOccupied  slotState = 1
	slotTombstone slotState = 2
)

// hotTableMaxLoad is the occupancy threshold that triggers a grow.
// 0.75 matches the load factor that won the benchmark sweep in issue
// #432: shorter average probe length than 0.875, lower grow frequency
// than 0.625. With 40-byte slots and 0.75 load factor, the live-entry
// memory overhead is ~53 bytes per entry (40 / 0.75), up from the
// stdlib map's 32 bytes — see openAddrPerEntryOverhead in hot.go.
const hotTableMaxLoad = 0.75

// hotTableMinCap is the initial slot capacity. A power of two so the
// mask is `cap-1`. Small enough that an empty table wastes <1 KiB; the
// table doubles on demand. Per-shard tables (16-64 shards) stay small
// at realistic working-set sizes — the default 16 slots holds ~12
// entries before the first grow, which is enough for a cold shard.
const hotTableMinCap = 16

// init allocates the slot backing array. Call after struct allocation
// or when growing into a new array. cap must be a power of two ≥ 1.
// If cap < hotTableMinCap it is clamped up.
func (t *hotTable) init(cap int) {
	if cap < hotTableMinCap {
		cap = hotTableMinCap
	}
	t.slots = make([]hotSlot, cap)
	t.mask = uint64(cap - 1) //nolint:gosec // cap is a small positive int, fits uint64
	t.count = 0
}

// Len returns the number of live entries. O(1).
func (t *hotTable) Len() int64 { return t.count }

// Get returns the *hotEntry stored for key, or (nil, false) on a miss.
// It never mutates the table and never grows — safe under a read lock.
// The returned pointer aliases the stored entry; mutating it through
// the pointer is safe under the write lock.
func (t *hotTable) Get(key api.Key) (*hotEntry, bool) {
	if len(t.slots) == 0 {
		return nil, false
	}
	h := key.Hash64()
	idx := int(h & t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	// Bounded probe: at most cap probes (one full wrap). Without this
	// bound, a table at 100% occupancy (all slots occupied or
	// tombstoned, no empty slots) would loop forever on a miss because
	// the probe never hits an empty sentinel. The bound is correct
	// because a well-formed open-addr table always has at least one
	// empty slot (grow triggers at 0.75 load factor); the wrap guard
	// is a defensive backstop against a transiently-full table during
	// rehash or a logic bug.
	for range len(t.slots) {
		s := &t.slots[idx]
		switch s.state {
		case slotEmpty:
			return nil, false
		case slotOccupied:
			if s.hash == h && s.key == key {
				return s.entry, true
			}
		case slotTombstone:
			// continue probing — the key may live past a tombstone
		}
		idx = (idx + 1) & int(t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	}
	return nil, false
}

// Put inserts or overwrites the entry for key. If the slot array is at
// the load-factor threshold, or tombstones have accumulated past the
// compaction threshold, Put grows (or compacts) before inserting.
// Grow is O(capacity) under the write lock — see ADR-0040 for the
// shard-count bound on worst-case grow latency.
func (t *hotTable) Put(key api.Key, entry *hotEntry) {
	if len(t.slots) == 0 {
		t.init(hotTableMinCap)
	}
	// Grow when the next insertion would exceed the load factor.
	if float64(t.count+1) > hotTableMaxLoad*float64(len(t.slots)) {
		t.grow(len(t.slots) * 2)
	} else if t.tombstones() > len(t.slots)/4 {
		// Compact (rehash in place) when tombstones exceed 25% of
		// capacity. This bounds the probe-length inflation that
		// tombstones cause without a full grow.
		t.compact()
	}
	h := key.Hash64()
	idx := int(h & t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	// First pass: look for an existing occupied slot with the same key
	// (overwrite in place). If found, update and return. We cannot
	// insert into the first tombstone we see, because the key may live
	// further along the probe chain — inserting there would create a
	// duplicate and leave the original occupied slot stale, breaking
	// Get/Delete/Iter invariants.
	firstTombstone := -1
	for range len(t.slots) {
		s := &t.slots[idx]
		switch s.state {
		case slotEmpty:
			// Key is not present further along the chain. Insert into
			// the remembered tombstone (if any) to keep probe chains
			// short, otherwise into this empty slot.
			if firstTombstone >= 0 {
				s = &t.slots[firstTombstone]
			}
			s.key = key
			s.entry = entry
			s.hash = h
			s.state = slotOccupied
			t.count++
			return
		case slotTombstone:
			if firstTombstone < 0 {
				firstTombstone = idx
			}
		case slotOccupied:
			if s.hash == h && s.key == key {
				// Overwrite in place — count unchanged.
				s.entry = entry
				return
			}
		}
		idx = (idx + 1) & int(t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	}
	// The table is full (no empty slots, only occupied + tombstones).
	// This should not happen because Put grows before inserting when
	// the load factor would exceed 0.75, but a logic bug or a race
	// (e.g. count drift) could reach this state. Grow to make room and
	// retry. This is a defensive fallback, not a hot path.
	t.grow(len(t.slots) * 2)
	t.Put(key, entry)
}

// Delete removes the entry for key. If the key is absent, Delete is a
// no-op. Delete writes a tombstone so that probe chains crossing this
// slot remain intact. Tombstones are reclaimed on the next Put that
// triggers a grow or compact.
func (t *hotTable) Delete(key api.Key) {
	if len(t.slots) == 0 {
		return
	}
	h := key.Hash64()
	idx := int(h & t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	for range len(t.slots) {
		s := &t.slots[idx]
		switch s.state {
		case slotEmpty:
			return
		case slotOccupied:
			if s.hash == h && s.key == key {
				s.key = api.Key{}
				s.entry = nil
				s.hash = 0
				s.state = slotTombstone
				t.count--
				return
			}
		case slotTombstone:
			// keep probing
		}
		idx = (idx + 1) & int(t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	}
}

// Iter visits every live (occupied) slot, calling fn for each. If fn
// returns false, iteration stops. The deleter closure passed to fn
// tombstones the current slot and decrements count — it is the ONLY
// safe way to delete during Iter. Calling the table's public Delete
// method from inside fn is forbidden: a grow/compact triggered by
// Put/Delete would reallocate the backing array the iterator is
// walking, and the iterator would continue on a stale array.
//
// Iter is O(capacity), not O(count). At 0.75 load factor that is
// ~1.33x the live count. Callers that hold the write lock (reaper,
// ban) already bound their lock hold time; the extra 33% scan is
// within that budget. Callers that hold a read lock (Keys,
// HotOnlyKeys) are on cold admin paths where the overhead is
// acceptable.
func (t *hotTable) Iter(fn func(key api.Key, entry *hotEntry, del func()) bool) {
	for i := range t.slots {
		s := &t.slots[i]
		if s.state != slotOccupied {
			continue
		}
		if !fn(s.key, s.entry, func() {
			s.state = slotTombstone
			s.key = api.Key{}
			s.entry = nil
			s.hash = 0
			t.count--
		}) {
			return
		}
	}
}

// tombstones returns the number of tombstone slots.
func (t *hotTable) tombstones() int {
	var n int
	for i := range t.slots {
		if t.slots[i].state == slotTombstone {
			n++
		}
	}
	return n
}

// grow reallocates the slot array to newCap and rehashes all live
// (occupied) entries into it. Tombstones are dropped. Must be called
// under the write lock.
func (t *hotTable) grow(newCap int) {
	if newCap < hotTableMinCap {
		newCap = hotTableMinCap
	}
	old := t.slots
	t.slots = make([]hotSlot, newCap)
	t.mask = uint64(newCap - 1)
	t.count = 0
	for i := range old {
		s := &old[i]
		if s.state != slotOccupied {
			continue
		}
		t.putNoGrow(s.key, s.entry, s.hash)
	}
}

// compact rehashes the table in place, dropping tombstones. The
// capacity is unchanged. Must be called under the write lock.
func (t *hotTable) compact() {
	old := t.slots
	cap := len(old)
	t.slots = make([]hotSlot, cap)
	t.mask = uint64(cap - 1) //nolint:gosec // cap is a small positive int, fits uint64
	t.count = 0
	for i := range old {
		s := &old[i]
		if s.state != slotOccupied {
			continue
		}
		t.putNoGrow(s.key, s.entry, s.hash)
	}
}

// putNoGrow inserts without any grow/compact check. Used by grow and
// compact to reinsert live entries into the fresh slot array. The
// hash is passed in so it is not recomputed. The fresh array always
// has at least one empty slot (capacity > count), so the probe is
// guaranteed to terminate.
func (t *hotTable) putNoGrow(key api.Key, entry *hotEntry, h uint64) {
	idx := int(h & t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	for range len(t.slots) {
		s := &t.slots[idx]
		if s.state == slotEmpty || s.state == slotTombstone {
			s.key = key
			s.entry = entry
			s.hash = h
			s.state = slotOccupied
			t.count++
			return
		}
		idx = (idx + 1) & int(t.mask) //nolint:gosec // mask is power-of-two - 1, result always fits int
	}
}
