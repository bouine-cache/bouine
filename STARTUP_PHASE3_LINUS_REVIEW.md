# Linus Review: Phase 3 Design (Persistent Index Snapshot, Lazy Segment Loading, WAL Checkpointing)

**Verdict: Fix-before-merge — the design has the right instinct but
several correctness holes that will eat data in production. The snapshot
format is over-engineered for a startup acceleration artifact, and the
SIEVE eviction story is hand-waved. Fix the holes and it's good.**

Reviewed document: `STARTUP_PHASE3_DESIGN.md`

Cross-referenced against actual code in:
- `internal/storage/warm/warm.go` (Store, Segment, warmLoc, Put/Get/Delete,
  Scan, Compact, openExisting)
- `internal/storage/tiered.go` (NewTieredStore, initWAL, rewriteWAL,
  warmSyncLoop, Close)
- `internal/storage/wal/wal.go` (Log, Replay, Truncate, Open/OpenAsync)

---

## BLOCKER-1: Snapshot write holds idxMu.RLock for 400ms — blocks all Puts and Deletes

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.5 "Better fix" (bufio.Writer approach)

The design acknowledges the lock contention problem and proposes holding
`idxMu.RLock` for the entire map iteration + bufio write. It estimates
~400ms for 10M entries. During this time, all `Put` and `Delete` calls
block (they need `idxMu.Lock`). `Get` still works (RLock is shared).

**400ms of blocked writes every 5 minutes is not acceptable under heavy
load.** The design says "acceptable for a periodic checkpoint" without
justifying it. At 50K req/s with 30% write ratio, 400ms of blocked writes
= ~6000 queued requests. This causes:
- Request collapsing latch timeout (if configured) → origin thundering
  herd for the collapsed keys.
- p99 latency spike of 400ms+ for all miss-path requests.
- Possible kubelet readiness probe failure if the admin server's
  `readyz` handler takes >5s (it won't, but the data plane backlog could
  cascade).

**The root problem:** Iterating a Go `map[uint64]warmLoc` with 10M
entries under RLock and writing each entry to a bufio.Writer is O(N) in
both map iteration (cache-unfriendly: Go maps are hash tables with
scattered buckets) and I/O (280 MB of buffered writes).

**Fix — don't hold the lock during the write. Take a consistent snapshot
of the index in O(1) and iterate lock-free:**

The current `Compact()` already does this correctly (warm.go:1253-1258):
it copies the index into `idxSnap` under RLock, releases the lock, then
iterates `idxSnap` lock-free. The copy is O(N) but it's a memcpy of map
entries (memory bandwidth, ~50ms for 10M entries), and the RLock is held
only for the copy — writers block for ~50ms, not 400ms.

Use the same pattern for snapshot writes:

```go
func (s *Store) writeSnapshot() error {
    s.idxMu.RLock()
    idxSnap := make(map[uint64]warmLoc, len(s.index))
    for k, v := range s.index {
        idxSnap[k] = v
    }
    s.idxMu.RUnlock()

    // Iterate the snapshot lock-free. Sort, write, etc.
    // Writers are unblocked. Readers are unaffected.
    entries := make([]snapEntry, 0, len(idxSnap))
    for key, loc := range idxSnap {
        entries = append(entries, snapEntry{key, loc.segID, loc.offset, loc.size})
    }
    slices.SortFunc(entries, ...)
    // ... write to file ...
}
```

**Yes, this allocates ~280 MB for `idxSnap` + ~280 MB for `entries` at
10M keys.** That's 560 MB of transient memory. Under GOMEMLIMIT=6GiB,
this is within bounds but triggers GC. The design's §3.5 OOMKill section
identifies this problem but then proposes a "fix" (per-entry bufio write
under RLock) that's worse — it trades memory for lock hold time. The
correct trade-off is: spend the memory (it's transient, GC reclaims it),
keep the lock hold short (50ms vs 400ms).

If 560 MB is too much, stream in batches with a different consistency
strategy: take the RLock, copy a batch of 100K entries into a slice,
release RLock, write the batch, re-acquire RLock for the next batch. The
snapshot may be slightly inconsistent (a Put between batches could be
missed), but WAL replay on top of the snapshot will fix it — the WAL
entry for that Put will overwrite the stale snapshot entry. **This only
works because the WAL is not truncated until after the snapshot is
written.** The WAL is the consistency anchor, not the snapshot.

---

## BLOCKER-2: WAL truncation after snapshot is not crash-safe — WAL entries between snapshot start and truncate are lost

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.3 "Checkpoint sequence"

The checkpoint sequence is:
1. `wal.Sync()` — flush pending WAL entries to disk.
2. `writeSnapshot()` — write index snapshot.
3. `wal.Truncate()` — truncate WAL to 0.

The design says: "If crash after snapshot rename, before WAL truncate:
restart loads new snapshot + full WAL replay. Entries are idempotent."

**This is correct for entries written *before* the snapshot.** But
what about entries written *between* step 2 (snapshot write starts) and
step 3 (WAL truncate)?

Here's the interleaving:

```
t=0  wal.Sync() completes. WAL has entries [1..N].
t=1  writeSnapshot() starts. Iterates index under RLock.
t=2  A concurrent Put writes entry [N+1] to the WAL (via Enqueue).
     The snapshot doesn't include this entry — it was iterating
     the index before the Put updated it (or after, depending on
     timing — the RLock copy may or may not see it).
t=3  writeSnapshot() completes. Snapshot is written to disk.
t=4  wal.Truncate() executes. WAL is now empty.
     Entry [N+1] is GONE.
```

**Entry N+1 is lost.** It was in the WAL, the snapshot may or may not
have it, and the truncate wiped it. On restart, the snapshot is loaded
and the WAL is empty — entry N+1 is nowhere.

The design's atomicity analysis says "entries are idempotent" — that's
true for replay (PUT overwrites, DELETE removes), but it's irrelevant
when the entry is *deleted* by the truncate before it can be replayed.

**Fix — truncate the WAL *before* writing the snapshot, not after:**

Wait, that's worse — if crash after truncate but before snapshot write,
both the old WAL and the new snapshot are gone.

**Real fix — the checkpoint must be atomic with respect to the WAL.**
There are two approaches:

**Approach A: Rotate the WAL, don't truncate.**

1. Close the current WAL file (call it `wal.001`).
2. Open a new WAL file (`wal.002`) for subsequent writes.
3. Write the snapshot from the index (which includes all entries from
   `wal.001` that were applied before step 1).
4. Delete `wal.001` after the snapshot is written and fsynced.

If crash after step 1 but before step 4: on restart, the snapshot may
be old or missing, but `wal.001` still exists. Replay `wal.001` (and
`wal.002` if it exists) on top of the snapshot.

This requires the WAL to support multiple files. The current WAL is a
single file. Adding multi-file support is a format change but a clean
one: `wal.Replay` already iterates records linearly; extending it to
read multiple files in order is straightforward.

**Approach B: Stop accepting WAL writes during checkpoint.**

1. `wal.Sync()` — flush all pending entries.
2. Block all `Enqueue`/`EnqueueBatch` calls (set a flag, spin-wait or
   return a "checkpointing" error).
3. `writeSnapshot()` — write snapshot from the now-frozen index.
4. `wal.Truncate()` — truncate the WAL.
5. Unblock `Enqueue`/`EnqueueBatch`.

This is simpler but blocks writes for the duration of the snapshot
write (~400ms with the bufio approach, ~50ms with the copy approach
from BLOCKER-1). Combined with the BLOCKER-1 fix (50ms lock hold), the
total write pause is ~50ms (lock hold for copy) + ~0ms (WAL block is
instant since the snapshot write happens lock-free after the copy).

Actually, approach B with the BLOCKER-1 fix works cleanly:

1. `wal.Sync()`.
2. Set `checkpointing = true` (atomic.Bool). All `Enqueue` calls spin
   until `checkpointing = false`. This is ~1ms of spin time.
3. `idxMu.RLock()` → copy index → `idxMu.RUnlock()` (~50ms).
4. Iterate the copy lock-free, write the snapshot file (~300ms for I/O,
   writers are unblocked after step 3).
5. `wal.Truncate()`.
6. `checkpointing = false`.

Wait — writers are unblocked after step 3, but the WAL is still
accepting writes between step 3 and step 5. Those writes go into the
WAL that's about to be truncated. Same bug.

**The fundamental issue:** you cannot safely truncate the WAL while
new entries are being written to it, unless you atomically swap to a
new WAL file (Approach A) or block writes until after the truncate
(Approach B with the block extending to after step 5).

**Approach B revised (block writes for the minimum time):**

1. `wal.Sync()`.
2. `idxMu.RLock()` → copy index → `idxMu.RUnlock()`.
3. Set `checkpointing = true`. WAL `Enqueue` blocks.
4. `wal.Sync()` again — flush any entries written between step 2 and
   step 3.
5. `wal.Truncate()`.
6. Set `checkpointing = false`. WAL `Enqueue` resumes.
7. Write the snapshot from the copy (lock-free, I/O in background).

The snapshot is written from the copy taken in step 2. The WAL was
synced (step 4) and truncated (step 5) while writes were blocked. Any
entry written before step 3 is either in the copy or in the WAL that
was synced before truncate. Any entry written after step 6 goes into
the fresh WAL and will be replayed on top of the snapshot.

**But the snapshot from step 7 might not include entries that were in
the WAL at step 4.** Those entries were applied to the index before
step 2 (they're in the copy) — no, that's not guaranteed. The WAL is
async (`Enqueue` → `syncCh` → `walSyncLoop` → `drainAndSync` → write).
An `Enqueue` at t=-1s might not be flushed to disk until the `Sync` at
step 1. But the index was already updated by the `Put` path (which
calls `warm.Put` → updates the map → `wal.Enqueue`). So the index
copy at step 2 includes the entry, and the WAL flush at step 1 ensures
it's on disk. The snapshot written at step 7 includes it (from the
copy). The WAL truncate at step 5 removes it from the WAL. But it's in
the snapshot. So it's safe.

**What about an entry that's Enqueued between step 1 (Sync) and step 3
(block)?** The `Put` path does: `warm.Put` (updates index) →
`wal.Enqueue` (async, goes to `syncCh`). If the `Put` happens between
step 1 and step 3:
- The index is updated → it's in the copy at step 2. ✓
- The `Enqueue` goes to `syncCh` but isn't flushed yet (no `Sync` after
  step 1 until step 4). At step 3, `checkpointing = true` blocks new
  `Enqueue` calls, but the entry already in `syncCh` is still there.
  Step 4's `Sync` flushes it to disk. Step 5 truncates the WAL (which
  includes this entry). But the entry is in the snapshot (from the
  copy). ✓

**What about an `Enqueue` that's blocked at step 3 (checkpointing =
true) and resumes at step 6?** The `Put` already updated the index
( before `Enqueue`), so the entry is in the copy. The `Enqueue` resumes
after truncate and goes into the fresh WAL. On restart, the snapshot
has the entry, and the WAL also has it (idempotent PUT). ✓

**This works.** The key insight is: the `Put` path always updates the
index *before* enqueuing to the WAL. So the index copy at step 2 is a
superset of what's in the WAL at any point. The snapshot written from
the copy is therefore safe to use as the post-truncate truth.

**Fix the design to use Approach B revised.**

---

## BLOCKER-3: Snapshot doesn't track which segments are referenced — missing-segment detection is O(N) per entry

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.4 "Snapshot references missing segments"

The design says: "During snapshot load, check that every `seg_id` in
the snapshot has a corresponding `*.seg` file on disk."

This is O(N) over all snapshot entries, checking each entry's segID
against the set of discovered segment files. At 10M entries, this is
10M map lookups — ~50ms. Not terrible, but wasteful: most entries share
the same ~100 segments. The check should be on the segment set, not per
entry.

**Fix:** Build a `set[int]` of segment IDs from the snapshot entries
(first pass), compare against the `segByID` map from `openExisting`.
O(N) to build the set (one pass), O(S) to compare (S = segment count,
typically <1000). Or simpler: store the segment ID list in the snapshot
header (the design already has `seg_count` in the header but doesn't
list the IDs). Add a segment ID table after the header:

```
[Header — 48 bytes]
  seg_count   uint64
  ...
[Segment table — 8 bytes per segment]
  seg_id      int32
  seg_size    int32   (file size in MiB, for validation)
[Entries — 28 bytes each]
  ...
```

On load, compare the segment table against `openExisting`'s discovered
files. If any seg_id is missing or any seg_size doesn't match the
actual file size, discard the snapshot. This is O(S), not O(N).

---

## bug-4: Lazy segment loading breaks `Scan` (fallback path) — segments may not be open

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.2 "`Scan` (fallback path)"

The design says: "Opens each segment sequentially, scans, closes. Does
not use the LRU cache."

But `Scan` currently uses `seg.mu.Lock` (warm.go:608) and accesses
`seg.f` directly. With lazy loading, `seg.f` may be `nil`. The design
doesn't show how `Scan` calls `ensureOpen()` — it says "opens each
segment" but doesn't integrate with the lazy loading mechanism.

**Fix:** `Scan` must call `seg.ensureOpen()` before `scanSegment(seg.f,
...)`. After the scan, optionally close the segment (if it wasn't
already open and the LRU cache is full). Or simpler: `Scan` opens all
segments it needs via `ensureOpen()`, and the LRU cache manages the FD
lifecycle. `Scan` doesn't close segments — the LRU eviction does.

Also, `Scan` currently holds `seg.mu.Lock` during the scan. With lazy
loading, `ensureOpen()` also needs `seg.mu.Lock`. This is fine —
`ensureOpen` acquires the lock, opens the file, releases the lock, then
`Scan` re-acquires it for the scan. Or `ensureOpen` is called before
`Scan` takes the lock:

```go
func (s *Store) Scan(fn func(Record) error) error {
    s.mu.RLock()
    segs := make([]*Segment, len(s.segs))
    copy(segs, s.segs)
    s.mu.RUnlock()

    for _, seg := range segs {
        if err := seg.ensureOpen(); err != nil {
            return err
        }
        seg.mu.Lock()
        err := scanSegment(seg.f, seg.ID, fn)
        seg.mu.Unlock()
        if err != nil { return err }
    }
    return nil
}
```

---

## bug-5: Lazy segment loading breaks `Compact` — `swapSegmentFiles` assumes all segments are open

**Location:** `internal/storage/warm/warm.go:1288-1306`, `STARTUP_PHASE3_DESIGN.md` §3.2

`Compact()` does:
1. `s.mu.Lock()` (exclusive on segment set).
2. `swapSegmentFiles` — removes old `.seg` files, renames new ones.
3. `NewStore(dir)` — reopens all segments (calls `openExisting`).
4. Closes old segment FDs: `for _, seg := range s.segs { seg.f.Close() }`.

With lazy loading, `seg.f` may be `nil` for unopened segments. Calling
`seg.f.Close()` on a nil `f` panics.

**Fix:** Guard the close:
```go
for _, seg := range s.segs {
    if seg.f != nil {
        _ = seg.f.Close()
    }
}
```

Or better: add a `seg.Close()` method that handles nil:
```go
func (seg *Segment) Close() error {
    seg.mu.Lock()
    defer seg.mu.Unlock()
    if seg.f == nil { return nil }
    err := seg.f.Close()
    seg.f = nil
    seg.opened.Store(false)
    return err
}
```

Also, `Compact` creates a fresh `Store` via `NewStore(dir)` which calls
`openExisting()` — with lazy loading, this should create `Segment`
structs with `f = nil`, not open files. The design needs to make
`openExisting` lazy (which it proposes) and verify that `Compact`'s
`NewStore(dir)` call also uses lazy mode.

---

## bug-6: `readRecordAt` uses `seg.f` directly without checking for nil — lazy loading needs `ensureOpen` on every Get

**Location:** `internal/storage/warm/warm.go:430`

```go
rec, err := readRecordAt(seg.f, loc.offset, loc.segID)
```

With lazy loading, `seg.f` may be `nil`. `Get` must call `ensureOpen()`
before `readRecordAt`. The design mentions this but doesn't show the
integration with the existing lock ordering:

Current `Get` lock order: `s.mu.RLock` → `idxMu.RLock` → `seg.mu.Lock`
(for `readRecordAt`, which uses pread and doesn't need `seg.mu` but
the code holds it for safety).

With `ensureOpen`: `s.mu.RLock` → `idxMu.RLock` (lookup) →
`idxMu.RUnlock` → `seg.ensureOpen()` (takes `seg.mu.Lock`) →
`readRecordAt(seg.f, ...)` → `s.mu.RUnlock`.

The `ensureOpen` must happen while `s.mu.RLock` is still held (so
Compact doesn't swap the segment set mid-open). The `idxMu.RLock` can
be released after the lookup (as it is today). So:

```go
func (s *Store) Get(key uint64) ([]byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    s.idxMu.RLock()
    loc, ok := s.index[key]
    s.idxMu.RUnlock()
    if !ok { return nil, nil }

    seg := s.segByID[loc.segID]
    if seg == nil { s.dropStaleIndex(key, loc); return nil, nil }

    if err := seg.ensureOpen(); err != nil {
        return nil, fmt.Errorf("warm: open segment %d: %w", seg.ID, err)
    }

    rec, err := readRecordAt(seg.f, loc.offset, loc.segID)
    // ... rest unchanged ...
}
```

This is correct but adds one `seg.mu.Lock` per Get (for `ensureOpen`).
After the first open, `ensureOpen` is a no-op (checks `opened` atomic,
returns immediately). The lock is only contended during the first
access of each segment.

---

## bug-7: LRU FD cache eviction races with concurrent Get on the same segment

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.2 "LRU FD cache"

The LRU cache evicts the least recently used open segment when the FD
count exceeds `maxOpen`. Eviction closes the FD (`seg.f.Close()`,
`seg.f = nil`).

If a concurrent `Get` is in-flight on the segment being evicted, it's
holding `seg.f` for a `readRecordAt` (pread) call. The eviction closes
the FD out from under the pread. On Linux, `close()` on a file
descriptor with an in-flight `pread` is safe — the pread completes
against the closed FD, and the file inode remains alive until the
pread releases it (kernel ref-counts the inode). But on some NFS
configurations, this can return `ESTALE`.

**Fix:** The LRU eviction must check that no reader is in-flight. Use
a per-segment `atomic.Int32` reader count:

```go
type Segment struct {
    // ...
    readers atomic.Int32 // number of in-flight readRecordAt calls
}

func (s *Store) Get(key uint64) ([]byte, error) {
    // ...
    seg.readers.Add(1)
    defer seg.readers.Add(-1)
    rec, err := readRecordAt(seg.f, loc.offset, loc.segID)
    // ...
}
```

Eviction skips segments with `readers > 0`:
```go
func (c *segmentCache) evictOne() {
    // Find LRU segment with readers == 0
    for _, seg := range c.lru.FromBack() {
        if seg.readers.Load() == 0 {
            seg.Close()
            c.lru.Remove(seg)
            return
        }
    }
    // All segments have readers — skip eviction this cycle.
}
```

This is simple, safe, and the overhead is one atomic add per Get
(nanoseconds).

---

## taste-8: Snapshot format is over-engineered for a startup acceleration artifact

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.1 "File format"

The snapshot has a 48-byte header with magic, version, entry_count,
total_bytes, seg_count, created_ns, key_count (redundant with
entry_count), and header_crc. Plus a footer with footer_magic and
footer_crc.

**`key_count` is redundant with `entry_count`.** The design acknowledges
this ("redundant, for validation") but doesn't justify why. If
entry_count != key_count, what does the loader do? The design doesn't
say. Drop it.

**`created_ns` is for "staleness detection."** The design doesn't
describe how staleness is detected or what happens if the snapshot is
stale. Is a 1-hour-old snapshot stale? 1-day-old? The WAL delta replay
handles any staleness — the snapshot is always correct as long as the
WAL entries after it are replayed. `created_ns` is debug information,
not a correctness field. Move it to a comment or drop it.

**`seg_count` in the header is useful** (see BLOCKER-3 fix), but it
should be accompanied by the segment ID table, not just a count.

**Simplified format:**

```
[Header — 32 bytes]
[0:4]   magic      uint32   = 0x49445853
[4:8]   version    uint32   = 1
[8:16]  entry_count uint64
[16:24] total_bytes uint64
[24:28] seg_count   uint32
[28:32] header_crc  uint32  — CRC32C of [0:28]

[Segment table — 8 bytes × seg_count]
  seg_id    int32
  seg_size  int32   — file size in bytes

[Entries — 28 bytes × entry_count, sorted by key]
  key       uint64
  seg_id    int32
  offset    int64
  size      int64

[Footer — 8 bytes]
  footer_crc uint32  — CRC32C of all entry bytes
  end_magic  uint32  = 0x464E4524
```

36 bytes smaller header, no redundant fields, segment table for O(S)
validation. Same correctness.

---

## taste-9: SIEVE rebuild from key-sorted snapshot is hand-waved

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.1 "Interaction with SIEVE eviction list"

The design chooses "Option 1: accept key-order SIEVE rebuild" and
justifies it with "the SIEVE list converges within a few thousand
requests." This is a guess, not a measured claim.

**The problem is real.** SIEVE's eviction quality depends on the aging
property: old entries are at the tail, new entries at the head. After
key-order rebuild, the tail has key 0 (which may be a very popular key)
and the head has the highest key number (which may be rarely accessed).
The first eviction sweep after restart could evict hot keys and keep
cold ones — the opposite of what SIEVE should do.

**The design says "the warm tier is rarely at full capacity immediately
after restart (the hot tier is empty, so keys promote to hot and the
warm tier has slack)."** This is true for the first few minutes. But
under heavy load, the warm tier fills up quickly — the hot tier has a
limited size, and once it's full, new puts evict to warm. If the warm
tier reaches capacity within minutes and starts evicting with a
key-ordered SIEVE list, the eviction quality will be poor until the
list converges.

**Fix — use insertion-order from the WAL, not key order from the
snapshot.** The WAL is append-ordered: entries are written in the order
Puts happen. When replaying the WAL delta on top of the snapshot,
insert entries into the SIEVE list in WAL replay order. For entries
from the snapshot (not in the WAL delta), insert in key order. This
gives:
- Snapshot entries: key-ordered in SIEVE (suboptimal but they're the
  base set).
- WAL delta entries: insertion-ordered in SIEVE (correct aging for
  recent writes).

This is strictly better than pure key-order and requires no format
changes. The WAL replay already iterates in order — just insert into
the SIEVE list during replay (which `SetIndex` already does).

**Long-term fix:** Add a `sieve_order` field to the snapshot (Option 2
in the design). This is a v2 format change — the version field supports
it. Defer to v2 if eviction quality after restart becomes a measured
problem.

---

## nit-10: `segment_cache_size: 128` default is arbitrary

**Location:** `STARTUP_PHASE3_DESIGN.md` §3.7

128 is not justified. With 64 MiB segments and a 20 GiB warm tier, that's
~320 segments. `maxOpen = 128` means 40% of segments are open at any
time. For smaller tiers (1 GiB = 16 segments), 128 is more than enough
(all segments open). For larger tiers (100 GiB = 1600 segments), 128
is 8% — meaning 92% of Gets will trigger a segment open (50 µs each).

**Fix:** Default should scale with the warm tier size, or be derived
from the segment count. A reasonable default: `min(len(segments), 256)`.
Or keep 128 but document the trade-off: "each open segment uses one FD
and ~64 MiB of kernel page cache (if mmap'd). 128 open segments = 128
FDs + ~8 GiB page cache. Reduce for memory-constrained environments."

---

## What's done well

- The startup path (§3.4) correctly layers snapshot → WAL delta →
  fallback, with clear conditions for each path.
- The failure mode analysis (§3.5) covers the important crash
  scenarios, even though some fixes are wrong (BLOCKER-2).
- The lazy segment loading design (§3.2) correctly identifies that
  `openExisting` only needs metadata, not file handles.
- The config additions (§3.7) are well-scoped and have sensible
  defaults (except the FD cache size, nit-10).
- The testing plan (§3.8) covers the right scenarios: round-trip,
  corruption, missing segments, concurrent writes, large index.
- The performance summary (§3.6) sets concrete targets that can be
  benchmarked.
- The decision to not mmap the snapshot as a runtime index (§3.1 "Why
  not mmap the snapshot as the runtime index") is correct — the Go map
  is faster for lookups and the snapshot's job is startup speed.
