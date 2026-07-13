# Phase 3 Design: Persistent Index Snapshot, Lazy Segment Loading, WAL Checkpointing

**Reviewed and revised based on `STARTUP_PHASE3_LINUS_REVIEW.md`.**

All review findings (BLOCKER-1 through nit-10) are addressed inline,
marked **[review-fix]**.

## Goal

Turn warm-tier startup from an O(N) segment scan into an O(1) mmap + an
O(delta) WAL replay, where delta is the number of writes since the last
checkpoint. Target: sub-second startup for 10M keys, <5s for 100M keys.

---

## 3.1 On-Disk Index Snapshot

### File format: `index.snap`

Written to `{warm_dir}/index.snap`. Atomic write via `index.snap.tmp` →
fsync → rename.

**[review-fix taste-8]** Simplified header: dropped redundant `key_count`
and `created_ns`. Added segment ID table after the header for O(S)
missing-segment validation instead of O(N) per-entry checks.

```
File layout (little-endian):

[Header — fixed 32 bytes]
[0:4]   magic        uint32   = 0x49445853  ("SXDI")
[4:8]   version      uint32   = 1
[8:16]  entry_count  uint64
[16:24] total_bytes  uint64   — sum of all live record sizes (for stats)
[24:28] seg_count    uint32   — number of segments in the table below
[28:32] header_crc   uint32   — CRC32C of bytes [0:28]

[Segment table — 8 bytes × seg_count]
Per segment:
[0:4]   seg_id       int32
[4:8]   seg_size     int32    — file size in bytes (for validation)

[Entries — sorted by key, 28 bytes each]
Per entry:
[0:8]   key          uint64
[8:12]  seg_id       int32
[12:20] offset       int64
[20:28] size         int64    — on-disk record size (HeaderLen + body + FooterLen)

[Footer — fixed 8 bytes]
[0:4]   footer_crc   uint32   — CRC32C of all segment-table + entry bytes
[4:8]   end_magic    uint32   = 0x464E4524  ("$END")
```

### Design decisions

**Why sorted by key, not a hash table:**
- Sorted entries allow binary search (O(log N)) directly on the mmap'd
  region — no hash table rebuild, no buckets, no probing.
- The snapshot is a **startup acceleration artifact**, not a runtime
  lookup structure. It is read once, converted to `map[uint64]warmLoc`,
  and never touched again until the next startup.
- Converting snapshot → map is O(N) at memory bandwidth (~10-20
  GiB/s), not disk I/O. 10M entries × 28 bytes = 280 MB → ~14-28 ms.

**Why not mmap the snapshot as the runtime index (zero-copy):**
- The runtime index needs `sieve.Entry[uint64]` pointers and `protected`
  flags — runtime-only state that can't live in a read-only mmap.
- A Go `map[uint64]warmLoc` is faster for lookups (O(1) hash vs O(log N)
  binary search) and supports concurrent reads with RWMutex.

**Why `size` is in the snapshot:**
- The snapshot is a checkpoint of the full index state. Including `size`
  means the in-memory map is fully populated with byte-accounting data
  on load — no segment scan needed for stats or accurate Delete.
- Phase 2 item 2.3 adds `size` to the WAL for the incremental delta
  case. The snapshot is the better place for it — it avoids replaying
  millions of WAL entries at all.

**Why no `protected` flag in the snapshot:**
- `protected` means "this key also exists in the hot tier." On restart,
  the hot tier is empty. All entries start unprotected. The flag is set
  by `warmSyncLoop` or `Put` as keys promote to hot.

### Startup path with snapshot

```
1. openExisting() — discover segment files, record IDs.
   With lazy loading (§3.2): creates Segment structs with f=nil.
   No file I/O — just ReadDir + parse filenames.

2. Load index snapshot:
   a. os.Open(index.snap)
   b. f.Stat() → check size ≥ 32 (header) + 8 (footer)
   c. mmap entire file (PROT_READ, MAP_SHARED, MmapPopulate)
   d. Verify header magic, version, header_crc.
   e. Parse segment table (seg_count entries).
      [review-fix BLOCKER-3] Compare each seg_id against the segments
      discovered in step 1. If any seg_id is missing or seg_size
      doesn't match the actual file size → discard snapshot, fall
      back to WAL replay + RecomputeStats.
   f. Verify footer_crc (CRC of segment-table + entry bytes).
   g. If any check fails → discard snapshot, fall back.

3. Build in-memory index from snapshot:
   a. Pre-allocate map[uint64]warmLoc with entry_count capacity.
   b. Iterate entries (sorted by key), insert into map:
      - warmLoc{segID, offset, size, sieve: nil, protected: false}
      - SIEVE entry inserted via evictList.Access (visited=false)
      [review-fix taste-9] SIEVE entries from snapshot are inserted
      in key order (suboptimal aging). WAL delta entries (step 4)
      are inserted in WAL replay order (correct aging for recent
      writes). See "SIEVE rebuild" below.
   c. Set stats.entries = entry_count, stats.bytes = total_bytes
      (from snapshot header — no segment scan needed).

4. WAL replay (incremental delta):
   a. wal.Replay() — replay entries written since the snapshot.
   b. For each PUT: SetIndexWithSize(key, segID, offset, size) —
      overwrites snapshot entry if key matches, adds new entry if
      written after snapshot. SIEVE entry inserted in replay order.
   c. For each DELETE: DelIndex(key) — removes snapshot entry.
   d. After replay, update stats counters for delta entries.

5. Skip RecomputeStats entirely if:
   - Snapshot was valid AND
   - WAL replay succeeded (rErr == nil) AND
   - All delta PUTs have size (v2 WAL or snapshot already has size).

6. If WAL replay fails but snapshot was valid:
   - Snapshot gives us the base index with sizes.
   - WAL failure means we may have missed recent writes.
   - Log a warning. The snapshot is a consistent point-in-time
     checkpoint; missing a few seconds of writes is acceptable
     (the origin will re-cache on miss). No RecomputeStats needed
     (snapshot has sizes).

7. If snapshot is missing or invalid:
   - Fall back to the existing path: WAL replay + RecomputeStats
     (or segment scan if WAL is also empty).
   - This is the cold-start path.
```

**Startup time budget:**
- Segment discovery (ReadDir): ~1 ms.
- Snapshot mmap + validate: ~1 ms (header + footer CRC + segment table
  comparison).
- Map build (10M entries): ~15-30 ms (memory bandwidth limited).
- WAL delta replay (target <100K entries): ~5-20 ms.
- Total: <100 ms for 10M keys. vs. current: 30-300 seconds.

### SIEVE eviction list rebuild

**[review-fix taste-9]** The review calls out that key-order SIEVE
rebuild produces poor eviction quality until the list converges, and
that the "converges within a few thousand requests" claim is
unmeasured. The fix uses WAL replay order for delta entries:

- **Snapshot entries** (step 3b): inserted in key order. Suboptimal
  aging, but these are the base set. All start with `visited=false`.
- **WAL delta entries** (step 4b): inserted in WAL replay order
  (append order = insertion order). Correct aging for recent writes.
  `SetIndex`/`SetIndexWithSize` already inserts into the SIEVE list
  via `evictList.Access` — no change needed.

**Why this is good enough:**
- The snapshot is written periodically (default 5 min). The WAL delta
  is typically <100K entries (writes in 5 min at moderate load). The
  SIEVE list has the correct aging for these recent entries — the ones
  most likely to be evicted first.
- The older snapshot entries (millions) are in key order, but they're
  also the ones least likely to be evicted (they've survived at least
  one checkpoint interval). SIEVE's `visited` bit will be set by Gets
  before the eviction hand reaches them in most cases.
- If the warm tier is at capacity immediately after restart and
  eviction starts before the list converges, the worst case is
  evicting some entries that would have been kept with correct aging.
  These will be re-cached from origin on next access. This is a
  transient quality degradation, not a correctness issue.

**Future improvement (v2 snapshot format):** Add a `sieve_seq` field
to each entry (8 bytes, total entry size becomes 36 bytes). On load,
sort by `sieve_seq` before building the SIEVE list. This preserves
exact aging. Deferred until eviction quality after restart is a
measured problem.

### Snapshot write path

**When to write:**
1. **On clean shutdown** — `Store.Close()` calls `writeSnapshot()`
   before closing segment files. Captures exact state for next startup.
2. **After compaction** — `Compact()` replaces all segments and
   rewrites the WAL. The snapshot must be rewritten too (segIDs and
   offsets change).
3. **Periodically** — via `checkpointLoop` (see §3.3), every
   `checkpoint_interval` (default 5 min), as part of the WAL
   checkpoint operation.

**[review-fix BLOCKER-1]** The review catches that the original design
held `idxMu.RLock` for 400ms during the bufio write, blocking all Puts
and Deletes. The fix uses the same pattern as `Compact()` (warm.go:1253):
copy the index under RLock (~50ms), release the lock, iterate the copy
lock-free.

```go
func (s *Store) writeSnapshot() error {
    // Take a consistent copy of the index under RLock.
    // This blocks writers for ~50ms (10M entries at memory bandwidth),
    // not 400ms. Readers (Get) are unaffected (shared RLock).
    s.idxMu.RLock()
    idxSnap := make(map[uint64]warmLoc, len(s.index))
    for k, v := range s.index {
        idxSnap[k] = v
    }
    s.idxMu.RUnlock()

    // Build the segment table from idxSnap (deduplicate segIDs).
    segSizes := make(map[int]int64)
    for _, loc := range idxSnap {
        if seg, ok := s.segByID[loc.segID]; ok {
            segSizes[loc.segID] = seg.size // current file size
        }
    }

    // Sort segment IDs for deterministic output.
    segIDs := make([]int, 0, len(segSizes))
    for id := range segSizes {
        segIDs = append(segIDs, id)
    }
    slices.Sort(segIDs)

    // Build sorted entry slice from the lock-free copy.
    entries := make([]snapEntry, 0, len(idxSnap))
    var totalBytes int64
    for key, loc := range idxSnap {
        entries = append(entries, snapEntry{
            key:    key,
            segID:  loc.segID,
            offset: loc.offset,
            size:   loc.size,
        })
        totalBytes += loc.size
    }
    slices.SortFunc(entries, func(a, b snapEntry) int {
        return cmp.Compare(a.key, b.key)
    })

    // Write to tmp file, fsync, rename — all lock-free.
    tmpPath := filepath.Join(s.dir, "index.snap.tmp")
    f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
    if err != nil { return err }
    defer func() { _ = f.Close() }()

    // Write header.
    hdr := make([]byte, 32)
    binary.LittleEndian.PutUint32(hdr[0:4], snapMagic)
    binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
    binary.LittleEndian.PutUint64(hdr[8:16], uint64(len(entries)))
    binary.LittleEndian.PutUint64(hdr[16:24], uint64(totalBytes))
    binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(segIDs)))
    binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(hdr[0:28], crcTable))
    if _, err := f.Write(hdr); err != nil { return err }

    // Write segment table.
    segTbl := make([]byte, 8*len(segIDs))
    for i, id := range segIDs {
        binary.LittleEndian.PutUint32(segTbl[i*8:i*8+4], uint32(id))
        binary.LittleEndian.PutUint32(segTbl[i*8+4:i*8+8], uint32(segSizes[id]))
    }
    if _, err := f.Write(segTbl); err != nil { return err }

    // Write entries.
    entryBuf := make([]byte, snapEntryLen) // 28 bytes, reused
    for _, e := range entries {
        encodeSnapEntry(entryBuf, e)
        if _, err := f.Write(entryBuf); err != nil { return err }
    }

    // Compute footer CRC over segment-table + entry bytes.
    // (Re-read from file or compute incrementally — see implementation.)
    // Write footer.
    // ... f.Sync() ...
    // ... f.Close() ...
    // ... os.Rename(tmpPath, filepath.Join(s.dir, "index.snap")) ...
    return nil
}
```

**Memory cost:** `idxSnap` (~100-150 MB per 1M keys) + `entries` slice
(~28 MB per 1M keys). At 10M keys: ~1.3 GB + ~280 MB = ~1.6 GB
transient. Under GOMEMLIMIT=6GiB with 8Gi container, this triggers GC
but is within bounds. GC reclaims both immediately after the function
returns.

**[review-fix BLOCKER-1 alt]** For extremely large indexes (50M+ keys)
where 1.6 GB transient is too much, use a batched copy with WAL
consistency:

```go
// Batched: copy 100K entries under RLock, release, write batch,
// re-acquire RLock for next batch. The snapshot may be slightly
// inconsistent (a Put between batches could be missed or duplicated),
// but WAL replay on top of the snapshot corrects it — the WAL is
// not truncated until after the snapshot is written (see §3.3).
const batchSize = 100_000
```

**Default: use the full-copy approach.** It's simpler, correct, and
the memory cost is acceptable for the default warm-tier sizes. The
batched approach is a documented escape hatch for 50M+ key tiers.

### Interaction with compaction

Compaction changes all segIDs and offsets. The snapshot must be
rewritten after compaction. The sequence in `compactLoop` becomes:

```
1. t.warm.Compact()           — rewrites segments, replaces index
2. t.rewriteWAL()             — rewrites WAL from new index
3. t.warm.writeSnapshot()     — writes snapshot from new index
```

**[review-fix BLOCKER-3]** If step 3 fails, the snapshot references
old segIDs. On restart, the segment table validation (step 2e in the
startup path) detects the mismatch: the snapshot's segment IDs won't
match the segment files on disk (compaction renamed them). The
snapshot is discarded and the startup falls back to WAL replay +
RecomputeStats. The WAL was rewritten in step 2 with the correct
segIDs, so this fallback is correct.

### Interaction with `Close`

**[review-fix]** `Store.Close()` writes a final snapshot before closing
segment files. This ensures the next startup uses the fast path.

```go
func (s *Store) Close() error {
    // Write final snapshot for next startup.
    if err := s.writeSnapshot(); err != nil {
        // Log but don't fail close — the WAL + segment scan fallback
        // still works on next startup.
        s.metrics.IncSnapshotWriteFailed()
    }
    s.mu.Lock()
    defer s.mu.Unlock()
    // ... existing close logic (sync + close segment files) ...
}
```

---

## 3.2 Lazy Segment Loading

### Problem

`openExisting()` opens all segment files at startup. With 100+
segments, this holds 100+ FDs for the process lifetime. With the
snapshot, we no longer scan segments on startup — FDs are only needed
for `Get()` (pread) and `Put()` (append to active segment).

### Design

**[review-fix bug-4, bug-5, bug-6, bug-7]** The review catches four
integration issues: `Scan` needs `ensureOpen`, `Compact` closes nil
FDs, `Get` needs `ensureOpen` before `readRecordAt`, and LRU eviction
races with in-flight reads. All addressed below.

```go
type Segment struct {
    ID       int
    Path     string
    mu       sync.Mutex
    f        *os.File      // nil until first access
    size     int64         // 0 until first open
    maxBytes int64
    opened   atomic.Bool
    readers  atomic.Int32  // [review-fix bug-7] in-flight read count
}

// ensureOpen opens the segment file if not already open.
// Must be called while s.mu (Store-level) is held to prevent
// Compact from swapping the segment set mid-open.
func (seg *Segment) ensureOpen() error {
    if seg.opened.Load() {
        return nil
    }
    seg.mu.Lock()
    defer seg.mu.Unlock()
    if seg.f != nil { // double-check under lock
        return nil
    }
    f, err := os.OpenFile(seg.Path, os.O_CREATE|os.O_RDWR, 0o600)
    if err != nil {
        return fmt.Errorf("warm: open %s: %w", seg.Path, err)
    }
    info, err := f.Stat()
    if err != nil {
        _ = f.Close()
        return fmt.Errorf("warm: stat %s: %w", seg.Path, err)
    }
    seg.f = f
    seg.size = info.Size()
    seg.opened.Store(true)
    return nil
}

// Close closes the segment file if open. Safe to call on unopened
// segments. [review-fix bug-5]
func (seg *Segment) Close() error {
    seg.mu.Lock()
    defer seg.mu.Unlock()
    if seg.f == nil {
        return nil
    }
    err := seg.f.Close()
    seg.f = nil
    seg.opened.Store(false)
    return err
}
```

**Startup:** `openExisting()` discovers segment files by name
(ReadDir), creates `Segment` structs with `f = nil`, and inserts them
into `segByID`. No file is opened. `nextID` is set to max+1.

**`Get` path [review-fix bug-6]:**

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

    // Ensure the segment file is open before reading.
    if err := seg.ensureOpen(); err != nil {
        return nil, fmt.Errorf("warm: open segment %d: %w", seg.ID, err)
    }

    // [review-fix bug-7] Track in-flight reader to prevent
    // LRU eviction from closing the FD mid-read.
    seg.readers.Add(1)
    defer seg.readers.Add(-1)

    rec, err := readRecordAt(seg.f, loc.offset, loc.segID)
    // ... rest unchanged (SIEVE visited bit, return) ...
}
```

**`Put` path (active segment):**
`activeSeg()` returns the last segment. `seg.ensureOpen()` is called
before writing. If the segment is full, `newSegment()` creates a new
`Segment` with `f = nil` and calls `ensureOpen()` immediately (we need
to write to it).

**`Scan` path (fallback) [review-fix bug-4]:**

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

**`Compact` path [review-fix bug-5]:**

In `Compact()` (warm.go:1302), replace:
```go
for _, seg := range s.segs {
    _ = seg.f.Close()
}
```
with:
```go
for _, seg := range s.segs {
    _ = seg.Close() // safe for nil f
}
```

`Compact` also calls `NewStore(dir)` which calls `openExisting()`. With
lazy loading, `openExisting` creates `Segment` structs with `f = nil`.
`Compact`'s `compactSegments` function accesses segments via `Scan`,
which calls `ensureOpen` (see above). The fresh store returned by
`NewStore(dir)` also has lazy segments — they'll be opened on demand by
traffic after compaction completes.

### LRU FD cache

**[review-fix bug-7]** The review catches that LRU eviction can close
an FD out from under an in-flight `readRecordAt`. Fix: per-segment
reader count, eviction skips segments with `readers > 0`.

```go
type segmentCache struct {
    mu      sync.Mutex
    maxOpen int
    openCnt atomic.Int32
    lru     *list.List // LRU of open segments, front = most recent
}

func (c *segmentCache) onOpen(seg *Segment) {
    c.openCnt.Add(1)
    c.mu.Lock()
    c.lru.PushFront(seg)
    if c.openCnt.Load() > int32(c.maxOpen) {
        c.evictOne()
    }
    c.mu.Unlock()
}

func (c *segmentCache) evictOne() {
    // Walk from back (least recently used). Skip segments with
    // in-flight readers or segments that are the active append target.
    for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
        seg := elem.Value.(*Segment)
        if seg.readers.Load() > 0 {
            continue // in-flight read, can't close
        }
        if isActive(seg) {
            continue // active append target, can't close
        }
        _ = seg.Close()
        c.lru.Remove(elem)
        c.openCnt.Add(-1)
        return
    }
    // All segments have readers or are active — skip eviction.
}
```

**`Get` touches the LRU** (moves segment to front) after `ensureOpen`:
```go
if s.segCache != nil {
    s.segCache.touch(seg)
}
```

### FD limit and default cache size

**[review-fix nit-10]** The review calls the default 128 arbitrary.
Scale with warm-tier size:

```go
func defaultSegmentCacheSize(segCount int) int {
    return min(segCount, 256)
}
```

With 16 segments (1 GiB tier): all open (16 FDs). With 320 segments
(20 GiB tier): 256 open (256 FDs). With 1600 segments (100 GiB tier):
256 open (16% — 84% of Gets trigger an open, ~50 µs each, negligible
under normal request rates).

Kubernetes default `ulimit -n` is 1048576. 256 segment FDs + listener
FDs + admin + cluster ≈ 300 total — well within limits.

### Config

```yaml
storage:
  segment_cache_size: 0  # 0 = auto (min(segCount, 256))
```

---

## 3.3 WAL Checkpointing

### Problem

The WAL grows indefinitely between compactions (30 min default). With
high write throughput, the WAL can accumulate millions of entries,
making delta replay slow even with the snapshot.

### Checkpoint = snapshot write + WAL truncate

**[review-fix BLOCKER-2]** The review catches a data-loss bug: if
entries are written to the WAL between the snapshot write and the WAL
truncate, those entries are lost on restart (snapshot doesn't have
them, WAL was truncated). The fix blocks WAL writes during the
truncate window and uses the index copy (taken before the block) as
the snapshot source. The key insight: the `Put` path always updates
the index *before* enqueuing to the WAL, so the index copy is a
superset of what's in the WAL.

### Checkpoint sequence (crash-safe)

```go
func (t *TieredStore) checkpoint() error {
    // Step 1: Flush all pending WAL entries to disk.
    t.walMu.Lock()
    if t.wal != nil {
        t.wal.Sync()
    }
    t.walMu.Unlock()

    // Step 2: Take a consistent copy of the index under idxMu.RLock.
    // Writers block for ~50ms (10M entries). Readers unaffected.
    t.warm.idxMu.RLock()
    idxSnap := make(map[uint64]warmLoc, len(t.warm.index))
    for k, v := range t.warm.index {
        idxSnap[k] = v
    }
    t.warm.idxMu.RUnlock()

    // Step 3: Block WAL writes. Any Enqueue call now spins until
    // the block is released. This is ~1ms of spin time.
    t.checkpointing.Store(true)

    // Step 4: Flush any WAL entries written between step 2 and step 3.
    // These entries' index updates are in idxSnap (Put updates index
    // before Enqueue). The WAL flush ensures they're on disk before
    // truncate.
    t.walMu.Lock()
    if t.wal != nil {
        t.wal.Sync()
    }
    t.walMu.Unlock()

    // Step 5: Truncate the WAL. All entries up to this point are
    // either in idxSnap or were applied to the index before the copy.
    t.walMu.Lock()
    if t.wal != nil {
        if err := t.wal.Truncate(); err != nil {
            t.walMu.Unlock()
            t.checkpointing.Store(false)
            return fmt.Errorf("checkpoint: truncate wal: %w", err)
        }
        t.walEntryCount.Store(0)
    }
    t.walMu.Unlock()

    // Step 6: Unblock WAL writes. New entries go into the fresh WAL.
    t.checkpointing.Store(false)

    // Step 7: Write the snapshot from idxSnap (lock-free).
    // This takes ~300ms for 10M entries (I/O bound) but writers
    // are already unblocked. The snapshot is written to index.snap.tmp,
    // fsynced, and renamed atomically.
    if err := t.warm.writeSnapshotFromCopy(idxSnap); err != nil {
        return fmt.Errorf("checkpoint: snapshot: %w", err)
    }

    return nil
}
```

**Why this is crash-safe:**

| Crash point | Snapshot state | WAL state | Restart recovery |
|-------------|---------------|-----------|-----------------|
| Before step 2 | Old or missing | Intact | Old snapshot + full WAL replay, or WAL replay + RecomputeStats |
| During step 2 (copy) | Old or missing | Intact | Same as above |
| After step 2, before step 3 | Old or missing | Intact (synced) | Same — the copy is in memory, lost on crash |
| During step 4 (second Sync) | Old or missing | Intact | Same |
| During step 5 (Truncate) | Old or missing | Partially truncated | Old snapshot (if any) + WAL replay stops at corruption. Some recent writes lost. Acceptable — re-cached from origin. |
| After step 5, before step 6 | Old or missing | Empty | Old snapshot + empty WAL = correct state minus writes during checkpoint. But those writes' index updates are lost too (process crashed). On restart, snapshot is old, WAL is empty. The lost writes are the ones between the copy (step 2) and the crash. These are lost in both the index and the WAL. Acceptable. |
| After step 6, before step 7 | Old or missing | Empty (new writes arriving) | Old snapshot + WAL replay (new writes). The snapshot from step 7 hasn't been written yet. The old snapshot + new WAL entries = correct state (new entries overwrite/extend old snapshot). |
| During step 7 (snapshot write) | Old (rename hasn't happened) | Empty + new writes | Old snapshot + WAL replay = correct. |
| After step 7 (rename done) | New (current) | Empty + new writes | New snapshot + WAL replay (new writes) = correct. |

**WAL `Enqueue` blocking:**

```go
func (t *TieredStore) walEnqueue(entry wal.Entry) {
    for t.checkpointing.Load() {
        runtime.Gosched() // spin until checkpoint unblocks
    }
    t.walMu.Lock()
    defer t.walMu.Unlock()
    if t.wal != nil {
        t.wal.Enqueue(entry)
        t.walEntryCount.Add(1)
    }
}
```

The spin window is steps 3-6: the second `Sync` (~1ms) + `Truncate`
(~1ms) = ~2ms. This is a busy-wait, not a condition variable, because
the checkpoint window is too short to justify the overhead of
`sync.Cond`. At 50K req/s with 30% write ratio, ~30 writes spin for
~2ms each — negligible.

### Checkpoint trigger

```go
type TieredStore struct {
    // ... existing fields ...
    checkpointInterval    time.Duration // default: 5m
    checkpointWALThreshold int          // default: 100_000
    checkpointing         atomic.Bool
    walEntryCount         atomic.Int64
    checkpointWg          sync.WaitGroup
}
```

A checkpoint runs when either:
- `checkpointInterval` has elapsed since the last checkpoint, OR
- `walEntryCount` exceeds `checkpointWALThreshold`.

### Checkpoint goroutine

```go
func (t *TieredStore) checkpointLoop() {
    defer t.checkpointWg.Done()
    ticker := time.NewTicker(t.checkpointInterval)
    defer ticker.Stop()
    for {
        select {
        case <-t.done:
            return
        case <-ticker.C:
            if t.walEntryCount.Load() > 0 {
                if err := t.checkpoint(); err != nil {
                    t.logger.Warn("checkpoint failed", "error", err)
                }
            }
        }
    }
}
```

**Interaction with compaction:** Compaction calls `rewriteWAL()` +
`writeSnapshot()` after `Compact()`. The checkpoint loop should skip if
compaction is in progress. Use the existing `s.mu` (segment mutex) as
the compaction indicator — if `s.mu.TryLock()` fails, compaction is
running, skip the checkpoint.

### Config

```yaml
storage:
  checkpoint_interval: 5m          # default
  checkpoint_wal_threshold: 100000 # default
```

---

## 3.4 Integration: New Startup Path

### `initWAL` with snapshot + WAL checkpoint

```go
func (t *TieredStore) initWAL(walDir string) error {
    t.walPath = walDir
    l, err := wal.OpenAsync(walDir, t.walSyncInterval)
    if err != nil { return err }
    t.wal = l
    if t.warm == nil { return nil }

    // Try to load the index snapshot first.
    snapshotLoaded := false
    if snapPath := t.warm.SnapshotPath(); snapPath != "" {
        if err := t.warm.LoadSnapshot(snapPath); err != nil {
            t.logger.Warn("index snapshot load failed; falling back to WAL replay",
                "error", err)
        } else {
            snapshotLoaded = true
            t.logger.Info("index snapshot loaded",
                "entries", t.warm.IndexLen(),
                "bytes", t.warm.Stats().Bytes)
        }
    }

    // Replay WAL on top of the snapshot (or from scratch if no snapshot).
    rErr := wal.Replay(walDir, func(e wal.Entry) error {
        switch {
        case e.IsPut():
            if e.HasSize() {
                t.warm.SetIndexWithSize(e.Key, int(e.SegID), e.Offset, e.Size)
            } else {
                t.warm.SetIndex(e.Key, int(e.SegID), e.Offset)
            }
        case e.IsDelete():
            t.warm.DelIndex(e.Key)
        }
        return nil
    })

    if rErr != nil {
        t.logger.Warn("wal replay failed", "error", rErr)
    }

    if !snapshotLoaded {
        // No snapshot — may need full scan if WAL was empty/corrupt.
        needRebuild := rErr != nil || t.warm.IndexLen() == 0
        if needRebuild {
            t.logger.Warn("rebuilding index from segment scan")
            if err := t.rebuildIndexFromScan(); err != nil {
                t.logger.Warn("segment scan failed", "error", err)
            }
        }
        // Always run RecomputeStats when no snapshot (sizes may be 0).
        if err := t.warm.RecomputeStats(); err != nil {
            t.logger.Warn("recompute stats failed", "error", err)
        }
    } else if rErr != nil {
        // Snapshot loaded but WAL replay failed. The snapshot has
        // correct sizes, so no RecomputeStats needed. We may have
        // missed recent WAL writes — log and continue.
        t.logger.Warn("wal replay failed after snapshot; recent writes may be lost",
            "error", rErr)
    }

    // Track WAL entry count for checkpoint threshold.
    t.walEntryCount.Store(0)

    return nil
}
```

### `NewTieredStore` with snapshot + checkpoint + lazy segments

```go
func NewTieredStore(cfg TieredConfig) (*TieredStore, error) {
    // ... existing setup ...

    if cfg.Warm != nil {
        if err := ts.initWarm(cfg.Warm, cfg.WarmMetrics); err != nil {
            return nil, err
        }
    }

    // compactLoop — delayed first check (Phase 2 item 2.6)
    if ts.warm != nil {
        ts.compactWg.Add(1)
        go ts.compactLoop()
    }

    if cfg.WALDir != "" {
        if err := ts.initWAL(cfg.WALDir); err != nil {
            return nil, err
        }
    }

    // warmSyncLoop — existing
    if ts.warm != nil && warmSyncInterval > 0 {
        ts.syncWg.Add(1)
        go ts.warmSyncLoop()
    }

    // checkpointLoop — NEW
    if ts.warm != nil && ts.wal != nil && ts.checkpointInterval > 0 {
        ts.checkpointWg.Add(1)
        go ts.checkpointLoop()
    }

    return ts, nil
}
```

### `Close` with snapshot + checkpoint shutdown

```go
func (t *TieredStore) Close(ctx context.Context) error {
    close(t.done)
    t.compactWg.Wait()
    t.syncWg.Wait()
    t.checkpointWg.Wait()

    t.walMu.Lock()
    if t.wal != nil {
        t.wal.Sync()
        t.wal.Close()
    }
    t.walMu.Unlock()

    if t.warm != nil {
        // Write a final snapshot for the next startup.
        if err := t.warm.writeSnapshot(); err != nil {
            t.logger.Warn("final snapshot write failed", "error", err)
        }
        t.warm.Close()
    }
    return t.hot.Close(ctx)
}
```

---

## 3.5 Failure Modes and Resilience

### Snapshot corruption

**Cause:** Disk error, partial write (crash mid-rename), filesystem bug.

**Detection:** Header magic mismatch, version mismatch, header CRC
mismatch, footer CRC mismatch, segment table seg_id missing from disk,
segment table seg_size mismatch with actual file size.

**Recovery:** Discard the snapshot, delete the file, fall back to WAL
replay + RecomputeStats (existing path). Log a warning with corruption
details.

### Snapshot references missing segments

**Cause:** Compaction completed (segments swapped) but snapshot write
failed.

**Detection:** [review-fix BLOCKER-3] Segment table in snapshot header
is compared against `openExisting`'s discovered files. O(S) check, not
O(N).

**Recovery:** Fall back to WAL replay + RecomputeStats. The WAL was
rewritten by `rewriteWAL()` after compaction with correct segIDs.

### WAL corruption after snapshot

**Cause:** Disk error, crash mid-write.

**Detection:** `wal.Replay()` returns error (CRC mismatch).

**Recovery:** If snapshot was loaded, the base index is correct with
sizes. WAL failure means recent writes may be lost. Log warning. No
RecomputeStats needed (snapshot has sizes). Missing entries re-cached
from origin on miss.

### Snapshot + WAL both missing/corrupt

**Recovery:** `rebuildIndexFromScan()` — full segment scan. Always
works if segment files are intact.

### Process crash during checkpoint

See the crash-safety table in §3.3. Every crash point is analyzed. The
worst case is losing writes between the index copy (step 2) and the
crash — but those writes are lost in both the index and the WAL, so
there's no inconsistency. The origin re-caches on miss.

### OOMKill during snapshot write

**[review-fix BLOCKER-1]** The full-copy approach uses ~1.6 GB
transient at 10M keys. Under GOMEMLIMIT=6GiB with 8Gi container, this
triggers GC but is within bounds. For 50M+ keys, use the batched copy
approach (documented in §3.1) — the WAL is the consistency anchor, so
a slightly inconsistent snapshot is corrected by WAL replay on
restart.

---

## 3.6 Performance Summary

| Operation | Current | Phase 3 | Improvement |
|-----------|---------|---------|-------------|
| Startup (10M keys) | 30-300s | <100ms | 300-3000× |
| Startup (100M keys) | 300-3000s | <1s | 300-3000× |
| Snapshot write (10M keys) | N/A | ~50ms lock hold + ~300ms I/O | New |
| Checkpoint (snapshot + WAL truncate) | N/A | ~52ms write block + ~300ms I/O | New |
| WAL replay (post-checkpoint) | O(total writes since compaction) | O(writes since checkpoint) | Bounded by checkpoint interval |
| Segment open at startup | All (100+) | 0 (lazy) | 100% reduction |
| Memory at startup | index + idxSnap copy (2× index) | index only (from snapshot) | 50% reduction |
| Writer block during checkpoint | N/A | ~50ms (index copy) + ~2ms (WAL block) | Acceptable under load |

---

## 3.7 Config Additions

```yaml
storage:
  # Index snapshot for fast startup. Written to {warm_dir}/index.snap.
  # On restart, the snapshot is mmap'd and the in-memory index is built
  # from it, skipping the WAL replay + RecomputeStats scan.
  snapshot_enabled: true

  # How often to write a fresh snapshot + truncate the WAL.
  # Shorter interval = faster restart but more background I/O.
  checkpoint_interval: 5m

  # Checkpoint when the WAL exceeds this many entries, regardless of
  # the interval. Bounds WAL replay time on unclean restart.
  checkpoint_wal_threshold: 100000

  # Maximum concurrently open segment file descriptors.
  # 0 = auto (min(segment_count, 256)).
  # Each open segment uses 1 FD. 256 open segments = 256 FDs +
  # ~256 × segMax (64MiB) kernel page cache if mmap'd.
  segment_cache_size: 0
```

---

## 3.8 Testing Plan

### Unit tests

- **Snapshot write + load round-trip:** Write 10K entries to warm store,
  write snapshot, close store, reopen, load snapshot. Verify all keys
  present with correct segID, offset, size. Verify stats counters match.
  Verify segment table matches actual files.
- **Snapshot corruption recovery:** Corrupt header magic, footer CRC,
  entry data, segment table. Verify `LoadSnapshot` returns error and
  startup falls back to WAL replay + RecomputeStats.
- **Snapshot with missing segments:** Write snapshot, delete a segment
  file, reload. Verify snapshot is discarded via segment table
  validation, fallback to WAL replay.
- **Snapshot with wrong segment size:** Write snapshot, truncate a
  segment file, reload. Verify segment table size mismatch is detected,
  snapshot discarded.
- **Snapshot + WAL delta:** Write snapshot, perform 100 Put/Delete
  operations (enqueued to WAL), close without checkpoint, reopen.
  Verify snapshot loaded + WAL replayed = correct final state.
- **Checkpoint crash safety:** Simulate crash at each step of the
  checkpoint sequence (before snapshot, during truncate, after
  truncate, during snapshot write). Verify restart produces correct
  state in all cases.
- **Checkpoint blocks WAL writes:** Start checkpoint, attempt WAL
  Enqueue during the block window. Verify Enqueue spins and resumes
  after checkpoint unblocks. Verify no entries are lost.
- **Lazy segment loading:** Create 10 segments, write entries to all,
  close store, reopen with lazy loading. Verify no segments are open
  initially (`opened == false` for all). Verify first Get opens the
  correct segment. Verify LRU eviction closes old segments when cache
  is full. Verify evicted segment is reopened on next access.
- **LRU eviction safety:** Start 200 Gets on 200 different segments
  concurrently with cache size 128. Verify no `readRecordAt` errors
  (FD not closed mid-read). Verify `readers` count prevents eviction
  of in-use segments.
- **SIEVE rebuild from snapshot:** Load snapshot, verify SIEVE list has
  all entries with `visited=false`. Perform Gets on a subset, verify
  SIEVE visited bits are set. Trigger eviction, verify it evicts
  unvisited entries.
- **SIEVE ordering with WAL delta:** Load snapshot (key-order SIEVE),
  replay WAL delta (insertion-order SIEVE). Verify delta entries are
  at the head of the SIEVE list, snapshot entries are in key order
  behind them.
- **Snapshot write under concurrent writes:** Start 100 goroutines
  doing Put/Delete, trigger snapshot write. Verify snapshot is
  consistent (CRC valid, segment table matches). Verify all entries
  in snapshot exist in the live index.
- **Compact + snapshot interaction:** Load 1M keys, trigger
  compaction. Verify snapshot is rewritten after compaction with new
  segIDs. Verify segment table in new snapshot matches the
  post-compaction segment files.
- **`Segment.Close` on unopened segment:** Create Segment with `f=nil`,
  call `Close()`. Verify no panic, returns nil.
- **`Scan` with lazy segments:** Create 10 segments with `f=nil`,
  call `Scan`. Verify all segments are opened via `ensureOpen`, scan
  completes, all records returned.

### Integration tests

- **Cold start → warm start cycle:** Deploy fresh (no snapshot), load 1M
  keys, shut down cleanly (snapshot written). Restart (snapshot loaded).
  Verify startup time < 2s. Verify all keys accessible.
- **Crash recovery:** Load 1M keys, kill process (no snapshot on
  unclean shutdown). Restart (WAL replay + RecomputeStats). Verify all
  keys present. Wait for checkpoint (snapshot written). Restart again
  (snapshot loaded). Verify startup time < 2s.
- **Compaction + snapshot:** Load 1M keys, trigger compaction (segments
  rewritten), verify snapshot is rewritten after compaction. Restart,
  verify snapshot loaded correctly with new segIDs.
- **Checkpoint under load:** Run 10K req/s write throughput, trigger
  checkpoint. Verify no request latency spike > 100ms (writer block is
  ~52ms). Verify WAL is truncated after checkpoint. Verify all writes
  during checkpoint are present in the index after checkpoint completes.

### Benchmark

- **BenchmarkSnapshotLoad:** Load snapshot with N entries (1K, 100K,
  1M, 10M). Measure mmap + validate + map build time. Assert
  O(N) linear, <50ms per 1M entries.
- **BenchmarkSnapshotWrite:** Write snapshot with N entries. Measure
  lock hold time (idxMu.RLock duration) + total write time. Assert
  lock hold <100ms per 10M entries, total write <500ms per 10M entries.
- **BenchmarkCheckpointUnderLoad:** Checkpoint while running 10K ops/s.
  Measure p99 latency during checkpoint. Assert p99 < 100ms (writer
  block ~52ms + normal latency).
- **BenchmarkLazySegmentOpen:** First Get after restart with N segments.
  Measure segment open time. Assert <100µs per open. Measure hit rate
  of `ensureOpen` fast path (opened.Load() == true) after warmup.
- **BenchmarkWALReplayWithSnapshot:** Replay WAL with N delta entries
  on top of a loaded snapshot. Measure replay time. Assert <20ms per
  100K entries.
