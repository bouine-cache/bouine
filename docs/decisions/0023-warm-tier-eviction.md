# ADR-0023: Warm-tier eviction (SIEVE)

- **Status**: Accepted
- **Date**: 2026-07-07
- **Deciders**: @thylong
- **Phase**: phase 2
- **Consulted**: (none)
- **Informed**: (none)

## Context

The warm tier (L1) is append-only segment-based storage. Until this ADR it
had no eviction algorithm — when `MaxBytes` was exceeded, `Put` rejected
with `ErrOverBudget` and the only way to reclaim space was compaction
(which drops tombstones and superseded keys, not live entries).

This is insufficient under sustained write pressure with live data: if the
disk fills with live entries that are rarely accessed, there is no
mechanism to reclaim their space. The hot tier (`HotStore`) uses SIEVE
eviction (`internal/storage/sieve`), and the warm tier now reuses the same
SIEVE implementation — the index is a `map[uint64]warmLoc` that can embed
a `*sieve.Entry[uint64]` pointer alongside the segment location.

Issue #204 requires: evict live entries when over budget, tombstone
evicted keys, clear hot-tier `hasBackup`, and avoid evicting entries that
are also in the hot tier.

Code-symbol mapping (see plan `docs/plans/decouple-hot-warm-naming.md`):
`Protect` = "mark as hot-resident", `ClearBacked` = "clear warm-backup flag".
This ADR keeps the architectural terms ("hot tier" / "warm tier") when
describing the system, and uses the current code names (`protected`,
`Protect`, `hasBackup`, `ClearBacked`) when naming fields or methods.

The warm tier is expected to hold several million keys at steady state.
This rules out an O(n) timestamp-scan LRU: scanning millions of map
entries under `idxMu.Lock` on every over-budget `Put` would block all
`Get` and `Put` operations for milliseconds per eviction, and
`evictToFit` may need multiple evictions in a loop.

## Decision

We use **SIEVE** for warm-tier eviction, reusing `internal/storage/sieve`
— the same algorithm the hot tier uses — because:

1. **O(1) eviction and access tracking.** SIEVE's hand sweeps the list,
   evicting unvisited entries and clearing visited bits for second
   chances. `Get` sets the visited bit atomically (via
   `Entry.MarkVisited()`) under `idxMu.RLock` — no write lock needed.
   `Put` inserts at the head in O(1). `Evict` is O(1) amortized. This
   scales to millions of entries without the O(n) scan of a
   timestamp-based LRU.

2. **No lock upgrade on `Get`.** The previous LRU-by-timestamp approach
   required `idxMu.Lock` to update `lastAccess` on every warm `Get`. With
   SIEVE, `Get` sets the visited bit via `atomic.Bool.Store` under
   `idxMu.RLock` — the read lock is never upgraded to a write lock. This
   reduces contention on the warm-tier read path.

3. **Low per-entry overhead.** Each warm entry stores a
   `*sieve.Entry[uint64]` (8-byte pointer in `warmLoc`). The SIEVE
   entries themselves are pooled (`sync.Pool`), so allocation is amortized.
   The `visited` field is an `atomic.Bool` (8 bytes with padding). Total
   per-entry overhead: ~32 bytes for the SIEVE entry + 8 bytes for the
   pointer in `warmLoc` = ~40 bytes per million entries (~40 MB at 1 M
   entries). Acceptable for disk-tier metadata.

4. **Consistency with the hot tier.** Both tiers use the same SIEVE
   implementation, reducing cognitive load and maintenance burden.

### Algorithm

- **Access tracking**: `Get` calls `Entry.MarkVisited()` (atomic store on
  `visited.Bool`) under `idxMu.RLock` after re-checking entry identity
  (segID + offset) to avoid marking a stale entry. No write lock needed.

- **Insert**: `Put` and `SetIndex` call `evictList.Access(key, lookup)`,
  which either sets `visited=true` on an existing entry (overwrite) or
  inserts a new entry at the head with `visited=false`.

- **Evict()**: `evictOne()` calls `evictList.Evict()` to get a
  candidate (O(1) amortized). If the candidate is protected, the
  entry is re-inserted at the head with `visited=true` (second chance)
  and the loop continues. The scan is bounded by `maxWarmEvictSkips`
  (16) so the worst case is O(1) under `idxMu` even when most entries
  are protected (the steady state once the warm sync loop has
  marked the majority). If no non-protected victim is found within
  the skip budget, `evictToFit` rejects the `Put` with `ErrOverBudget`
  rather than scanning the whole list. There is no fallback to evicting
  protected entries on the `Put` path — evicting a protected warm
  entry is wasteful because the hot tier will re-sync it on the next
  warm sync cycle.

  Both `seg.mu` (active segment) and `idxMu` are held across the
  tombstone write, the index removal, AND the `OnEvict` callback. This
  closes the TOCTOU window where a concurrent `Put` for the victim key
  could insert a new live record between the index removal and the
  tombstone append — which would have the tombstone clobber the fresh
  data. With both locks held, the on-disk order is
  tombstone-then-any-future-live, which `rebuildIndexFromScan` honors on
  replay. Firing `OnEvict` under the locks also closes a second race:
  if the callback ran after unlock, a concurrent `Put` could re-insert
  the key and set `hasBackup=true` on the hot entry, only to have the
  late `ClearBacked` clobber it back to `false` — permanently stranding
  the warm entry as `protected=true` and un-evictable.

- **Put over budget**: `evictToFit` rejects early if `recSize > maxBytes`
  (a record larger than the entire budget can never fit). Otherwise it
  repeatedly calls `evictOne()` until `stats.bytes + recSize <= maxBytes`.
  Only non-protected entries are evicted on the Put path.

- **Protected flag**: `warmLoc.protected` marks entries that also
  live in the hot tier. Set by `TieredStore` via `Protect(key)`.
  There is deliberately no unprotect API — the flag is cleared only by
  deletion (hot-tier eviction tombstones the warm entry, or warm
  eviction removes the index entry). The SIEVE list does not know about
  protection — it is checked at the warm-store level after SIEVE
  selects a candidate.

- **Delete**: `Delete` and `DelIndex` remove the entry from both the
  index and the SIEVE list (`evictList.Remove(loc.sieve)`) under
  `idxMu.Lock`.

- **Compaction**: `Compact` rebuilds the SIEVE list from the compacted
  index, sorted by `(segID, offset)` to preserve the on-disk append
  order. Map iteration is randomized, so naively iterating the index
  would produce a random SIEVE tail — defeating the aging property where
  the tail holds the oldest entries. All entries start with
  `visited=false` — the first eviction sweep after compaction will
  prefer the tail (oldest) entries. `protected` is preserved from the
  pre-compaction index.

- **Callback**: `Store.OnEvict(uint64)` is called under `idxMu` and
  `seg.mu` after eviction. `TieredStore` wires this to
  `HotStore.ClearBacked(key)` (clears `hasBackup` so the hot tier stops
  preferring the entry for eviction) and enqueues a WAL delete entry
  for async persistence. Firing under the locks is safe because
  `ClearBacked` is O(1) and lock-only (no I/O), and no code path holds
  a hot shard lock while acquiring warm `idxMu`, so there is no
  AB-BA deadlock risk.

## Consequences

### Positive
- Reclaims space from live data under disk pressure.
- O(1) eviction and access tracking — scales to millions of entries.
  The skip scan is bounded by `maxWarmEvictSkips` (16), so even when all
  entries are protected the worst case is 16 SIEVE probes under
  `idxMu`, not an O(n) list scan.
- No write lock on `Get` — visited bit is set atomically under RLock.
- SIEVE entries are pooled, minimizing allocation.
- Consistent with hot-tier eviction algorithm.
- Protected flag prevents wasteful eviction of entries the hot tier
  will immediately re-sync.
- WAL delete entries ensure evictions survive restart (best-effort;
  see crash-durability risk above).

### Negative / trade-offs
- `*sieve.Entry` per warm entry: ~40 bytes per entry at scale (pointer in
  `warmLoc` + pooled entry struct). Acceptable for disk-tier metadata.
- SIEVE is an approximation of LRU, not exact LRU. The visited bit gives
  one second chance per sweep, not full recency tracking. For a disk tier
  that is rarely accessed, this approximation is sufficient.
- Compaction resets all visited bits. After compaction, the first
  eviction sweep prefers the oldest entries (tail of the rebuilt list).
  This is a minor efficiency loss, not a correctness issue — compaction
  is infrequent.
- Tombstones increase `diskBytes` — eviction doesn't shrink the segment
  files. The budget check in `evictToFit` uses `stats.bytes` (live bytes),
  not `diskBytes`. Compaction is still needed to reclaim dead space.

### Risks
- If `OnEvict` callback blocks, it stalls the `Put` path. Mitigated by the
  constraint (documented on `OnEvict`) that it must not block or do I/O —
  it only enqueues to a buffered channel and does an O(1) lock-only
  `ClearBacked`. The callback runs under `idxMu` and `seg.mu`; the extra
  hold time is one shard lock acquire.
- If the warm sync cycle hasn't marked an entry as `protected` yet, it
  may be evicted even though it's in the hot tier. This is a transient
  state — the entry will be re-synced to warm on the next cycle. Not a
  correctness issue, just a minor efficiency loss.
- **Crash durability**: the tombstone written by `evictOne` is in the
  segment page cache but is not fsynced before the callback returns.
  If the process crashes before the next warm sync cycle, both the
  tombstone and the enqueued WAL delete may be lost. This is acceptable:
  the resurrected entry is warm-only (the hot entry was cleared) and
  will be re-evicted on the next over-budget `Put`. The WAL delete is a
  fast-replay optimization; `rebuildIndexFromScan` honors tombstones in
  segment order when the WAL is missing.

## Alternatives considered

- **LRU by last-access timestamp (O(n) scan)**: rejected because the warm
  tier holds several million keys. An O(n) scan of the index under
  `idxMu.Lock` on every over-budget `Put` would block all reads and writes
  for milliseconds per eviction candidate. `evictToFit` may need multiple
  evictions, compounding the lock hold time. Additionally, updating
  `lastAccess` on every `Get` requires `idxMu.Lock` (write lock), causing
  contention on the warm read path. SIEVE's atomic visited bit avoids
  this entirely.

- **LRU with separate doubly-linked list**: a linked list keyed by access
  time, moved-to-head on `Get`. Rejected because it requires the same
  per-entry linked-list nodes as SIEVE but with more overhead on `Get`
  (must move the node to head under a write lock, not just set a bit).
  SIEVE is simpler and cheaper: `Get` only sets a bit, never moves a node.

- **TTL-based eviction**: evict entries past their `TTL + SWR + SIE`.
  Rejected because the warm tier doesn't store TTL metadata in the index
  (it's in the encoded object body). Reading the body on every eviction
  candidate would be I/O-bound. The hot tier's reaper already handles
  expired entries; the warm tier inherits this via the tombstone queue.

## References

- Issue #204: "feat(storage): implement warm-tier eviction (SIEVE/LRU for L1)"
- ADR-0020: hot-to-warm sync
- ADR-0021: refresh popularity gate
- `internal/storage/sieve/sieve.go` — SIEVE implementation (shared by hot
  and warm tiers)
- `internal/storage/warm/warm.go` — warm-tier eviction implementation
- Zhang et al., "SIEVE is Simpler than LRU: an Efficient Turn-Key Eviction
  Algorithm for Web Caches", NSDI 2024
