# Eviction Hardening & OOM Resilience Plan

> One-line summary: fix the O(N) SIEVE sweep, eliminate the 60 s allocation
> spike, batch warm eviction, cap warm index by GOMEMLIMIT, and parallelize
> bans — all benchmark-gated at 1 M entries before shipping.

## Motivation

This plan merges two independent analyses into a single execution plan:

1. **Eviction hardening** — under heavy read load with 1 M+ entries, SIEVE's
   `Evict()` degrades to O(N) worst case (`sieve.go:131`: `maxProbes =
   l.len * 2`), blocking all warm Gets for 10-20 ms per call. `Ban()` locks
   all shards sequentially for 1-3 s. Warm eviction acquires two locks per
   tombstone instead of batching.

2. **OOM resilience** — bouine pods are OOMKilled (exit 137) because RSS
   grows to 9-13 GiB with a 10-14 GiB limit. The warm index map lives
   entirely in Go heap with no cap (`warm_max_bytes` controls disk, not
   heap). At 10 M warm entries the index alone consumes ~1.5 GiB. The warm
   sync cycle's `collectHotOnlyKeys()` allocates ~500 MB every 60 s.

The two plans overlap on `collectHotOnlyKeys()`: the OOM plan proposes a
streaming-Lookup approach (eliminates the 480 MB `warmSet` map, keeps the
24 MB `hot.Keys()` call), while the eviction plan proposes incremental
per-shard `hotOnly` sets (zero allocation on the sync path). The
incremental approach is strictly better and supersedes the streaming
approach. The remaining OOM step (cap warm index) has no overlap and
slots in cleanly.

Every fix is gated by a benchmark at 1 M entries that must show
improvement before the code is merged. No benchmark = no fix.

---

## Phase 0 — Baseline benchmarks (prerequisite, no code changes)

Before changing anything, we need numbers. All claims in this plan are
hypotheses until measured.

### Tasks

0.1. **`BenchmarkSIEVE_Evict_AllVisited_1M`** — 1 M SIEVE entries, all with
  `visited=true`, measure single `Evict()` call ns/op. This proves or
  disproves the O(N) worst case. Put it in `internal/storage/sieve/`.

0.2. **`BenchmarkWarmEvict_AllVisited_1M`** — 1 M warm entries, all accessed
  via `Get` (visited=true), then `evictOne()` under budget pressure. Measure
  ns/op and allocs/op. Put it in `internal/storage/warm/`.

0.3. **`BenchmarkWarmSyncCycle_1M`** — 1 M hot entries + 1 M warm entries,
  measure `runWarmSyncCycle` ns/op and allocs/op. Put it in
  `internal/storage/`.

0.4. **`BenchmarkBan_1M`** — 1 M hot entries across 64 shards, ban with a
  non-matching regex, measure wall-clock time. Put it in
  `internal/storage/`.

0.5. **`BenchmarkWarmEvictToFit_MultiEvict`** — 10 K warm entries at budget,
  insert a record requiring 10 consecutive evictions, measure total
  `evictToFit` ns/op. Put it in `internal/storage/warm/`.

### Gate

These benchmarks must run on the current `main` branch and produce
reproducible numbers (N >= 10, `benchstat`) before any code changes. If a
benchmark does not show the predicted problem, the corresponding fix is
dropped or redesigned.

---

## Phase 1 — SIEVE sweep cap (fixes the O(N) worst case)

**Problem**: `sieve.List.Evict()` (`sieve.go:131`) bounds the scan at
`maxProbes = l.len * 2`. When all entries have `visited=true` (the steady
state under heavy read load), the hand sweeps the entire list clearing
visited bits, then sweeps again to find the first unvisited entry. At 1 M
warm entries under `idxMu.Lock`, this is 10-20 ms per `Evict()` call.

**Fix**: Add a `maxSweepProbes` parameter to `Evict()` (or a new
`EvictBounded(maxProbes int) (K, bool)` method). When the hand probes more
than `maxSweepProbes` entries without finding an unvisited one, return
`(zero, false)` instead of continuing the full sweep.

The caller handles the false return:
- **Hot tier** (`hot.go:337-339`): `evictPreferBacked()` returns
  `(zero, false)`. The inline eviction loop (`hot.go:333-353`) already
  breaks on `!ok`. The sweeper (`hot.go:462-466`) also breaks on `!ok`. The
  shard stays over budget temporarily; the next Put re-signals the sweeper,
  which retries. Budget overshoot is bounded by one Put's object size per
  shard.
- **Warm tier** (`warm.go:1009-1011`): `evictToFit` returns `ErrOverBudget`
  on `!ok`. `Put` rejects the write. The warm sync loop skips promotion.
  Compaction will eventually reduce dead space. No data loss.

**Proposed constant**: `maxSweepProbes = 128` (hot tier), `maxSweepProbes =
  256` (warm tier). These are large enough that under normal eviction
patterns (some entries unvisited) the sweep almost always succeeds, but
small enough to cap the worst case at ~128-256 pointer chases (~50-100 us)
instead of 2 M (~10-20 ms).

The warm tier gets a 2x larger cap because warm entries are disk-backed
and larger (each eviction frees more bytes, so fewer total evictions are
needed). The higher probe budget gives SIEVE more room to find
unprotected entries before falling back to `ErrOverBudget`, which has a
higher cost (origin fetch) than a hot-tier miss (warm or origin fetch).

### Interaction with `evictPreferBacked`

`evictPreferBacked` (`hot.go:716-737`) calls `Evict()` in a loop of up to
`maxEvictSkips` (4) iterations, then falls back to a final `Evict()`. Each
of those 5 `Evict()` calls can sweep O(N) entries. With `EvictBounded(128)`,
each call is capped at 128 probes, so the total worst case for
`evictPreferBacked` is 5 x 128 = 640 probes. Without the cap, the worst
case is 5 x 2N = 10N probes. The cap turns O(10N) into O(640) — a
significant improvement even with the `evictPreferBacked` skip loop.

The `backedCount == 0` fast path (`hot.go:717-718`) calls `Evict()` once
directly. With the cap, this becomes `EvictBounded(128)` — O(128) worst
case.

### Tasks

1.1. Add `EvictBounded(maxProbes int) (K, bool)` to `sieve.List` in
  `internal/storage/sieve/sieve.go`. Refactor `Evict()` to call
  `EvictBounded(l.len * 2)` for backward compatibility.

1.2. Update `evictPreferBacked()` in `internal/storage/hot.go` to call
  `s.evict.EvictBounded(maxSweepProbes)` instead of `s.evict.Evict()`.
  This applies to all three `Evict()` calls in the function: the
  `backedCount == 0` fast path (line 718), the skip loop (line 721),
  and the fallback (line 736). Add `const maxSweepProbes = 128` near
  `maxEvictSkips`.

1.3. Update `pickEvictVictim()` in `internal/storage/warm/warm.go` to call
  `s.evictList.EvictBounded(maxSweepProbes)` instead of
  `s.evictList.Evict()`. Add `const maxSweepProbes = 256` near
  `maxWarmEvictSkips`.

1.4. Add unit tests in `sieve_test.go`: all-visited list, verify
  `EvictBounded(N)` returns false after N probes and does not scan the
  full list. Verify entries still have `visited=true` for the unscanned
  portion (they were not given a second chance). Verify `EvictBounded(0)`
  returns false immediately.

1.5. Run `BenchmarkSIEVE_Evict_AllVisited_1M` and
  `BenchmarkWarmEvict_AllVisited_1M` before and after. Gate: ns/op must
  drop by >= 100x at 1 M entries.

### Risk

- **Warm tier rejects more Puts under heavy read load**: `ErrOverBudget`
  returns to the caller, which falls through to origin. This is the correct
  behavior — serving from origin is better than blocking all warm Gets for
  10-20 ms.
- **Hot tier budget overshoot**: the shard stays over `perShardMax` by one
  Put's object size until the sweeper retries. Already the existing
  behavior when `evictPreferBacked` exhausts `maxEvictSkips`.
- **SIEVE aging property**: capping the sweep means some visited entries
  keep their visited bit across multiple Evict calls. This is fine — they
  will eventually be swept when the hand reaches them. The algorithm's
  "second chance" property is preserved; we just limit how many second
  chances we hand out per call.
- **Progress guarantee**: `EvictBounded` advances the hand and clears
  visited bits during the capped sweep, just like `Evict` but with a
  lower probe limit. On the next call, the hand resumes from the
  advanced position and encounters an entry whose visited bit was just
  cleared — it evicts immediately. The system is NOT stuck; it makes
  progress across multiple capped calls, each bounded at 128/256 probes.
  The only scenario where progress stalls is if no new `Put` re-signals
  the sweeper after a `!ok` return — but the shard is over budget, so
  the next `Put` will re-signal. Under a pure read workload with no
  writes, there is no over-budget condition triggering eviction in the
  first place.

### ADR

Required (eviction-algorithm change per AGENTS.md section 10). Draft
`docs/decisions/0026-sieve-sweep-cap.md`.

---

## Phase 2 — Incremental hot-only key tracking (eliminates 60 s allocation spike)

**Problem**: `collectHotOnlyKeys()` (`tiered.go:731-759`) copies ALL hot
keys (`hot.Keys()`, ~24 MB at 3 M entries) and ALL warm keys (`warm.Keys()`,
~80 MB at 10 M entries), then builds a `map[uint64]struct{}` to diff them
(~400 MB at 10 M entries). Total transient allocation: ~500 MB every 60 s.

**Fix**: Maintain a `hotOnly` set per hot shard — keys that are in the hot
tier but not yet in the warm tier. Updated incrementally on the hot Put
and warm sync paths:

- `hot.Put` for a key not in warm: add to `s.hotOnly`
- `writeHotOnlyToWarm` success: remove from `s.hotOnly` via `SetBacked`
- `hot.Delete` / inline eviction / sweeper eviction: remove from
  `s.hotOnly`
- `warm.OnEvict` -> `hot.ClearBacked`: re-add to `s.hotOnly` if the key is
  still in the hot tier (warm evicted it, so it's hot-only again)

`collectHotOnlyKeys()` becomes a snapshot of `hotOnly` bounded by
`warmSyncBatchSize` with rotation. No warm key copy, no diff map, no
`hot.Keys()` call. Zero transient allocation on the sync path.

### Implementation

Each hot shard maintains its own `hotOnly map[api.Key]struct{}`, protected
by the shard's `mu`. `collectHotOnlyKeys` iterates shards (RLock each)
and collects up to `warmSyncBatchSize` keys, rotating via
`warmSyncOffset`. This reuses the existing shard-level locking and avoids
a new lock on the hot Put path.

**Persistent memory cost**: `api.Key` is `uint64`, so `map[uint64]struct{}`
has ~16-22 bytes overhead per entry (Go map bucket at load factor 6.5).
At 3 M hot-only entries, the `hotOnly` maps consume ~50-70 MB of
persistent heap. This is acceptable: it replaces ~500 MB of transient
allocation every 60 s, and the persistent cost scales with the hot-only
working set (entries backed by warm are removed from `hotOnly`).

### Tasks

2.1. Add `hotOnly map[api.Key]struct{}` to the `shard` struct in
  `internal/storage/hot.go`. Initialize in `NewHotStore` loop
  (`hot.go:199-203`).

2.2. On `Put` (`hot.go:324`): after inserting the new entry, if the key
  is not backed (`!e.hasBackup`), add to `s.hotOnly`. This applies to
  both new inserts and replacements. When replacing a backed entry
  (`old.hasBackup == true`), the key transitions from backed to
  hot-only — it was not in `hotOnly` (backed entries are excluded) and
  must be added. When replacing a non-backed entry, the key is already
  in `hotOnly` and the map insertion is a no-op.

2.3. On `Delete` (`hot.go:482`): remove from `s.hotOnly` (add
  `delete(s.hotOnly, key)` before `hotEntryPool.Put`).

2.4. On inline eviction (`hot.go:341-352`): remove from `s.hotOnly` before
  `hotEntryPool.Put`.

2.5. On sweeper eviction (`hot.go:467-473`): remove from `s.hotOnly`.

2.6. On `evictBanned` (`hot.go:303-318`): remove from `s.hotOnly`.

2.7. On reaper eviction (`hot.go:431-444`): remove from `s.hotOnly`.

2.8. Add `HotOnlyKeys(offset, limit int) []api.Key` to `HotStore`. Iterates
  shards (RLock each), collects up to `limit` keys from `hotOnly`, starting
  at `offset % total`. Returns the collected keys and advances the offset.

2.9. Rewrite `collectHotOnlyKeys()` (`tiered.go:731`) to call
  `t.hot.HotOnlyKeys(t.warmSyncOffset, t.warmSyncBatchSize)`. Remove the
  `warm.Keys()` call and the `warmSet` diff map. Update `warmSyncOffset`
  based on the number of keys checked (not just found), preserving the
  rotation semantics.

2.10. On `SetBacked` (`hot.go:687`): remove from `s.hotOnly` (the key is
  now in warm, no longer hot-only).

2.11. On `ClearBacked` (`hot.go:700`): re-add to `s.hotOnly` if the key
  is still in the hot tier (warm evicted it, so it's hot-only again).

2.12. Unit tests: verify `hotOnly` is consistent after Put/Delete/evict/
  SetBacked/ClearBacked. Verify `HotOnlyKeys` returns the correct subset
  with rotation. Run with `-race`.

2.13. Run `BenchmarkWarmSyncCycle_1M` before and after. Gate: allocs/op
  must drop by >= 90%, ns/op must drop by >= 50%.

### Risk

- **Stale entries in `hotOnly`**: a key evicted from hot but not yet
  removed from `hotOnly` causes a redundant warm Put attempt — harmless,
  `writeHotOnlyToWarm` does `hot.Get` which returns nil and the key is
  skipped.
- **Missing entries in `hotOnly`**: a key that is hot-only but not in the
  set delays its warm promotion by one cycle — harmless, it will be
  added on the next Put or caught by the fallback scan.
- **Fallback**: keep a periodic full-scan (every 10th cycle, ~10 min at
  60 s interval) to reconcile `hotOnly` against actual state. This bounds
  drift and catches any races the incremental updates miss. The
  full-scan path streams `hot.Keys()` and checks each key against
  `warm.Lookup()` (no `warm.Keys()` call, no diff map), bounded by
  `warmSyncBatchSize` with rotation. This is ~24 MB transient (the
  `hot.Keys()` slice) vs the old ~500 MB, and runs rarely.

### Files touched

| File | Changes |
|------|---------|
| `internal/storage/hot.go` | `hotOnly` field on `shard`, update Put/Delete/evict/sweeper/reaper/ban paths, `SetBacked`/`ClearBacked`, `HotOnlyKeys` method |
| `internal/storage/tiered.go` | Rewrite `collectHotOnlyKeys` to use `HotOnlyKeys`, add fallback scan counter |

---

## Phase 3 — Batch warm eviction (amortize lock acquisition)

**Problem**: `evictToFit` (`warm.go:995`) calls `evictOne()` in a loop.
Each `evictOne()` acquires `seg.mu.Lock` + `idxMu.Lock`, writes one
tombstone (pwritev syscall), removes from index, fires `OnEvict`, then
releases both locks. For a record requiring 10 x 100 KB evictions, that's
10 lock/unlock cycles, each involving a syscall under the locks.

**Fix**: Add `evictToFitBatch(recSize int64) error` that acquires
`seg.mu.Lock` + `idxMu.Lock` once, loops the eviction inner logic N times
until space is freed, then releases both locks once.

### Tasks

3.1. Extract the eviction inner loop from `evictOne()` into
  `evictOneLocked() (uint64, bool)` that assumes both `seg.mu` and
  `idxMu` are already held. `evictOne()` becomes a thin wrapper that
  acquires the locks and calls `evictOneLocked()`.

3.2. Add `evictToFitBatch(recSize int64) error` in
  `internal/storage/warm/warm.go`:
  ```go
  func (s *Store) evictToFitBatch(recSize int64) error {
      if s.maxBytes <= 0 && s.maxEntries <= 0 { return nil }
      if s.maxBytes > 0 && recSize > s.maxBytes { return ErrOverBudget }
      seg, err := s.activeSeg()
      if err != nil { return err }
      seg.mu.Lock()
      defer seg.mu.Unlock()
      s.idxMu.Lock()
      defer s.idxMu.Unlock()
      for {
          bytesOK := s.maxBytes <= 0 || s.stats.bytes.Load()+recSize <= s.maxBytes
          entriesOK := s.maxEntries <= 0 || s.stats.entries.Load()+1 <= s.maxEntries
          if bytesOK && entriesOK {
              return nil
          }
          if s.evictList.Len() == 0 { return ErrOverBudget }
          key, loc, found := s.pickEvictVictim()
          if !found { return ErrOverBudget }
          // Write tombstone, update index, fire callback — inline.
          // (Same logic as evictOneLocked, minus lock acquisition.)
          if err := s.writeTombstoneLocked(seg, key); err != nil {
              s.restoreSIEVEEntry(key, loc)
              return err
          }
          delete(s.index, key)
          s.stats.entries.Add(-1)
          if loc.size > 0 { s.stats.bytes.Add(-loc.size) }
          if cb := s.OnEvict; cb != nil { cb(key) }
          s.metrics.IncEvictions()
      }
  }
  ```

3.3. Update `Put` (`warm.go:495`) to call `evictToFitBatch` instead of
  `evictToFit`. Keep `evictToFit` for single-eviction callers (tests).

3.4. Extract `writeTombstoneLocked(seg *Segment, key uint64) error` from
  `evictOne` to avoid code duplication.

3.5. Unit tests: verify `evictToFitBatch` evicts the correct number of
  entries, fires `OnEvict` for each, and releases locks cleanly. Test
  the case where eviction fails mid-batch (tombstone write error) —
  verify partial eviction state is consistent (index matches disk).

3.6. Run `BenchmarkWarmEvictToFit_MultiEvict` before and after. Gate:
  ns/op must drop by >= 30% for the 10-eviction case.

### Risk

- **Longer lock hold per acquisition**: `seg.mu` + `idxMu` held for N
  tombstone writes instead of 1. This increases the worst-case lock hold
  time but decreases the total lock acquisition count. Under contention,
  fewer acquisitions means less queueing overhead. Net positive.
- **Tombstone write failure mid-batch**: if `writeTombstoneLocked` fails
  on the 5th eviction, the first 4 are committed (tombstone on disk,
  index removed). The batch returns an error, and the caller (`Put`)
  gets `ErrOverBudget`. The 4 evicted entries are permanently gone —
  same as the current per-eviction behavior. No new consistency risk.
- **`OnEvict` called under both locks**: unchanged from current behavior
  (`evictOne` already fires `OnEvict` under `idxMu` + `seg.mu`). The batch
  version fires it N times under the same lock pair. `ClearBacked` is
  O(1) lock-only, so each callback is fast. However, the batch extends
  the warm lock hold time by N hot-shard lock acquisitions (one per
  `OnEvict` callback). Under uncontended hot shards this adds ~N us;
  under contention it could add ~N * (lock wait) us. In practice N is
  small (10-20 evictions per `evictToFitBatch`), so the total is bounded.
- **`activeSeg()` called before lock acquisition**: `evictToFitBatch`
  calls `activeSeg()` before acquiring `seg.mu`. This is correct —
  `activeSeg()` may create a new segment (acquiring `s.mu`), and holding
  `seg.mu` during that would deadlock if `newSegment` also touches the
  same segment. The returned `seg` pointer is stable because segments
  are never removed from `s.segs` except during `Compact`, which holds
  `s.mu.Lock` — and `activeSeg` holds `s.mu.RLock` during the check.

---

## Phase 4 — Cap warm index by GOMEMLIMIT (long-term heap bound)

**Problem**: `warm_max_bytes` controls disk bytes, not Go heap. The warm
index (`map[uint64]warmLoc` + SIEVE entries) lives in Go heap with no cap.
At ~128 bytes/entry, 10 M entries = ~1.5 GiB of heap. Combined with the
3 GiB hot store and GC fragmentation, total RSS exceeds the container limit.

**Fix**: Add a `warm_max_entries` config field derived from GOMEMLIMIT.
Default ratio: 15% of GOMEMLIMIT for the warm index. At 14 GiB GOMEMLIMIT:
15% = 2.1 GiB = ~16 M entries max. When the warm index exceeds the cap,
skip hot->warm promotion (same as `warm.OverBudget()` for disk bytes).

### Tasks

4.1. Add config fields to `internal/config/config.go` `Storage` struct
  (after line 91, `WarmMaxBytes`):
  ```go
  // WarmMaxEntries caps the warm-tier index size in entries. Zero means
  // derive from GOMEMLIMIT (see WarmMaxEntriesRatio). A positive value
  // overrides the derived limit. Negative means unlimited.
  WarmMaxEntries int64 `yaml:"warm_max_entries,omitempty" json:"warm_max_entries,omitempty"`
  // WarmMaxEntriesRatio is the percentage of GOMEMLIMIT used to derive
  // WarmMaxEntries when warm_max_entries is not explicitly set. Zero
  // means use the default (15). At 14 GiB GOMEMLIMIT with 15%, the
  // warm index is capped at ~16M entries (~2 GiB heap).
  WarmMaxEntriesRatio int `yaml:"warm_max_entries_ratio,omitempty" json:"warm_max_entries_ratio,omitempty"`
  ```

4.2. Add named constant to `internal/storage/warm/warm.go`:
  ```go
  // EstimatedWarmLocHeapBytes is the approximate Go heap cost per warm
  // index entry: warmLoc struct (segID int + offset int64 + size int64 +
  // sieve pointer 8B + protected bool padded to 8B = ~40B) + map overhead
  // (~50B at load factor 6.5) + sieve.Entry (32B pooled, but pointer 8B
  // in warmLoc) = ~98B. Rounded to 128 for alignment and safety margin.
  // Update if warmLoc or sieve.Entry struct layout changes.
  const EstimatedWarmLocHeapBytes = 128
  ```

4.3. Add `ResolveWarmMaxEntries` to `internal/config/loader.go` (after
  `ResolveHotMaxBytes`, line 165):
  ```go
  const defaultWarmMaxEntriesRatio = 15

  func (s *Storage) ResolveWarmMaxEntries(goMemLimit string) {
      if s.WarmMaxEntries > 0 { return }
      raw := strings.TrimSpace(goMemLimit)
      if raw == "" { return }
      n, err := parseByteSize(raw)
      if err != nil || n <= 0 { return }
      ratio := s.WarmMaxEntriesRatio
      if ratio == 0 { ratio = defaultWarmMaxEntriesRatio }
      // 128 = warm.EstimatedWarmLocHeapBytes. Inlined to avoid a
      // circular import (config -> warm -> storage).
      s.WarmMaxEntries = n * int64(ratio) / (100 * 128)
  }
  ```

  Note: `loader.go` is in package `config`, not `warm`. To avoid a
  circular import (`config` -> `warm` -> `storage`), the constant `128`
  is inlined with a comment citing `warm.EstimatedWarmLocHeapBytes`.
  If the `warmLoc` or `sieve.Entry` struct layout changes, update both
  the constant in `warm.go` and the inlined value in `loader.go`.

4.4. Call `ResolveWarmMaxEntries` alongside `ResolveHotMaxBytes` in
  `internal/config/loader.go:83` and `cmd/bouine/cmd/serve.go:71`.

4.5. Add `maxEntries int64` field to `warm.Store` (`warm.go:311`). Set in
  `NewStore` from `Config`. Add to `warm.Config`:
  ```go
  MaxEntries int64 // 0 = unlimited
  ```

4.6. Extend `OverBudget()` (`warm.go:1249`) to check entry count:
  ```go
  func (s *Store) OverBudget() bool {
      if s.maxBytes > 0 && s.stats.bytes.Load() >= s.maxBytes {
          return true
      }
      if s.maxEntries > 0 && s.stats.entries.Load() >= s.maxEntries {
          return true
      }
      return false
  }
  ```

4.7. Update `Put` (`warm.go:493`) to check both byte budget and entry
  count before triggering eviction. Replace the current `if s.maxBytes > 0`
  guard with a check that covers both limits:
  ```go
  if s.maxBytes > 0 || s.maxEntries > 0 {
      overBytes := s.maxBytes > 0 && s.stats.bytes.Load()+recSize > s.maxBytes
      overEntries := s.maxEntries > 0 && s.stats.entries.Load()+1 > s.maxEntries
      if overBytes || overEntries {
          if evictErr := s.evictToFitBatch(recSize); evictErr != nil {
              s.metrics.IncOverBudget()
              return 0, 0, fmt.Errorf("warm: put %d bytes: %w", recSize, ErrOverBudget)
          }
      }
  }
  ```
  This ensures the entry cap is enforced on the `Put` path, not just via
  `OverBudget()` in the sync cycle.

4.8. Add validation in `config.Validate()`: if `WarmMaxEntriesRatio` is
  not 0 and not in 1-100, return error (mirrors `HotMaxBytesRatio` check at
  `loader.go:128-131`).

4.9. Unit tests: `ResolveWarmMaxEntries` with explicit override, with
  GOMEMLIMIT derivation, with invalid GOMEMLIMIT, with zero ratio.
  Verify `OverBudget` returns true when entry count exceeds cap.
  Verify `Put` rejects when entry count exceeds cap and eviction cannot
  free enough.

4.10. Run existing warm tests + `make test-short`. Gate: no regression.

### Files touched

| File | Changes |
|------|---------|
| `internal/config/config.go` | `WarmMaxEntries`, `WarmMaxEntriesRatio` fields |
| `internal/config/loader.go` | `ResolveWarmMaxEntries`, call in `Parse`, validation |
| `cmd/bouine/cmd/serve.go` | Call `ResolveWarmMaxEntries` |
| `internal/storage/warm/warm.go` | `EstimatedWarmLocHeapBytes` const, `maxEntries` field, `Config.MaxEntries`, `OverBudget` check, `evictToFitBatch` entry gate |

---

## Phase 5 — Parallelize Ban across shards

**Problem**: `Ban()` (`hot.go:500-541`) acquires the write lock on each
shard sequentially and iterates all entries. At 1 M entries / 64 shards,
this takes 1-3 seconds where each shard is locked in sequence.

**Fix**: Scan shards in parallel using `errgroup.Group` with
`min(numShards, runtime.NumCPU())` workers. Each worker scans one shard
independently. Aggregate the `total` count atomically.

### Tasks

5.1. Refactor `Ban()` (`hot.go:500`) to use `errgroup.Group`:
  ```go
  func (h *HotStore) Ban(ctx context.Context, expr api.BanExpr) (int, error) {
      pred, err := compileBanPredicate(expr)
      if err != nil { return 0, err }
      var total atomic.Int64
      g, _ := errgroup.WithContext(ctx)
      g.SetLimit(min(len(h.shards), runtime.NumCPU()))
      for i := range h.shards {
          i := i
          g.Go(func() error {
              n := h.banShard(i, pred)
              total.Add(int64(n))
              return nil
          })
      }
      _ = g.Wait()
      // Register lazy ban (unchanged)
      ...
      return int(total.Load()), nil
  }
  ```

  Note: `errgroup.WithContext` is used for `SetLimit` convenience; no
  goroutine returns an error, so the context is never cancelled. The
  `ctx` parameter is passed to `errgroup.WithContext` for future
  cancellation support but is not forwarded to `banShard` (shard scanning
  is not cancellable mid-iteration).

5.2. Extract `banShard(idx int, pred banPredicate) int` from the current
  per-shard loop body (`hot.go:512-528`). This function locks one shard,
  iterates entries, evaluates the predicate, evicts matches, and returns
  the count. No context parameter needed — shard scanning is not
  cancellable mid-iteration (the shard lock is held).

5.3. Unit tests: verify `Ban` with a matching regex evicts the correct
  entries across all shards. Verify `total` is correct under concurrent
  shard scanning. Run with `-race`.

5.4. Run `BenchmarkBan_1M` before and after. Gate: wall-clock time must
  drop by >= 4x (roughly `numShards / NumCPU`).

### Risk

- **`errgroup` dependency**: `golang.org/x/sync` already in `go.mod`.
  `SetLimit` is available (confirmed via `go doc`).
- **Shard lock ordering**: each worker locks only one shard. No
  cross-shard lock ordering, no deadlock risk.
- **`errgroup.Go` blocks when `SetLimit` is reached**: this is correct
  behavior — it bounds the number of goroutines to `NumCPU`, preventing
  goroutine explosion. The blocking is on the calling goroutine, not on
  the shard locks.

---

## Phase 6 — Documentation and ADRs

6.1. Write `docs/decisions/0026-sieve-sweep-cap.md` — documents the
  `maxSweepProbes` constant, the O(N) worst case it fixes, the tradeoff
  (temporary budget overshoot / `ErrOverBudget` rejection), the
  interaction with `evictPreferBacked` (5 x 128 = 640 probes worst case),
  the progress guarantee (continuous writes produce unvisited entries),
  and why 128/256 probes were chosen.

6.2. Write `docs/decisions/0027-warm-index-heap-cap.md` — documents the
  `warm_max_entries` config, the `EstimatedWarmLocHeapBytes` constant,
  the 15% GOMEMLIMIT default ratio, the inline constant in `loader.go`
  to avoid circular import, and the tradeoff (warm Put rejection when
  index exceeds cap vs. OOMKill from unbounded heap growth).

6.3. Update `internal/storage/sieve/sieve.go` doc comments to state
  "O(1) amortized, O(maxSweepProbes) worst case" instead of "O(1) per
  operation."

6.4. Update `internal/storage/warm/warm.go` `evictOne` / `pickEvictVictim`
  comments to reference `maxSweepProbes` instead of claiming "O(1) under
  idxMu."

6.5. Update `docs/runbook/10-cluster-modes.md` with tuning guidance for
  `maxSweepProbes`, `warmSyncBatchSize`, `warm_max_entries`, and
  `warm_max_entries_ratio` under high-cardinality workloads.

6.6. Update `docs/architecture.md` section 4.2 (Eviction) to mention the
  sweep cap, batch eviction, and warm index heap cap.

6.7. Add `warm_max_entries` and `warm_max_entries_ratio` to
  `config/default.yaml` comments so operators discover them.

---

## What this plan deliberately does NOT do

### Pool `encodeObject` buffers

Rejected. The original OOM plan proposed pooling `encodeObject` buffers
with a `sync.Pool`, claiming it was safe because `warm.Put`'s
`writeRecordAt` uses `O_DSYNC`. This is **wrong**: segment files are
opened with plain `os.O_RDWR` (`warm.go:1308`: `os.OpenFile(path,
os.O_CREATE|os.O_RDWR, 0o600)`) — no `O_DSYNC` or `O_SYNC`. Only the WAL
file uses `O_DSYNC` (`wal/wal.go:129`). The warm sync cycle calls
`t.warm.Sync()` (which does `f.Sync()` on all segments) **after** all
`writeHotOnlyToWarm` calls complete (`tiered.go:701`), not after each
individual `warm.Put`.

This means `warm.Put`'s `writeRecordAt` (via `platform.Pwritev`) writes
to the page cache, not directly to disk. The data is not guaranteed to be
on stable storage until `warm.Sync()` is called. If the encode buffer is
returned to the pool and reused by the next `encodeObject` call before
the kernel flushes the page cache, the previous write could be corrupted.

Pooling `encodeObject` buffers without first making `warm.Put` durable
before return would cause **silent data corruption**. The correct fix
would be to either:
- Open segment files with `O_DSYNC` (significant I/O performance
  regression — every `pwritev` becomes a synchronous disk flush), or
- Call `seg.f.Sync()` after each `warm.Put` in `writeHotOnlyToWarm`
  (same I/O regression, plus a per-entry `fsync` syscall), or
- Copy the body before writing (defeats the purpose of pooling — the copy
  is a new allocation).

None of these are acceptable. The ~20 MB transient allocation from
`encodeObject` is better addressed by Phase 2 (incremental hot-only
tracking), which reduces the number of `encodeObject` calls per cycle by
only promoting keys that are actually hot-only. The per-call allocation
is small (~4 KB) and short-lived (GC reclaims it within one cycle).

### Warm index sharding

Rejected for now. The warm tier's single `idxMu` is a contention point,
but sharding requires per-shard SIEVE lists, per-shard budgets, and
per-shard segment management. The SIEVE sweep cap (Phase 1) and batch
eviction (Phase 3) reduce the `idxMu` hold time by 100x and 10x
respectively, which may be sufficient. If benchmarks after Phases 1+3
still show unacceptable `idxMu` contention, a warm sharding ADR will be
written.

### Streaming compaction

Rejected for now. `Compact()` holds `s.mu.Lock` for the full swap +
rebuild, blocking all warm Gets. At 1 M entries this can freeze the warm
tier for seconds. However, compaction runs every 30 minutes (not on the
request path), and the `compactStartupDelay` (5 min default) prevents
startup contention. The fix (streaming compaction with concurrent reads)
is a fundamental protocol change with high complexity. It will be
revisited if operators report compaction-induced latency cliffs.

### Per-shard expiry min-heap

Rejected. The reaper's 10 ms budget already bounds the lock hold. Random
map iteration may miss entries on each pass, but the 30 s interval
ensures they are found within 1-2 cycles. A min-heap adds 24 bytes/entry
(24 MB at 1 M entries) to save a 10 ms scan every 30 s — a bad trade.

### Async warm eviction (sweeper pattern)

Deferred. After Phase 3 (batch eviction), the warm Put path acquires
locks once per `evictToFitBatch` call instead of once per eviction. If
benchmarks show this is still too slow, an async sweeper mirroring the
hot tier's pattern can be added. But the synchronous batch path is
simpler and avoids the "warm tier exceeds budget" tradeoff.

---

## Validation matrix

| Phase | Benchmark | Entry count | Metric | Gate |
|-------|----------|-------------|--------|------|
| 0 | `BenchmarkSIEVE_Evict_AllVisited_1M` | 1 M | ns/op | Baseline |
| 0 | `BenchmarkWarmEvict_AllVisited_1M` | 1 M | ns/op | Baseline |
| 0 | `BenchmarkWarmSyncCycle_1M` | 1 M | allocs/op, ns/op | Baseline |
| 0 | `BenchmarkBan_1M` | 1 M | wall-clock ms | Baseline |
| 0 | `BenchmarkWarmEvictToFit_MultiEvict` | 10 K | ns/op | Baseline |
| 1 | `BenchmarkSIEVE_Evict_AllVisited_1M` | 1 M | ns/op | >= 100x improvement |
| 1 | `BenchmarkWarmEvict_AllVisited_1M` | 1 M | ns/op | >= 100x improvement |
| 2 | `BenchmarkWarmSyncCycle_1M` | 1 M | allocs/op | >= 90% reduction |
| 2 | `BenchmarkWarmSyncCycle_1M` | 1 M | ns/op | >= 50% reduction |
| 3 | `BenchmarkWarmEvictToFit_MultiEvict` | 10 K | ns/op | >= 30% improvement |
| 5 | `BenchmarkBan_1M` | 1 M | wall-clock ms | >= 4x improvement |

All benchmarks must run with `-race`, N >= 10, compared via `benchstat`.
The `make conformance` gate (AGENTS.md section 16.4) must not regress. The
`make bench` gate (section 7) must not regress on the canonical RPS or
p99.

---

## Execution order

```
Phase 0 (baseline benchmarks)    --- 1 day, prerequisite for all code changes
Phase 1 (SIEVE sweep cap)        --- 2 days  (sieve.go + hot.go + warm.go + ADR)
Phase 2 (incremental hot-only)   --- 3 days  (hot.go + tiered.go + fallback)
Phase 3 (batch warm eviction)    --- 2 days  (warm.go refactor)
Phase 4 (warm index GOMEMLIMIT)  --- 3 days  (config + warm.go)
Phase 5 (parallel ban)           --- 1 day   (hot.go + errgroup)
Phase 6 (docs + ADRs)            --- 1 day   (ADRs + runbook + architecture)
```

**Phase 0 goes first** — it establishes baselines that gate all
subsequent phases.

**Phases 1-5 are independent** — they can be developed in parallel by
different agents (see AGENTS.md section 15). Each phase is a separate PR,
<= 400 changed lines (AGENTS.md section 15.4), behind feature flags where
applicable.

**Phase 6 goes last** — it documents all prior phases.

### Dependency graph

```
Phase 0 (benchmarks)  --- no dependencies, prerequisite for 1-5
Phase 1 (SIEVE cap)   --- depends on 0
Phase 2 (hot-only)    --- depends on 0
Phase 3 (batch evict) --- depends on 0, benefits from 1 (sweep cap in pickEvictVictim)
Phase 4 (warm cap)    --- depends on 0, benefits from 3 (evictToFitBatch entry gate)
Phase 5 (parallel ban) --- depends on 0
Phase 6 (docs)        --- depends on 1, 4 (ADRs reference those decisions)
```

### Files touched summary

| File | Phases | Changes |
|------|--------|---------|
| `internal/storage/sieve/sieve.go` | 1, 6 | `EvictBounded`, doc updates |
| `internal/storage/hot.go` | 1, 2, 5, 6 | `maxSweepProbes`, `hotOnly` per shard, `HotOnlyKeys`, `banShard` extraction, doc updates |
| `internal/storage/warm/warm.go` | 1, 3, 4, 6 | `maxSweepProbes`, `evictToFitBatch`, `writeTombstoneLocked`, `maxEntries`, `OverBudget` entry check, `EstimatedWarmLocHeapBytes`, doc updates |
| `internal/storage/tiered.go` | 2 | Rewrite `collectHotOnlyKeys` to use `HotOnlyKeys`, add fallback scan counter |
| `internal/config/config.go` | 4 | `WarmMaxEntries`, `WarmMaxEntriesRatio` fields |
| `internal/config/loader.go` | 4 | `ResolveWarmMaxEntries`, call in `Parse`, validation |
| `cmd/bouine/cmd/serve.go` | 4 | Call `ResolveWarmMaxEntries` |
| `config/default.yaml` | 6 | `warm_max_entries` / `warm_max_entries_ratio` comments |
| `docs/decisions/0026-sieve-sweep-cap.md` | 6 | New ADR |
| `docs/decisions/0027-warm-index-heap-cap.md` | 6 | New ADR |
| `docs/runbook/10-cluster-modes.md` | 6 | Tuning guidance |
| `docs/architecture.md` | 6 | Section 4.2 update |
