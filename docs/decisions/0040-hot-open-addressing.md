# ADR-0040: Open-addressing hash table for the hot tier

- **Status**: Proposed
- **Date**: 2026-08-25
- **Deciders**: @thylong
- **Phase**: phase 2
- **Closes**: #432

## Context

The hot tier (`internal/storage/hot.go`) stored entries in
`map[api.Key]*hotEntry`, where `api.Key` is `[16]byte` (XXH128). The
stdlib map hashes all 16 bytes with the random-seed AES hash on every
lookup. A microbenchmark comparing five lookup strategies (issue #432)
showed a custom open-addressing table using the already-computed
`key.Hash64()` — free, because it is computed for shard selection —
with inline 16-byte key compare and linear probing is 1.7–2.4× faster
on the hit path at all working-set sizes, with zero allocations and a
tighter p99 tail (no hash-function overhead to cache-miss).

The hot tier is the hottest path in the system (AGENTS.md §7: < 5 µs
CPU per request at p50, 0 allocs/op). Any per-lookup cost reduction
directly improves cache-hit latency and RPS.

## Decision

Replace `map[api.Key]*hotEntry` in `internal/storage/hot.go` with a
custom open-addressing hash table (`hotTable`) keyed by
`key.Hash64()`, behind the existing shard mutex.

### Design

- **Slot layout**: 40 bytes per slot — 16-byte key, 8-byte `*hotEntry`
  pointer, 8-byte cached `Hash64`, 1-byte `state` (empty / occupied /
  tombstone), 7 bytes padding. The `state` byte replaces a
  sentinel-pointer tombstone design (rejected — three-way pointer
  branch on every probe step, and a comparison against a global
  variable address that may not be in L1).
- **Hash**: `key.Hash64()` (the high 64 bits of the XXH128 hash). This
  is already computed for shard selection at `hot.go:319`; the table
  reuses it with no rehash. 128-bit collision resistance is preserved
  by the inline 16-byte key compare in the probe loop.
- **Probing**: linear. Cache-friendly (sequential slot access) and
  simple. The probe loop is bounded at `len(slots)` iterations to
  prevent infinite loops on a transiently-full table.
- **Load factor**: 0.75. Matches the winning load factor in the issue
  benchmark sweep. At 40-byte slots, per-live-entry memory overhead is
  40 / 0.75 ≈ 53 bytes (up from the stdlib map's 32 bytes).
- **Growth**: double-and-rehash, triggered when `count+1 >
  0.75 × capacity`. Tombstones are dropped during rehash. Grow is
  O(capacity) under the shard write lock. At the default shard count
  (16–64), each shard holds ~1M/64 = 15K entries at 1M total; grow
  cost is ~15K × 100ns ≈ 1.5 ms — well within the reaper's 10 ms
  shard-lock budget.
- **Compaction**: when tombstones exceed 25% of capacity, the table
  is rehashed in place (same capacity, tombstones dropped). This
  bounds probe-length inflation from delete churn without a full grow.
- **Deletion**: tombstones preserve probe chains. A tombstone is
  reused on the next `Put` that probes past it for the same key (after
  confirming the key is not already present further along the chain).
- **Iteration**: `Iter(func(key, entry, deleter) bool)` — a callback
  API. O(capacity), not O(count). The `deleter` closure tombstones the
  current slot without grow/compaction, making delete-during-iter
  safe. Calling the public `Delete` method from inside `Iter` is
  forbidden (it could trigger a grow that reallocates the backing
  array the iterator is walking). The reaper, ban, `Keys`, and
  `HotOnlyKeys` paths use `Iter`; the extra 33% scan at 0.75 load
  factor is within their lock-hold budgets.

## Consequences

### Positive

- 1.57× hit-path speedup measured on `BenchmarkGate_HotStore_Get_Hit`
  (11.86 ns/op → 7.53 ns/op, 0 allocs/op preserved). The p99 tail is
  tighter because there is no AES hash function overhead to
  occasionally cache-miss.
- No new dependencies. The table is ~200 lines of pure Go in
  `internal/storage/hot_table.go`.
- The cached `Hash64` in each slot enables a cheap fast-path skip
  during probing: if the hash doesn't match, the 16-byte key compare
  is skipped entirely.

### Negative / trade-offs

- ~21 bytes/entry more memory (53 B vs 32 B). At 1M entries this is
  ~21 MB — within the 5% RSS gate.
- `Iter` is O(capacity), not O(count) as `range map` was effectively
  O(count). The reaper and ban paths scan 33% more slots; this is
  absorbed by their existing lock-hold budgets.
- Grow is O(capacity) under the write lock. At realistic shard counts
  (16–64) this is ≤ 1.5 ms per shard. A single-shard deployment at 1M
  entries would see a ~100 ms grow stall — documented as a known
  limitation; the default config uses `min(NumCPU, 64)` shards.
- The table is not thread-safe; it relies on the shard's
  `sync.RWMutex`. This matches the prior `map` semantics.

### Risks

- **Probe-chain degradation under adversarial keys**: keys sharing
  the same `Hash64()` (high half) but differing in the low half
  produce a linear probe chain. `BenchmarkHotGet_CollisionChain` and
  `TestHotTable_CollisionChainDistinct` pin the behavior: the table
  degrades linearly (not catastrophically), and the existing
  `TestHotStore_KeysWithSameFirstHalfAreDistinct` test confirms 128-bit
  collision resistance is preserved. An attacker controlling cache
  keys could craft a worst-case probe chain; the XXH128 hash makes
  this computationally infeasible without breaking the hash.
- **Tombstone accumulation under delete-heavy workloads**: the 25%
  tombstone threshold triggers compaction, but a workload that
  deletes faster than it puts could oscillate between compaction and
  grow. The `BenchmarkHotPut_Overflow` and `BenchmarkHotMixed_80_20`
  benchmarks exercise this churn and show no pathological behavior.

## Alternatives considered

- **`map[api.Key]*hotEntry` (status quo)**: 11.86 ns/op on the hit
  path. The stdlib map rehashes all 16 bytes on every lookup. Kept as
  the baseline.
- **`map[uint64]*hotEntry` with a guard field**: ~same speed as
  baseline (the guard check replaces the hash savings). Drops to
  2^32 birthday bound. Rejected for correctness risk (issue #51
  class of bug).
- **`dolthub/swiss` SwissTable**: parity at 256K, ~20% faster than
  baseline at 1M hit, but consistently worst-in-class on the miss path
  and adds a dependency. Requires an ADR and `docs/deps.md` entry.
  Rejected — the custom table matches its hit-path performance
  without the miss-path regression or the dependency cost.
- **Two-level `map[uint64]map[uint64]`**: 2–6× slower at scale (two
  hash probes + pointer chase), 4× memory. Not viable.
- **Robin Hood probing**: bounds probe-length variance but adds
  per-insert shift cost. The linear-probe benchmark showed p99 is
  already tight (167 ns at 256K vs 146 ns baseline); Robin Hood's
  complexity is not justified.
- **Incremental rehashing** (Go map style): avoids the O(capacity)
  grow stall but doubles the implementation complexity. Deferred
  until a deployment hits the grow-stall limitation at low shard
  counts.

## References

- Issue #432: the benchmark evidence and proposed change.
- ADR-0030: 128-bit cache key via XXH128 — the key design that makes
  the cached `Hash64` valid as a table hash.
- AGENTS.md §7: hit-path performance budget (< 5 µs p50, 0 allocs/op).
- AGENTS.md §8: test policy (major functionality requires tests).
