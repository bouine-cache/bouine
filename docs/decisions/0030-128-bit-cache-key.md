# ADR-0030: 128-bit cache key via 2×xxhash64

- **Status**: Accepted
- **Date**: 2026-08-06
- **Deciders**: @thylong
- **Phase**: phase 1
- **Supersedes**: none
- **Closes**: #51

## Context

The cache key was a bare `uint64` (single xxhash64 of the canonical
request URI). A 64-bit hash has a ~1% collision probability at ~6×10⁸
entries (birthday bound). A collision causes the cache to serve the
wrong response body — a correctness and security bug (issue #51).

The fix is to widen the key to 128 bits so that the map lookup itself is
a full collision check, not just a hash-bucket probe. This eliminates
the wrong-body class of bug entirely: two distinct canonical requests
cannot occupy the same map entry.

## Decision

Replace `type Key uint64` with `type Key [16]byte` — two xxhash64
halves packed little-endian.

- **First 8 bytes**: `xxhash64(canonical, seed=0)` — the original hash,
  unchanged. Used for shard selection, ring hashing, and access
  sampling. This avoids a hot-path hash call on the full 16 bytes for
  routing decisions that only need 64 bits of uniformity.
- **Second 8 bytes**: `xxhash64(canonical, seed="bouine2")` — an
  independent hash with a different seed. Computed via a `sync.Pool` of
  `*xxhash.Digest` to maintain 0 allocations on the hit path after
  warm-up (AGENTS.md §7).

### Why `[16]byte` over a two-field struct

A `[16]byte` array is directly usable as a Go map key with no custom
`hash()` / `equal()` required. It is comparable, copyable, and its
`unsafe.Sizeof` is exactly 16. A struct `{lo, hi uint64}` would require
the same properties but adds field naming that implies a
primary/guard split (as seen in the abandoned PR #345). The array form
makes it clear that both halves are equally part of the key identity.

### Why 2×xxhash64 instead of a single 128-bit hash

- `cespare/xxhash/v2` is already on the pre-approved dependency
  allow-list (AGENTS.md §5). No new crypto or hash dependency.
- xxhash64 is fast (~2 GB/s) and has no internal slice allocations,
  making it pool-safe.
- Two independent 64-bit hashes with different seeds provide the same
  collision resistance as a 128-bit hash for the adversary model we
  face (natural collisions, not adversarial preimage attacks).

### Wire and on-disk format changes (no backward compatibility)

This is a clean break. No production data exists. All versioned formats
are bumped:

| Format | Old version | New version | Key width |
|--------|-------------|-------------|-----------|
| Object codec | v2 | v3 | 8→16 B inline |
| WAL | v2 | v3 | 8→16 B per record |
| Warm segment header | 16 B | 24 B | magic(4)+key(16)+body_len(4) |
| Warm snapshot | v1 | v2 | 8→16 B per entry |
| Cluster purge/ban frame | v2 | v3 | 8→16 B per payload |
| Peer-fetch request | v1 | v2 | 8→16 B per request |
| Key JSON (`pkg/api`) | decimal uint64 | `[lo, hi]` decimal array | 128-bit explicit |

### Memory impact

- `api.Object` struct: 264→272 B (Key grew 8→16 B inline).
- `sieve.Entry[api.Key]`: 32→40 B (Key 16 B + atomic.Bool 4 B + pad 4 B + prev 8 B + next 8 B).
- `mapPerEntryOverhead`: 22→32 B (8-slot bucket with 16 B keys = 208 B / 6.5 load factor).
- `EstimatedWarmLocHeapBytes`: 128→160 B (per-entry warm location cost).
- `hotEntry` struct: unchanged (32 B — Key is not stored inline in hotEntry).

### AGENTS.md §13 waiver

`pkg/api` types marked `// Stable.` are not supposed to change shape
without a major version bump. The `Key` type change violates this
letter. The waiver is justified because:

1. No production deployment exists — this is pre-1.0 software.
2. The change is a clean break across all wire formats, not a silent
   reinterpretation.
3. A migration guide is unnecessary for the same reason.

## Consequences

- **Positive**: Eliminates the wrong-response-body class of bug. Map
  lookup is a full 128-bit collision check.
- **Positive**: No new dependencies. xxhash is already approved.
- **Positive**: 0 allocations on the hit path maintained via `sync.Pool`.
- **Negative**: `map[[16]byte]` lookup is ~2-4 ns slower than
  `map[uint64]` due to the larger hash input. Acceptable per the
  performance budget.
- **Negative**: All on-disk and wire formats are incompatible with
  previous versions. Acceptable pre-1.0.
- **Negative**: ~8-10 B per cache entry additional memory overhead.
  Accounted for in the admission-control constants.
