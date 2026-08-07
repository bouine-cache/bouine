# ADR-0030: 128-bit cache key via XXH128

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
the wrong-body class of bug on the primary-key axis: two distinct
canonical requests cannot occupy the same map entry. The Vary
dimension remains a single 64-bit xxhash64 (`WithVary` XORs one
`uint64` varyHash into both halves), unchanged from the prior design —
a Vary-hash collision can still produce a wrong body. The 128-bit key
removes the weakest link (the primary key), not every link.

## Decision

Replace `type Key uint64` with `type Key [16]byte` — a single XXH128
hash stored in the canonical big-endian representation from
`xxhash.Uint128.Bytes()` (high 64 bits first, then low 64 bits, each
in big-endian byte order).

- **First 8 bytes (k[:8])**: the high 64-bit half of the XXH128 hash.
  Used for shard selection, ring hashing, and access sampling via
  `Hash64()`. This avoids a hot-path hash call on the full 16 bytes for
  routing decisions that only need 64 bits of uniformity.
- **Second 8 bytes (k[8:])**: the low 64-bit half of the XXH128 hash.
  Both halves are computed in a single `xxhash.Sum128` call — no
  `sync.Pool`, no second hash, no seeded digest.

### Why `[16]byte` over a two-field struct

A `[16]byte` array is directly usable as a Go map key with no custom
`hash()` / `equal()` required. It is comparable, copyable, and its
`unsafe.Sizeof` is exactly 16. A struct `{lo, hi uint64}` would require
the same properties but adds field naming that implies a
primary/guard split. The array form makes it clear that both halves are
equally part of the key identity.

### Why XXH128 over 2×xxhash64

XXH128 is a single hash function that produces two independent 64-bit
halves (high and low) in one pass over the data. The alternative — two
independent xxhash64 calls with distinct seeds — was rejected because it
needs a `sync.Pool` of seeded digests to stay zero-alloc, a second hash
call on every key construction, and a seed constant duplicated across
packages (`cache` and `testutil`). XXH128 needs none of that:
`xxhash.Sum128` is a one-shot function with no heap state — zero
allocations by construction, not by pooling.

`cespare/xxhash/v2` is already on the pre-approved dependency allow-list
(AGENTS.md §5). The `bouine-cache/xxhash` fork adds XXH3-128 to the same
package with the same module path under a `replace` directive; no new
dependency. The canonical big-endian byte layout from `Uint128.Bytes()`
matches the official XXH128 canonical representation, making
cross-language debugging and external tooling straightforward.

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
| Key JSON (`pkg/api`) | decimal uint64 | `[hi, lo]` decimal array | 128-bit explicit |

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

- **Positive**: Eliminates the wrong-response-body class of bug on the
  primary-key axis. Map lookup is a full 128-bit collision check. The
  Vary dimension remains 64-bit (a future widening would need to mix
  both halves independently, not XOR the same varyHash into both).
- **Positive**: No new dependencies. xxhash is already approved; the
  fork adds XXH128 to the same package.
- **Positive**: 0 allocations on the hit path — `Sum128` is a one-shot
  function with no heap state, no `sync.Pool` needed.
- **Negative**: `map[[16]byte]` lookup is ~2-4 ns slower than
  `map[uint64]` due to the larger hash input. Acceptable per the
  performance budget.
- **Negative**: All on-disk and wire formats are incompatible with
  previous versions. Acceptable pre-1.0.
- **Negative**: ~8-10 B per cache entry additional memory overhead.
  Accounted for in the admission-control constants.
- **Negative**: The `replace` directive in `go.mod` points to a local
  fork until the XXH128 addition is upstreamed or the fork is published
  with a stable tag.
