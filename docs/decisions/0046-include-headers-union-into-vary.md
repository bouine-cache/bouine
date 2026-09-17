# ADR-0046: Fold `cache.key.include_headers` into the stored Vary

- **Status**: Accepted
- **Date**: 2026-09-17
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: issue #632, threat-model T06, RFC 9111 §4.1, RFC 9110 §5.2

## Context

`docs/architecture.md` and the threat model (T06) promised a per-route
allow-list of request headers that participate in the cache key
(`cache.key.include_headers`), but the field did not exist: the strict
YAML decoder (`dec.KnownFields(true)`) rejected the flagship config
example outright (issue #632). The use case is an origin that varies
its responses by a request header (e.g. `Accept-Language`) but does
not — or cannot — declare it in `Vary`.

The cache has six copies of the variant-key hash-input construction
(`VariantKey`, `VariantKeyFast`, `variantKeySlow`, `variantKeyFromRaw`,
`BuildVaryKey`, and the inline fastpath copy) kept in lockstep by parity
tests precisely to prevent cross-variant body swaps. Every consumer of
the variant key — hit path, H1 fast path, peer gates
(`internal/cache/handler.go`, `internal/cache/fastpath.go`,
`internal/cluster/peerfetch.go`) — reads the stored
`Object.VaryValue`/`Object.VaryKey`, which are serialized in the
stored object (`internal/storage/codec.go`).

## Decision

`include_headers` adds request headers to the variant key by folding
the include list into the stored `VaryValue`/`VaryKey` at object-build
time — the **union** of the response's `Vary` field names and the
route's include list, never a replacement. A new helper,
`effectiveVary`, computes the union at every key-construction site
(`buildObject`, the 304-revalidation path, the buffered and streaming
store paths). Lookup-side and peer-side code is unchanged: it reads
the stored value, which now carries the union. An include-listed
header inherits whatever value normalization each variant-key site
already applies to Vary fields, so "en, FR" and "fr, en" collapse
exactly as they would for an origin-declared Vary field.

A nil policy or empty include list is a zero-allocation passthrough to
`joinedVary`, so the include-free miss path stays on its alloc budget.

Validation: capped at 16 entries; every comparison runs on the
**trimmed** entry (matching `NewKeyPolicy`'s storage normalization, so
`" *"` and `"x, y"` cannot slip past on padding). Rejected: `*`
(padded or not — a wildcard Vary is unkeyable), whitespace-only
entries, non-token entries (anything but an RFC 9110 §5.1 tchar
sequence — a comma in an entry would be one union field to
`effectiveVary` but two Vary fields to the variant-key builders),
case-insensitive duplicates, and overlap with `exclude_headers` (an
excluded header force-included into the key would silently collapse
variants — the overlap check is the T06 control, not cosmetic).

## Consequences

### Positive
- One hashing path: the six variant-key constructions need no changes,
  so the parity-test contract and the zero-alloc hit-path budgets hold.
- Cluster consistency comes free: `VaryValue`/`VaryKey` serialize with
  the stored object.
- Include-only routes (origin sends no `Vary` at all) flow through the
  existing primary-entry-as-Vary-resolver machinery unchanged.

### Negative / trade-offs
- The stored `VaryValue` contains fields the origin never declared, so
  `Vary`-mirroring tooling sees the union. This is visible in
  `/v1/debug/cachecheck` output and peer-fetch logs.
- A union larger than `maxVaryFields` (16) falls back to the
  allocation path; the config cap keeps this bounded.
- A 304 that changes `Vary` forces a `VaryKey` recompute on
  revalidation (`refreshFrom304`), so the merged union and its hash
  stay a matching pair; a `Vary: *` 304 blanks `VaryKey` (fail-safe:
  the object is unkeyable until re-fetched, failed hits not wrong
  bodies).

### Risks
- Mixed-config clusters (nodes disagreeing on the include list) store
  and resolve variants under different keys. This is the same
  pre-existing hazard class as `exclude_headers`: the variant gates
  fail safe (miss, never a wrong body). Documented in the runbook;
  not solved here.

## Alternatives considered

- **A parallel include-headers hashing path**: rejected. It would
  require teaching six hash-input constructions about include lists;
  missing one is precisely the cross-variant body-swap bug class the
  peer gates exist to prevent.
- **Reusing `exclude_headers` with inverted semantics**: rejected —
  conflates two opposite operations on one field and breaks existing
  configs.
- **Forwarding the include list to the origin and waiting for it to
  echo `Vary`**: rejected — the use case is origins that cannot send
  `Vary`.
