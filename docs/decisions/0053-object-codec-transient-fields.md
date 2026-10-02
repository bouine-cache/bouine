# ADR-0053: Carry the transient object fields on the wire (codec v6)

- **Status**: Accepted
- **Date**: 2026-10-02
- **Deciders**: @thylong
- **Phase**: phase 6 (hardening)

## Context

A production investigation (doorman logging
`upstream sent duplicate header line: "Date: ..."`, ~100 warnings/hour
in prod-eu) traced to the warm-tier/peer object codec dropping the
`api.Object` transient fields — the ones tagged `json:"-"`, computed
once by `buildObject` at cache-fill time:

- `HasDate`, `HasConnectionList`, `HasNoCacheFields` — hit-path
  serialization gates,
- `RespNoCache`, `RespMustRevalidate` — the RFC 9111 §5.2.2 freshness
  gate inputs consumed by `evaluate`,
- `CacheControl` (pre-merged multi-line value, CDN-Cache-Control
  precedence applied per RFC 9213), `OriginAge` (apparent-age adjusted
  per RFC 9111 §4.2.3).

The binary codec (issue #187) is used by three paths, and all three
decoded flag-less objects:

1. **peer fetch** (`/v1/peer/fetch`) — served objects had
   `HasDate=false`; the slow path (`serveObject`) then emitted **no**
   Date header, and the fast path's peer branch papered over it with a
   lazy `Header.Has` re-derivation,
2. **peer put** (`/v1/peer/put`, write-to-owner) — the owner stored the
   decoded object verbatim with `HasDate=false` while the header map
   kept its `Date` entry. Every subsequent fast-path hit emitted the
   stored Date from the pre-serialized static head **and** a
   synthesized Date from `appendDynamicHeaders` (`if !obj.HasDate`),
   producing the duplicate Date nginx logged. In strong cluster mode
   with 3 replicas, ~2/3 of all origin fills reach the owner exactly
   this way, so most of the fleet's entries carried the defect.
3. **warm tier** — same on promotion back to hot.

Worse than the noise: `RespNoCache`/`RespMustRevalidate` decoding as
false silently disabled revalidation for `no-cache`/`must-revalidate`
responses stored via peer put — an RFC 9111 §5.2.2 conformance
violation and a real overcaching vector. `no-cache="fields"` stripping
(`HasNoCacheFields`) was skipped too, so fields the origin asked to
suppress were served.

The Cache-Control tokenizer lived in `internal/cache` (L3). Fixing the
decode path inside `internal/storage` (L2) would have required an
upward import that depguard forbids.

## Decision

1. **Move the RFC 9111 §5.2 tokenizer to `pkg/header`**
   (`header.ParseCacheControl`, `header.Directives`,
   `header.ParseCacheControlBytes`, plus shared helpers
   `header.EqFold`, `header.ParseIntNoAlloc`). `internal/cache` keeps
   API-compatible aliases, so no call site changes. The shared kernel
   is the sanctioned home (ADR-0050); the parser is a leaf with no
   dependencies.

2. **Bump the object codec to v6**: after the v5 grace block (KeepGrace,
   Pool), append a transient block: the `CacheControl` string, a
   varint `OriginAge`, and a flags byte packing `HasDate`,
   `RespNoCache`, `RespMustRevalidate`, `HasConnectionList`,
   `HasNoCacheFields`. The block is unconditional so field positions
   stay version-stable.

3. **Backfill pre-v6 blobs at decode time**: `decodeObject` restores
   `HasDate` via `Header.Has(Date)` and re-derives the gate flags
   (RespNoCache, RespMustRevalidate, HasNoCacheFields) from the stored
   Cache-Control for v3–v5 blobs. This heals in-flight peer frames and
   on-disk warm blobs during a rolling deploy with no migration step.
   Bare `no-cache="fields"` keeps `RespNoCache=false` — matching
   `buildObject`, which only pre-computes `HasNoCacheFields` for it.

4. **Defense-in-depth at both Date-emitting sites**: the fast-path
   `appendDynamicHeaders` and the slow-path `getOrComputeFastHeader`
   now consult the header map when the flag is false, so no future
   flag-less object path can synthesize a duplicate (or drop the only)
   Date header. The flag short-circuits the scan in the common case;
   the map fallback runs at most once per composed second
   (`ComposedHead` cache), never per hit.

5. **Gate the warm-tier re-derivation** (`TieredStore.Get`): it now
   only fills empty `CacheControl`/`OriginAge` (pre-v6 blobs) instead
   of unconditionally overwriting them, so v6-restored values —
   including the merged multi-line Cache-Control and apparent-age
   OriginAge — survive the promote.

## Consequences

- Wire compatibility: v3/v4/v5 blobs decode correctly during a rolling
  deploy (backfilled); mixed-version peer fleets interoperate
  (old pods encode v5, new pods backfill). No restart migration, no
  warm-tier rewrite required.
- Rolling-deploy window: a v6 pod serving a peer-put from a v5 pod
  backfills the gate flags from headers; the values can differ from
  the fill-time merge only for multi-line Cache-Control or
  CDN-Cache-Control overrides — CDN-CC responses are rare and the
  window closes when the fleet is uniform.
- `internal/cache` no longer owns the tokenizer; its tokenizer unit
  tests moved to `pkg/header`. `cache.Directives` is a type alias, so
  downstream code (including `pkg/api` references) compiles
  unchanged.
- The hit path pays nothing: v6 decode reads 1 string + 1 varint + 1
  byte; the map fallback in `appendDynamicHeaders` is once per second
  per object, not per request.
- Doorman's `duplicate header` warnings stop at the source; the
  served Date is the stored origin Date (RFC 9110 §6.6.1), which
  downstream nginx was keeping anyway.
