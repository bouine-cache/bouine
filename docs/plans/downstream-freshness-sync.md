# Downstream Freshness Sync — Synthesized `CDN-Cache-Control`

- **Status**: Implemented (P1–P6 landed in a single PR on this branch,
  2026-10-23; design approved in review session 2026-10-16)
- **ADR**: [ADR-0063](../decisions/0063-downstream-freshness-sync.md) — context, rationale, rejected alternatives, behavior matrix
- **Related**: [ADR-0011](../decisions/0011-ttl-override-per-route.md) (ttl_override header
  passthrough), [ADR-0022](../decisions/0022-refresh-persist-cycles.md) (downstream-stale
  concern), [ADR-0009 §8](../decisions/0009-cache-state-machine-hardening.md) (CDC parsing
  strictness)

## 1. Problem

`api-stats` runs behind Cloudflare: bouine TTL = 1h,
`stale_while_revalidate` = 10m; ~30% of the route's traffic is served STALE.

Cloudflare behaviors (verified against published docs, 2026-10):

1. `Age` resets on admit/revalidation — bouine's RFC 9111 §4.2.3-correct
   `Age` (`internal/cache/httpdate.go`, `internal/cache/fastpath.go`)
   is discarded downstream.
2. CF ignores `Expires` for its caching decision when CDC-family headers are
   present; `max-age` takes precedence anyway (RFC 9111 §4.2.1).
3. CF caching precedence: `Cloudflare-CDN-Cache-Control` >
   `CDN-Cache-Control` (RFC 9213) > `Cache-Control`; `Cache-Control` is
   proxied to browsers untouched.

Effect: a CF fetch of a stale bouine serve is re-admitted as fresh for a
full CF TTL. End-user ceiling = bouine TTL + SWR + CF TTL ≈ **2h10**
(~2h even without SWR: a CF fetch at t=59m59s serves until ~1h59m59s).

## 2. Goals / Non-Goals

**Goal**: on opt-in routes, no downstream cache trusts content beyond
bouine's clock. End-user ceiling: TTL + SWR + one revalidation cycle
(~1h10).

**Non-goals**: `Cache-Control` mutation (ADR-0011 contract; only the
RFC 9213 header is synthesized); CF API purge integration (§9);
`s-maxage` (implies `proxy-revalidate`, RFC 9111 §5.2.2.10; kills CF async
SWR); `Age` emission changes.

## 3. Design Decisions (approved 2026-10-16)

| # | Decision | Choice |
|---|----------|--------|
| D1 | Stale-serve mode | **Mirror**: `max-age=0, stale-while-revalidate=<SWR remaining>` |
| D2 | Intent-stale (soft purge, SIE) | **Mirror everywhere** — one rule, one code path |
| D3 | Header + config | **`CDN-Cache-Control`** (RFC 9213); per-route `downstream_freshness: passthrough \| sync` |
| D4 | Fill (MISS) serves | **min() rule**: `max-age=min(origin effective max-age, bouine final TTL)` |

Rationale and rejected alternatives: ADR-0063.

## 4. Behavior Specification

Modes differ only in downstream-facing headers; status, body, `Age`,
`X-Cache` are identical in both. `passthrough` (default): no synthesis,
origin CDC-family lines forwarded. `sync`: the §4.1 cap is synthesized.

Per-scenario comparison (all serve classes × both modes, end-user
ceilings): [ADR-0063 "Behavior matrix"](../decisions/0063-downstream-freshness-sync.md#behavior-matrix).
Per-downstream-CDN behavior (Cloudflare vs Fastly vs bunny.net — which
TTL input each honors, `Age` handling, SWR/SIE support, what `sync`
achieves): [ADR-0063 "Downstream portability"](../decisions/0063-downstream-freshness-sync.md#downstream-portability-fastly-bunnynet).
The I8 stale-304 trigger (§4.2) fires in **both** modes.

### 4.1 Synthesis formula (`downstream_freshness: sync`)

| Serve state | Qualifying window | Synthesized `CDN-Cache-Control` |
|---|---|---|
| Fresh (HIT / REVALIDATED) | — | `public, max-age=<TTL − Age>` (floor 0) |
| Fresh, stored `no-cache` object | — | `public, no-cache` — no numeric clock (I2) |
| Stale: natural aging within SWR — incl. soft purge (sets `TTL=0`, rides the SWR machinery, `internal/cache/handler.go:1077` (SoftPurge)) and heuristic staleness | `obj.StaleWhileRevalidate` | `public, max-age=0, stale-while-revalidate=<window remaining>` |
| Stale: stale-if-error serve | `obj.StaleIfError` | same mirror |
| Stale: `stayin_alive` grace (past `TTL+SWR+SIE`) | none | strict `public, max-age=0` — no `swr` |
| Stale: request `max-stale` authorization beyond the object's window | none | strict `public, max-age=0` — no `swr` |

`<window remaining>` is always > 0 on sanctioned serves (`evaluate`
serves stale only within the window); negative/zero `swr` values are
never synthesized.

Serve classes carrying the formula:

| Serve class | Cap |
|---|---|
| Downstream 304 | same cap as the corresponding 200 serve + `Warning: 110` when stale-sourced (RFC 9111 §4.3.4) |
| Range (206) from cache | cap rides along (like `Warning: 110`) |
| Fill / MISS (buffered + streaming) | `public, max-age=min(origin effective max-age, bouine final TTL)` + origin qualitative directives (I2); storable `no-cache` responses → `public, no-cache` (I2) |
| BYPASS / uncacheable-to-shared (`private`, `no-store`, `Vary: *`, Set-Cookie-blocked, unauthorized) | no synthesis — nothing is stored, and a synthesized `public` line must never override an origin privacy marker at a shared downstream cache (RFC 9111 §5.2.2.5, §5.2.2.7) |

`Age`, `X-Cache`, `X-Cache-Source`, `Warning` behavior unchanged.

### 4.2 Synthesis invariants

- **I1** — `Cache-Control` never mutated; byte-for-byte passthrough
  (ADR-0011), every serve class, both modes.
- **I2** — CDC = origin qualitative directives + bouine numeric clock:
  `must-revalidate` / `proxy-revalidate` preserved; `no-store`/`private`
  never stored (`internal/cache/cacheable.go:233-240`) — and therefore
  never synthesized, on any serve class incl. fills (fill-class
  carve-out above). `no-cache` objects ARE stored
  (`internal/cache/cacheable.go:24`) and served REVALIDATED
  (`internal/cache/engine.go:141`) — synthesis `public, no-cache`, never
  a numeric clock (RFC 9111 §5.2.2.4). `public` always present (CF: with
  `Authorization`, absence of `public`/`s-maxage`/`must-revalidate` →
  BYPASS).
- **I3** — origin `CDN-Cache-Control` / `Cloudflare-CDN-Cache-Control` /
  `Surrogate-Control` lines are **replaced** under `sync`, forwarded under
  `passthrough`. Fill-time parsing of origin CDC for bouine's own TTL
  unchanged.
- **I4** — 304s never uncapped (RFC 9111 §4.3.4: a 304's headers update
  the downstream stored entry).
- **I5** — synthesized CDC never flows peer-ward: the fill path ignores
  CDC-family headers on peer fetches (`buildObject`,
  `internal/cache/handler.go:3966`), or TTLs cascade-shrink across the
  cluster.
- **I6** — integer values, unquoted (ADR-0009 §8). No `s-maxage`, ever.
- **I7** — synthesis runs before route response rewrites;
  `header_set` / `header_remove` remain the operator escape hatch.
- **I8** — stale 304s trigger background revalidation, both modes: the
  conditional-304 path (`tryConditional304Fast`,
  `internal/cache/fastheader.go:58`) returns before the 200-path trigger
  (the 200-path trigger in the same case block); the same trigger triple
  (`StaleHit && obj.StaleWhileRevalidate > 0 && isOwnerOrUnmanaged(key)`)
  must fire there too, else CF revalidations (all conditional under
  `sync`) freeze content for the full SWR window.

### 4.3 Ceiling math

CF's entry expires at min(bouine's TTL wall, fetch time + capped
max-age) = bouine's wall. CF revalidates: fresh bouine → 304 +
recomputed remaining (never runs ahead); stale bouine → mirror cap until
bouine's background reval lands. CF revalidations are conditional
requests, so I8 makes them trigger bouine's background reval. End-user
content age ≤ TTL + SWR + one revalidation cycle.

## 5. Implementation Record (P1–P6, landed in this PR)

Implemented in a single PR on the plan branch (user-approved), one
commit per phase below. Anchors are refreshed against the final merged
code — the original draft's anchors were written against the 2026-10-15
tree and drifted ~13–200 lines when the 2026-10-20 conformance batch
landed; this record cites the final locations.

### P1 — Config and wiring ✓

- `internal/config/config.go`: `type DownstreamFreshness string`
  (`passthrough` / `sync`, string-enum convention of `TLSVersion` /
  `EvictionAlgorithm`); `RouteCache.DownstreamFreshness` + godoc
  (D1–D4, CF caveats).
- `internal/config/loader.go`: `validateDownstreamFreshness` (value ∈
  {passthrough, sync}; extracted from `validateRouteCache` to stay
  within its complexity budget).
- `cmd/bouine/cmd/builder.go`: `DownstreamFreshness:
  string(rc.Cache.DownstreamFreshness)` wired into both `HandlerConfig`
  sites.

### P2 — Engine, slow path ✓

- New `internal/cache/downstreamcc.go`: the cap builders
  (`appendServeCDCDirectives`, `appendStaleMirrorDirectives`,
  `appendFillCDCDirectives`, `fillFinalTTLFor`, `removeCDCFamily`) —
  zero-alloc, shared with the fast path.
- `serveObject` (`internal/cache/handler.go:2241`): CDC after the
  `Age`/`X-Cache` switch, before `applyResponseRewrites` (I7);
  origin CDC-family lines removed from the copied head (`removeCDCFamily`)
  rather than via the `getOrComputeFastHeader` / `WriteToFastHTTP` skip
  lists — the pre-built cached header stays mode-agnostic and the serve
  strips the lines per request, which also avoids mode-flip staleness of
  the cached header (fastheader2.go:44, pkg/header/headermap.go:472 are
  unchanged).
- `write304Fast` / `tryConditional304Fast` are now `*Handler` methods
  (`internal/cache/fastheader.go:58,97`): the 304 path had no notion of
  staleness (X-Cache is a literal, no result input), so the cacheResult
  is threaded through both — this was the plumbing this plan's first
  review flagged. The 304 carries the cap + `Warning: 110` when
  stale-sourced; X-Cache labels keep today's values.
- I8: `ServeRequest` `Hit, StaleHit` case (`internal/cache/handler.go`
  — `tryConditional304Fast` call site in the case block): a stale
  conditional 304 calls `triggerBgRevalidate` under the same triple
  gate as the 200 path, both modes.
- `serveRange` (`internal/cache/range.go:25`): 206 cap via
  `setRangeCDC`; stored-header copy skips the CDC family under sync
  (`copyStoredHeaders`).
- Fills: `writeAndMaybeStore` (`internal/cache/handler.go:3065`) and
  `streamMiss` (`internal/cache/stream.go:354`) compute the final TTL
  once at header-processing time via `fillFinalTTL`
  (`internal/cache/handler.go:4132` — the single definition, extracted
  from buildObject's inline ladder) and thread it into `buildObject`
  as a `presetTTL` so the advertised cap and the stored clock are the
  same number. One deviation: the relayed 304 in the vary-miss
  resolver path (`res.Header.CopyToFastHTTP` in the client-own-tag
  branch) is capped strict (`public, max-age=0`): that branch relays
  the origin's 304 while the stored resolver keeps its stale clock
  untouched, so the only honest cap is the strict one.

### P3 — Fast path (zero-alloc budget) ✓

- `internal/cache/fastpath.go`: `FastPathHandler.syncCDC` (inherited
  from the owning `Handler` in `NewFastPathHandler`; store-constructed
  handlers stay passthrough — they share one store across routes, so
  no single mode applies, and the slow path still synthesizes);
  `skipStaticHeader` (:795) gains the policy-conditional CDC-family
  skip; `appendDynamicHeaders` (:585) appends the CDC line — per-second
  dynamic, stack-buffer formatting, zero allocs.
- Composed-head cache (`ComposedHeadFor`, `pkg/api/storage.go:170`):
  the CDC rides in the per-second composed head exactly like `Age` —
  no pkg/api change needed.
- Bench: `BenchmarkGate_FastPath_Hit_SyncCDC` +
  `BUDGETS[FastPath_Hit_SyncCDC]=0` in `bench/run.sh` (drift/stale fail
  both directions). Verified: allocs/op == 0 locally.

### P4 — Cluster isolation (I5) ✓ (verified moot, pinned by test)

The plan assumed peer fetches flow through the HTTP fill parser; they
do not: the owner-first transport is the binary object codec
(`internal/cluster/peerfetch.go` — `storage.DecodeObject`), peer
objects are served and never re-stored, and no `buildObject` call ever
sees peer headers. The synthesized CDC never enters the stored
object's headers (it is written only to the client-facing response
head), so it cannot flow peer-ward by construction. Pinned by
`TestSyncCDC_PeerWardIsolation` and `TestSyncCDC_PeerResponseCDCIgnored`
in `internal/cache/downstreamcc_test.go`.

### P5 — Tests, conformance, benchmarks ✓

- `internal/cache/downstreamcc_test.go`: the serve-class matrix —
  fresh HIT (wall remaining, must-revalidate carry-over), stored
  no-cache REVALIDATED (never numeric), SWR mirror, soft purge mirror
  (window from purge time), stayin-alive grace + max-stale strict
  clamp, origin CDC-family replacement, fill min() (both override
  directions, qualitative carry-over), uncacheable-to-shared
  (private) never synthesizes, 304 cap + Warning, 206 cap, fast-path
  head bytes, passthrough byte-for-byte.
- I8: `TestSyncCDC_I8_Stale304TriggersBgRevalidate` (both modes,
  synctest, deterministic) and `..._Fresh304TriggersNothing`.
- `internal/config/loader_test.go`: `TestValidate_DownstreamFreshness`.
- `make conformance`: default passthrough is a no-op — score unchanged
  (CI gate).

### P6 — Docs and ops ✓

- ADR-0063 (revised with this PR: renumber note for the 0060
  collision resolution).
- `docs/architecture.md` §3.1: L3 feature-list addition only.
- Runbook `docs/runbook/58-downstream-freshness-sync.md` + README row.
- `CHANGELOG.md` entries under `## [Unreleased]`.
- Dashboard: `sync-cdc` badge in the route features panel
  (`internal/dashboard/templates/models.go` — a models.go change only,
  no `.templ` edit, so no `templ generate` output changed).
- Optional metric deferred: `bouine_downstream_cc_serves_total{route,
  mode}` does not clear the cardinality bar's *usefulness* test — the
  mode is already visible per route in config; revisit only if
  operators ask for serve-rate visibility.

## 6. Verification Gates (AGENTS.md §16.4)

Touches `internal/{cache,server,cluster}` → `make lint`, `make test`,
`make bench-gate` (+ baseline), `make conformance`, `make integration`.
Hit-path allocs/op unchanged — enforced via `BUDGETS`.

## 7. Risks

| Risk | Handling |
|---|---|
| CF zone rules (Edge Cache TTL, `set_cache_control`) override CDC | Runbook + config godoc; not fixable header-side |
| Always Online zones ignore `swr`/SIE | Mirror degrades to strict at CF; documented |
| Streaming fill: TTL not final before first flush | P2: final TTL computed at header-processing time |
| Composed-head / FastHeader staleness after refresh | Existing invalidation-on-store; new tests pin CDC after 304-refresh |
| CF keeps pre-purge content ≤ 1 reval cycle after soft purge | Documented (D2); hard purge / CF API purge for immediate semantics |
| Origin outage during grace: CF-side visitors pay a sync reval RTT per request | Bouine serves stale from the grace store; self-heals via I8 |
| CDC + `Authorization` → CF BYPASS without `public` | I2 always emits `public` |
| Non-CF downstreams: Fastly caps at the origin's clock (`Age` subtraction, CDC inert); bunny.net ignores CDC-family entirely | Documented in ADR-0063 "Downstream portability"; runbook covers zone-side mitigations; `sync` degrades to inert, never unsafe |

## 8. Open Questions

None blocking. Deferred: dashboard panel placement; CF API purge
integration.

## 9. Future Work

CF API purge on `PURGE`/soft purge — invalidates already-admitted CF
entries mid-freshness. Additive, independent of this plan.
