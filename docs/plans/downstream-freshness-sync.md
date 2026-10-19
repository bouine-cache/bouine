# Downstream Freshness Sync — Synthesized `CDN-Cache-Control`

- **Status**: Proposed (design approved in review session, 2026-10-16)
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
   `Age` (`internal/cache/handler.go:1992`, `internal/cache/fastpath.go:588`)
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
| Stale: natural aging within SWR — incl. soft purge (sets `TTL=0`, rides the SWR machinery, `internal/cache/handler.go:1089-1093`) and heuristic staleness | `obj.StaleWhileRevalidate` | `public, max-age=0, stale-while-revalidate=<window remaining>` |
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
  `internal/cache/handler.go:3485`), or TTLs cascade-shrink across the
  cluster.
- **I6** — integer values, unquoted (ADR-0009 §8). No `s-maxage`, ever.
- **I7** — synthesis runs before route response rewrites;
  `header_set` / `header_remove` remain the operator escape hatch.
- **I8** — stale 304s trigger background revalidation, both modes: the
  conditional-304 path (`tryConditional304Fast`,
  `internal/cache/handler.go:1495`) returns before the 200-path trigger
  (`internal/cache/handler.go:1510-1513`); the same trigger triple
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

## 5. Implementation Phases

PRs ≤ 400 changed lines each (AGENTS.md §15.4); P2–P5 each land behind
the default-off config, independently mergeable.

### P1 — Config and wiring

- `internal/config/config.go`: `type DownstreamFreshness string`
  (`passthrough` / `sync`, string-enum convention of `TLSVersion` /
  `EvictionAlgorithm`); `RouteCache.DownstreamFreshness` + godoc
  (D1–D4, CF caveats).
- `internal/config/loader.go` (`config.Validate`): value ∈
  {passthrough, sync}; no cross-field constraints.
- `cmd/bouine/cmd/builder.go`: wire `RouteCache.DownstreamFreshness` →
  `HandlerConfig` (next to `defaultSWR` wiring, ~:421, :600).

### P2 — Engine, slow path

- New `internal/cache/downstreamcc.go`: pure cap helpers
  (fresh-remaining, stale mirror window, fill `min()`,
  qualitative-directive carry-over), table-tested.
- `serveObject` (`internal/cache/handler.go:1982`): set CDC after the
  `Age`/`X-Cache` block; origin CDC-family skip policy-conditional in
  `getOrComputeFastHeader` / `WriteToFastHTTP` skip lists
  (`internal/cache/fastheader2.go:44`, `pkg/header/headermap.go:472`).
- `write304Fast` (`internal/cache/fastheader.go:86`): cap + `Warning:
  110` when stale-sourced.
- I8: `ServeRequest` `Hit, StaleHit` case (`internal/cache/handler.go:1495`)
  — `tryConditional304Fast` firing on a `StaleHit` calls
  `triggerBgRevalidate` before returning, both modes.
- `internal/cache/range.go:52`: cap on 206 serves.
- Fill paths `writeAndMaybeStore` (~`handler.go:2631`) and
  `internal/cache/stream.go:374`: `min()` rule. Final TTL (override,
  negative ladder, `JitterTTL` applied once) computed at
  header-processing time, before first body-byte flush on streaming;
  same value reused for storage.
- `TestOverrideTTL_ForwardsOriginalCacheControlHeader`
  (`internal/cache/override_test.go:59`): sync-mode sibling — CDC
  synthesized, `Cache-Control` untouched.

### P3 — Fast path (zero-alloc budget)

- `internal/cache/fastpath.go`: `skipStaticHeader`/`shouldSkipHeader`
  (:747-781) gain the policy-conditional CDC-family skip;
  `appendDynamicHeaders` (:570-617) appends the CDC line next to `Age` —
  per-second dynamic, reused buffers, zero allocs.
- Composed-head cache (`ComposedHeadFor`, `pkg/api/storage.go:170`): CDC
  appended per serve like `Age`, not baked into the cached head;
  invalidation-on-refresh verified.
- Bench: `BenchmarkGate_*` + `BUDGETS` entries in `bench/run.sh`
  (drift/stale fail both directions). Acceptance: allocs/op == 0 on hit
  path; byte-identical to baseline in passthrough mode.

### P4 — Cluster isolation (I5)

- Fill side: skip the CDC-family parse in `buildObject`/respCC on peer
  fetches (owner-first lookup transport).
- Test: peer response carrying CDC → stored TTL from origin CC only; no
  cascade across a 3-node integration cluster.

### P5 — Tests, conformance, benchmarks

- Unit matrix (table-driven, same package): each serve class × mode ×
  {override longer, override shorter, SWR, SIE, soft purge, heuristic,
  no-freshness origin, error fill, Date-less object, stored no-cache
  object (incl. its fill), **uncacheable-to-shared fills (`private`,
  `no-store`): no CDC synthesized, `Cache-Control` forwarded
  byte-for-byte**, stayin-alive grace serve, `max-stale`-authorized
  serve} — assert exact CDC bytes, `Cache-Control` untouched, `Age`
  unchanged.
- 304 cap tests (stale ETag match → 304 carries mirror cap + `Warning:
  110`); 206 cap tests; streaming-fill `min()` test (body > streaming
  threshold).
- I8 trigger tests: conditional reval of stale object triggers background
  reval (both modes); fresh-hit 304 triggers nothing; non-owner does not
  trigger. Deterministic (synctest), injected clock, no sleeps
  (AGENTS.md §8).
- `make conformance`: default passthrough is a no-op — score unchanged.
- `make bench-gate` + baseline comparison per §7 gates.

### P6 — Docs and ops

- ADR-0063 (accompanies this plan).
- `docs/architecture.md`: L3 row feature-list addition only (not exit
  criteria).
- Runbook (new `docs/runbook/58-downstream-freshness-sync.md`): zone Cache
  Rules with **Edge Cache TTL** ("ignore origin cache control") or
  `set_cache_control` override CDC (cap voided); **Always Online** ignores
  `swr`/SIE (mirror → strict); Browser Cache TTL rules govern the eyeball
  TTL independently; verification via `CF-Cache-Status` (expect `UPDATING`
  during bouine's SWR window) and direct curl against bouine; per-CDN
  portability notes from ADR-0063 (Fastly `Age`-subtraction residual under
  `ttl_override`, bunny.net CDC-blindness and its "Override Cache Time"
  Edge Rules).
- `CHANGELOG.md` entry under `## [Unreleased]`.
- Dashboard: route config panel surfaces `downstream_freshness` (templ
  regen committed alongside `_templ.go`, AGENTS.md §4).
- Optional metric `bouine_downstream_cc_serves_total{route, mode}` — only
  if it clears the §9 cardinality bar.

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
