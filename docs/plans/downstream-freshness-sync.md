# Downstream Freshness Sync — Synthesized `CDN-Cache-Control`

- **Status**: Proposed (design approved in review session, 2026-10-16)
- **ADR**: [ADR-0060](../decisions/0060-downstream-freshness-sync.md)
- **Related**: [ADR-0011](../decisions/0011-ttl-override-per-route.md) (ttl_override header
  passthrough), [ADR-0022](../decisions/0022-refresh-persist-cycles.md) (downstream-stale
  concern), [ADR-0009 §8](../decisions/0009-cache-state-machine-hardening.md) (CDC parsing
  strictness)

## 1. Problem

`popularity-stats` runs behind Cloudflare with bouine TTL = 1h and
`stale_while_revalidate` = 10m. 30% of CSR traffic is served STALE by bouine
while background revalidation runs.

Cloudflare behaviors that create the amplification (all verified against
Cloudflare's published docs, 2026-10):

1. **`Age` is discarded at admit.** CF's `Age` is "the number of seconds an
   asset has been in *Cloudflare's* cache since it was admitted or last
   revalidated" — it resets its own clock on every fill. Bouine's
   RFC 9111 §4.2.3-correct `Age` (`internal/cache/handler.go:1992`,
   `internal/cache/fastpath.go:587`) signals into a void.
2. **`Expires` is not a lever.** `max-age` takes precedence over `Expires`
   (RFC 9111 §4.2.1), and CF ignores `Expires` entirely for its caching
   decision when `CDN-Cache-Control`-family headers are in play.
3. **`CDN-Cache-Control` (RFC 9213) is the reliable input.** CF evaluates
   `Cloudflare-CDN-Cache-Control` > `CDN-Cache-Control` > `Cache-Control`
   for its own caching decision while proxying `Cache-Control` untouched to
   browsers.

Result: a CF fetch of a stale bouine serve is re-admitted fresh for CF's own
TTL. End-user ceiling today is bouine TTL + SWR + CF TTL ≈ **2h10** — and even
without SWR, fresh re-aging alone yields ~2h (CF fetch at t=59m59s keeps
serving until ~1h59m59s).

ADR-0011 guarantees byte-for-byte header passthrough, so no existing knob can
fix this. ADR-0022 option 3 rejected downstream-stale mitigation because it
"depends on downstream behavior"; this plan *commands* the downstream instead.

## 2. Goals / Non-Goals

**Goal**: on routes opting in, no downstream cache trusts content beyond
bouine's clock. End-user ceiling becomes TTL + SWR + one revalidation cycle
(~1h10 for popularity-stats, typically one RTT past TTL).

**Non-goals**:

- No `Cache-Control` mutation — ADR-0011's browser/CDN passthrough contract
  is preserved; only the RFC 9213 CDN-targeted header is synthesized.
- No CF API purge integration (future feature; see §9).
- No `s-maxage` use (implies `proxy-revalidate`, kills CF async SWR, and CF
  docs explicitly warn against combining it with `stale-while-revalidate`).
- No change to `Age` emission.

## 3. Design Decisions (approved 2026-10-16)

| # | Decision | Choice |
|---|----------|--------|
| D1 | Stale-serve mode | **Mirror**: `max-age=0, stale-while-revalidate=<SWR remaining>` |
| D2 | Intent-stale (soft purge, SIE) | **Mirror everywhere** — one rule, one code path |
| D3 | Header + config | **`CDN-Cache-Control`** (RFC 9213); per-route `downstream_freshness: passthrough \| sync` |
| D4 | Fill (MISS) serves | **min() rule**: `max-age=min(origin effective max-age, bouine final TTL)` |

### D1 rationale
Mirror keeps visitor latency flat during the window (CF serves `UPDATING`
from its cache, revalidates asynchronously, collapsed per entry — CF's own
documented async-SWR model, and CF's own workaround pattern "max-age +
stale-while-revalidate, never s-maxage"). Strict (`max-age=0`) taxes every
request on ~30% of CSR traffic with a blocking CF→bouine reval while serving
the same bytes either way — bouine answers revals from its stale store.
Lazy (`max-age=<SWR remaining>`) propagates fresh content only at window
end and misrepresents stale content as fresh.

### D2 rationale
Soft purge sets `TTL=0` and rides the normal SWR machinery
(`internal/cache/handler.go:1089-1093`); its stale serves are
byte-identical to SWR stale serves. Making the CF hop stricter than bouine
itself (which serves the pre-purge bytes during the window — that is the
soft-purge contract) buys no content freshness, only latency. Hard purge
exists for stop-serving semantics. SIE serves must be capped (uncapped,
origin-down stale content would be admitted at CF as fresh for a full TTL);
mirror preserves chain resilience. Header synthesis can never invalidate
CF entries that are fresh by their own clock mid-window — immediate
invalidation is CF API purge territory (§9).

### D3 rationale
`CDN-Cache-Control` is the IETF standard for exactly this; CF honors it
above `Cache-Control`, and proxies it to any further chained CDN (correct:
no cache should trust beyond bouine's clock). `Cloudflare-CDN-Cache-Control`
would be vendor lock-in and leave other chained CDNs uncapped. Enum config
(not bool) leaves room for future values (e.g. a strict variant) without a
breaking migration; `passthrough` default preserves ADR-0011 and cache-tests
conformance.

### D4 rationale
The invariant "downstream never trusts beyond bouine's clock" must hold on
fills too. `min()` is a no-op when origin and bouine agree (popularity-stats:
`min(3600, 3600)`), preserves ADR-0011's decoupling case (origin 60s,
override 1h → CF gets 60s), and caps the inverse case (origin 24h, override
1h → CF gets 1h, not 24h). Error fills inherit the negative-TTL ladder
value. Cost: touches the fill path including streaming; all cold path.

### Review fixes (2026-10-16, post-linus-review)

1. **I8** — stale 304s trigger background revalidation, both modes: the
   304 path returned before the existing trigger, so conditional downstream
   traffic (all of CF's revalidations under `sync`) froze content for the
   full SWR window.
2. **No-cache synthesis** — stored `no-cache` objects synthesize
   `public, no-cache`, never a numeric clock; they ARE stored and served
   REVALIDATED, and a `max-age` would license CF to serve unvalidated,
   violating RFC 9111 §5.2.2.4.
3. **Clamped mirror window** — `swr=<window remaining>` with the window
   sourced per staleness reason (§4.1); stayin-alive grace and request
   `max-stale` serve strict `max-age=0` — no negative/zero `swr` values, no
   client-authorized staleness baked into CF entry policy.
4. **Fill no-cache carve-out** — `no-cache` responses are storable
   (`internal/cache/cacheable.go:24`) and carry no usable `max-age`; the
   fill min() rule would have shipped `public, max-age=<ttl_default or
   heuristic>` and admitted a no-revalidation-required CF entry for a full
   bouine TTL on a route whose origin demanded per-reuse validation. The
   I2 qualifier (no numeric clock for `no-cache`) extends to fills (§4.1);
   CF traffic identical to passthrough.

## 4. Behavior Specification

### 4.1 Synthesis formula (`downstream_freshness: sync`)

One formula covers every cache serve — no per-serve-class special cases:

| Object state at serve time | Synthesized `CDN-Cache-Control` |
|---|---|
| Fresh (HIT / REVALIDATED) | `public, max-age=<TTL − Age>` (floor 0) |
| Fresh, stored `no-cache` object | `public, no-cache` — no numeric clock (I2) |
| Stale, qualifying window > 0 | `public, max-age=0, stale-while-revalidate=<window remaining>` |
| Stale, qualifying window ≤ 0 | `public, max-age=0` (strict — no `swr`) |

The **qualifying window** is whichever sanctioned the serve:

| Staleness reason | Window |
|---|---|
| Natural aging within SWR — incl. soft purge (sets `TTL=0`, rides the SWR machinery) and heuristic staleness | `obj.StaleWhileRevalidate` |
| Stale-if-error serve | `obj.StaleIfError` |
| `stayin_alive` grace (past `TTL+SWR+SIE`) | none — strict (grace is an outage posture; downstream must check per request) |
| Request `max-stale` authorization beyond the object's window | none — strict (per-request client authorization must not become CF entry policy) |

Serve classes carrying the formula:

| Serve class | Cap |
|---|---|
| Downstream 304 | **same cap as the corresponding 200 serve** + `Warning: 110` when stale-sourced (RFC 9111 §4.3.4) |
| Range (206) from cache | cap rides along (like `Warning: 110` today) |
| Fill / MISS (buffered and streaming) | `public, max-age=min(origin effective max-age, bouine final TTL)`, plus origin qualitative directives (I2) — **except `no-cache` responses: `public, no-cache`, never a numeric clock (I2)** |
| BYPASS / uncacheable | passthrough (nothing stored, no bouine clock exists) |

`Age`, `X-Cache`, `X-Cache-Source`, `Warning` behavior is unchanged. The
strict-when-windowless clamp composes with the I8 trigger: every downstream
conditional revalidation during grace revalidates synchronously *and*
triggers bouine's background revalidation, so the chain self-heals on the
first healthy origin response rather than waiting out the window.

### 4.2 Synthesis invariants

- **I1 — `Cache-Control` is never mutated.** Byte-for-byte passthrough
  (ADR-0011) holds on every serve class, both modes.
- **I2 — CDC = origin qualitative directives + bouine numeric clock.**
  `must-revalidate` / `proxy-revalidate` from the origin CC are preserved in
  the synthesis; numeric freshness always comes from bouine's clock. Objects
  with `no-store` / `private` are never stored, so those directives never
  reach synthesis. **`no-cache` objects ARE stored** (explicit cacheability
  marker, `internal/cache/cacheable.go:24`) and served REVALIDATED after
  per-request origin revalidation (`internal/cache/engine.go:141`) — their
  synthesis is `public, no-cache`, never a numeric clock: a `max-age` would
  license CF to serve without validation, which RFC 9111 §5.2.2.4 forbids
  and the origin explicitly rejected. CF traffic is identical to passthrough
  (CF "caches and always revalidates"). `public` is always included (CF: CDC
  presence turns Origin Cache Control on; with an `Authorization` request
  header, anything other than
  `public`/`s-maxage`/`must-revalidate` yields BYPASS).
- **I3 — origin CDC-family lines are replaced, never forwarded, when sync is
  active.** `CDN-Cache-Control` and `Cloudflare-CDN-Cache-Control` from the
  origin are dropped from serves (the synthesis is authoritative);
  `Surrogate-Control` likewise (CF ignores `Cache-Control` when
  `Surrogate-Control` is present). Under `passthrough`, all are forwarded as
  today. Bouine's *fill-time* parsing of origin CDC for its own TTL is
  unchanged.
- **I4 — 304s are never uncapped.** RFC 9111 §4.3.4: a 304's headers update
  the downstream stored entry; an uncapped 304 would re-freshen CF's entry
  unboundedly.
- **I5 — synthesized CDC never flows peer-ward.** Bouine's fill path
  consumes CDC (RFC 9213, `buildObject`, `internal/cache/handler.go:3485`);
  peer fetches must ignore CDC-family headers on responses from bouine
  peers, or TTLs cascade-shrink across the cluster. Fill-side ignore is the
  authoritative fix (belt and braces: the peer transport may also strip
  them).
- **I6 — integer values, unquoted.** ADR-0009 §8 made the CDC parser reject
  quoted-string directive values; synthesis must emit what we accept. No
  `s-maxage`, ever.
- **I7 — synthesis runs before route response rewrites.** `header_set` /
  `header_remove` (`applyResponseRewrites`) remain the operator's escape
  hatch applied after synthesis.
- **I8 — stale 304s trigger background revalidation (both modes).** The
  conditional-304 path (`tryConditional304Fast`,
  `internal/cache/handler.go:1495`) returns before the existing trigger
  (`internal/cache/handler.go:1510-1513`), so a downstream revalidation of a
  stale object is answered from bouine's stale store — confirming content the
  origin was never asked about — and triggers nothing. Under `sync`, CF's
  revalidations are precisely such conditional requests; without a trigger
  there, they freeze content for the full SWR window and the §4.3 ceiling is
  unreachable. The trigger fires under the same triple gate as the
  200-path trigger (`disp.Decision == StaleHit &&
  obj.StaleWhileRevalidate > 0 && isOwnerOrUnmanaged(key)`). This
  also fixes a pre-existing passthrough hole: downstream conditional
  revalidations during SWR today refresh nothing.

### 4.3 Ceiling math

CF's entry for a given fetch expires at min(bouine's TTL wall, fetch time +
capped max-age) = exactly bouine's wall. After that, CF revalidates: against
a fresh bouine → 304 + recomputed remaining (never runs ahead); against a
stale bouine → mirror cap (UPDATING, no visitor latency) until bouine's
background revalidation lands. CF's revalidations are conditional requests,
so I8 makes that reval traffic itself trigger bouine's background
revalidation — the "typically one RTT past TTL" bound holds even when all
arriving traffic is CF revalidation. End-user content age ≤ TTL + SWR + one
revalidation cycle.

## 5. Implementation Phases

PRs ≤ 400 changed lines each, per AGENTS.md §15.4. Phases P2–P5 each land
behind the default-off config, so each is independently mergeable.

### P1 — Config and wiring

- `internal/config/config.go`: `type DownstreamFreshness string` with
  `DownstreamFreshnessPassthrough = "passthrough"` /
  `DownstreamFreshnessSync = "sync"` (mirrors `TLSVersion` /
  `EvictionAlgorithm` string-enum convention); `RouteCache.DownstreamFreshness`
  field + godoc covering D1–D4 semantics and CF caveats.
- `internal/config/loader.go` (`config.Validate`): value ∈ {passthrough,
  sync}; no cross-field constraints (composes with `ttl_override`, SWR, SIE,
  `jitter_percent`, negative TTL, stayin_alive).
- `cmd/bouine/cmd/builder.go`: wire `RouteCache.DownstreamFreshness` →
  `HandlerConfig` (next to `defaultSWR` wiring, ~:421, :600).

### P2 — Engine, slow path (cold-to-warm path)

- New `internal/cache/downstreamcc.go`: cap computation helpers
  (fresh-remaining, stale mirror window, fill `min()`, qualitative-directive
  carry-over). Pure functions, table-tested.
- `serveObject` (`internal/cache/handler.go:1982`): compute + set CDC after
  the `Age`/`X-Cache` block; origin CDC-family skip is
  policy-conditional in the static header builders
  (`getOrComputeFastHeader` / `WriteToFastHTTP` skip lists,
  `internal/cache/fastheader2.go:44`, `pkg/header/headermap.go:472`).
- `write304Fast` (`internal/cache/fastheader.go:86`): attach cap + `Warning:
  110` when stale-sourced.
- Stale-304 trigger (I8): in `ServeRequest`'s `Hit, StaleHit` case
  (`internal/cache/handler.go:1495`), when `tryConditional304Fast` fires on
  a `StaleHit` object under the standard trigger triple, call
  `triggerBgRevalidate` before returning — both modes, not gated on `sync`.
- `internal/cache/range.go:52`: cap on 206 serves.
- Fill paths: `writeAndMaybeStore` (~`handler.go:2631`) and
  `internal/cache/stream.go:374` — `min()` rule. **Timing constraint**: the
  final TTL (override, negative ladder, `JitterTTL` applied once) must be
  computed at header-processing time, before the first body byte flushes on
  the streaming path; the same value is reused for storage (no double
  jitter).
- ADR-0011 pin extension: `TestOverrideTTL_ForwardsOriginalCacheControlHeader`
  (`internal/cache/override_test.go:59`) gets a sync-mode sibling asserting
  CDC synthesis while `Cache-Control` stays untouched.

### P3 — Fast path (hot path, zero-alloc budget)

- `internal/cache/fastpath.go`: `skipStaticHeader`/`shouldSkipHeader`
  (:747-781) gain the policy-conditional CDC-family skip; `appendDynamicHeaders`
  (:570-617) appends the CDC line next to `Age` — per-second dynamic, same
  cost class as the existing `Age` append, reused buffers, zero allocs.
- Composed-head cache (`ComposedHeadFor`, `pkg/api/storage.go:170`): CDC is
  appended per serve like `Age`, not baked into the cached head — verify no
  interaction with per-second head caching; verify invalidation on refresh
  keeps working (existing store machinery).
- Bench: new `BenchmarkGate_*` entries + `BUDGETS` map entries in
  `bench/run.sh` (gate fails on drift/stale — both directions). Acceptance:
  allocs/op == 0 on hit path, byte-identical to baseline in passthrough mode.

### P4 — Cluster isolation (I5)

- Fill side: when the upstream response arrives via peer fetch
  (owner-first lookup transport, `internal/cluster` + cache layer), the
  CDC-family parse in `buildObject`/respCC is skipped.
- Test: peer fetch response carrying CDC → stored TTL derived from origin
  CC only; no cascade across a 3-node integration cluster.

### P5 — Tests, conformance, benchmarks

- Unit matrix (table-driven, same package): each serve class × mode ×
  {override longer, override shorter, SWR, SIE, soft purge, heuristic,
  no-freshness origin, error fill, Date-less object, stored no-cache
  object (incl. its fill), stayin-alive grace serve, `max-stale`-authorized
  serve} — assert exact CDC bytes, CC untouched, Age unchanged.
- 304 cap tests (stale ETag match → 304 carries mirror cap + `Warning: 110`);
  206 cap tests; streaming-fill `min()` test (body > streaming threshold).
- I8 trigger tests: conditional reval of a stale object triggers background
  revalidation (both sync and passthrough); fresh-hit 304 triggers nothing;
  non-owner does not trigger — deterministic (synctest), no sleeps.
- Deterministic clocks per AGENTS.md §8 (injected clock, no sleeps).
- `make conformance` (cache-tests): default passthrough must be a no-op —
  score unchanged.
- `make bench-gate` + baseline comparison per §7 gates.

### P6 — Docs and ops

- ADR-0060 (accompanies this plan).
- `docs/architecture.md`: scope addition to the L3 row — user approval is
  this design session; update only the feature list, not exit criteria.
- Runbook (new `docs/runbook/57-downstream-freshness-sync.md`): CF caveats —
  zone Cache Rules with **Edge Cache TTL** ("ignore origin cache control")
  or `set_cache_control` override CDC (cap voided); **Always Online**
  ignores `stale-while-revalidate`/`stale-if-error` (mirror degrades to
  strict, not unsafe); Browser Cache TTL rules govern the eyeball TTL
  independently; how to verify with `CF-Cache-Status` (expect UPDATING
  during bouine's SWR window) and curl against bouine directly.
- `CHANGELOG.md` entry under `## [Unreleased]`.
- Dashboard: route config panel surfaces `downstream_freshness` (templ
  regen committed alongside `_templ.go`, AGENTS.md §4).
- Optional metric: `bouine_downstream_cc_serves_total{route, mode}` —
  low-cardinality; include only if it clears the §9 observability bar.

## 6. Verification Gates (per AGENTS.md §16.4)

Touches `internal/{cache,server,cluster}` → `make lint`, `make test`,
`make bench-gate` (+ baseline), `make conformance`, `make integration`.
Hit-path allocs/op unchanged — enforced via `BUDGETS`.

## 7. Risks and Caveats

| Risk | Handling |
|---|---|
| CF zone rules (Edge Cache TTL, `set_cache_control`) override CDC | Runbook + config godoc; cannot be fixed header-side |
| Always Online zones ignore swr/SIE | Degrades mirror → strict at CF automatically; documented |
| Streaming fills: TTL not final before first flush | P2 timing constraint; jitter computed once and reused |
| Composed-head / FastHeader caching staleness after refresh | Existing invalidation on store; new tests pin CDC after 304-refresh |
| Operators surprised CF keeps pre-purge content ≤1 reval cycle after soft purge | D2 documented; hard purge / CF API purge for immediate semantics |
| Origin outage during stayin-alive grace: CF-side visitors pay a synchronous reval RTT per request | Bouine answers stale from the grace store — availability unchanged, latency only; self-heals via I8 on first healthy origin response |
| CDC + `Authorization` → CF BYPASS if no `public` | I2 always emits `public` |

## 8. Open Questions

None blocking. Deferred: dashboard panel placement; CF API purge
integration (separate feature, would also cover hard-purge propagation to
already-admitted CF entries).

## 9. Future Work (explicitly out of scope here)

CF API purge on `PURGE`/soft-purge — the only mechanism that can invalidate
already-admitted CF entries mid-freshness. Additive, independent of this
plan.
