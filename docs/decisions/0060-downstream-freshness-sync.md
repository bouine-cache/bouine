# ADR-0060: Downstream freshness sync via synthesized CDN-Cache-Control

- **Status**: Accepted
- **Date**: 2026-10-16
- **Deciders**: product + data-plane review session
- **Plan**: [docs/plans/downstream-freshness-sync.md](../plans/downstream-freshness-sync.md)

## Context

Bouine sits behind Cloudflare on routes like `popularity-stats` (bouine TTL
1h, `stale_while_revalidate` 10m, ~30% of CSR traffic served stale during
background revalidation). Three verified Cloudflare behaviors create a TTL
amplification chain:

1. CF **resets its own `Age` clock** on every admit/revalidation — bouine's
   RFC 9111 §4.2.3-correct `Age` on serves is discarded downstream.
2. **`Expires` cannot carry the fix**: `max-age` takes precedence
   (RFC 9111 §4.2.1), and CF ignores `Expires` for its caching decision when
   `CDN-Cache-Control`-family headers are present.
3. **`CDN-Cache-Control` (RFC 9213) is the input CF actually honors** for its
   caching decision, with precedence `Cloudflare-CDN-Cache-Control` >
   `CDN-Cache-Control` > `Cache-Control`, while proxying `Cache-Control`
   untouched to browsers.

Net effect: CF re-admits bouine's stale serves as fresh for CF's own TTL —
an end-user ceiling of bouine TTL + SWR + CF TTL (~2h10 on
popularity-stats), and ~2h even without SWR from fresh re-aging alone.
ADR-0011 guarantees byte-for-byte `Cache-Control` passthrough, so no
existing knob addresses this. ADR-0022 noted the general problem and rejected
`Warning: 110`-reliance because it "depends on downstream behavior"; this
decision makes the downstream's contract explicit instead.

## Decision

Add a per-route `downstream_freshness` mode to `RouteCache`
(`internal/config/config.go`), default `passthrough` (current behavior,
ADR-0011 and cache-tests conformance untouched). When set to `sync`, bouine
synthesizes `CDN-Cache-Control` on client-facing serves so that no
downstream cache trusts content beyond bouine's own clock:

1. **Fresh serves (HIT / REVALIDATED)** — `public, max-age=<TTL − Age>`
   (floor 0). CF's clock then expires exactly at bouine's wall, whenever CF
   fetched. Required: without this, a CF fetch at t=59m59s re-ages a full
   TTL with no SWR involved.
2. **Stale serves — mirror mode** — `public, max-age=0,
   stale-while-revalidate=<window remaining>` within the sanctioned window,
   for *all* stale reasons: natural SWR aging, soft purge (which rides the
   SWR machinery, `TTL=0`), SIE, and heuristic staleness. The window is
   sourced per staleness reason: `obj.StaleWhileRevalidate` for SWR-class
   serves, `obj.StaleIfError` for SIE serves. Outside any sanctioned window
   — `stayin_alive` grace past `TTL+SWR+SIE`, and request `max-stale`
   authorization beyond the object's window — the synthesis clamps to
   strict `public, max-age=0` (no `swr`): grace is an outage posture the
   downstream must probe per request, and per-request client authorization
   must not become CF entry policy. CF mirrors bouine: serves `UPDATING`
   instantly from its cache and revalidates asynchronously, collapsed,
   against bouine's store. Chosen over strict `max-age=0` everywhere (taxes
   ~30% of traffic with a blocking CF→bouine reval while bouine answers from
   its stale store either way — same bytes, only latency) and over lazy
   `max-age=<SWR remaining>` (propagates fresh content only at window end;
   misrepresents stale as fresh).
3. **Stored `no-cache` objects** — `public, no-cache`, never a numeric
   clock. `no-cache` is an explicit cacheability marker: those objects ARE
   stored and served REVALIDATED after per-request origin revalidation
   (`internal/cache/cacheable.go:24`, `internal/cache/engine.go:141`), so
   they reach synthesis — and handing CF a `max-age` would license
   unvalidated serving, which RFC 9111 §5.2.2.4 forbids and the origin
   explicitly rejected. CF traffic is identical to passthrough (CF
   "caches and always revalidates").
4. **Fill (MISS) serves — min() rule** — `public,
   max-age=min(origin effective max-age, bouine final TTL)` plus the
   origin's qualitative revalidation directives — except `no-cache`
   responses, which synthesize `public, no-cache`, never a numeric clock
   (point 3): `no-cache` responses are storable and carry no usable
   `max-age`, so the min() rule would admit a no-revalidation-required CF
   entry for a full bouine TTL on a route whose origin demanded per-reuse
   validation. No-op when origin and bouine agree; preserves ADR-0011's
   decoupling case (origin shorter); caps the inverse case (bouine
   deliberately tighter than origin, error fills via the negative-TTL
   ladder) which would otherwise re-introduce the amplifier on first
   fill.
5. **Downstream 304s carry the same cap** as the corresponding 200 serve
   (plus `Warning: 110` when stale-sourced). RFC 9111 §4.3.4: a 304's
   headers update the downstream stored entry — an uncapped 304 would let
   CF re-freshen unboundedly. Range serves carry the cap likewise.
6. **Stale 304s trigger background revalidation — both modes.** The
   conditional-304 path (`tryConditional304Fast`,
   `internal/cache/handler.go:1495`) returns before the existing trigger
   (`internal/cache/handler.go:1510-1513`), so a downstream revalidation of
   a stale object was answered from bouine's stale store — confirming
   content the origin was never asked about — and triggered nothing. Under
   `sync`, CF's revalidations are precisely such conditional requests;
   without a trigger there they freeze content for the full SWR window and
   the TTL + SWR + one-cycle ceiling is unreachable. The trigger fires
   under the same triple gate as the 200-path trigger
   (`StaleHit && obj.StaleWhileRevalidate > 0 && isOwnerOrUnmanaged(key)`),
   ungated by `sync`, which also fixes the pre-existing passthrough hole.

Invariants:

- `Cache-Control` itself is **never mutated** (ADR-0011's browser/CDN
  contract holds; only the RFC 9213 CDN-targeted header is synthesized).
- Under `sync`, origin `CDN-Cache-Control`, `Cloudflare-CDN-Cache-Control`,
  and `Surrogate-Control` lines are **replaced** (synthesis is
  authoritative); under `passthrough` they forward as today. Bouine's
  fill-time consumption of origin CDC for its own TTL is unchanged.
- Synthesized CDC **never flows peer-ward**: the fill path ignores
  CDC-family headers on peer fetches, else bouine TTLs cascade-shrink
  across the cluster (peers parse CDC per RFC 9213).
- `public` always present (CF: CDC presence turns Origin Cache Control on;
  with `Authorization`, absence of `public`/`s-maxage`/`must-revalidate`
  yields BYPASS). Integer values unquoted (ADR-0009 §8). No `s-maxage`, ever
  (implies `proxy-revalidate`, kills CF async SWR).

End-user ceiling on `sync` routes: bouine TTL + SWR + one revalidation
cycle; visitor latency flat during the window (CF `UPDATING`). Point 6 is
what keeps that ceiling honest when the arriving traffic is itself
conditional (CF revalidation) rather than plain requests.

## Rejected Alternatives

- **Synthesizing `Expires`** — loses to `max-age` (RFC 9111 §4.2.1); CF
  ignores it once CDC-family headers exist; cannot express stale-mirror
  semantics.
- **Relying on `Age`** — discarded by CF at admit by design.
- **`s-maxage`** — implies `proxy-revalidate`; CF documents that it disables
  stale-while-revalidate and explicitly recommends against combining them.
- **Mutating `Cache-Control`** — breaks ADR-0011's contract, changes browser
  TTLs, regresses cache-tests.
- **`Cloudflare-CDN-Cache-Control`** — highest precedence at CF but vendor
  lock-in in a CDN-agnostic proxy; chained CDNs would stay uncapped.
- **Strict stale cap (`max-age=0`)** — reintroduces, one hop downstream, the
  blocking that SWR exists to avoid; serves the same stale bytes anyway.
- **Lazy stale cap (`max-age=<SWR remaining>`)** — up to SWR of propagation
  lag; lies about freshness during SIE.
- **Bool config** — no room for a future strict variant without a breaking
  migration; enum matches existing string-enum conventions.

## Consequences

### Positive

- Opt-in routes cap the chain at bouine's clock; popularity-stats ceiling
  drops ~2h10 → ~1h10 with zero added visitor latency during stale windows.
- ADR-0011's use case (bouine longer than downstream) composes cleanly via
  the min() rule.
- Default `passthrough`: zero behavioral change for existing deployments and
  cache-tests conformance.
- Fast path cost is one extra header append in the same cost class as the
  existing `Age` line, zero-alloc, gated by bench budgets.

### Negative / Trade-offs

- Adds a policy-conditional branch to the hot-path static-header builder and
  one dynamic append on `sync` routes (measured, budgeted).
- Fill path (including streaming) must finalize TTL before first flush.
- Operators must understand CF zone caveats (runbook): Edge Cache TTL /
  `set_cache_control` rules override CDC; Always Online zones ignore swr
  (mirror degrades to strict, not unsafe).
- Soft-purged content can survive at CF for ≤ one revalidation cycle —
  documented; immediate invalidation of already-admitted CF entries requires
  CF API purge integration (future work).
- During `stayin_alive` grace or an origin outage, the strict clamp makes
  CF-side visitors pay a synchronous reval RTT per request — bouine answers
  stale from the grace store, so availability holds; latency only, and the
  point-6 trigger self-heals the chain on the first healthy origin response.

## References

- RFC 9213 (CDN Cache Control), RFC 9111 §3.2 (304 header
  update), §4.2.1 (max-age over Expires), §4.2.3 (age computation),
  §4.2.4 (serving stale), §4.3.4 (freshening on 304), §5.1 (Age),
  §5.2.2 (directives).
- Cloudflare docs: "Cache responses" (Age resets on admit/revalidation),
  "CDN-Cache-Control" (precedence; Expires ignored; Surrogate-Control),
  "Origin Cache Control" (Always Online ignores swr/SIE; OCC mode
  behavior), "Revalidation" (asynchronous UPDATING model).
- [ADR-0011](0011-ttl-override-per-route.md), [ADR-0009](0009-cache-state-machine-hardening.md) §8,
  [ADR-0022](0022-refresh-persist-cycles.md).
- Code anchors: `internal/cache/handler.go` — `serveObject`, `buildObject`,
  `writeAndMaybeStore`, `SoftPurge`; `internal/cache/fastpath.go` —
  `appendDynamicHeaders`, `skipStaticHeader`; `internal/cache/fastheader.go`
  — `write304Fast`; `internal/cache/fastheader2.go` — static header build;
  `internal/cache/stream.go` — streaming fill; `pkg/header/headermap.go` —
  `WriteToFastHTTP`.
