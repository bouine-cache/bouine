# ADR-0066: Fill flights — collapse anonymous cold include-free misses via the store

- **Status**: Accepted
- **Date**: 2026-10-23
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 9111 §3, §3.5, §4.1; ADR-0052 (never collapse authorized or unsafe-method requests), ADR-0054 (bypass-on-cookie), ADR-0057 (key the collapsing flight on the declared variation dimensions); `internal/cache/collapse.go`, `internal/cache/fillflight_test.go`

## Context and Problem Statement

ADR-0057 §3 refuses to collapse the **anonymous cold miss on an
include-free route**: with no stored object the origin's Vary is
unknowable at park time, so no flight key can be *proven* to encode the
origin's variation surface, and every concurrent caller fetches its own
copy. nginx (`proxy_cache_lock on`) and Varnish (waitinglist) both
collapse this case on the cache key alone, and bouine loses to nginx on
cold-burst origin load and tail latency exactly there: a burst of N
concurrent misses for one cold key issues N origin fetches (serialized
in fetch-semaphore waves) where nginx issues 1.

The refusal is over-broad because it holds every flight to the standard
of ADR-0057's byte hand-off — the follower receiving the leader's
fetched bytes, which is only safe when the flight key provably encodes
the variation surface. That proof is genuinely unavailable for a cold
include-free miss. But a second, weaker standard is available and was
already implemented for the streaming path (ADR-0042's
`ErrStreamUnshareable` release): the follower can wait for the fill and
be served **from the store** — never from the leader's bytes — through
the same gates a sequential request passes.

## Decision

**An anonymous, safe-method, cold include-free miss parks a fill
flight on the primary cache key (in a dedicated key space), and the
follower never receives the leader's response bytes.** The follower
waits for the fill to settle in the store (`inflightStream.stored`,
closed after the leader's store attempt or at its verdict when nothing
will be stored), then **re-enters `ServeRequest` as a freshly-arrived
request**: it serves its own Vary variant from the store, or — when the
fill stored nothing usable for it (unstorable response, absent variant)
— fetches its own, guarded against re-parking
(`fillReentryUserKey`). Every response a fill follower can receive
passes the same gates a sequential request passes: Vary variant
selection, RFC 9111 §3.5 storage (the fill only hands the follower a
stored object when the origin's response was storable for a shared
cache), freshness, and conditional handling. This is **sequential
equivalence**: byte-for-byte, the follower's response equals what the
same request would have received had it arrived after the fill —
pinned by `TestFetchAndStore_FillFollowerSequentialEquivalence`.

Unchanged, bit-for-bit:

- **ADR-0052/ADR-0054 refusals**: requests carrying `Authorization` or
  `Cookie` never share any flight, at any site. Their production
  incident shapes (the shared-JWT per-tenant selector; per-user SSR
  session cookies) are held to the strict standard deliberately; see
  Consequences for the open question of relaxing them under the §3.5
  storage gate.
- **Unsafe methods** never share a flight (ADR-0052 extension).
- **Warm and declared-dimension flights** keep ADR-0057's byte hand-off
  under the proven-key rule; fill flights live in a separate key space
  (`fillFlightSuffix` XOR) so a byte-handoff follower can never park
  on a fill flight and vice versa.

## Consequences

- **Cold bursts on include-free routes collapse to one origin fetch**
  (measured: 32 concurrent cold misses, 2 ms origin RTT, 4 fetch slots —
  32 fetches → 1; p99 18.2 ms → 2.3 ms; see `BenchmarkSingle_`
  benchmarks in `internal/cache/fillflight_bench_test.go`).
- **Followers of a storable fill report `X-Cache: HIT`** (served from
  the store) where the pre-ADR-0066 behavior was N independent `MISS`
  fetches; sequential requests already reported HIT.
- **Unstorable fills cost the follower one leader-fill wait** before
  its own fetch (previously it fetched immediately, in parallel). This
  is the deliberate trade: pass-through/`no-store` routes with
  concurrent misses now serialize on the leader's fill. Bounded by the
  fetch semaphore and shed machinery as before; if an operator's
  pass-through route cannot tolerate it, the route should declare
  include_headers (which keeps different-dimension callers fetching in
  parallel) or the origin should send `no-store` (the fill verdict
  releases followers at header time, before the body completes).
- **A follower whose variant is absent fetches its own** (the ADR-0057
  incident shape now yields correct per-variant content instead of
  cross-variant bytes).
- **Error verdicts are sequential-equivalent too**: a fill follower
  whose leader failed with a transport error re-enters and fetches its
  own — a transient leader failure is not the follower's response, and
  nothing usable was stored for it (pinned by
  `TestFetchAndStore_FillFollowerTransportErrorFetchesOwn`). The one
  exception is `ErrFetchShed`: the shed proves the fetch semaphore is
  saturated, and the follower's own acquire would wait on the same
  saturated semaphore, so the follower is answered with the shed 503 —
  the verdict a sequential request receives under the same load — plus
  the bounded background refill (pinned by
  `TestFetchAndStore_FillFollowerShedServes503`).
- **Residual exposure equals the pre-existing sequential exposure**:
  an origin that varies on a selector header without declaring `Vary`
  already serves wrong content to sequential requests through the
  store; after ADR-0066 concurrent requests receive the same (wrong)
  content the sequential request would. No concurrent observation is
  possible that was not already possible sequentially — the property
  the fill-flight test suite pins.
- **Store stats double-count the follower's re-lookup** (its miss
  counter increments once on the original lookup and once after
  re-entry). `bouine_requests_total` is unaffected (middleware counts
  requests, not lookups).
- **Cluster: the follower's re-entry re-asks the owner.** The H1 fast
  path's peer hints (OwnerMiss / OwnerGateReject / OwnerShed) prove the
  owner was asked exactly this question — *before the leader's fill
  settled*. The fill may have changed the owner's answer (the leader
  stored locally or forwarded the object via peerPut), so
  `serveFillFollower` drops the hints before re-entering and the
  follower peer-fetches like any freshly-arrived request: the
  store-mediated serve holds on non-owner nodes, instead of the
  follower silently skipping the owner lookup and fetching its own
  origin copy (pinned by
  `TestFetchAndStore_FillFollowerReentryReasksOwner`).
- **Open question (not decided here)**: whether personal requests
  (Authorization/Cookie) may park fill flights gated by the §3.5
  storage certification (`public`/`s-maxage`/`must-revalidate`). The
  argument is sound — an unstorable fill releases the follower to its
  own fetch, and a storable fill is the origin certifying cross-caller
  reuse — but ADR-0052's strict stance was taken after a production
  cross-tenant incident and should only be revisited deliberately.

## Amendment: staggered own-fetch release (#838)

The unstorable-fill consequence above — "the follower waits one
leader-fill duration, then re-fetches its own" — shipped with a
synchronization bug: every follower of a released wave woke on the same
settle signal and re-fetched at the same instant, so the origin observed
N same-status responses completing within microseconds of each other.
The pool's passive-health accounting (ADR-0051) counts consecutive
same-status completions, and a single wave could carry the counter past
the `consecutive_5xx` ejection threshold while the origin was healthy
and serving successes on other paths — ejecting the sole healthy target
and failing every origin-bound request fast with 502 until `eject_for`
restored it, after which the next wave re-ejected it. The pre-ADR-0066
world never hit this only because its fetch flood kept a constant
stream of successes resetting the counter between the 5xx completions.

The release is now staggered (`fillRefetchDelay`): a follower that
fetches its own after an unstorable fill sleeps a uniform-random slice
of a window proportional to the wave size (1 ms per follower, capped at
100 ms — the `fetch_wait_timeout` scale). Sequential equivalence is
unchanged byte-for-byte and in fetch count; what is restored is the
*arrival spread* of those fetches — the property a sequential world has
by construction. A lone follower (wave of one) never delays; followers
served from the store never pass through the own-fetch site and keep
zero added latency. The wave-synchronized completion shape — and with
it the healthy-origin ejection — is pinned gone by
`TestFetchAndStore_ReleasedFillWaveStaggersOwnFetches` (synctest).

The residual pre-existing behavior (#839) is fixed alongside in the
ADR-0051 amendment: consecutive-5xx ejection is now success-windowed
(5 s), so an origin answering successes on any path is never ejected
by 5xx adjacency — with or without waves. An origin whose *only*
origin-bound path returns 5xx still ejects once the threshold
completes: that is the operator's `consecutive_5xx` contract.

## Amendment: the unstorable-fill memo (tail follow-up)

The staggered release above fixed the ejection burst, but its cost
landed on exactly one population and stayed there: a hot route whose
origin answers **`no-store` or `private`** (the load-test `/miss`
shape: one key, 1.5k rps) settles *every* fill unstorable, so every
wave of parked followers paid the leader-fill wait plus a
uniform-random slice of the stagger window before its own fetch — a
sustained long-tail regression on ~20% of the mixed-leg traffic, the
very tail PR #838 traded for origin protection.

**A fill that settles with a complete, well-formed response refusing
shared storage by its own directives — RFC 9111 §5.2.2.3 `no-store`
(with the must-understand escape, mirrored from `isCacheBlocked`) or
§5.2.2.7 `private`, under CDN-Cache-Control precedence (RFC 9213) —
memoizes its primary key for 5 s (`unstorableMemoWindow`, the
`passive5xxCountWindow` family; a fixed engineering default, not an
operator knob). While memoized, an anonymous cold include-free miss
for that key never parks on a fill flight: it fetches its own
immediately** — the exact behavior of a request `collapseDenied`
refuses, and of the pre-ADR-0066 world. Re-parking could only hand
every follower one leader-fill wait plus the released-wave stagger
before the same own-fetch, because such a fill provably stores
nothing shareable.

Correctness is unchanged by construction:

- **A memoized request is a sequential request.** It fetches its own
  response through the same gates as any non-collapsing request; no
  bytes, verdicts, or partial bodies are ever shared on this path.
  The memo can only *remove* collapse, never add sharing, so the
  concurrent-observation exposure ADR-0066 pins cannot increase — a
  per-request-varying origin now serves each caller its *own* answer
  on memoized keys, which is strictly the sequential behavior.
- **The verdict is directive-driven, never store-outcome-driven**: in
  strong cluster mode a non-owner leader stores nothing locally even
  for storable fills (the ownerFn gate), so memoizing on "nothing was
  stored" would suppress fill flights on keys the owner holds. The
  memo reads the origin's response directives — node-independent.
- **The evidence is flight-share-eligible evidence**: only a request
  that was itself eligible to share flights (anonymous, safe method —
  `collapseDenied`'s complement) may arm the memo. A cookied or
  authorized request's no-store response is not evidence about the
  anonymous population's storage policy — origins vary Cache-Control
  by request state, and anonymous collapse behavior stays driven by
  anonymous evidence, the same refusal ADR-0052/0054 make for content,
  applied to the memo's control-plane signal.
- **Exclusions**: error statuses (transient — a struggling origin
  needs collapse's dedup on recovery; negative caching covers the
  operator's policy), responses blocked by the client-state gate
  without a directive (ADR-0067's surface — minted per caller, so the
  memo never encodes an assumption about session-minting origins;
  a genuine `no-store` directive still memoizes even when Set-Cookie
  rides along, since the outcome for every caller is its own fetch),
  HEAD-led fills (nothing stored by method, not by policy), and size
  caps (capacity, not origin policy) all keep the settle-then-stagger
  flow.
- **Bounded and fail-open**: 16 shards × 512 keys, lazily swept, no
  reaper goroutine; a full shard refuses writes and falls back to
  ADR-0066 collapse (a missing memo entry costs one collapsed wave).
- **The memo expires in 5 s**, and the first wave after expiry (or
  boot) still collapses and staggers once — bounded by the wave size
  and invisible at p99 under the mixed-leg traffic mix. An origin
  flipping unstorable to storable loses at most one window of dedup
  on the flip and self-heals: memoized requests that receive a
  storable response store it, so later requests hit the store and
  never consult the memo.
- **Fetch counts and origin load return to the sequential rate** —
  nothing shareable was ever stored for these keys, so the
  pre-ADR-0066 world, nginx, and a memoized bouine all issue one
  fetch per request; only the *timing* changes (immediate, not
  wave-serialized). Single-shot evidence
  (`BenchmarkSingle_UnstorableBurstLatency`, 32 callers, 2 ms RTT):
  parked-wave p99 36-45 ms → memoized p99 ~2.4 ms, fetch counts
  identical (32); steady state
  (`BenchmarkSingle_UnstorableSteadyState`, the exact /miss leg shape —
  1500 rps fixed arrivals, 13 ms RTT, 2 s): memoized p50 13.1 ms =
  RTT, p99 ~18 ms, p999 ~21 ms, against the parked-wave world measured
  on the same harness with the memo read disabled — p50 28.7 ms,
  p99 ~44 ms, p999 ~46-49 ms (the analytic wait + stagger + fetch
  model, confirmed). Fetch counts identical (3001); miss-path
  allocations drop ~17% (wave parking removed).

Pinned by `TestFetchAndStore_UnstorableFillMemoizesBypass` (the first
wave still collapses + staggers, the second bursts immediately with
per-caller unique bodies and zero arrival spread),
`TestFetchAndStore_MemoExclusions` (directive-only memoization),
`TestFetchAndStore_MemoExpiresRefillsCollapse` (collapse resumes when
the window lapses), and `TestUnstorableMemo_BoundedFailOpen`.
Observable as `bouine_fill_bypass_total`: a sustained non-zero rate on
a route means its origins send `no-store`/`private` and miss latency
there is sequential, not wave-shaped.
