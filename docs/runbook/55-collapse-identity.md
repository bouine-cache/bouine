# 55 — Request collapsing: identity-split flights and dimension-keyed cold flights

**Audience**: operators seeing origin load or hit-ratio changes after
ADR-0052/0054 (identity carriers removed from the collapsing space)
or ADR-0057 (cold flights keyed on declared dimensions).

## What changed

Request collapsing parks concurrent requests for the same cache key on
one leader's in-flight origin fetch and hands the leader's response to
all of them. Two gates now apply:

**Identity gate (ADR-0052/0054).** `Authorization`/`Cookie` carriers
and unsafe methods always fetch on their own. Collapsing is not
storage — the in-flight handoff leaked cross-tenant data in
production, and no credential-equality scheme can prove that shape
safe. Cookied requests still *use* the cache; they never *share a live
fetch*.

**Flight-key gate (ADR-0057).** A shared flight hands the leader's
*body* to followers, so the flight key must encode every dimension
the body varies on:

- **Warm flight** (stored object exists): lookup key — a stored
  object is the origin's own declaration for the URL.
- **Cold flight, `include_headers` route**: primary key extended with
  the declared headers. Same-dimension callers still collapse;
  different-dimension callers never meet.
- **Cold flight, include-free route**: refused — one fetch per
  concurrent caller for the first fill per key; the next burst is
  warm and collapses.

**Vary drift (ADR-0058).** A revalidation observing a different Vary
surface than the stored resolver carries purges the resolver and its
variants (`bouine_vary_drift_total`).

## When origin load increases — and when it is expected

- **Authorized or cookied traffic on one URL**: one origin fetch per
  concurrent caller. Intended — those responses were never provably
  shareable.
- **Cold start of an include-free route** (deploy, restart, purge
  reclaim): one fetch per concurrent caller for the first fill per
  key; converges on the next burst. Pre-warm hot routes if the origin
  cannot take the spike.
- **Cold start of an `include_headers` route**: only the declared
  dimensions split the herd — exactly what the route asked for.
- A rise in `fetch_shed_total` during these bursts is the semaphore
  bound working.

**Rollout**: the flight key is per-process. A mixed fleet splits
flights (lost dedup, never a wrong body) — roll uniformly.

## Mitigations for authorized routes that genuinely share responses

- Origin `Cache-Control: public, s-maxage=...` — storage then serves
  every subsequent caller a HIT; only the first burst misses.
- Front genuinely public content on routes without per-request auth.

## Diagnosing

1. `bouine_requests_total{cache_result="MISS"}` rises on an
   authorized route → expected; check convergence to HITs once the
   origin's directives allow caching.
2. Hit-ratio drop after adding `include_headers` → storage variants,
   not the flight key. A drop persisting past the first fill cycle is
   not the flight gate.
3. One-shot MISS burst on an include-free route after a deploy or
   restart → the refused cold fill, once per key. If it persists, check
   eviction metrics (resolver objects being evicted), not the gate.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Origin fetches ≈ concurrent authorized/cookied callers on one URL | The identity gate working as designed | Accept; use origin `public, s-maxage` for convergence to HITs |
| `fetch_shed_total` rising during authorized/cookied bursts | Fetch semaphore saturated | Raise `max_fetch_concurrency`; shedded callers retry |
| One-shot cold-burst origin spike after deploy/restart/purge | The refused cold fill (ADR-0057) | Accept — converges on the next burst; pre-warm if the origin cannot take it |
| `bouine_vary_drift_total` non-zero | Origin changed its Vary under a live cache (ADR-0058); stale resolver purged | Confirm the new declaration is intended; expect a one-cycle hit-ratio dip |
| Cross-caller response leakage still suspected in-flight | Impossible on authorized, cookied, or undeclared-cold traffic after these gates; suspect storage keying | Check the route's `include_headers`/Vary for an undeclared selector |
