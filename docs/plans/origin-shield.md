# Plan: Cluster origin shield via request forwarding

> One-line summary: on a cold key, the non-owner forwards the original
> client request to the ring owner, which runs its standard miss path —
> one origin fetch for the whole cluster, no request envelope, no new
> wire format. Second attempt: supersedes the envelope-based design of
> PR #731 (see ADR-0052 for the decision record).

**Status:** Draft — awaiting review before implementation
**Date:** 2026-09-28
**ADR:** ADR-0052 (cluster origin shield via request forwarding)
**Motivation:** strong-mode cold-key storms (mass purge, deploy, TTL
expiry) cost one origin fetch per node; the ring owner cannot fetch on
the requester's behalf because peer-fetch carries only a key.
**Depends on:** ADR-0007 (consistent-hash ring), ADR-0030 (128-bit
key), ADR-0035 (peer wire over mTLS), issue #509 (owner-only
partition).

---

## 1. What we are building

```
pod A (non-owner)                          pod B (owner)
───────────────────                         ───────────────────
client request
  → local miss
  → peer-fetch(key)  ─────── v2, key-only ────────→  store lookup
      ← 404 (owner has nothing)              ← miss
  → forward original request ─── mTLS, admin ───→ /v1/peer/forward
                                                  → ownership gate
                                                  → standard miss path
                                                  → origin fetch
                                                  → store (owner fills)
      ← proxied response bytes  ←────────────────← serve
  → serve client (+ optional backfill)
```

Warm hits never change. The v2 peer-fetch wire format never changes.
The flag, backfill knob, metrics and validation semantics carry over
from PR #731.

## 2. Design decisions (each is a paragraph to flip here, a day to flip in code)

| # | Decision | Rationale |
|---|---|---|
| D1 | Forward the **original, un-rewritten** client request | Pod B's route applies `strip_prefix`/`path_rewrite`/header rewrites itself; a post-rewrite forward applies them twice |
| D2 | Endpoint `/v1/peer/forward` on the **admin plane** (mTLS, hop header, peer trust boundary) | Same boundary as `PeerFetchPath`; pod B can label peer-served metrics distinctly |
| D3 | Pod B runs its **standard data-plane miss path** (router → handler → origin), not a parallel fetch lane | Vary, HEAD, streaming, size caps, negative caching, rewrites, latch-collapsing: all correct by construction |
| D4 | Forward carries a **deadline**; pod B clamps it (min) against its own fetch budget | Identical-config pods: without clamping, pod B can outlive pod A's patience and burn a wasted origin fetch |
| D5 | Endpoint checks **ring ownership** and decrements the **hop header**; a non-owner answers 404 | Loop prevention under ring disagreement; requester falls back to origin, never to a third hop |
| D6 | Response bytes are **proxied** to the requester (not re-encoded as an `api.Object`) | Preserves streaming; avoids re-serialization; the requester already knows how to serve origin responses |
| D7 | Requester **skips `peerPut`** for shield-sourced fills | Pod B already stored the fill; forwarding it back is a redundant RPC |
| D8 | **GET/HEAD only** | Matches cacheable-method semantics; keeps request-body streaming out of scope |
| D9 | Flag `cluster.origin_shield` (strong mode, **default off**); a flag-off node neither forwards nor serves forwards | Same rollout valve as #731; behavior change for every strong-mode cluster otherwise. Renamed from #731's `peer_fetch_coalesce` — that name described the deleted v3 mechanism; the flag never shipped, so the rename is free |
| D10 | Backfill applies to **shield fills only** (settled in design review, not generalized to warm peer hits) | The shield's job is deduplicating origin fetches. Storing warm peer hits re-creates the every-pod-caches-hot-keys failure #509's owner-only partition exists to prevent (ring goes decorative, capacity becomes N× working set). If that product is wanted someday it gets its own flag and evidence |

**Settled decisions (design review, 2026-09-28):** D9 renamed the
feature to `cluster.origin_shield` / `bouine_shield_*` metrics (free
before first release; a compat break after). D10 scoped backfill to
shield fills only, keeping #731's knob semantics
(`origin_shield_backfill_probability`, default 1.0).

## 3. What transfers from PR #731 (salvage list)

| Piece | Disposition |
|---|---|
| Flag + `config.Validate` bounds | Keep, **renamed**: `cluster.peer_fetch_coalesce` → `cluster.origin_shield`; generalize the fetch-timeout validation (effective `fetch_timeout` must exceed the shield deadline, both sides identical config) |
| Backfill knob (default 1.0) + bounds tests | Keep, **renamed**: `peer_fetch_backfill_probability` → `origin_shield_backfill_probability`; scoped to shield fills per D10 |
| Metrics | Keep, **renamed**: `bouine_coalesced_fetch_total` → `bouine_shield_requests_total{role=owner/waiter/failure/fallback}`, `bouine_coalesced_fetch_shed_total` → `bouine_shield_shed_total`, `bouine_origin_requests_saved_total` → `bouine_shield_origin_requests_saved_total`, `bouine_coalesced_fetch_duration_seconds` → `bouine_shield_duration_seconds`. Role names unchanged |
| Fast-path `OwnerMiss` hint interaction (`WithCoalesce` → rename to `WithShield`) | Keep — the fast path must still not set OwnerMiss when the shield is on |
| `ownsKey`/`IsLocal` ownership gate | Keep, moves to the forward endpoint (D5) |
| Integration test shape (cold key → origin sees 1 request, ring-convergence polling) | Keep, mechanism rewritten |
| v3 wire format, `OriginRequest`/`PeerHeader` types, `FetchOrigin`, `OriginFetcher` registry, coalesce lane/semaphore/timeout constants, HEAD canonicalizations, `originEnvelope` | **Delete — replaced by the real request path** |

Estimate: ~30–40% of #731 transfers by line count.

## 4. Implementation sketch (shape, not signatures)

1. **Endpoint** (`internal/cluster`): a thin `fasthttp` handler that
   decodes the forwarded request, checks ownership + hop header, then
   drives the data-plane router via `fasthttp.RequestCtx.Init`-style
   replay. Response written back on the same connection.
2. **Requester branch** (`internal/cache` miss path, behind the flag):
   after a 404 peer-fetch on a hard GET/HEAD miss, forward the original
   request bytes; on any failure, fall through to the local origin
   fetch (existing code, unchanged).
3. **Deadline**: the forward carries `X-Bouine-Deadline` (absolute
   unix-nano); the owner min()s it against its own fetch budget.
4. **Backfill**: on a successful shield response, the requester stores
   locally per the probability knob (same `storeObjectLocal` path as
   #731).
5. **Wiring** (`cmd/bouine/cmd`): register the endpoint on the admin
   server next to `PeerFetchHandler`; the flag gates both sides.

Deliberately **not** in scope: designated shield tiers, request-body
forwarding (POST), coordinated revalidation of stale-usable objects
(the plain peer-fetch path stays for those), and generalizing backfill
beyond the D10 decision.

## 5. Test plan

- Unit: endpoint ownership gate (owner/non-owner/stale ring), hop
  decrement, deadline clamping, GET/HEAD-only, forward failure →
  origin fallback, backfill p=0/1.
- Integration (extend `test/integration/cluster_origin_shield_test.go`
  shape): N-node cluster, cold key → origin counter reads exactly 1;
  flag-off node → origin counter reads N; mixed-version (endpoint
  absent) → per-node fallback; slow origin → deadline clamping.
- Bench: `make bench-gate` — the hit path is untouched (no new
  branches before the store lookup); the miss path gains one guarded
  branch, benchmarked.
- Conformance: untouched (no cache-semantics change), run as CI does.

## 6. Rollout

This plan lands and stays open as the **design phase** of one PR;
implementation follows as commits on the same branch, and the PR
merges once, complete (design-first, single merge — the docs are not
merged ahead of the code they describe).

1. Plan + ADR-0052 (this commit) — the design record.
2. Implementation commits on this branch, mining §3: salvage first
   (flag, knob, metrics, ownership gate, fast-path interaction —
   renamed per D9/D10), then the forward endpoint (§4.1), the
   requester branch (§4.2), deadline + backfill (§4.3–4.4), wiring
   (§4.5), tests (§5).
3. Flag ships default-off; flipping the default is a separate decision
   (its own changelog entry — it changes cold-miss latency for every
   strong-mode cluster and interacts with the fetch-timeout
   validation).

## 7. Mining #731 — mechanical guide

Salvage lives on branch `feat/cluster-origin-shield` (PR #731,
closed). Do **not** cherry-pick commits — they interleave salvage with
to-delete code. Copy the pieces instead:

| Salvage piece (§3) | Source | Commit(s) |
|---|---|---|
| Flag + `validateRouteCoalesceFetchTimeout` | `internal/config/loader.go` | `36f294bd`, `fab58b96` |
| Backfill knob + bounds tests | `internal/config/{config.go,loader_test.go}` | `8b53fa46`, `36f294bd` |
| The four metrics + nil-safe helpers | `internal/cluster/metrics.go` | `8b53fa46` |
| Fast-path OwnerMiss interaction (`WithCoalesce` → rename to `WithShield`) | `internal/cache/fastpath.go` | `8b53fa46`, `fab58b96` |
| Ownership gate (`SetOwnerCheck` shape) | `internal/cluster/peerfetch.go` | `5ffe7f80` — moves to the forward endpoint |
| Integration test shape (cold key → origin counter = 1) | `test/integration/cluster_origin_shield_test.go` | `8b53fa46` |

Everything else on that branch (v3 wire, `OriginRequest`, `FetchOrigin`,
coalesce lane, HEAD canonicalizations, `originEnvelope`) is deleted by
this design.

**Known blocker, external to this work:** `test/integration` fails 4
tests deterministically in full-suite runs **on `main` itself**,
passing in isolation (cross-test interference) — issue #735. Run the
plan's integration tests with `-run` filters until #735 is fixed;
their full-suite failure is not a regression signal for this feature.

**Settled in design review (2026-09-28):** backfill scope (D10) and
naming (D9) — see §2. No open questions remain on the design.

## References

- ADR-0052 — the decision record for this design.
- PR #731 — first attempt, closed; its "Known trade-offs" section
  documents the envelope design's costs.
- Varnish origin shield — the property worth borrowing: the shield
  node sees the full client request.
