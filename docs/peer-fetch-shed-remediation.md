# bouine peer-fetch shed — follow-up fixes

Date: 2026-10-12 · Context: prod-eu `bouine_peer_fetch_shed_total` investigation
Repo: `bouine` (code) / `bouine-config` (config) · Grafana: "bouine — overview", panel "Peer fetch shed rate by pod"

## Background (one paragraph)

A shed = a peer fetch **or** peer-put RPC that waited >100ms (`peerFetchWaitTimeout`, `internal/cluster/peerfetch.go:77`) for one of the `cluster.peer_fetch_concurrency` slots (was 16, from `deploy/common/values.yaml`) and was dropped. The Oct 7–9 routing changes tripled prod-eu peer-fetch demand (18.5M → 57M RPCs/day), pushing daily sheds from 0.8–2.5k to 20–41k (prod-us/ap: zero, same version/config → demand-driven). RPCs are fast (p50 0.5ms, p99.9 4–10ms even during bursts); saturation comes from **hot-key miss fan-out**: origin fetches are singleflight-collapsed, but N concurrent misses on a non-owner each fire their own peer fetch, and after the fill each fires its own peer-put.

**Shipped:** config bump 16 → 32 in BackMarket/bouine-config#321 (draft). The items below are the code-side follow-ups, roughly in recommended order.

---

## 1. Split the shed counter (Fetch vs Put)

**Problem.** `bouine_peer_shed_total` lumps fetches and puts into a single metric. We cannot tell whether the cluster is drowning in inbound peer reads (followers fetching from owners) or peer writes (followers pushing filled objects back to owners).

**Change.** Add a `direction` label (`direction="fetch"` vs `direction="put"`) to `bouine_peer_shed_total`. Update instrumentation in `internal/cluster/peerfetch.go`.

**Effort:** trivial. **Risk:** none. **Tests:** metric unit tests verifying labels/increments.

---

## 2. Shed-aware retry hint (`ErrPeerFetchShed`)

**Problem.** When a peer fetch hits the wait timeout and is dropped, `tryPeerFetch` returns an error. The caller (`handleCacheMiss`) treats this like a transient network error, falling back to retry paths that may re-attempt peer operations or hit slow paths that immediately shed again, wasting CPU and adding latency under heavy saturation.

**Change.** In `tryPeerFetch`, when the error is `ErrPeerFetchShed` (errors.Is), set a hint (e.g. `req.OwnerBusy = true`, transferred via a context key like the existing ones) and honor it in `handleCacheMiss` the same way `peerHintsApply` honors OwnerMiss: skip the peer retry, go straight to origin. Keep *other* errors (transient network) retryable — a shed is a capacity signal, not evidence about the key, but under saturation the retry is the wrong trade.
*Note:* Bypassing peer fetch on shed and falling back to origin relies on origin singleflight (`collapsedFetch`) to prevent an origin stampede under cluster-wide hot-key saturation.

**Effort:** small. **Risk:** a shed request skips a peer hit that might have succeeded a few ms later — acceptable: the slow-path retry would most likely queue and shed again. **Tests:** pin fast-path shed → slow path skips the peer RPC (mirror the OwnerMiss tests); `handleCacheMiss` still origin-fetches.

---

## 3. Dedup peer-puts (N identical puts per hot-key fill)

**Problem.** Every singleflight *follower* still runs `writeAndMaybeStore` → `forwardToOwnerIfRemote` (`handler.go:2687`, also `2714` for the Vary resolver entry and `3179` in `doShedRefill`). Each call spawns its own goroutine (`cmd/bouine/cmd/builder.go:425-434`) that acquires a `putSem` slot (16) and uploads the full object. A hot-key fill with 100 concurrent misses = **100 identical puts**, each pinning a slot for the body upload. Correctness is fine (owner store is idempotent, put is best-effort) — it is pure capacity waste and the likely dominant `put`-side shed source.

**Change options (pick one):**
- **a) Mutex-protected recent-forwarded set** (recommended, simplest): in `forwardToOwnerIfRemote`, check a mutex-protected TTL map or `sync.Map` (`key → forwarded_at`, ~1–5s). *Crucial concurrency requirement:* Because N concurrent goroutines arrive within sub-milliseconds during hot-key fill, a naive check-then-set race condition will cause all of them to miss an unprotected map. The map check and insertion must be atomic (e.g., protected by a `sync.Mutex` or `sync.Map.LoadOrStore`). First follower forwards, the rest skip. Safe because a duplicate put within seconds adds nothing (owner already stored it; invalidations purge independently of puts).
- **b) Leader-only forward:** not directly expressible with `x/sync/singleflight` (the `shared` flag is true for all callers when n>1), so it needs the same key-timestamp machinery anyway.

**Effort:** small-medium. **Risk:** a put skipped because a *stale-timestamped* peer forwarded a *different* object — bounded by keeping the TTL well below any realistic re-fill cycle; or by storing the forwarded object's ETag/hash in the set. **Tests:** N concurrent misses → exactly 1 put RPC to the owner; TTL expiry re-allows the forward.

---

## 4. Singleflight the peer fetch per key (collapse hot-key fan-out)

**Problem.** The structural burst source: N concurrent misses for the same key on a non-owner fire N identical peer fetches (`fastpath.go:288` fast path, `handler.go:1667` slow path). No collapsing exists on the peer path, unlike the origin path (`collapsedFetch`, `handler.go:2035`). With 16–32 slots and 0.5ms RPCs, a single hot key with >32 concurrent requests saturates the fetcher on its own.

**Change.** Wrap the peer-fetch call in the shared `singleflight.Group` with a key that includes the Vary assertion (the peer question is `key + VaryKey`): followers wait on the leader's RPC and share the decoded `api.Object`. The object returned by `PeerFetcher.Fetch` is already a private copy (`peerfetch.go:799`), immutable after decode — verify follower-safe sharing (no mutation before serve; `servePeerHit` treats it read-only). The slow path and fast path can share one flight group per handler instance; the fast path must not block its connection goroutine on group admission — it already blocks on the RPC itself, so the flight adds no new blocking class.

**Effort:** medium. **Risks:** in-flight sharing across goroutines (needs an immutability audit of `api.Object` on the peer branch); a follower whose request selects a different Vary must NOT share — the sf key already encodes VaryKey, so they don't collapse; cancellation semantics (one follower aborting must not cancel the leader's RPC — singleflight keeps the leader running, which is the desired behavior).

**Tests:** concurrent misses for one key → 1 RPC (mock PeerFetcher call count); different VaryKeys → separate RPCs; shed-refill still peer-fetches... (note: shed refills origin-fetch, not peer-fetch — no interaction).

---

## 5. Traffic-side and scaling knobs (config, evaluate after the above)

- **Search SWR**: search routes run `ttl_override: 30s` + `stale_while_revalidate: 30s` over 19.5M req/day (prod-eu values.yaml `&search-cache` anchor). Every key expires every 30s and non-owner requests peer-fetch the stale copy until the owner revalidates. Raising SWR to 60–120s directly cuts steady-state peer-fetch demand on the hottest pool. Tradeoff: freshness (the ACQ squad owns `SEARCH_API_CACHE_TTL`).
- **Replica count / HPA**: per-pod peer-fetch demand ≈ `RPS × (N−1)/N²` — 3→4 pods cuts it ~16%, 3→5 ~28%. HPA scales 3→5 on CPU only (2500m trigger); peer saturation is invisible to it. If sheds persist after fixes 1–4, consider a custom HPA metric on shed rate (bigger lift) or raising `minReplicas`.
- **`peer_fetch_active` observability gap**: the gauge (1m scrape) misses sub-second saturation — during burst windows it read 0–16 with sheds between scrapes. A `max_over_time` in the dashboard panel, or a "slots exhausted" counter, would make saturation visible.

---

## Suggested order

1. #321 (config 32) — measure a day of `peer_shed_total`
2. Fix 1 (split counter) — tells us fetch vs put share
3. Fix 2 (shed hint) — latency + pressure win, bounded by origin singleflight
4. Fix 3 (put dedup with atomic/mutex guard) — likely kills the put-side sheds entirely
5. Fix 4 (peer-fetch singleflight) — the structural fix; only if hot-key fan-out persists
6. Fix 5 (traffic/HPA) — if still needed after the above

---

## Key file references

| What | Where |
|---|---|
| Shed + wait bound | `internal/cluster/peerfetch.go:77`, `:428`, `:654-673`, `:975` |
| Fast-path peer fetch (no hint on error) | `internal/cache/fastpath.go:280-298` |
| Hint honoring / slow-path retry | `internal/cache/handler.go:1618-1676` |
| Follower puts | `internal/cache/handler.go:2687`, `:2714`, `:3179`; `writeAndMaybeStore` callers `:2297`, `:2399` |
| Put goroutine spawn | `cmd/bouine/cmd/builder.go:425-434` |
| Origin singleflight (reference) | `internal/cache/handler.go:2035`, `:3153` |
| Concurrency config | bouine-config `deploy/common/values.yaml` (`cluster.peer_fetch_concurrency`) |
| Search TTL/SWR | bouine-config `deploy/main/prod/eu/services/values.yaml` (`&search-cache` anchor) |
