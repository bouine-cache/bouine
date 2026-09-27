# Plan: stayin_alive grace retention (P1b + P5)

## Problem

`stayin_alive` routes promise "serve stale indefinitely while the origin is
unreachable / 5xx-ing". Today the storage layer breaks that promise:

1. The TTL reaper (`internal/storage/hot.go reapShard`) deletes any entry past
   `TTL + SWR + SIE` (default tick 30s) and cascades the delete to the warm
   tier — regardless of route policy or origin availability. After the
   freshness horizon + one reaper tick, the stale copies the feature exists to
   serve are gone.
2. SIEVE memory-pressure eviction is route-blind; for hot-only entries (no
   warm copy yet) it is a true deletion, not a demotion.

Key facts from the analysis:

- The reaper is redundant for memory safety: hot tier boundedness comes from
  SIEVE admission against `MaxBytes`; the warm tier is bounded by its own disk
  budget. The reaper is hygiene (free dead bytes early), not safety.
- The reaper's implicit contract ("expired → a future miss can be refilled from
  origin") is false while the origin is down.
- `PoolFastClient.doSingleFetch` (the cache-path fetch) does not record
  passive health, so the existing pool ejection signal is dead on cached
  routes today.

## Design (P1b + P5)

**P1b — health-gated grace.** The *object* carries the policy intent; the
*store* implements a generic mechanism; the *wiring* supplies origin health.

- `api.Object` gains two additive fields, stamped at fill time and
  carried through the binary object codec (v5) so every serialized hop
  (warm tier, peer wire) keeps them:
  - `KeepGrace bool` (`keep_grace`) — true when the route has `stayin_alive`.
  - `Pool string` (`pool`) — the origin pool name (set when KeepGrace is set).
- `HotConfig.MayReap func(obj *api.Object) bool` — a reap gate invoked by the
  TTL reaper **only** for expired entries with `KeepGrace`. Nil (default) keeps
  the historical behavior: reap. Must be O(1), non-blocking (reaper holds the
  shard lock — same constraint family as OnEvict).
- The builder wires `MayReap` as: graced entry whose pool has a healthy
  target → reap; pool has no healthy target → hold. Unknown/pool-less → reap.
- The reaper increments a grace-holds counter per held entry (observability).

**P5 — eager warm backing for graced fills.** `TieredStore.Put` writes graced
objects to the warm tier (and `Protect`s them) regardless of `BodyThreshold`,
so hot SIEVE pressure on them is a *demotion* and `TieredStore.Get` re-promotes.
Capacity still always wins: SIEVE governs hot, the warm disk budget governs
warm.

**Enabling the health signal on cached routes.** `PoolFastClient.doSingleFetch`
now records passive health (consecutive connection errors / 5xx eject;
success resets) exactly like the proxy `FastHandler` path, gated on
`consecutive_5xx > 0`. This makes the ejection signal real for cache-route
outages, and fail-fast picks once all targets are ejected.

### Limitations (documented, accepted)

- Pools without passive (`consecutive_5xx`) or active health checks never
  eject, so grace cannot engage on the reaper side; recommend enabling
  `consecutive_5xx` (+ `eject_for` for auto-restore) on stayin_alive pools.
- Without `eject_for`, ejected targets stay out until an active probe or
  manual `MarkHealthy` — existing semantics, unchanged.
- Capacity (SIEVE + warm budget) always wins; grace only suppresses
  time-based deletion.
- Pre-v5 warm blobs (v4 and older) decode without the new fields → reaped
  as today; rewritten in v5 on the next Put.

## Definition of Done

1. Graced entry + pool with no healthy target: reaper holds it past
   `TTL+SWR+SIE`; a later request still serves stale (stayin_alive holds
   indefinitely during an outage). [test]
2. Pool healthy again (target restored): reaper removes expired graced
   entries on the normal schedule — healthy-route hygiene unchanged. [test]
3. Non-graced entries and nil `MayReap`: reaper behavior unchanged. [test]
4. Graced fill (any body size) is warm-backed and warm-protected; hot SIEVE
   eviction of it leaves it recoverable via `TieredStore.Get`. [test]
5. 304 refresh restamps `KeepGrace`/`Pool` from the route's current config
   (flag cannot be lost mid-life via clone). [test]
6. Cache-path fetch failures drive passive ejection when `consecutive_5xx` is
   configured; `HasHealthyTarget()` reflects it. [test]
7. Gates: `make lint`, `make test`, `make bench-gate` (alloc budgets exact,
   no RPS/p99 regression vs baseline), `make conformance`, `make integration`.
8. Docs: ADR, CHANGELOG, runbook note; PR with benefit/impact estimates.

## Wrong-but-plausible failures each test must catch

- W1: `CloneForRefresh` misses the new fields → 304 refresh drops grace.
- W2: decider inverted (holds when healthy / reaps when down).
- W3: nil `MayReap` silently holds graced entries forever (memory pin).
- W4: eager warm backing still gated on `BodyThreshold` → small graced
  bodies unrecoverable after hot SIEVE eviction.
- W5: passive health still unrecorded on the cache path → pool stays
  "healthy" during an outage → grace never engages.
- W6: `CloneForReturn` misses fields → grace lost after warm promote / re-put.

## Files

| File | Change |
|---|---|
| `pkg/api/storage.go` | Object: `KeepGrace`, `Pool` (+ clones); Stats: `ReaperGraceHolds` |
| `internal/cache/handler.go` | `buildObject` stamps grace; `refreshFrom304` restamps; 6 call sites |
| `internal/cache/stream.go` | buildObject call site |
| `internal/storage/hot.go` | `HotConfig.MayReap`, reaper gate, grace-holds stat |
| `internal/storage/tiered.go` | eager warm write + Protect for graced fills |
| `internal/origin/pool.go` | `consecutive5xx` on Pool, `HasHealthyTarget`, passive recording in `doSingleFetch` |
| `cmd/bouine/cmd/builder.go` | buildStore takes pools; wires `MayReap` |
| `cmd/bouine/cmd/engine.go` | pass pools to buildStore; grace-holds metric delta |
| `internal/observability/dataplane.go` | `bouine_hot_store_reaper_grace_holds_total` |
| `docs/decisions/0051-*`, `CHANGELOG.md`, `docs/runbook/54-origin-ejection.md` | docs |

## Performance budget

- Hit path: untouched (Get fast path reads only the SIEVE visited bit).
- Miss path: two field assignments per fill; graced fills pay one eager warm
  write (stayin_alive routes only).
- Reaper (cold): one bool check per expired entry; callback + counter only
  for graced-and-held entries.
- `doSingleFetch`: one atomic store per successful fetch when
  `consecutive_5xx > 0` (zero cost when unset, as today).
- `make bench-gate` before/after must show allocs/op identical on all
  `BenchmarkGate_*`.
