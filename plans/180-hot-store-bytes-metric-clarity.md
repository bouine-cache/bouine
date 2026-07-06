# Plan: Make `bouine_hot_store_bytes` honest and diagnose the heap gap (#180)

**Status:** Draft
**Issue:** https://github.com/thylong/bouine/issues/180
**Scope:** `internal/admin`, `internal/observability`, `internal/storage`,
`pkg/api`, `docs/`. No data-plane hot-path changes. No new dependencies.
**Layers touched:** L7 (observability, admin), L4 (storage accounting), L0
(api doc), L8 (docs/runbook, dashboard).

---

## 1. Problem Statement

In production (prod/eu, 5 pods, full cluster mode, GOMEMLIMIT=30 GiB,
GOGC=100), `bouine_hot_store_bytes` reports ~6.4 GiB while
`go_memstats_heap_inuse_bytes` reports ~23 GiB — a 3.4× gap. Operators
cannot tell which number represents "cached objects," so they cannot size
the hot tier, tune GOGC, or distinguish a cache leak from a non-cache heap
leak.

The gap is **not** a bug in the cache. It is a documentation and
observability failure: `bouine_hot_store_bytes` is a hand-rolled eviction
accounting estimate (`objSize`, `internal/storage/hot.go:677`), but it is
documented and dashboarded as if it were a runtime memory metric. The
architecture docs also promise `GET /debug/pprof/*` on the admin port
(`docs/architecture.md:343`, `docs/architecture.md:436`), but pprof is not
mounted — the security review confirms this
(`docs/security/reviews/phase5-threat-walkthrough.md:47`). Two bench
scenarios already call `/debug/pprof/heap` and `/debug/pprof/goroutine`
(`bench/loadtest/scenarios/5.6e_ring_memory_pressure/run.sh:103-104`),
so those scenarios are silently broken today.

---

## 2. Root Cause Analysis (verified against current code)

`bouine_hot_store_bytes` is set every 15 s from `Stats().HotBytes`
(`cmd/bouine/cmd/engine.go:340`), which sums `s.bytes` across shards
(`internal/storage/hot.go:563-579`). `s.bytes` is maintained
incrementally: `+= objSize(obj)` on Put (`hot.go:323`), `-= objSize(...)`
on eviction/delete (`hot.go:270,301,312,377,406,424,447`). So `HotBytes`
is exactly `Σ objSize(live objects)`.

`objSize` (`hot.go:677-697`) sums:
- `len(obj.Body)` — accurate.
- `objectStructSize(256) + hotEntrySize(24) + sieveEntrySize(32) +
  mapPerEntryOverhead(50)` — struct footprint estimate.
- `headerEntriesSlice(24) + headerValuesSlice(24) +
  (headerEntrySize(24)+headerValueHeader(16)) * Len()` — header slice
  footprint, using **`Len()` = `len(entries)`**
  (`pkg/header/headermap.go:230`).
- `Σ len(v)` over active header values via `Range` — accurate for live
  values.
- `len(VaryKey) + len(ETag) + len(CacheControl) + Σ len(SurrogateKeys)`.

What `objSize` cannot see, and why the gap exists:

1. **Go allocator size-class rounding** (~10-15% per allocation; ~16
   allocations per object). At 1.07M entries: ~256 MB. Not fixable without
   runtime accounting, and not worth it.
2. **Orphaned `values` slots from `Del`**
   (`pkg/header/headermap.go:144-162`). `Del` removes the `headerEntry`
   but leaves the value string in `values[]`. `Set-Cookie` is always
   deleted from cached objects (`internal/cache/handler.go:1357`), so
   every cached response with a `Set-Cookie` orphans one value. `objSize`
   uses `Len()` (active entries) not `len(values)` (total slots). At
   1.07M entries: ~43 MB. **One-line fix.**
3. **`mapPerEntryOverhead = 50` is 2.3× too high** (`hot.go:669`). Go's
   `map[uint64]*hotEntry` bucket overhead is ~22 B/entry at load factor
   6.5 (144 B per 8-slot bucket). The constant overestimates by ~28
   B/entry, partially offsetting the underestimates above. At 1.07M
   entries: ~30 MB of overestimation. **One-line fix or document.**
4. **Non-cache heap consumers**: `heap_alloc_bytes` includes HTTP server
   buffers, goroutine metadata, memberlist state, warm-tier index, request
   path transients (`http.Header.Clone` in `doFetch`, JSON marshal in
   `BroadcastReplicate`). This dominates the gap.
5. **GC fragmentation**: `heap_inuse` > `heap_alloc` by 5-10 GiB on some
   pods due to churn-heavy SIEVE eviction fragmenting spans.

**Key evidence:** bouine-0 and bouine-2 have the same cache size (~6.87
GiB, ~1.07M entries) but `heap_alloc` differs by 6.7 GiB (15.7 vs 22.4
GiB). The gap is traffic- and GC-dependent, not per-entry inaccuracy.
Fixing `objSize` improves honesty by ~73 MB; it does **not** close the
3-4× gap. The gap is closed by giving operators the right three numbers
and a heap profile.

---

## 3. Design Overview

Five changes, ordered by operator impact. The first three close the
diagnosis and documentation gap (the actual complaint). The last two make
the estimate honest. No. 5 is optional and operator-decided.

```
 ┌─────────────────────────────────────────────────────────────┐
 │ 1. pprof on admin port (behind auth)  ← unblocks diagnosis  │
 │ 2. dashboard: add go_memstats_heap_alloc_bytes              │
 │ 3. re-document hot_store_bytes as eviction proxy            │
 │ 4. objSize: count len(values), fix mapPerEntryOverhead      │
 │ 5. (optional) operator tunes GOGC after seeing pprof        │
 └─────────────────────────────────────────────────────────────┘
```

No architectural change. No new layer dependencies. No public API
breakage. All changes are additive or doc-only, except the two `objSize`
constant corrections (internal, no external contract).

---

## 4. Implementation Steps

### Step 1 — Mount pprof on the admin port, behind bearer auth

**Why first:** the bench scenarios at
`bench/loadtest/scenarios/5.6e_ring_memory_pressure/run.sh:103-104` and
`5.6d_config_reload/run.sh:69` already depend on `/debug/pprof/heap` and
`/debug/pprof/goroutine` existing. They are broken today. This is the
single highest-leverage fix: it unblocks the operator, fixes the bench
harness, and satisfies the architecture contract.

**How:**

- Add `Pprof bool` (or `PprofEnabled bool`) to `admin.Config`
  (`internal/admin/server.go:31`). Default off for tests; the engine
  enables it in production. A bool (not a handler) keeps the admin package
  free of a `net/http/pprof` import side effect in tests that don't want
  the pprof goroutine/registry registration.
- In `mountOptionalRoutes` (`server.go:170`), when `cfg.Pprof` is true,
  register the pprof handlers explicitly against the mux (not via
  `http.DefaultServeMux`):

  ```go
  if cfg.Pprof {
      mux.HandleFunc("GET /debug/pprof/", pprof.Index)
      mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
      mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
      mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
      mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
      mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
      mux.Handle("GET /debug/pprof/goroutine", pprof.Handler("goroutine"))
      mux.Handle("GET /debug/pprof/block", pprof.Handler("block"))
      mux.Handle("GET /debug/pprof/mutex", pprof.Handler("mutex"))
      mux.Handle("GET /debug/pprof/threadcreate", pprof.Handler("threadcreate"))
  }
  ```

  Import `net/http/pprof` in `server.go`. The explicit `GET`-pattern
  registration (Go 1.22+ `ServeMux` pattern matching, already used by
  every other route in this file) avoids the `http.DefaultServeMux`
  global that `net/http/pprof`'s `init()` registers against — that global
  is a §2.3 violation waiting to happen.
- **Auth:** pprof paths are NOT added to the `exempt` map in
  `authMiddleware` (`server.go:459-475`). They must require the bearer
  token. This satisfies `docs/architecture.md:436` ("pprof mounted on
  admin port **behind auth**") and the security review's stance
  (T46). The `/debug/pprof/profile` endpoint can run for up to 30 s; it is
  a GET so the existing auth check (which exempts only specific paths, not
  all GETs) applies — pprof will be gated. Verify with a test.
- Wire `Pprof: true` in `cmd/bouine/cmd/engine.go:401` (the `admin.New`
  call). Gate it on a config flag `admin.pprof` defaulting to `true` so
  operators can disable it; add the field to `internal/config` Admin
  struct + `Validate`. The admin port is already network-isolated
  (K8s NetworkPolicy, never exposed externally), so default-on matches the
  architecture promise.
- **`WriteTimeout` problem:** the admin server sets `WriteTimeout: 5s`
  (`server.go:123`). `pprof.Profile` and `pprof.Handler("heap")` with a
  `seconds` query param can run longer. Two options:
  - (a) Wrap pprof handlers in a per-request `http.Server`-agnostic
    deadline using `r.Context()` — not possible, `WriteTimeout` is
    connection-level.
  - (b) Set `WriteTimeout: 0` (no write deadline) on the admin server and
    rely on `ReadHeaderTimeout` + `IdleTimeout` + request-scoped
    cancellation. This is the standard pprof-on-admin pattern. **Choose
    (b)** but document the rationale: the admin port is internal, auth'd,
    rate-limited, and the only long-running endpoints are pprof profile
    captures. Add a comment at `server.go:123` citing pprof.
- Update `docs/security/reviews/phase5-threat-walkthrough.md` T46 row
  from "not mounted → ✅ by absence" to "mounted, bearer-gated,
  admin-port only → ✅".

**Tests** (`internal/admin/server_test.go`):
- `TestPprof_RequiresAuth`: GET `/debug/pprof/heap` with no token → 401.
- `TestPprof_WithAuth_Returns200`: GET `/debug/pprof/heap?debug=1` with
  bearer token → 200, body contains `heap profile` (cheap, returns
  immediately when `debug=1`).
- `TestPprof_DisabledByDefault`: server built with `Pprof: false` →
  `/debug/pprof/heap` → 404 (not registered).
- `TestPprof_GoroutineDebug`: GET `/debug/pprof/goroutine?debug=1` with
  token → 200, body contains `goroutine`.

### Step 2 — Add `go_memstats_heap_alloc_bytes` to the Grafana dashboard

**Why:** operators need three numbers side by side to understand the
relationship:
- `bouine_hot_store_bytes` — cache accounting estimate (eviction budget).
- `go_memstats_heap_alloc_bytes` — actual live heap (GC's view).
- `go_memstats_heap_inuse_bytes` — actual heap spans (includes
  fragmentation).

These are already scraped by the Prometheus collector
(`prometheus/client_golang` exposes `go_*` via the default
`prometheus.DefaultGatherer` registered in `internal/observability`).
No new metric to emit — just a dashboard panel edit.

**How:** This is a Grafana dashboard change, not a code change. The
dashboard lives at the Grafana instance referenced in the issue
(`https://backmarket.grafana.net/d/ag5g5z/bouine-e28094-red-dashboard`).
The plan records the requirement; the actual panel edit is done via the
Grafana UI or provisioning, out of scope for this repo's code. Add a
runbook section (Step 3) that points operators at the three numbers.

If a JSON model of the dashboard is checked into this repo, add a panel
group "Memory" with the three queries; otherwise note the edit in the
runbook.

### Step 3 — Re-document `hot_store_bytes` as an eviction proxy

**Why:** the metric Help text and the `Stats.HotBytes` godoc both imply
"total bytes used," which is wrong. The dataplane metric Help already
says "estimated" (`internal/observability/dataplane.go:85`) — good — but
the `pkg/api` godoc does not.

**How:**
- `pkg/api/storage.go:159-161`: change
  `// HotBytes is the total bytes used by hot-tier objects (bodies + overhead).`
  to
  `// HotBytes is an estimated byte footprint of hot-tier objects used for
  // eviction budgeting. It is NOT a runtime memory metric; it cannot see
  // Go allocator size-class rounding, non-cache heap consumers, or GC
  // fragmentation. For actual heap usage, use go_memstats_heap_alloc_bytes
  // (see docs/runbook/40-memory-accounting.md).`
- `internal/observability/dataplane.go:85`: already says "estimated" —
  append `; for runtime heap, see go_memstats_heap_alloc_bytes.`
- Add `docs/runbook/40-memory-accounting.md` (new file, follows the
  `NN-topic.md` numbering of the existing runbook). Contents:
  - The three numbers table (hot_store_bytes, heap_alloc, heap_inuse).
  - "When to trust which": eviction tuning → hot_store_bytes; OOM
    investigation → heap_alloc + pprof; capacity planning → heap_inuse.
  - How to capture a heap profile: `go tool pprof
    http://<admin-addr>/debug/pprof/heap` (bearer token in
    `Authorization` header). Note the `WriteTimeout=0` change.
  - The ~73 MB known underestimate from `objSize` (Step 4) and why it is
    not worth closing further.
- Update `docs/architecture.md:343` entry to note pprof is bearer-gated
  (it already says "behind auth" at line 436; the table row should match).

### Step 4 — Fix the two `objSize` blind spots

**Why:** small (~73 MB at 1.07M entries) but makes the estimate honest
and removes the misleading `mapPerEntryOverhead=50` fudge.

**4a. Count orphaned values slots.**
`objSize` uses `Len()` (`len(entries)`) for the per-entry header overhead
but the `values` slice can be longer due to `Del`
(`pkg/header/headermap.go:154-162`). Replace the header sizing block in
`hot.go:681-683`:

```go
// Map: two slice headers + per-entry overhead. Use len(values) not Len()
// because Del orphans value slots (see headermap.go:144); the orphaned
// string headers still occupy heap until the Map is replaced.
nVals := int64(len(obj.Header.ValuesLen()))
size += headerEntriesSlice + headerValuesSlice +
    headerEntrySize*int64(obj.Header.Len()) +
    headerValueHeader*nVals
```

Add `func (h Map) ValuesLen() int { return len(h.values) }` to
`pkg/header/headermap.go` (exported, since `objSize` lives in
`internal/storage` and `Map` is in `pkg/header`). The `Range` loop that
adds `len(v)` for active values stays as-is — orphaned values' bytes are
already counted via the `headerValueHeader * nVals` term; their string
*data* is still referenced by the orphaned slot, so we should count it.
Adjust the `Range` to iterate over `values` instead of `entries`? No —
`Range` yields `(key, value)` for active entries only; orphaned values
have no key. To count orphaned string *data* bytes, add:

```go
// Count bytes of orphaned value strings (Del leaves them in values[]).
for i := nActive; i < nVals; i++ {
    // We can't index orphaned values by position safely because offsets
    // are not sorted; instead, sum all values and subtract active.
}
```

Simpler and correct: sum `len(v)` over the **whole** `values` slice via a
new `func (h Map) ValuesBytes() int`, and drop the `Range`-based sum.
This counts orphaned data bytes too. Replace the `Range` block
(`hot.go:684-687`) with:

```go
size += int64(obj.Header.ValuesBytes())
```

with

```go
// ValuesBytes returns the total byte length of all value strings in the
// values slice, including slots orphaned by Del. Used by objSize for
// accurate heap footprint accounting.
func (h Map) ValuesBytes() int {
    var n int
    for i := range h.values {
        n += len(h.values[i])
    }
    return n
}
```

This is O(n) per `objSize` call, same as the existing `Range`. Not on the
hit path (`objSize` runs on Put/evict only). Acceptable.

**4b. Correct `mapPerEntryOverhead`.**
Change `mapPerEntryOverhead` from `50` to `22` at `hot.go:669` with a
comment citing the Go runtime bucket size (8-slot bucket = 144 B at load
factor 6.5 → ~18 B/entry; round up to 22 for the hmap header amortized).
This removes ~28 B/entry of overestimation. Combined with 4a, the net
effect is a small *increase* in reported `HotBytes` (~73 MB at 1.07M
entries), making it a tighter lower bound.

**Tests** (`internal/storage/hot_test.go`):
- `TestObjSize_OrphanedValues`: build a `Map` with 3 headers, `Del` one,
  verify `objSize` counts the orphaned value's bytes (compare via a
  helper that exposes the delta, or assert `HotBytes` after Put reflects
  it).
- `TestObjSize_MapOverheadConstant`: sanity-check the constant equals 22
  (regression guard against accidental revert).
- `TestHotStore_Stats_AccountsForDelOrphan`: Put an object, `Del` a
  header on the stored object's Map (via a test helper that mutates in
  place), re-Put, verify `HotBytes` reflects the orphan.
- Existing `TestHotStore_Stats` (`hot_test.go:317`) and
  `TestHotStore_EvictsOnFull` (`hot_test.go:88`) must still pass — the
  eviction budget uses `objSize`, so the byte threshold shifts slightly.
  Re-baseline any hardcoded byte constants in those tests if they assert
  exact `HotBytes`.

**Benchmark:** `BenchmarkHotStore_Put` (`hot_bench_test.go:36`) —
`objSize` is on the miss path, not the hit path, so `allocs/op` on the hit
path is unaffected. Confirm with `go test -bench=BenchmarkHotStore_Put`
and compare to `main`. No hit-path benchmark should move.

### Step 5 — (Optional) Operator tunes GOGC after seeing pprof

Not a code change. Documented in the runbook (Step 3). With GOGC=100 and
a ~7 GiB live cache, the GC target is ~14 GiB; under traffic,
`heap_alloc` reaches 15-22 GiB. GOGC=50 targets ~10.5 GiB, narrowing the
gap and OOM risk at the cost of more frequent GC. The operator decides
after reading the pprof heap profile from Step 1. Out of scope for this
plan's code changes.

---

## 5. Test Plan

| Gate | Target | What changes |
|------|--------|--------------|
| `make test` | `internal/admin` | pprof auth/disabled/debug tests |
| `make test` | `internal/storage` | objSize orphan + constant tests |
| `make test` | `pkg/header` | `ValuesLen`, `ValuesBytes` tests |
| `make lint` | full repo | new import `net/http/pprof`, no depguard violation (admin is L7, pprof is stdlib) |
| `make bench` | `internal/storage` | `BenchmarkHotStore_Put` no hit-path regression |
| `make conformance` | none | no cache-logic change to freshness/validity |
| `make integration` | 3-node cluster | admin pprof reachable on each node, bearer-gated |

No fuzz, no chaos, no conformance regression expected — none of these
changes touch request parsing, cache key computation, freshness, or
invalidation semantics.

---

## 6. Risks & Tradeoffs

- **pprof exposure:** pprof reveals goroutine stacks and heap addresses.
  Mitigation: bearer token (constant-time compare, already implemented),
  admin port only (K8s NetworkPolicy, not exposed externally), default-on
  matches architecture promise. The `WriteTimeout=0` change is the price
  of `pprof.Profile`; the admin port is internal and rate-limited
  (`RateLimitPerSecond`), so this is acceptable. Document it.
- **`objSize` change shifts eviction budget:** `HotBytes` will tick up by
  ~73 MB at 1.07M entries (net of the orphan fix + map constant
  correction). This may cause marginally earlier eviction for operators
  whose `hot.max_bytes` is set close to the live set. The shift is <2% of
  a 7 GiB cache and makes the budget *more* accurate. Call it out in the
  CHANGELOG.
- **`WriteTimeout=0` on admin:** removes a defense against slowloris on
  the admin port. Mitigated by `ReadHeaderTimeout=5s`, `IdleTimeout=30s`,
  rate limiting, auth, and network isolation. The only long-running admin
  endpoint is pprof. Acceptable and standard.
- **Dashboard edit (Step 2) is out-of-repo:** the Grafana dashboard is
  not in this repo. The plan records the requirement; the runbook tells
  operators the three queries. If a dashboard JSON model is later added to
  the repo, wire the panel then.

---

## 7. Out of Scope

- Replacing `objSize` with runtime memory accounting (e.g.
  `runtime.MemStats`-driven per-object attribution). Not possible without
  a custom allocator; the estimate is good enough for eviction.
- Adding `go_memstats_*` metrics to the bouine Prometheus registry —
  `prometheus/client_golang` already exposes them via the default
  gatherer. No code change.
- Changing GOGC in code or config defaults — operator decision (Step 5).
- Reclaiming orphaned `values` slots in `header.Map.Del` — the comment at
  `headermap.go:144-153` says it's not worth the offset shifting; this
  plan agrees and only *counts* the orphans, does not reclaim them.
- Mounting pprof on the data plane — forbidden by AGENTS.md §2 and the
  architecture docs.

---

## 8. Sequencing

1. Step 1 (pprof) — unblocks bench scenarios and operator diagnosis.
2. Step 3 (docs) — cheap, no code risk, immediate operator clarity.
3. Step 4 (objSize) — small code change, tests + bench.
4. Step 2 (dashboard) — out-of-repo, coordinate with Grafana owner.
5. Step 5 (GOGC) — operator runbook, no code.

Each step is independently mergeable. Step 1 and Step 3 can land in the
same PR (both small). Step 4 is its own PR (touches storage accounting +
tests + bench re-baseline). File an ADR (0017) for the pprof-on-admin
decision since it changes the admin surface and the `WriteTimeout` trade.

---

## 9. Linus Review (applied to this plan)

**Verdict: Fix-before-merge** — one real design hole, the rest is polish.

### Finding 1 — BLOCKER: the plan lies about avoiding DefaultServeMux pollution

**Location:** Step 1, the pprof registration block.

**The claim:**
> "The explicit `GET`-pattern registration ... avoids the
> `http.DefaultServeMux` global that `net/http/pprof`'s `init()` registers
> against — that global is a §2.3 violation waiting to happen."

**The problem:** This is wrong. `net/http/pprof` is, in the Go docs' own
words, "typically only imported for the side effect of registering its
HTTP handlers." Its `init()` registers on `http.DefaultServeMux`
**unconditionally on import**, regardless of whether you call
`pprof.Index` yourself. You import the package → the init runs →
DefaultServeMux gets the `/debug/pprof/*` handlers. Calling
`pprof.Index` explicitly does not prevent the init; it runs *in addition
to* it. So the plan gets the global-state pollution it claims to avoid,
and misleads the implementer into thinking they dodged it.

**The evidence:** `go doc net/http/pprof` — "The package is typically
only imported for the side effect of registering its HTTP handlers.
The handled paths all begin with /debug/pprof/." The init is in the
package; you cannot import `pprof.Index` without it.

**The fix:** Pick one and say it honestly:
- (a) Accept the DefaultServeMux pollution. It is harmless because bouine
  never serves `http.DefaultServeMux` — every server uses its own
  `http.NewServeMux()`. State this explicitly: "importing `net/http/pprof`
  registers handlers on `http.DefaultServeMux` via init(); this is
  acceptable because bouine never serves DefaultServeMux, and the
  explicit registration on our own mux is what actually serves the
  endpoints."
- (b) Avoid the side effect entirely by importing `runtime/pprof` and
  writing thin handlers that call `pprof.Lookup("heap").WriteTo(w,
  debug)` etc. More code, zero global pollution.

Choose (a) — it's the standard Go approach, and bouine's "no global
mutable state" rule (§2.3) is about *bouine's* state, not stdlib init
registration of a mux bouine never uses. But say it honestly.

### Finding 2 — bug: anti-entropy backfill behavior change not addressed

**Location:** Step 4 Risks section.

**The problem:** `HotStore.OverBudget()` (`hot.go:591`) uses
`Stats().HotBytes > h.maxBytes`. The doc comment at `hot.go:587-589`
says it is "Used by anti-entropy to skip backfill under memory pressure
(#175)." Step 4 raises `HotBytes` by ~73 MB at 1.07M entries, which can
flip `OverBudget()` true sooner, causing anti-entropy to skip backfill
sooner. The Risks section mentions eviction but not anti-entropy
backfill. That's a behavioral change to cluster consistency behavior,
not just eviction timing.

**The fix:** Add to Risks: "Step 4 raises `HotBytes` by ~73 MB at 1.07M
entries, which can flip `OverBudget()` true sooner. This affects not
just eviction but anti-entropy backfill skip logic (`hot.go:587-592`,
#175). The effect is <2% of a 7 GiB budget and makes the budget more
accurate, so earlier skip is *correct* behavior — but it is a behavioral
change that must be called out in the CHANGELOG and tested via
`make integration` (anti-entropy backfill under memory pressure
scenario)."

### Finding 3 — taste: two exports where one method would be cleaner

**Location:** Step 4a, `ValuesLen()` + `ValuesBytes()`.

**The problem:** The plan exports two new accessors on `pkg/header.Map`
to expose `len(values)` and `Σ len(values[i])`. Two exports for one
concern (orphan-aware footprint) is grubby. A single method returning
the three numbers `objSize` needs would be cleaner and keep the
accounting logic in one place:

```go
// Footprint returns the heap footprint components of the Map for
// eviction accounting: the number of header entries, the number of
// value slots (including Del-orphaned slots), and the total bytes of
// all value string data. Used by internal/storage.objSize.
func (h Map) Footprint() (entries, valueSlots, valueBytes int) {
    return len(h.entries), len(h.values), h.valuesBytes()
}
```

Then `objSize` calls `obj.Header.Footprint()` once. One export, one
concept, and the storage package doesn't reach into the header's slice
layout twice.

### Finding 4 — nit: "silently broken" overstates the bench failure

**Location:** Step 1, "Why first" paragraph.

**The problem:** `bench/loadtest/scenarios/5.6d_config_reload/run.sh:71`
has `except Exception as e: print('pprof fetch failed:', e)` — it prints
a failure message. That is not "silently broken," it is "noisily fails
and continues." 5.6e may be silent (its error handling wasn't verified
in the plan). The claim is overstated for one of the two cited scripts.

**The fix:** Change "silently broken" to "broken — 5.6d prints a
failure and continues, 5.6e fails the pprof capture step."

### Finding 5 — nit: mapPerEntryOverhead 22 hmap-amortization wording

**Location:** Step 4b.

**The problem:** The plan says "round up to 22 for the hmap header
amortized." The 22 comes from bucket overhead (144 B / 6.5 load factor
= 22.2). The hmap struct header (~96 B) amortized over 1M entries is
negligible (~0.0001 B/entry) and is NOT what the rounding accounts for.
The wording implies 22 includes the hmap header; it doesn't.

**The fix:** "8-slot bucket = 144 B at load factor 6.5 → ~22 B/entry.
The hmap struct header (~96 B) is negligible at 1M+ entries and not
included in the constant."

---

### Review notes (not findings)

- The plan's core conception is correct: the gap is a docs/observability
  failure, not a cache bug, and pprof + three-numbers + honest doc is
  the right fix. Step 4's ~73 MB is honestly labeled as "honesty, not
  gap closure." No conception drift.
- The `WriteTimeout=0` tradeoff is correctly identified and justified.
- The sequencing is sound: pprof first unblocks everything else.
- The ADR requirement (0017) is correctly noted for the admin surface
  change.

## 10. Fixes applied

Findings 1-5 addressed below in the revised Step 1 and Step 4 text.

### Revised Step 1 — pprof import side effect (Finding 1)

Import `net/http/pprof` directly. Its `init()` registers handlers on
`http.DefaultServeMux`; this is **acceptable** because bouine never
serves `http.DefaultServeMux` — every server constructs its own
`http.NewServeMux()` (admin at `server.go:116`, data plane separately).
The "no global mutable state" rule (AGENTS.md §2.3) governs *bouine's*
state; a stdlib init registering on a mux bouine never serves is not a
violation. The explicit `GET`-pattern registration on our own mux is
what actually serves the endpoints — the DefaultServeMux registration is
dead code in our process. State this in the ADR (0017) and in a comment
at the import.

### Revised Step 4 — anti-entropy risk (Finding 2)

See Risks section, now updated to call out the anti-entropy backfill
behavioral change and the `make integration` requirement.

### Revised Step 4a — one footprint method (Finding 3)

Replace the `ValuesLen()` + `ValuesBytes()` two-export approach with a
single `Footprint()` method on `pkg/header.Map`:

```go
// Footprint returns the heap footprint components of the Map for
// eviction accounting by internal/storage.objSize. entries is the
// number of active header keys; valueSlots is len(values) including
// slots orphaned by Del; valueBytes is the total byte length of all
// value strings (active + orphaned). Used for eviction budgeting only;
// not a runtime memory metric.
func (h Map) Footprint() (entries, valueSlots, valueBytes int) {
    var n int
    for i := range h.values {
        n += len(h.values[i])
    }
    return len(h.entries), len(h.values), n
}
```

Then `objSize` (`hot.go:681-687`) becomes:

```go
entries, valueSlots, valueBytes := obj.Header.Footprint()
size += headerEntriesSlice + headerValuesSlice +
    headerEntrySize*int64(entries) +
    headerValueHeader*int64(valueSlots) +
    int64(valueBytes)
```

One export, one call, no Range loop in objSize.

### Revised Step 4b — mapPerEntryOverhead wording (Finding 5)

Change `mapPerEntryOverhead` from `50` to `22` with comment:
"8-slot bucket = 144 B at load factor 6.5 → ~22 B/entry. The hmap struct
header (~96 B) is negligible at 1M+ entries and not included."

### Revised Step 1 — "silently broken" wording (Finding 4)

Changed to: "broken — 5.6d prints a failure and continues, 5.6e fails
the pprof capture step."
