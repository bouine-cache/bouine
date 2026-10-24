# H1 reactor performance — round 5 plan

Date: 2026-10-10
Branch: `perf/h1-reactor-round-5`, stacked on `perf/h1-reactor-return-path`
(#605) — the base decision and the landed-workstream record are below.
See also: ADR-0041, docs/plans/h1-reactor-perf-round-4.md (all eight
workstreams landed), docs/plans/hit-path-p99-optimization.md

## Implementation record (2026-10-10)

Landed on `perf/h1-reactor-round-5` (stacked on #605):

- **W2 — realistic-header gates**: `BenchmarkGate_H1Parse_Get_Headers8`
  and `BenchmarkGate_Reactor_Hit_Headers8` (production-shaped head: long
  request line with a query + the 8 canonical browser/proxy headers,
  ~500 B) with `BUDGETS` entries, both 0 allocs/op.
- **W1 — vectorized line scans**: `parseRequestLine` / `parseHeaders` /
  `skipRequestLine` now use the stdlib's `bytes.Index`/`bytes.IndexByte`
  (the SIMD-backed searches `findHeaderEnd` already used for the
  terminator) instead of scalar byte-at-a-time walks; the hand-rolled
  `indexByte` helper is deleted. Parity is pinned by
  `parser_scan_parity_test.go`: the pre-W1 scalar implementations are
  kept verbatim as reference oracles, and a deterministic differential
  (25k random inputs over the decision-byte alphabet + hand-derived
  boundary cases + seeded mutations of the realistic head) asserts
  byte-identical outputs — extracted fields, header array, scan flags,
  error identity. The parser fronts every cache key, so that
  differential is the cache-poisoning defense for this change.

Measured (M1 Pro darwin, alternating A/B, count=6–10 medians):

| bench | before | after | delta |
|---|---|---|---|
| parse only, 8-header head (isolated) | 245 ns | 146 ns | **−40%** |
| Gate_Reactor_Hit_Headers8 (new gate) | 653 ns | 548 ns | −16% |
| Gate_H1Parse_Get_Headers8 (new gate) | 561 ns | 421 ns | −25% |
| parse only, 2-header toy | 27.2 ns | 28.1 ns | +1.4 ns (stdlib search setup; noise-level) |
| Gate_Reactor_Hit (toy) | within machine noise | — | gates unchanged, 0 allocs |

Before/after on the toy gates ran alternating on identical code with
±40% machine variance (117–202 ns for the same binary), so only the
isolated parse benches are trusted locally; the nightly Linux runner's
benchstat (time-driven) is the authoritative verdict, per the repo
standard.

**Deferred from this PR, deliberately**:

- **W3 (clock threading)**: #605 rewrote the `advance`/`parsed`/
  `finishWrite` plumbing it would thread through, and the win is
  ~2 saved `CoarseNow` reads ≈ 4–8 ns/hit on Linux (the darwin 26 ns
  `time.Now` numbers do not transfer). Not worth churn on a stacked
  PR; land it solo once #605 merges, if at all.
- **W4–W6** (reactor workers, store de-contention, io_uring): unchanged
  from the sequencing below — separate PRs, W4 next.

**Base decision**: stacked on #605 rather than main. W1's scan
functions are untouched by #605 (applies to either base), but the
round-5 plan is written against #605's reactor (return-to-reactor,
  pipelined-inline serving, coalesced writev, miss worker pool), and
  W3+ would conflict with its rewritten state machine. #605 also
  lands two items this plan had deferred (pipelined-inline serving and
  reactorConn pooling) — the plan's rejection of inline pipelining is
  superseded by #605's implementation and its e2e/parity evidence.

## Reactor vs blocking fast path — A/B result (2026-10-10)

The on-demand `reactor-ab` workflow (added with the A/B arms) ran the
full comparison on a GitHub-hosted arm64 Linux runner — identical
fresh-response stub, identical request bytes, same run, medians of
n=15 (run 38082318433):

| arm | p50 | p99 |
|---|---|---|
| echo floor (control) | 6.9 µs | 9.9 µs |
| **blocking fast path, toy head** | **7.6 µs** | **12.0 µs** |
| **reactor, toy head** | 16.7 µs | 21.4 µs |
| blocking, 8-header head | 8.0 µs | 12.8 µs |
| reactor, 8-header head | 16.9 µs | 22.2 µs |
| blocking, 1 / 4 / 16 clients | 7.6 / 21.5 / 53.3 µs | 11.8 / 55.4 / 158 µs |
| reactor, 1 / 4 / 16 clients | 16.6 / 22.5 / 78.2 µs | 21.4 / 63.4 / 208 µs |

**Verdict: the reactor is still not faster — ~2.2x slower at p50 and
~1.8x at p99 on a keep-alive connection, and slower at every concurrency
(1/4/16 clients).** The previous verdict stands on this hardware class:

- The blocking path is within ~10% of the raw echo floor (7.6 vs
  6.9 µs): the Go runtime's netpoller park/wake is already cheap, and
  the hit runs on an already-scheduled goroutine. The reactor trades
  that for the locked-OS-thread wake chain — every request on a low-
  concurrency connection pays the epoll_wait wake plus the loop's
  serialized CPU, which costs ~9 µs here.
- The realistic head is noise in the RTT (blocking +0.45 µs, reactor
  +0.27 µs): scheduling dominates; the vectorized parse (W1) matters
  for loop CPU, not for single-client RTT.
- At 16 clients the single serialized loop degrades faster (78 vs
  53 µs p50) than goroutine-per-connection scaling across Ps.

Caveats: this is a shared cloud runner, not the pinned nightly runner
(CPU affinity, dedicated cores) where ADR-0041's batch-amortization win
was expected; the pinned-runner stress A/B (pending for #605's
return-path work) remains the authoritative end-to-end verdict. But
every measurement to date — round-4 RTT (10.2 vs 5.3 µs), the external
vegeta harness (zero benefit), and this A/B — points the same way on
commodity hardware. Per ADR-0041's own criterion ("if the nightly
numbers don't move, the flag goes back off"), the reactor flag decision
owes itself a pinned-runner verdict.

Two latent A/B-harness bugs were found and fixed while building this
(the read loops counted only the body, stranding stale response bytes;
the shared-response stub drained under blocking serveHit's
net.Buffers.WriteTo, serving empty bodies from the second hit) — both
in the benchmarks, not production paths.

## Where the reactor stands (written against #605)

Rounds 1–4 landed: single-goroutine epoll loop per listener, raw-fd
read/write/writev, writev zero-copy over retained buffers, epoll_ctl
elision via `rc.epollInterest`, fd-indexed connection table, fused
header scan (scan flags), soft RawRequest reset, async metrics ring,
off-loop handoff spawn, stuck-writer sweep, adaptive busy-poll (spin
budget 80, operator-overridable), and the per-second composed-head
cache. Gates: `Reactor_Hit` 0 allocs, 370–405 ns on the Linux runner
(toy request). #605 adds: return-to-reactor after blocking-path
requests, pipelined hits served inline with coalesced writev batches
(up to 5), per-connection miss worker pool with reactorConn recycling,
  and reactor telemetry counters.

## Per-hit cost audit (2026-10-10, M1 Pro darwin, fresh measurements)

Single-shot benches (`-benchtime=2000x`, count 3–5), unless noted:

| item | ns/op | notes |
|---|---|---|
| Reactor_Hit (2-header toy) | 181–185 | 0 allocs, state machine only |
| Reactor_Hit_Metrics | 234–249 | +55 ns = ring push + 1 extra clock read |
| H1Parse_Get (toy) | 236–262 | 0 allocs |
| parseBuffer, 0 extra headers | ~58 | realistic request line |
| parseBuffer, 8 extra headers | ~234 | **~22 ns/header** |
| parseBuffer, 16 extra headers | ~415 | linear in headers |
| FastPath_Hit (toy) | 161–168 | incl. store Get 33 + evaluate 70 |
| Evaluate_Hit | ~70 | |
| HotStore_Get_Hit | 33–35 | |
| time.Now / call (darwin) | ~26 | 3 reads/hit ≈ 84 ns darwin — **CoarseNow is 2–4 ns on Linux, so ~12 ns/hit there: hygiene only** |
| syscalls per hit | 2 (read + writev) | round-4 sustained profile: ~50% Syscall6 samples, parser <1% *at RTT-shaped low concurrency* |

Two structural facts rank everything below:

1. **The gates lie about the parser.** Every gate bench feeds a
   2-header toy request (`GET / HTTP/1.1\r\nHost: localhost\r\n\r\n`).
   Real traffic carries 8–15 headers, where `parseBuffer` costs
   234–415 ns — more than the entire rest of the Go-side hit path
   (fast path 164 + dispatch + ring push). The cost is almost entirely
   the scalar byte-at-a-time line scans:
   `parseRequestLine` (parser.go:480–518) walks the request line
   three times looking for `'\r'` and `' '`; `parseHeaders`' line-end
   loop (parser.go:535–541) and `skipRequestLine` (parser.go:591–598)
   walk every header line byte by byte; the `indexByte` helper
   (parser.go:917–924) is a scalar loop. `findHeaderEndFrom`
   (parser.go:469) already uses `bytes.Index` — the SIMD-substituted
   stdlib search — for the terminator; the per-line scans predate it.

2. **Syscalls are the residual structural cost.** Per hit the loop
   pays exactly two syscalls (read + writev) plus an amortized
   `epoll_wait` share, and one `epoll_ctl` per miss (handoff DEL,
   reactor_epoll_linux.go:468). Round-4's sustained-load profile
   already showed ~50% Syscall6 samples — ADR-0041's io_uring revisit
   condition ("epoll_wait itself as the residual cost") is met by the
   same evidence class: the syscall boundary, not readiness, is the
   residual.

Everything else measured is small or already amortized: ring push
~25–30 ns, dispatch ~10–20 ns/event (fd table), mod elision zero
syscalls on the full-flush path, composed-head cache zero appends
inside a second.

## Workstreams

Standard gates for all of them (AGENTS §16.4): `make lint`,
`make test` (-race), `make bench-gate` (allocs/op must equal main
exactly), and — anything touching the parser or keying is cache
logic — `make conformance`, plus nightly A/B on the Linux runner
(§3.2 + §3.6, benchstat, ±2% gates).

### W1 — Vectorized line scans in the parser (S–M)

**Change**: replace the hand-rolled scalar scans with stdlib
SIMD-backed searches, preserving first-match semantics exactly:

- `parseRequestLine`: `lineEnd` scan → `bytes.IndexByte(buf, '\r')`
  with the existing `len(buf)-1` / `buf[lineEnd+1] != '\n'` guards
  unchanged; `sp1`/`sp2` scans → `bytes.IndexByte(line, ' ')`.
- `parseHeaders`' line-end loop → `bytes.Index` over a two-byte
  needle from `pos` (equivalent to the
  `buf[lineEnd] != '\r' || buf[lineEnd+1] != '\n'` pair walk, bounded
  identically by `len(buf)-1`).
- `skipRequestLine` → same treatment.
- `indexByte` (parser.go:917) → `strings.IndexByte` (delete the
  helper; callers read subslices of the buffer either way).

**Risk**: low-medium. This is the parser that fronts every cache key:
a semantic drift here is a *cache-poisoning* surface (different
Method/Path/Query/Host → different key), not just a perf change.
Mitigation is output parity, not review confidence: the parser's
extracted outputs (`RawRequest` fields, `NHeaders`, `ScanFlags`,
`ConnectionClose`) are byte-identical by construction only if each
replacement preserves first-match and boundary behavior — pin it with
new boundary table tests (no CRLF, CRLF exactly at end, lone `\r` at
end, `\r` followed by non-`\n`, header line ending at `len(buf)-1`),
the committed fuzz corpus (`go test -fuzz` seeds under
`testdata/fuzz/`), and a corpus-differential test: record
`parseBuffer` outputs for the fuzz corpus before the change, assert
identical outputs after (same technique as the xxhash differential).

**Verification**: new realistic-header gates (W2) must show the win;
toy gates (`H1Parse_Get`) must not regress; `make conformance`
mandatory; fuzz corpus green.

**Expected**: 8-header parse 234 → ~140–160 ns (the scans are the
per-header cost; `bytes.Index`/`IndexByte` use AVX2/SVE paths). This
is the single largest removable Go-side per-hit cost.

### W2 — Realistic-header gates (S, land first)

**Change**: the toy-request gates measure a parser that production
never sees. Add `BenchmarkGate_H1Parse_Get_Headers8` and
`BenchmarkGate_Reactor_Hit_Headers8` using a production-shaped head
(long request line + the 8 canonical browser/proxy headers, ~500 B),
both with `BUDGETS` entries (0 allocs). Nothing else changes; the
gates just stop lying.

**Verification**: `make bench-gate` green with the new entries
(drift/stale check enforces the BUDGETS pairing automatically).

**Expected**: no behavior change; every later parser claim is
measured against production-shaped input.

### W3 — Clock-read threading hygiene (S)

**Change**: `parsed()` takes the batch `now` from `dispatch`
(reactor_epoll_linux.go:287 already computes it per batch; thread it
through `advance` → `advanceReading` → `parsed`), reuses it for
`TryHit` (coarse clock is 1 ms-resolution — batch skew is µs) and for
`finishWrite`'s `reqStart` re-arm (reactor.go:450); the metrics `dur`
keeps its one fresh read (reactor.go:310). Three reads → one.

**Risk**: low. CoarseNow is 2–4 ns on Linux (fp_conn.go:34 comment),
so the production win is ~8 ns/hit — this is hygiene, not a
workstream headline. The idle-window semantic shift is bounded by the
writev duration (~µs) against a 120 s budget and a 1 ms clock.

**Verification**: reactor tests (idle, slowloris, write safety net)
unchanged; gates unchanged or trivially better.

### W4 — Reactor workers per listener (M)

**Problem**: without `reuse_port`, one loop serves the whole
listener's hit traffic on a single core (ADR-0041 says so
explicitly); `reactorMaxConns` (4096, reactor.go:474) bounds it but
adds no parallelism. The parallel deployment today requires the
operator to configure `reuse_port` with N listeners.

**Change**: `experimental.h1_reactor_workers: N` (default 1). The
accept goroutine distributes accepted connections round-robin (or
least-loaded by pending-queue depth) across N loops' pending queues;
each loop keeps its own epoll instance, fd table, spin budget, sweep,
and handoff spawner. Every per-loop ownership rule stays identical —
this is N copies of the existing loop behind one accept goroutine,
not a new sharing design.

**Design note**: the Parser is currently built once per listener
(fp_conn.go:30) and the metrics ring hangs off the Parser
(reactor_epoll_linux.go:206–210) — one ring, one drainer. With N
loops, each loop needs its own ring/drainer pair: either clone the
Parser per loop (it is a small immutable-after-construction struct)
or move the ring into the loop. Clone is simpler; the metrics hook
semantics (per-loop ordering, which already holds) are unchanged.

**Risk**: medium-low. All epoll/map state stays loop-goroutine-owned;
the accept→pending path already crosses goroutines safely
(reactor_epoll_linux.go:544–551). Shutdown drains each loop the way
one is drained today; the storm-shutdown test extends to N loops.

**Verification**: epoll e2e tests ×N loops; storm + concurrent-Close
shutdown race test under `-race` with N=4; nightly §3.2 with workers
= core count and reuse_port off — the A/B that decides the default.

**Expected**: hit throughput scales to N cores without any operator
config change; removes the reuse_port dependency for reactor
scaling.

### W5 — Store/metrics read-path de-contention (M, profile-gated)

**Problem** (only once W4 ships N loops + blocking goroutines hit the
same shards concurrently): `HotStore.Get` (hot.go:414–434) takes a
shard RWMutex read lock and does `e.windowHits.Add(1)` — an atomic
RMW on the entry — plus `h.stats.hits.Add(1)` — a global atomic — per
hit. Uncontended that is the measured 33 ns; across N cores it is a
cacheline ping per hit per shard, and the RLock itself bounces when
miss Puts interleave.

**Change options**, in escalation order, gated on a pprof of the
multi-loop daemon under §3.2 showing the read path ≥ 5% of loop CPU
(the repo standard: measured increments, not convictions):

1. Move `windowHits` and `stats.hits/misses` increments off the read
   path: the reactor already ships the hit record to the metrics ring
   (reactor_metrics.go) — the drainer goroutine (one per loop) can
   own these counters, or they become per-CPU aggregates sampled at
   scrape. The shard entry's `windowHits` needs the entry pointer, so
   the drainer form requires a small record extension (shard index +
   key), evaluated against the ring's fixed-size contract.
2. Per-shard `sync.Map` for `entries` (read-optimized; SIEVE mutation
   still under the shard mutex) — bigger surgery in a ≥95%-coverage
   package; only on profile evidence that the RLock, not the
   atomics, is the cost.

**Risk**: option 1 changes counter timing (visibility lags the drain
interval) — document in the runbook; `windowHits` feeds the
refresh-prioritization gate, so its consumer must accept the lag
(refresh decisions are already periodic).

**Verification**: `HotStore_Get_Hit` gate unchanged (0 allocs);
concurrent-hit benchmark (`-cpu 4,8`) before/after; §3.2 nightly A/B
with workers=N.

### W6 — io_uring transport behind a flag (L, ADR required)

**Motivation**: two syscalls per hit is the dominant per-hit cost
that no Go-side work touches (round-4 profile: ~50% Syscall6). With
batch size B, submitting B reads/B writevs per cycle in one
`io_uring_enter` amortizes the syscall cost to ~1/B per op — the
same batching property epoll bought over park/unpark, one layer
down. `IORING_OP_EPOLL_CTL` even lets the per-miss handoff DEL join
the batch.

**Design sketch** (ADR-0041's "larger batch, fewer syscalls, far more
complex lifecycle" — the complexity is why this is last):

- Same state machine, same handoff-before-any-response-byte rule.
  The raw-fd `readFn`/`writeFn`/`writeVecFn` seams are replaced by
  submission at the points the syscalls happen today; completions
  drive `advance` the way readiness does now.
- Per-op kernel pinning of the writev iovecs — the retained-response
  zero-copy body aliasing survives (registered buffers would force a
  copy and drag RLIMIT_MEMLOCK into ops; avoid them).
- Kernel probe at boot (5.10+; tuned for 5.10+ per ADR-0041) with
  silent epoll fallback — the flag is
  `experimental.h1_iouring`, requiring `h1_reactor`.
- Cancellation discipline on drop/handoff of a conn with in-flight
  ops (`IORING_OP_ASYNC_CANCEL` or completion-drain before `close`),
  and the retained-response Release must move from
  write-completion to op-completion — same contract, new trigger.

**Risk**: high — lifecycle, cancellation, and shutdown drain are
exactly where correctness bugs live. This workstream is gated on the
nightly evidence that W1–W4 have landed and syscalls still dominate
the profile; if they do not, close it with the evidence recorded.

**Verification**: Linux-only e2e suite mirrored from the epoll tests
(same scenarios, both transports); chaos suite with the flag on;
nightly §3.2/§3.6 A/B; a dedicated `BenchmarkGate_Reactor_Dispatch`
equivalent for the completion path; ADR-0042+.

**Expected**: at batch B, per-hit syscall cost 2 → ~2/B; sustained
RPS/core up proportionally to the Syscall6 profile share.

### W7 — Hygiene batch (S)

1. `pushHit` (reactor_metrics.go:67): the second `tail.Load` per
   push is a store-published counter read on the producer's own
   line — free to keep, but the `head.Load` can be cached in the
   producer between drains (drain is the only head mover) — saves
   one atomic read per hit. Only on measurement.
2. Loadtest-config experiments (no code): `SO_ATTACH_REUSEPORT_CBPF`
   accept steering for reuse_port deployments, CPU affinity for the
   loop threads (`worker_cpu_affinity` model, already noted in
   round 4), `GOGC`/`GOMEMLIMIT` on the runner — one nightly A/B
   slot each, before any of them becomes code.
3. gopls hints from the audit: `min`/`max` modernizations
   (hot.go:349, tiered.go:216, tiered.go:1379), `range n` at
   reactor_epoll_linux.go:292, `b.Loop()` in the routedfastpath
   bench (cosmetic, batch with any touch of those files).

## Deferred / rejected (with rationale)

- ~~**Inline pipelined-hit serving**~~ — landed by #605 (with the
  framing-safety guards this plan demanded: the excess is always the
  next request because qualifying requests carry no body, non-hits
  keep the handoff-with-replay semantics, and an intercepted handoff
  flushes already-served hits before replaying the follower). The
  original rejection rationale is superseded; the remaining risk
  (body-bearing GETs) is excluded by the same request qualification
  that disqualifies CL/TE.
- **EPOLLET**: round-4 rationale stands (LT re-event is the
  partial-pipeline correctness backstop).
- **Lock-free hot-shard map**: Go maps are unsafe for concurrent
  read during mutation; RCU-style generation swap or sync.Map is
  surgery in a 95%-coverage package — only as W5 option 2, on
  profile evidence.
- **Registered io_uring buffers**: forces a copy of the aliased
  response body (kills zero-copy writev) plus RLIMIT_MEMLOCK ops
  burden. Per-op pinning suffices.
- ~~**reactorConn pooling under churn**~~ — landed by #605 (the miss
  worker pool recycles reactorConn structs; the miss round-trip gate
  `Reactor_MissRoundTrip` budgets 4 allocs where the spawn-per-miss
  path cost ~45 KiB).

## Sequencing

1. **W2** (realistic gates) — everything after is measured against
   production-shaped input.
2. **W1** (vectorized scans) — biggest Go-side win, output-parity
   pinned.
3. **W3** (clock threading) — rides along with W1's parser touch.
4. **W4** (workers) — throughput scaling; decides the
   `reuse_port`-free default via nightly A/B.
5. **W5** (de-contention) — profile-gated, only meaningful with W4
   shipped.
6. **W6** (io_uring) — ADR first, gated on post-W1 profile showing
   syscalls still ≥ the round-4 ~50% share.
7. Nightly A/B after each step; stop or reorder on evidence, per
   the round-4 rule.

## Success criteria

- `Reactor_Hit_Headers8` and `H1Parse_Get_Headers8`: 0 allocs/op
  exactly; ns/op improved (W1) with toy gates non-regressing.
- `Reactor_Hit` and `Reactor_Hit_Metrics`: 0 allocs, non-regressing.
- `make conformance`: zero pass→fail regressions (parse changes are
  cache logic — mandatory gate).
- Nightly §3.2: p99/RPS within ±2% vs previous nightly at minimum,
  with W4/W6 expected to move RPS up.
- Nightly §3.6/§3.3: no hit-path degradation under miss mixes.
- No new dependency for W1–W5 (stdlib only); W6 is stdlib-adjacent
  (io_uring via raw syscalls or `golang.org/x/sys` — already
  allowed) but requires an ADR.
- Every workstream states and tests its no-poisoning invariant: the
  bytes that feed `buildKeyFromRaw`, `VariantKeyFromRaw`, and the
  peer vary gate are identical before/after (W1's differential test
  is the template).

*This plan follows docs/plans/h1-reactor-perf-round-4.md; measured
numbers above were produced 2026-10-10 on the M1 Pro dev machine
(single-shot benches, count 3–5) and are reproducible via
`make bench-gate` plus the W2 gates once landed.*
