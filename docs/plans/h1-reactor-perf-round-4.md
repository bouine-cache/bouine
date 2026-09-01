# H1 reactor performance — round 4 plan

Date: 2026-08-30 (implemented 2026-09-01)
Branch: `perf/h1-epoll-reactor`
See also: ADR-0041, docs/plans/hit-path-p99-optimization.md

## Implementation record (2026-09-01)

All workstreams landed on this branch. Two design deviations from
the plan, both deliberate:

- **W3 overflow policy**: drop-newest instead of overwrite-oldest.
  Overwrite would let the producer rewrite a slot mid-consumer-read
  (records are shared, not double-buffered); dropping the record we
  are pushing is always safe. Drops are counted and logged at reactor
  shutdown (`H1 reactor dropped hit-metric records`, runbook 51).
- **W4 tracker shape**: a dedicated spawner goroutine fed by a bounded
  queue (close-on-full, conn reset) instead of a lock-free intrusive
  list. Same loop-decoupling, far less ABA/generation hazard; the
  WaitGroup-Add ordering hazard is solved by joining the spawner
  (spawnerDone) before the Close-time wg.Wait, since every Add
  happens on the spawner.

Also during implementation, the tracker + stuck-writer + handoff
spawn code moved from the portable reactor.go into the
Linux transport file: the `unused` linter on non-Linux builds was
correct that no portable code path could reach them, and blank
reference tricks don't satisfy its call-graph analysis. The portable
state machine keeps `handoffConn` (prefix replay, tested on all
platforms); the spawn machinery is Linux-only, like its only caller.

### Measured results (M1 Pro, darwin, gate benchmarks, count=5)

| gate                | before  | after   | delta   | allocs |
|---------------------|---------|---------|---------|--------|
| Reactor_Hit         | 193.3   | 138.6   | −28.3%  | 0      |
| H1Parse_Get         | 236.9   | 198.4   | −16.3%  | 0      |
| FastPath_Hit        | 131.1   | 130.6   | (path untouched) | 0 |
| Reactor_Hit_Metrics | (new)   | ~170    | gates hook path | 0 |
| Reactor_Dispatch    | (new, Linux) | ~12.5 µs/64-event batch | gates dispatch | 0 |

Linux (Docker arm64) confirms the same 0-alloc gates. The nightly
runner (§3.2/§3.6) remains the authoritative end-to-end verdict, per
ADR-0041's "if the numbers don't move, the flag goes back off".

### Verification record

- `go test -race` full suite: green on darwin/arm64 and linux (Docker;
  one pre-existing warm-tier test failure in Docker is a
  running-as-root environment artifact — fails identically on HEAD).
- `make bench-gate` equivalents: 15/15 darwin, 16/16 linux, all
  budgets enforced (BUDGETS gained Reactor_Hit_Metrics portable and
  Reactor_Dispatch linux-only entries).
- cache-tests conformance: 365 tests, zero pass→fail regressions vs
  the committed baseline (per-test diff; one flaky setup test flipped
  fail→pass, one upstream harness rename).
- `make lint`: every finding on changed lines resolved; the remaining
  funlen/gocyclo findings pre-date this branch (verified on HEAD).
- New tests: metrics ring unit tests (round-trip, overflow, drainer
  order, ticker drain), stuck-writer sweep drop, handoff-storm +
  concurrent Close shutdown race, scan-flag stale-state pinning
  (first-Host-wins / last-Cache-Control-wins / duplicate-CL saturate /
  flags reset per parse), multi-segment terminator scan resume.

### Linus review round (2026-09-01, post-implementation)

The review of the full stack found and fixed:

1. **eachConn copied the 512 KB fd table per sweep** — range over an
   array field copies; slice range reads lazily. One sweep per second
   per loop was paying a 64 Ki-element memcpy for nothing.
2. **Pending-queue socket leak at shutdown** — connections the accept
   goroutine parked but the loop never registered had no owner after
   loop exit; cleanup now drains `pending` and closes them. Caught by
   the storm-shutdown test hanging 87 s on half-open sockets.
3. **Retained fast-path response never released on dropped writers**
   — every mid-flush drop path (stuck-writer sweep, EPOLLERR) now
   funnels Release through `reactorConn.release()`, honoring the
   Release-after-every-TryHit pool contract.
4. **The metrics gate benchmark lied twice** — it still exercised the
   sync hook path (W3 moved production to the ring push), and without
   draining it measured the ring-overflow path after 2048 iterations.
   Now wires the ring and drains every 1024 iterations: ~170 ns
   steady state, 0 allocs.
5. Comment lies: "first Connection header wins" (code correctly
   honors any close token per RFC 9110), orphan duplicated doc
   comments, a stale symbol name, and a dispatch benchmark that
   allocated 64/op from harness closures (0 now).
6. **Spawner shutdown races** (found by the hardened storm test under
   `-race`, fixed in the follow-up commit): handoffsDone closed before
   the spawner joined, letting Close's wg.Wait race the spawner's
   in-flight wg.Add; and a handoff accepted after the loop's final
   batch would send on a closed spawn queue. Shutdown is now
   quit-channel based with dual-drain completion before the Wait.

## Where the reactor stands

Rounds 1–3 (commits 9ec05a7, 4453b43, 867531f) plus the hardening round
(0ca2f4b) landed: single-goroutine epoll loop per listener, raw-fd
read/write/writev, writev zero-copy over retained buffers, epoll_ctl
elision via `rc.epollInterest`, per-request idle budget with an idle
sweep, composed-head per-second cache, shutdown drain, and handoff
ownership rules. `BenchmarkGate_Reactor_Hit` is 370–405 ns / 0 allocs on
the Linux runner. This plan is the next round of measured increments.

Everything below is evaluated against one structural fact that changes
the optimization math from the blocking path: **the loop goroutine is
single-threaded per listener**. Every nanosecond spent per hit is (a)
serial CPU on one core and (b) latency added to every other connection
multiplexed on the same listener. Contention-based rejections made on
the blocking path (e.g. the metrics A/B in
hit-path-p99-optimization.md) do not transfer: what was spread across
goroutines is now serialized on the loop. Each item must therefore be
re-justified for the loop, not assumed from prior blocking-path
measurements.

## Per-hit cost audit (as of HEAD of this PR)

Numbers in [] are estimates from the blocking-path profiles unless
stated; items are ordered below by expected value after the audit.

1. **`parseBuffer` re-zeroes the whole RawRequest per hit**
   (parser.go:404, `*req = api.RawRequest{Scheme: p.scheme}`).
   The struct embeds `[100]RawHeader` ≈ 3.3 KB; the assignment memsets
   it on every request — ~100–150 ns of pure memset plus ~56 cache
   lines evicted from L1/L2 that the subsequent header writes must
   refill. At 50k RPS on one loop that is ~165 MB/s of memory traffic
   whose only job is to erase data nobody reads (all readers bound by
   `NHeaders`). reactor.go:289 already removed the *second* zero and
   documents the reasoning; the remaining one is the target.
2. **Four separate O(NHeaders) scans per hit**, each doing ~10
   `EqualFold` calls per header:
   - parseHeaders' Host scan (parser.go:506)
   - `connectionCloseToken` → `req.Header("Connection")` scan
     (parser.go:518, fastpath-side scan at parser.go:792)
   - `qualifiesForFastPath`'s six conditional-name + TE + CL + CC +
     Pragma comparisons (fastpath.go:151–189)
   - `smugglingDetected`'s CL/TE duplicate scan
   For an 8-header request: ~80 case-insensitive compares + 4 loop
   passes. [~100–300 ns]
3. **The metrics hook runs inline on the loop goroutine**
   (reactor.go:283 → dataplane.go:890 `RecordHit`): route-table map
   lookup, four index switches, two counters, one histogram `Observe`.
   `Observe` was measured at ~410 ns under parallel load / ~2–8 ns
   unshared for plain counters on the blocking path; on the loop it is
   serial CPU regardless of contention. [~150–400 ns]
4. **Handoff work runs on the loop goroutine** (reactor_epoll_linux.go:285,
   reactor.go:484): `handoffTracker.spawn` takes `t.mu` — a mutex shared
   with every blocking-path goroutine's `unregister` — and the `go`
   statement (~1–2 µs) executes inline. Under mixed/missy traffic
   (§3.6, §3.3) each miss stalls all hits multiplexed on that listener.
5. **`map[epollFD]*reactorConn` lookup per event**
   (reactor_epoll_linux.go:147): one Go map hash+probe per readiness
   event, two events per request (read + write re-arm path). [~20–40 ns
   each] Plus map insert/delete churn per accept/handoff.
6. Hygiene items, each small but free:
   - `findHeaderEnd` rescans the buffer from offset 0 on every partial
     read (reactor.go:207) — O(n²) for multi-segment headers.
   - `drainPending` re-derives the fd via `connFD(rc.conn)`'
     SyscallConn dance although accept already cached `rc.fd`
     (reactor_epoll_linux.go:373 vs :349).
   - `newReactorConn` heap-allocates the `writeVec` backing array
     (reactor.go:157) — one alloc per connection, inlineable.
   - `wake()` writes a pipe byte per accepted connection even when the
     pipe already holds unread bytes (reactor_epoll_linux.go:309).
   - gopls hints: `for i := 0; i < n; i++` → `for i := range n`
     (reactor_epoll_linux.go:145) and `[]byte(fmt.Sprintf...)` →
     `fmt.Appendf` in reactor_epoll_test.go:256.

## Workstreams

Each workstream: change, risk, verification, effort. Workstreams are
independent; land smallest-first where value is equal. All of them
touch L1–L3 hot code, so every one carries the standard gate: `make
lint`, `make test` (race), `make bench-gate` (allocs/op must equal
main exactly — 0), `make conformance`, nightly A/B on the Linux runner
(scenarios 3.2 + 3.6, benchstat, ±2% p99/RPS gates).

### W1 — Soft-reset RawRequest instead of struct-literal zeroing (S)

**Change**: in `parseBuffer` (parser.go:404), replace
`*req = api.RawRequest{Scheme: p.scheme}` with field-wise reset of the
~7 scalar/short fields (`Method`, `Path`, `Query`, `Host`,
`HTTPVersion`, `NHeaders`, `ConnectionClose`); never touch `Headers`.
All readers iterate `[0:NHeaders)` only (`Header`, `HasHeader`,
`qualifiesForFastPath`, `smugglingDetected`, `onStale` copy), so stale
entries are unreachable garbage. Both the reactor and blocking paths
share `parseBuffer`; both get the win.

**Risk**: low. The invariant "no reader may touch headers beyond
NHeaders" is already the contract (the reactor comment at
reactor.go:289 documents it). Fuzz corpus for the parser runs in CI.

**Verification**: `BenchmarkGate_H1Parse_Get` and
`BenchmarkGate_Reactor_Hit` ns/op drop with allocs still 0; a new unit
test parses a many-header request followed by a one-header request and
asserts no stale header is observable; parser fuzz corpus green.

**Expected**: removes ~3.3 KB memset + cache refill per hit — the
single largest removable per-hit cost in the loop. [~100–200 ns/hit]

### W2 — Fuse the four header scans into one pass (M)

**Change**: parseHeaders' per-line loop already touches every header
once. Extend it to compute a flags bitmask + captured values in that
single pass: conditional-header presence (If-None-Match, If-Modified-
Since, Range, If-Range, If-Unmodified-Since, If-Match), TE/CL presence
and duplicate counts (feeds `smugglingDetected`), first `Host` value,
first `Connection` value (token scan stays as-is on that one value),
last `Cache-Control` raw value, `Pragma: no-cache` presence. Store the
bitmask in `RawRequest` (pkg/api — additive field, semver-safe);
`qualifiesForFastPath` and `smugglingDetected` become flag checks.

**Risk**: medium — must preserve exact duplicate-header semantics:
qualifiesForFastPath takes the *last* Cache-Control (overwrite order),
parseHeaders takes the *first* Host and *first* Connection (`break`),
smuggling counts CL/TE duplicates. Pin each with a dedicated test
before refactoring (some exist; add the missing duplicate-CC and
first-vs-last cases).

**Verification**: benchmark gate deltas, cache-tests score unchanged,
fuzz corpus green (Vary/CC tokenizers are the fuzz surface).

**Expected**: [~100–300 ns/hit] and removes most of the parser CPU
beyond line splitting.

### W3 — Move the metrics hook off the loop goroutine (M)

**Hypothesis**: the blocking-path metrics rejection
(hit-path-p99-optimization.md) does not transfer to the reactor: there,
contention never materialized because goroutines spread the CAS load;
here the *uncontended* `Observe` CPU itself (~150–400 ns) is serial
loop time added to every multiplexed connection's latency.

**Change**: per-loop SPSC ring (fixed `[N]record` array, N=4096,
power-of-two mask, no locks — one producer on the loop goroutine, one
drainer goroutine per loop). The record carries pre-indexed ints +
route string + durNs + bytesOut — no strings from the read buffer are
retained. The drainer calls the existing `RecordHit` semantics.
Overflow policy: overwrite-oldest + a `bouine_reactor_metrics_dropped_total`
counter. Push must be 0-alloc and ~20 ns.

**Gate before landing**: profile the reactor-enabled daemon under §3.2
on the Linux runner (pprof CPU on the loop goroutine); land only if
`RecordHit`/`Observe` shows ≥ 5% of loop CPU. If the profile shows
otherwise, record the evidence here and close the item — the repo
standard is measured increments, not convictions.

**Risk**: medium. Lossy metrics under overload is a behavior change —
document it in the ADR addendum and the runbook; the drop counter
makes it observable. Ordering across the ring is per-loop only
(already true today).

**Verification**: new `BenchmarkGate_Reactor_Hit_Metrics` (hook wired,
budget 0 allocs) in `bench/run.sh` BUDGETS; nightly A/B p99/RPS;
`bouine_requests_total` scrape totals within loss bound.

### W4 — De-synchronize handoff from the loop (M)

**Change**, two independent parts:

- **Lock-free tracker**: `handoffTracker` (reactor.go:478) exists only
  for shutdown drain. Replace mutex+map with an intrusive lock-free
  list (atomic head push on spawn; drain iterates) — unregister becomes
  an atomic CAS on the node, no mutex, no cross-goroutine cache line
  ping-pong with the loop.
- **Off-loop spawn**: the loop keeps the cheap, ownership-critical
  work (EPOLL_CTL_DEL, map delete, `release`, `wg.Add(1)) and pushes
  the rc onto a bounded channel; the existing accept goroutine (idle
  between accepts) or a single spawner goroutine builds the
  `prefixConn` and starts the blocking `Serve` goroutine.

**Risk**: medium. Shutdown ordering is the hazard: `wg.Add` must happen
on the loop goroutine *before* the push (Add must not race a Wait that
could see zero), and `drainForceClose` may miss a conn registered a
moment later — acceptable within the existing grace window, but write
the ordering down in the tracker's doc comment and add a shutdown
stress test (handoff storm + concurrent Close, `-race`).

**Verification**: §3.3/§3.6 nightly A/B (miss-storm is where the loop
stall shows); existing reactor epoll tests + new shutdown race test;
gate benchmarks unchanged.

**Expected**: removes a mutex round-trip + ~1–2 µs `go` statement from
the loop per miss; under miss mixes this is the difference between the
reactor degrading gracefully and stalling its hits.

### W5 — fd-indexed connection table (S–M)

**Change**: replace `conns map[epollFD]*reactorConn`
(reactor_epoll_linux.go:71) with an open-addressed array indexed by fd
(nginx's model): `[64 Ki]*reactorConn` = 512 KB per loop, O(1) probe,
no hashing, no map growth, insert/delete are single stores. fds are
dense kernel indices far below 64 Ki under any real RLIMIT_NOFILE;
larger fds fall back to a small overflow map (or straight handoff).

**Rejected sub-option**: stuffing the `reactorConn` pointer into
`epoll_event.data` (nginx's `data.ptr`). The current batch loop can
observe an event for a conn that was dropped/handed off *earlier in
the same epoll_wait batch*; today the map miss makes that a no-op
(reactor_epoll_linux.go:147), with raw pointers it becomes a
use-after-free unless a generation/epoch check is added per dispatch.
The fd-array keeps the same miss-is-no-op safety with the same O(1)
cost — take the array.

**Risk**: low. Table is loop-goroutine-owned, same as the map.

**Verification**: gate benchmarks; epoll e2e tests; memory RSS check
(512 KB/loop is inside the +5% steady-state gate at realistic listener
counts).

**Expected**: [~20–40 ns per readiness event] and cheaper accept/handoff
churn (no map resize under §5.1 connection storms).

### W6 — Hygiene batch (S)

Land together; each is a few lines:

1. Resume-scan offset in `advanceReading`: remember how far
   `findHeaderEnd` scanned; continue from there on the next partial
   read (reactor.go:204–215). O(n²) → O(n) for multi-segment headers.
2. `drainPending` uses `rc.fd` (set at accept) instead of
   `connFD(rc.conn)`'s SyscallConn control (reactor_epoll_linux.go:373).
3. Inline the `writeVec` backing array into `reactorConn`
   (`[3][]byte` + slice-over-array) — removes the per-connection heap
   allocation at reactor.go:157.
4. Coalesce `wake()` with an `atomic.Bool`: write the pipe byte only
   when transitioning false→true; the loop clears the flag in
   `drainWake` after draining. Ordering: set flag before write, clear
   after drain — a stray extra byte is harmless, a lost one is not.
5. Lint hints from gopls: range-over-int at
   reactor_epoll_linux.go:145, `fmt.Appendf` at reactor_epoll_test.go:256.

**Verification**: gate benchmarks + full test suite. [tens of ns/hit +
accept-path syscalls under storms]

### W7 — Reactor write safety net (S, robustness — unlocks headroom)

`sweepIdle` deliberately skips writers (reactor_epoll_linux.go:170–174)
and the comment claims the blocking path's 5-minute write deadline is
the safety net — but the reactor path never arms one: raw-fd writes
have no timeout, handoff mid-write is forbidden, so a connection stuck
in `rcWriting` (client that stops reading) parks a `reactorConn`
**forever**, silently consuming the 4096-conn budget per loop. 4096
stalled writers = a dead listener with no metric moving.

**Change**: in `sweepIdle`, drop writers whose write phase exceeded
`safetyNetWriteTimeout` (5 min — parity with the blocking path). The
start of write phase is already tracked: `reqStart` is set at request
start and only refreshed in `finishWrite`, so `state == rcWriting &&
now.Sub(reqStart) > safetyNet` is the condition. No new fields needed.
Document in the runbook (new failure mode: metric suggestion
`bouine_reactor_stuck_writers_total` if it ever fires).

**Verification**: Linux epoll test with a socketpair whose read end is
never drained (fill SO_SNDBUF, assert drop after deadline via injected
clock — tests must not sleep; use the fake clock seam the reactor
tests already have).

### W8 — New gates so the loop itself is measured (S)

`BenchmarkGate_Reactor_Hit` covers the state machine (parse→hit→flush)
but nothing gates the loop-side costs W3–W5 target. Add:

- `BenchmarkGate_Reactor_Dispatch` — drives `dispatch` with a
  pre-built events array over registered fake conns (no syscalls):
  covers map/table lookup, mod elision, idle check, action switch.
  Budget: 0 allocs.
- `BenchmarkGate_Reactor_Hit_Metrics` (from W3) — hook wired.
- A Linux-only `BenchmarkSingle_Reactor_EpollE2E` over a real listener
  + N socketpair conns, self-skipping under time-driven benchtime per
  the `BenchmarkSingle_*` convention.

Both new gates need `BUDGETS` entries in `bench/run.sh` (drift/stale
check enforces it). Land W8 first or with W1 so every later change has
a before/after.

## Deferred / rejected (with rationale)

- **Edge-triggered epoll (EPOLLET)**: rejected for now. `parsed()`
  can return with undrained socket bytes (partial pipeline) and LT's
  re-event is the correctness backstop; ET would need drain-to-EAGAIN
  plus re-arm discipline on every exit path for a saving the mod
  elision already captured. Revisit only if profiling shows spurious
  LT re-wakeups as measurable loop cost.
- **io_uring**: deferred per ADR-0041. Revisit only if nightly
  profiling shows `epoll_wait` itself as the residual cost after W1–W5.
- **Raw `accept4` to skip Go-poller registration of accepted sockets**:
  real win only under connection-churn storms (§5.1); large surgery
  (bypasses net.Listener). Candidate for a later round, measured
  against §5.1 first.
- **Sharded Prometheus collectors (LongAdder-style)**: still rejected
  for the blocking path per hit-path-p99-optimization.md; W3 solves the
  reactor-side cost with far less code.
- **reactorConn pooling under churn**: ~21 KB per conn (16 KiB inline
  readBuf) is GC churn under §5.1-style storms; zero effect on
  steady-state hit-only. Later round, gated on a §5.1 RSS/GC profile.
- **CPU affinity pinning of loop threads** (`SchedSetaffinity` after
  `LockOSThread`, nginx `worker_cpu_affinity` model) and
  `tcp_defer_accept` in the loadtest config: both are loadtest-config
  experiments on the runner, not code changes — try in the same nightly
  A/B slot before spending code.

## Sequencing

1. W7 + W6 + W8 (safety net, hygiene, gates) — one small PR, zero-risk
   order-of-operations, everything after lands against the new gates.
2. W1 (soft reset) — biggest single win, smallest diff.
3. W2 (fused scan) — next biggest, medium diff.
4. W3 (metrics ring) — *profile-gated*: run the §3.2 pprof first; land
   only on ≥5% loop-CPU evidence.
5. W4 (handoff de-sync) — matters most for §3.3/§3.6; ship with the
   shutdown race test.
6. W5 (fd table) — last of the hot-path items.
7. Nightly A/B after each step; stop or reorder on evidence. If the
   cumulative nightly delta is inside the noise band, the reactor flag
   decision (ADR-0041: "if the numbers don't move, it goes back off")
   is re-evaluated rather than papered over with micro-wins.

## Success criteria

- `BenchmarkGate_Reactor_Hit` and the two new gates: 0 allocs/op
  exactly (bench-gate hard requirement), ns/op improved per the
  expected column where measured.
- Nightly §3.2: p99 and RPS within gates (±2%) against the previous
  nightly with the reactor on — improvements counted in benchstat
  deltas vs main.
- Nightly §3.6/§3.3: no hit-path degradation under miss mixes (W4's
  payoff surface).
- cache-tests score: not regressed (touching parse qualifies as cache
  logic → `make conformance` mandatory after W1/W2).
- All AGENTS §16.4 gates green; no new dependency (everything is
  stdlib + golang.org/x/sys, already allowed).
