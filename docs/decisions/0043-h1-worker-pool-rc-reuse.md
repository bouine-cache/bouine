# ADR-0043: Worker pool and reactorConn reuse for the miss round trip

- **Status**: Accepted
- **Date**: 2026-09-07
- **Deciders**: @theotime
- **Phase**: perf round 4 follow-up (W2 of the client-perceivable plan)
- **Consulted**: ADR-0042 (return-to-reactor), the miss-churn analysis
  on the same A/B (pr567-5d38db0)

## Context

ADR-0042's return-to-reactor closes the starvation gap, but the return
path made every miss a full allocation round trip:

- `returnFromBlocking` built a **fresh `reactorConn`** per return
  (~20 KiB inline: 16 KiB readBuf + ~3.4 KiB scratch + three closures
  + the writev iovec slice).
- `Parser.Serve` grows each spawned goroutine's stack by ~32 KiB
  (16 KiB readBuf + scratch locals) — and handoff spawned a fresh
  goroutine per miss.
- `handleFallThrough` allocated a fresh **16 KiB `bufio.Reader`** plus
  the rebuilt request head per miss.
- A full handoff spawn queue (128) **reset connections** instead of
  queueing them.

At the hot-only target profile (6k RPS, 20% miss ≈ 1.2k misses/s) that
is ~55 MB/s of allocation churn and stack growth that main's
park-once model pays zero of — the same failure mode that once drove
63 GC cycles/s on the hit path (parser.go's scratch history). Under
sustained miss storms the spawn queue overflowed into client-visible
resets.

## Decision

Three changes, all behind the existing `experimental.h1_reactor` flag:

1. **Worker pool replaces spawn-per-miss.** The handoff tracker runs a
   pool: a dispatcher goroutine spawns workers on demand (capped at
   1024), workers loop over a 128-slot job channel. A queued miss now
   waits for a worker (bounded backpressure) instead of being shed;
   shedding returns only when both the queue and the worker cap are
   saturated. Shutdown contract unchanged: the dispatcher joins
   before the WaitGroup wait, and `stopSpawner` drains queued jobs
   into one-shot workers so no queued conn is orphaned.
2. **reactorConn reuse across handoff/return.** `handoffConn`
   attaches the parked rc to the replay's `prefixConn`; the return hook
   now receives `(conn, rc)`. `returnFromBlocking` resets and
   re-registers the SAME struct when the fd matches (reset clears
   request/flush state; the readBuf, closures, and iovec are
   conn-bound). When the conn dies on a worker, the worker recycles
   the rc into a `sync.Pool` — the ~20 KiB struct is amortized across
   every miss round trip of its conn's lifetime. An fd-identity check
   guards against cross-conn reuse; conns that never came from a
   handoff carry no rc and get a fresh (or pooled) struct.
3. **Pooled fall-through buffers.** `handleFallThrough` draws a
   per-request-cycle buffer set from a `sync.Pool`: the rebuilt head,
   the 16 KiB bufio reader (reset per request), and the owned leftover
   copy (W1's pipelining contract) — one pool draw per blocking
   goroutine instead of three allocations per miss. Retention capped
   at 64 KiB per slot.

Also fixed here: a declined return (queue full / shutdown) left the
conn's OS read deadline cleared with only the previous request's
leftover window; `Serve` now re-arms the deadline from now, restoring
the slowloris guarantee ADR-0042's return path had broken.

## Consequences

### Positive
- Miss round-trip heap churn drops from ~45-50 KiB (rc + bufio +
  head) to the fasthttp ctx internals (~3-6 allocs); the reactorConn
  itself is reused, not rebuilt.
- No goroutine spawn per miss under sustained missy traffic; worker
  stacks are paid once and amortized across the pool's lifetime.
- Miss storms queue instead of resetting clients until both the 128
  queue slots and the 1024 workers saturate — the honest shed point
  moves ~8x further out.
- The declined-return slowloris hole is closed.

### Negative / trade-offs
- Pool workers park on the job channel forever until quit — a missy
  listener keeps up to `handoffMaxWorkers` goroutines alive even
  after traffic stops (bounded, and identical in spirit to the
  blocking path's per-connection goroutines).
- The rc-reuse fd-identity check is one more branch on the return
  path (nanoseconds against the ~µs it saves).
- `sync.Pool` semantics: a GC cycle can drop pooled rc/buffer sets —
  the pool is an amortization, not a guarantee; steady state under
  load retains them (frequent Get keeps slots hot).

### Risks
- UAF-style bugs if a recycled rc's readBuf aliases a conn still
  served elsewhere: prevented by the fd-identity check, the
  serveJob-side recycle happening strictly after the conn close, and
  the race-detector storm/chaos suites.
- Worker-pool starvation of the reactor loop: the loop only pays a
  non-blocking channel send; dispatch and Serve run on worker
  goroutines (unchanged from ADR-0042's spawner placement argument).

## Alternatives considered

- **Keep spawn-per-miss, pool only the rc** — rejected: half the churn
  remains (spawn + stack growth), and the reset-on-RST cliff stays.
- **Unbounded worker pool** — rejected: a miss storm must degrade to
  shedding at a defined point, not OOM via goroutine count.
- **Reuse via a loop-owned parked-rc map keyed by conn** — rejected:
  workers would mutate a map the loop goroutine owns, breaking the
  single-owner invariant the transport's whole design leans on. The
  prefixConn carries the rc with the conn instead — data travels
  with ownership.
- **Serve-level shared readBuf pool** — deferred: `Serve`'s 16 KiB
  stack array is ~1 µs of memset; the worker pool amortizes the stack
  itself. Revisit only if profiles show it.

## References

- ADR-0042 — the return path this builds on.
- `internal/server/h1parser/reactor_epoll_linux.go` — worker pool,
  returnFromBlocking reuse, serveJob recycle.
- `internal/server/h1parser/reactor.go` — reset/recycle, handoffConn
  rc attachment.
- `bench/run.sh` — `Reactor_MissRoundTrip` / `FallThrough_Pooled`
  alloc budgets (the drift gate for this work).
