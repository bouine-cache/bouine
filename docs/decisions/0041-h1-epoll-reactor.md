# ADR-0041: epoll reactor for batch hit-path serving

- **Status**: Accepted (experimental, behind `experimental.h1_reactor`)
- **Date**: 2026-08-30
- **Deciders**: @thylong
- **Phase**: performance (post-phase 9 fast path)
- **See also**: ADR-0034 (fasthttp adoption), docs/plans/hit-path-p99-optimization.md

## Context

After the fast path reached ~180 ns/op with 0 allocations and the
loadtest configuration was fixed to actually enable it
(docs/plans/hit-path-p99-optimization.md, "2026-08-30 follow-up"), the
remaining structural gap to nginx's hit-path latency is the
scheduling model: the h1parser parks and unparks a goroutine for the
read and again for the writev of every request
(`pthread_cond_signal` was 10.7% of CPU samples under load), while
nginx's worker event loop batches many connections per `epoll_wait`
wakeup and processes them on one thread.

Profiling on the reference M1 Pro showed the same class of cost:
~2 goroutine context switches plus two scheduler wakeups per request
(~1-2 µs of CPU that produces no cache work), and the Go runtime
poller's `net.Conn.Read`/`Write` park path — a futex wait plus a
handoff through the scheduler — dominates everything else on the hit
path once the cache work itself is ~200 ns.

SO_REUSEPORT accept-loop sharding (listener.go serveMulti) already
removes accept contention; the per-connection goroutine remains.

## Decision

We add a single-goroutine event loop (the "reactor") to h1parser,
enabled by `experimental.h1_reactor` (requires `h1_fast_path`;
Linux only). Per listener:

- One reactor goroutine (pinned with `runtime.LockOSThread`) owns an
  epoll instance and all hit-path connections of that listener.
- A separate accept goroutine accepts connections, sets the sockets
  non-blocking, and parks them on a bounded channel; the loop
  goroutine registers them (epoll/map state is owned by exactly one
  goroutine).
- Readiness batching: one `epoll_wait` returns a batch of ready
  connections; the loop parses, looks up, serializes, and writes each
  inline via raw-fd syscalls (EAGAIN mapped to a sentinel —
  `net.Conn.Read/Write` are never used in the loop because they park
  the goroutine on the runtime poller, which is the cost this ADR
  removes).
- Everything that is not a plain cache hit — miss, conditional, range,
  pipelined body bytes, oversize headers, malformed input, idle
  expiry — hands off to a per-connection goroutine running the
  existing blocking Parser with the buffered bytes replayed through a
  `prefixConn`. Handoff only happens before any response byte is
  written; a partially-written hit is completed by the reactor via
  write readiness. Correctness semantics (fall-through framing,
  smuggling 400, SWR revalidation, keep-alive deadlines) are therefore
  shared with the blocking path rather than reimplemented.
- Connection cap per loop (`reactorMaxConns`, 4096): beyond it, new
  connections go straight to the blocking path, which scales across
  Ps. This bounds both the fd set and the single-threaded parse
  capacity.

The flag defaults to off in general configs but is enabled in the
loadtest configuration (`bench/loadtest/config/bouine.yaml`): the
nightly runner confirmed the blocking-path numbers with the fast path
on (PR #563), and the reactor is now the measured increment — if the
nightly numbers don't move, the flag goes back off.

Without `reuse_port`, one reactor loop serves the whole listener's hit
traffic on a single core: the 4096-connection cap does not add
parallelism, it only moves overflow to the blocking path. The intended
deployment is `reuse_port` with N listeners (serveMultiFastPath
spawns one reactor per listener), matching nginx's worker-per-core
model.

## Consequences

### Positive
- No goroutine park/unpark per hit request: one wakeup serves a batch,
  and the hit path runs entirely on one thread with core-local maps
  and buffers.
- Hit-path CPU drops by the scheduler costs (~1-2 µs/request class);
  expected to show at p99 and at high RPS on the Linux runner.
- Miss traffic is untouched: it flows through the existing blocking
  parser, so no risk to correctness-critical paths.
- The reactor is a transport for the same state machine the blocking
  path uses (shared `parseBuffer`/`parseRequest` parsing), keeping one
  HTTP grammar implementation.

### Negative / trade-offs
- Two serving paths (reactor + blocking) with a handoff boundary; the
  boundary is before any response byte, and the handoff replays
  buffered bytes, so no request can be served twice or lost — but the
  code is more subtle than a single path.
- The reactor loop is single-threaded per listener: a slow or
  CPU-heavy request can delay other connections in the same batch.
  Mitigated by the hit-only inline serving (misses hand off
  immediately) and the connection cap.
- Raw-fd I/O bypasses the Go runtime poller: the blocking handoff
  path must re-derive deadlines (it does — the blocking parser owns
  deadlines after handoff), and TLS connections are never
  reactor-served (the listener wiring only routes plaintext H1
  listeners through the reactor).

### Risks
- Handoff starvation: a flood of miss connections could churn
  register/handoff cycles. Bounded by the pending channel and
  reactorMaxConns; misses bypass the reactor entirely after handoff.
- Idle expiry depends on the loop's clock: uses the injected
  `nowFunc` (CoarseNow), same as the blocking parser.

## Alternatives considered

- **fasthttp Prefork** — rejected in
  docs/plans/hit-path-p99-optimization.md: forks diverge the
  in-memory cache, break admin/gossip port binding, and its 2019-era
  benefit targets scheduler overhead this reactor removes without
  forking.
- **io_uring** — larger batch and fewer syscalls still, but kernel
  requirements (5.1+, tuned for 5.10+) and a far more complex
  lifecycle; epoll is the incremental step with the same batching
  property. Revisit if the nightly runner shows epoll_wait itself as
  the residual cost.
- **Keep blocking path only** — the status quo; leaves the per-request
  scheduler cost as the dominant non-cache CPU on the hit path.

## References

- docs/plans/hit-path-p99-optimization.md (profiling evidence;
  prefork rejection)
- fasthttp `prefork/README.md` (the nginx-model trade-offs)
- nginx event loop documentation (batched epoll worker model)
