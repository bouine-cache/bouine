# ADR-0004: One `*http.Server` per listener

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 1 (pre-flight)

## Context

bouine's data plane terminates HTTP/1.1, HTTP/2, and HTTP/3 on
distinct addresses (`config.Listen` fields `http`, `https`, `http3`).
There are two reasonable structural choices:

1. **Shared mux** — a single `http.ServeMux` (or our pipeline) is
   wrapped by every listener; each listener decorates the same
   handler. Lifecycle is collapsed: one `Server.Shutdown(ctx)` drains
   all of them.
2. **One `*http.Server` per listener** — every listener has its own
   `http.Server` (or QUIC server) instance; they share the underlying
   pipeline handler but own their own goroutines, timeouts, and
   shutdown context.

`PLAN.md §14.1` mandates a precise graceful-shutdown sequence:

> Stop H1 listener with `Connection: close`, then send `GOAWAY` on H2,
> then `GOAWAY` on H3, then drain.

That sequence requires per-protocol control of the shutdown signal.
Per-listener is the natural shape; shared-mux forces us to fan out the
shutdown by hand.

## Decision

Each L1 listener (HTTP/1.1, HTTP/2-over-TLS, HTTP/3, optional h2c) is
owned by its own `*http.Server` (or `http3.Server`) instance.

- Every listener is launched in a goroutine owned by an
  `internal/runtime/supervised.Group`.
- Each listener has its own `Close(ctx)` method, called in the order
  spelled out in `PLAN.md §14.1`.
- Listeners share **only** the L2 pipeline handler (an `http.Handler`)
  and configuration — no state, no global mux.
- The PROXY-protocol shim, when enabled, lives at the `net.Listener`
  layer below the `*http.Server`, again per listener.

Listener ports are bound on `:0` in tests so they pick a free address;
production binds to the configured port. The supervised group
guarantees one panic does not take down the others without an audit
trail.

## Consequences

### Positive
- Per-listener timeouts, deadlines, and metrics labels — required by
  the threat-model rows for H1/H2/H3 (T05, T10, T28, T39).
- The graceful-shutdown sequence is a literal list of `Close(ctx)`
  calls; no fan-out logic needed.
- Independent reload/restart of a single listener becomes feasible if
  we ever want it.
- Each listener can be unit-tested with `httptest` in isolation.

### Negative / trade-offs
- Slightly more boilerplate — three `*http.Server` constructors
  instead of one. Mitigated by a small `listener` factory in
  `internal/listener`.
- More goroutines on the daemon (3–4 instead of 1). Negligible in
  practice; the supervised group keeps the count bounded.

### Risks
- Header / config drift between listeners. Mitigated by sharing the
  same handler and a single config struct.

## Alternatives considered

- **Shared mux** — rejected. Conflates protocol semantics, makes
  graceful shutdown harder, complicates threat-model bookkeeping.
- **Single mega-`http.Server` with multiple `net.Listener`** —
  rejected. Go's `http.Server` is designed for one listener at a time;
  the multi-listener pattern is non-idiomatic and breaks h2/h3.

## References

- `PLAN.md §2.2`, `§7`, `§14.1`
- `docs/security/threat-model.md` T05, T10, T28, T39
- `AGENTS.md §11` (concurrency / shutdown)
