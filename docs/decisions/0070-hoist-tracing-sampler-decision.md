# ADR-0070: Hoist the tracing sampler's drop decision ahead of attribute building

- **Status**: Accepted
- **Date**: 2026-10-27
- **Deciders**: @thylong
- **Phase**: performance (topology findings F4/D1/D2)
- **See also**: ADR-0034 (fasthttp adoption, tracing stack),
  `internal/observability/tracing/tracing.go`,
  bench/topology (the measurement harness), finding F4 in
  `notes/Bouine/topology-perf-audit.md`

## Context

With tracing enabled, `FastHTTPMiddleware` built four span attributes
(four byte→string conversions) and entered the OTel SDK's `Tracer.Start`
before the sampler decided anything. At a ratio sampler, ~99% of those
decisions are "drop", so the request goroutine paid the attribute
building and the SDK's span machinery to learn nothing would be
recorded. Measured on the topology harness (M1 Pro, darwin/arm64):

- enabled + NeverSample (root request): **502 ns / 10 allocs** per
  request, all to conclude "don't sample";
- enabled + ratio, unsampled remote traceparent: **705 ns / 13 allocs**.

The cost lands on every request that enters a wrapped handler chain —
data-plane misses, admin requests, and (a surface the audit's D3 missed
because it looked only at the client side) every served peer RPC: the
peer-fetch handler is mounted under the admin's `bouine.admin` tracing
middleware, so a 3-node sharded cluster pays it for ~2/3 of its hits.

Two behaviors were also wrong for a CDN-fronted tier:

1. The sampler was `TraceIDRatioBased` directly: an incoming sampled
   `traceparent` (CDN → L1 → L2 chains) did NOT force sampling — the
   sampled flag was ignored and a sampled trace silently dropped hops.
2. `StartOriginSpan` built its attributes before `Start` too, with the
   same waste on the miss path.

## Decision

1. The sampler becomes `ParentBased(TraceIDRatioBased(rate))` for
   `0 < rate < 1` (0 and 1 keep exact Never/Always semantics). A
   sampled parent now guarantees the child records — trace stitching
   across CDN → L1 → L2 is deterministic.
2. `InitTracer` publishes a **hoist mode** mirroring the sampler it
   installed. The middleware and `StartOriginSpan` consult it:
   - `hoistAll` (NeverSample): skip extraction-side attribute building
     and the SDK `Start` entirely; the extracted span context is still
     published so downstream spans and logs keep the trace chain.
   - `hoistUnsampledParent` (ParentBased): an unsampled parent — remote
     or local — deterministically drops the child, so the whole span
     pipeline is skipped for it.
   - Indeterminate cases (root spans under a ratio sampler — the trace
     ID is generated inside the SDK, so the decision cannot be hoisted)
     start the span **bare** and set attributes only when the returned
     span is recording: a dropped root never pays for attributes.
3. Attributes move from `Start`-time options to post-`Start`
   `SetAttributes`. Semantics are unchanged for recording spans (the
   batch processor reads attributes at `End`); the only observable
   difference is that a `SpanProcessor`'s `OnStart` sees the span
   before its attributes — bouine ships no such processor.

The hoist mode is written only by `InitTracer` and is the mirror of
its sampler; a directly-installed test provider leaves it at
`hoistNone`, degrading to the pre-hoist behavior.

## Consequences

- Every dropped-span request stops paying the attribute building
  (~4 allocs) and, in the two hoisted configurations, the SDK `Start`
  as well (~500-680 ns per request on the measured machine).
- Sampled requests pay one extra `IsRecording` check; their span
  content is unchanged.
- Effective sampling at a given `sampling_rate` rises for traces that
  arrive sampled (that is the point); unsampled incoming parents stay
  unsampled, roots keep the ratio semantics.
- Pinned by `tracing_hoist_test.go` (hoist bypasses a recording
  sampler; sampled parents record with the parent trace ID; the trace
  chain survives a hoisted drop) and measured by
  `BenchmarkTopology_TracingMiddleware_*` in `bench/topology`.
