# ADR-0038: Streaming origin responses via fasthttp SetBodyStreamWriter

Date: 2026-08-22

## Status

Deferred — not implemented in the current migration phase.

## Context

The cache handler uses `http.Flusher` for streaming chunked responses
(SSE, streaming origin passthrough) on the BYPASS path. fasthttp uses a
different streaming API: `SetBodyStreamWriter` takes a `StreamWriter`
func that receives a `*bufio.Writer`.

The `ServeRequest` shim (Phase 2) buffers the full response
synchronously — no streaming. This is acceptable for cached responses
(hits are fully buffered anyway) but not for BYPASS streaming
responses.

## Decision

Streaming origin responses via `SetBodyStreamWriter` is deferred to a
future phase. The current `ServeRequest` shim buffers all responses,
which is correct for cache hits and most miss paths. BYPASS streaming
will be reimplemented when the cache handler is fully migrated from
`http.Handler` to `fasthttp.RequestCtx` (a future phase that rewrites
the handler internals).

## Consequences

- BYPASS responses are buffered (no streaming) until the full
  migration is complete.
- Cached responses (hits, misses that store) are unaffected — they
  are always fully buffered.
- SSE and chunked transfer-encoding bypass responses will have higher
  latency (full response must arrive before the client sees any data).
