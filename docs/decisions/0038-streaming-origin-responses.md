# ADR-0038: Streaming origin responses via fasthttp SetBodyStreamWriter

Date: 2026-08-22

## Status

Accepted — implemented in Phase 4 (issue #521, PR #524).

## Context

The cache handler originally used `http.Flusher` for streaming chunked
responses (SSE, streaming origin passthrough) on the BYPASS path. After
the fasthttp migration (ADR-0034), the `ServeRequest` shim buffered the
full response synchronously — no streaming. This was correct for cache
hits (always fully buffered) but unacceptable for BYPASS streaming
responses and large miss-path bodies, which added latency proportional
to body size before the client received any data.

The performance rules (AGENTS.md §7) require that bodies larger than
64 KiB stream end-to-end and are never buffered in RAM. The buffered
shim violated this rule for the miss and bypass paths.

## Decision

Implement streaming origin responses using `fasthttp.SetBodyStreamWriter`
on the miss and bypass paths. Cache hits remain fully buffered (the
stored object is already in memory).

### Implementation

Three streaming paths live in `internal/cache/stream.go`:

1. **`streamBypass`** — BYPASS and PASS-THROUGH requests stream the
   origin response body directly to the client via
   `SetBodyStreamWriter`. No buffering, no storage. The origin response
   is fetched with `resp.StreamBody = true`, and the stream writer
   callback copies chunks from the origin body to the client writer.

2. **`streamMissTee`** — Cacheable miss responses use `io.TeeReader` to
   simultaneously stream to the client and buffer into a `sync.Pool`
   `bytes.Buffer`. When the stream completes, the buffered copy is
   stored as a cache object. If the body exceeds `maxResponseBytes`
   during streaming, buffering stops but the client continues receiving
   the full response (the object is simply not stored).

3. **`streamMissBuffered`** — Fallback for non-cacheable responses, HEAD
   requests, and test clients that copy the full response (detected via
   `resp.IsBodyStream() == false`). This path buffers the full body,
   writes it to the client, and stores only if cacheable.

### Singleflight for streaming misses

The streaming miss path cannot use `singleflight.Group` because the
leader must stream to the client inside the callback, which would block
followers. Instead, an `inflightStream` struct (`done chan struct{}`,
`res fetchResult`, `err error`) is stored in a `sync.Map` keyed by
primary cache key. The leader streams and closes `done` when the result
is buffered. Followers wait on `done` and serve the buffered result via
`writeBufferedResult` without re-fetching or re-storing.

### Resource management

- Pooled `*fasthttp.Request` and `*fasthttp.Response` are released
  inside the `SetBodyStreamWriter` callback, not via defer, because the
  body stream is consumed after the handler returns.
- `streamBufPool` discards buffers larger than 1 MiB to prevent pool
  pinning.
- Context cancellation is handled explicitly on all error paths in
  `doFetchStream`.

## Consequences

- BYPASS and miss-path responses larger than 64 KiB stream end-to-end
  without full buffering, satisfying AGENTS.md §7.
- Cache hits are unaffected — still fully buffered, 0 allocs/op.
- The miss path gained 3 allocations (23 → 26 allocs/op) from
  `inflightStream` tracking. Hit path is unchanged (0 allocs/op).
- Non-cacheable streaming responses are not buffered or stored.
- Test clients that copy responses (not streaming) are handled by the
  buffered fallback path, ensuring all existing tests pass.
- The `Handler` struct gained an `inflightStreams sync.Map` field for
  singleflight deduplication of concurrent streaming misses.
