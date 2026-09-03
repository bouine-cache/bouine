# ADR-0042: Server-Sent Events streaming semantics

- **Status**: Accepted
- **Date**: 2026-09-03
- **Deciders**: @theotime
- **Phase**: streaming (extends ADR-0038)
- **Consulted**: —
- **Informed**: —

## Context

AI services fronted by bouine keep HTTP connections open indefinitely and
push events with `Content-Type: text/event-stream` (SSE, WHATWG HTML
§9.2.2). Verified against the code, SSE through bouine was broken in five
independent ways:

1. **Latency**: `streamBypass`/`streamMissNoCache` copied the origin body
   with `io.Copy` into the 4 KiB bufio pipe buffer without flushing —
   sub-4 KiB events accumulated and were delivered in multi-kilobyte
   batches or only at stream end.
2. **Origin deadline**: fasthttp arms one absolute read deadline —
   `min(fetch_timeout, response_header_timeout)` (default 60s/30s) —
   before the response headers, and that deadline persists into the
   streamed body (fasthttp `client.go` transport). Every event stream was
   cut after at most 30 seconds.
3. **Client write deadline**: the 5-minute data-plane write safety net
   (`safetyNetWriteTimeout`, and `h1parser`'s `writeTime`) covers the
   whole streamed response write, killing any stream longer than five
   minutes.
4. **Buffering hang**: an SSE response with `no-store` arriving on a
   plain GET miss took `streamMissBuffered`, which reads the body to EOF
   synchronously — an endless stream stalled the request until the fetch
   deadline and then failed.
5. **Collapsing/semaphore pinning**: request collapsing would make
   followers wait for a leader's buffered result that only completes when
   the stream ends; each stream also pinned a `fetchSem` slot (default 32
   per route) for its lifetime. POST-based SSE (the dominant AI API
   shape) was fully buffered in `invalidateAndProxy` and stalled the same
   way.

## Decision

SSE is handled as a first-class streaming class. Detection is
response-side (`text/event-stream` media type, parameters ignored,
case-insensitive per RFC 9110 §8.3.1), because the origin — not the client
— decides to stream; the request `Accept: text/event-stream` header (the
WHATWG client contract) is the supported trigger for stream-capable
fetching.

1. **Hinted requests** (`Accept: text/event-stream`), any method, are
   served by `handleSSE` (`internal/cache/sse.go`): live unbuffered
   passthrough with per-chunk flush, never cached, never collapsed, fetch
   slot released at header time. POST/PUT/DELETE invalidation semantics
   are preserved (purge at header time on 2xx/3xx).
2. **Origin fetching for hinted requests** goes through a dedicated pool
   client (`internal/origin/sse.go`) whose Dial-wrapped connections
   convert the absolute read deadline into per-read idle semantics
   (10-minute budget, resetting on every byte) — the same model as nginx
   `proxy_read_timeout` and Varnish `between_bytes_timeout`. Live streams
   survive indefinitely; dead origins are cut after the idle budget.
3. **Client-side write deadline**: on the H1 fast path, streamed
   fall-through responses re-arm the write deadline per Write
   (`h1parser.idleWriteConn`) — a stream that keeps writing lives as long
   as it flows, a client that stops reading is dropped after one budget
   (slowloris protection preserved). On the plain `fasthttp.Serve` path
   (fast path disabled), the per-request `HeaderReceived` hook extends the
   write deadline to 1 hour for hinted requests; SSE clients reconnect on
   stream end per the WHATWG contract.
4. **Non-hinted SSE responses** (origin streams without the request hint)
   are routed to the unbuffered `streamMissNoCache` path instead of the
   buffered fallback — events flow live, but the stream remains bounded by
   `fetch_timeout` because the origin connection's deadline was armed
   before detection. Documented limitation; the Accept hint is the
   supported configuration.
5. `streamBypass` and `streamMissNoCache` flush per chunk for all traffic
   (not just SSE), and `doFetchStream` now forwards request bodies
   (POST-SSE payloads previously never reached the origin).

RFC 9111 note: an event stream is not a storable representation in
practice — the body never terminates, so storage can never complete, and
replaying a per-connection stream to another client would be a correctness
and privacy bug. Bouine therefore never stores `text/event-stream`
responses regardless of freshness headers. Cache hits are unaffected: a
stored response is complete and served normally; SSE intent only changes
origin-fetch behavior, so the hit path pays no detection cost.

## Consequences

- SSE clients MUST send `Accept: text/event-stream` (standard EventSource
  and AI SDK behavior) for unbounded streams; without it, streams are
  best-effort and bounded by `fetch_timeout`.
- A hinted fetch whose origin never sends headers pins one fetch slot for
  up to the 10-minute idle budget instead of the absolute
  `response_header_timeout` (fasthttp couples the header and body
  deadlines; the idle wrapper is the only seam). Origin conns stay bounded
  by `connect.max_conns_per_host` (default 64) — SSE-heavy routes should
  raise it.
- SSE streams no longer hold `fetchSem` slots after headers, so concurrent
  streams cannot starve a route's fetch concurrency; the cost (goroutine +
  client conn + origin conn per stream) is inherent to SSE and bounded by
  the data-plane connection caps.
- Every layer is covered by tests: media-type matchers (`pkg/header`),
  handler routing/streaming/flush (`internal/cache`), idle-deadline
  conn + stream client (`internal/origin`), fall-through write re-arm
  (`internal/server/h1parser`), `HeaderReceived` hook (`internal/server`),
  and a full-daemon end-to-end test proving event-by-event delivery
  (`cmd/bouine/cmd`).
