# SSE (Server-Sent Events) streaming

Symptoms, semantics, and knobs for routes that front SSE / AI streaming
endpoints (`Content-Type: text/event-stream`). Design: ADR-0042.

## Contract

- Clients MUST send `Accept: text/event-stream` (standard EventSource and
  AI SDK behavior). Hinted requests are served as live unbuffered streams:
  each event is flushed as it arrives, nothing is cached or collapsed, and
  the connection stays open as long as the origin keeps sending bytes.
- Hinted requests skip the cache read entirely (`X-Cache: BYPASS`). POST /
  PUT / DELETE on hinted requests still purge the affected cache entries
  on 2xx/3xx — at header time, not after the (endless) body.
- An SSE response arriving WITHOUT the request hint is streamed unbuffered
  but remains bounded by the route's `fetch_timeout` (the origin
  connection's read deadline was armed before the response was known to
  be a stream). Fix the client, don't raise the timeout. Concurrent
  non-hinted requests for the same URL are not collapsed onto one stream:
  each client gets its own origin fetch (nothing is buffered, so there is
  no shareable result — `ErrStreamUnshareable`).

## Timeouts (where streams can be cut)

| Boundary | Default | Semantics for hinted SSE |
|---|---|---|
| Origin read (`internal/origin`) | 10 min idle | per-read idle budget, resets on every event — a live stream is never cut by wall clock |
| Client write, H1 fast path | 5 min idle | re-armed per write; only a client that stops reading is dropped |
| Client write, no fast path | 1 h absolute | extended per-request via `HeaderReceived`; clients reconnect on close |
| Fetch queue wait | `fetch_wait_timeout` (100 ms) | unchanged; SSE streams release their slot at header time |

## Tuning

- **Sparse event feeds** (gaps > 10 min with no heartbeat): raise nothing —
  the origin must send heartbeats (SSE comment lines), or the stream is
  cut after the 10-minute idle budget and clients reconnect. This matches
  nginx `proxy_read_timeout` / Varnish `between_bytes_timeout` defaults in
  spirit.
- **Many concurrent streams**: SSE streams hold one client conn + one
  origin conn each for their lifetime. Raise `connect.max_conns_per_host`
  (default 64) on pools fronting SSE-heavy routes, and
  `listen.max_connections` for the data plane. `fetch_concurrency` does
  NOT need raising — streams release their fetch slot at header time.
- **Hung origins**: a hinted fetch whose origin accepts the connection but
  never sends headers pins one fetch slot for up to the 10-minute idle
  budget (instead of `response_header_timeout`). Only requests that
  explicitly announce stream intent take this path.

## Failure modes

- **Stream ends after ~10 min of silence**: idle budget fired — the origin
  stopped sending without heartbeats. Check origin keepalive behavior.
- **Stream ends at exactly `fetch_timeout`**: the client did not send
  `Accept: text/event-stream` (non-hinted best-effort path).
- **Stream ends at 1 h with the fast path disabled**: expected on the
  plain fasthttp serving path; enable `experimental.h1_fast_path` for
  idle-based (unbounded) client writes, or rely on client reconnects.
- **503 + Retry-After on stream start**: fetch queue full for
  `fetch_wait_timeout` — raise `max_fetch_concurrency` or investigate
  origin latency.
