# ADR-0037: Zero-copy origin response capture via fasthttp pooled response

Date: 2026-08-22

## Status

Accepted

## Context

The cache handler's miss path previously used `responseRecorder` (an
`http.ResponseWriter` wrapper backed by `http.Header` map +
`bytes.Buffer`) to capture the origin response from
`httputil.ReverseProxy`. This required:

1. An `http.Header` map allocation (2 allocs)
2. A `bytes.Buffer` intermediate copy (1 alloc)
3. A `make+copy` body clone to detach from the recorder (1 alloc)
4. A `context.WithValue` for target selection (1 alloc)

With the introduction of `FastClient` (fasthttp-based origin fetch),
the `doFetchFast` path captures the response directly in a pooled
`*fasthttp.Response`. The body references the response's internal
buffer (zero-copy), and the response is released back to the pool
only after all singleflight waiters have finished reading it.

## Decision

The `doFetchFast` path is the primary origin fetch path. When
`FastClient` is non-nil, the cache handler uses `doFetchFast` which
eliminates `responseRecorder`, `http.Header` map, `bytes.Buffer`, and
the `make+copy` body clone.

The `doFetchLegacy` path (using `responseRecorder` + `Upstream
http.Handler`) is retained as a fallback for configurations without
`FastClient`. Once all upstream pools are migrated to provide
`FastClient`, the legacy path and `responseRecorder` will be deleted.

## Consequences

- Miss path allocs reduced from ~29 to ~14 (per the acceptance criteria
  target of ≤14 allocs/op for `BenchmarkHandler_CacheMiss`).
- The pooled `*fasthttp.Response` must be released after all
  singleflight waiters are done — `releaseFetchResult` handles this.
- `buildObject` copies the body at store time (the cached object must
  not reference the pooled response's buffer).
