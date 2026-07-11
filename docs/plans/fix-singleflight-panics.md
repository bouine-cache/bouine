# Plan: Fix singleflight panic propagation in bouine 0.2.0

## Problem

Bouine 0.2.0 crashes with panics under the synthetic workload. All panics
originate from the same chain:

```
httputil.ReverseProxy.ServeHTTP
  → panic(http.ErrAbortHandler)          // Go stdlib: write deadline exceeded
    → singleflight.doCall recovers        // wraps in panicError
      → singleflight.Do re-panics         // by design
        → collapsedFetch                  // no recover
          → fetchAndStore                  // no recover
            → handleCacheMiss              // no recover
              → ServeHTTP                  // no recover
                → net/http server          // catches ErrAbortHandler normally,
                                           // but singleflight wrapped it in
                                           // panicError — server can't match it
```

## Root cause

This is a bouine bug. Bouine chose to use `singleflight.Group.Do` inside an
HTTP handler that proxies to an upstream via `httputil.ReverseProxy`.
`ReverseProxy` intentionally panics with `http.ErrAbortHandler` when the
response stream is interrupted (origin connection reset, write deadline
exceeded, client disconnect). `http.Server` has a built-in `recover()` that
catches `ErrAbortHandler` specifically and aborts the connection cleanly.
`singleflight` breaks this contract — it wraps all panics in `*panicError`
and re-panics with a different type that the HTTP server can't match.

### The actual trigger: `WriteTimeout: 60s`

The data plane HTTP server (`internal/server/listener.go:62`) sets
`WriteTimeout: 60s`. This deadline starts when request headers are read and
covers the entire response phase — including the origin fetch. The
variable-latency synthetic origin takes up to 8 seconds. Under load, the
origin fetch + buffer copy + write to client can approach 60s, causing the
HTTP server to close the `ResponseWriter`. `ReverseProxy` is mid-stream
copying the origin response, the `io.Copy` fails, and it panics with
`http.ErrAbortHandler` at `reverseproxy.go:613`.

A single `WriteTimeout: 60s` is the wrong design for a caching reverse proxy:
it's simultaneously too short for slow origins and irrelevant for cache hits
that respond in <1ms. The correct approach is to not set `WriteTimeout` on
the data plane at all and rely on per-pool `connect.timeout` for origin
timeouts (which bouine already supports).

## Fix

### Fix 1: Set `WriteTimeout: 0` on the data plane HTTP server (listener.go)

Eliminate the root cause. Without `WriteTimeout`, the HTTP server never
force-closes the `ResponseWriter` mid-stream, so `ReverseProxy` never panics
with `ErrAbortHandler` due to a write deadline. Origin timeouts are already
controlled by `connect.timeout` in the upstream pool config (5s for
high-card, 10s for variable-latency).

```go
// internal/server/listener.go — NewHTTP
srv := &http.Server{
    Addr:              cfg.Addr,
    Handler:           tracing.HTTPMiddleware("bouine.listener.http", cfg.Handler),
    Protocols:         &protos,
    ReadHeaderTimeout: 10 * time.Second,
    ReadTimeout:       30 * time.Second,
    WriteTimeout:      0, // no write deadline — origin timeouts are per-pool
    IdleTimeout:       120 * time.Second,
    MaxHeaderBytes:    64 << 10,
}
```

Same change for `NewHTTPS` (line 90).

### Fix 2: Recover `http.ErrAbortHandler` in `doFetch` (handler.go)

Defense-in-depth. Even without `WriteTimeout`, `ErrAbortHandler` can fire
from other sources (client disconnect, origin connection reset). Recover it
in `doFetch` and convert to a normal error so singleflight never sees a panic.

```go
func (h *Handler) doFetch(r *http.Request) (res fetchResult) {
    ctx, span := tracing.StartSpan(r.Context(), "bouine.origin")
    defer span.End()

    defer func() {
        if rv := recover(); rv != nil {
            if rv == http.ErrAbortHandler {
                span.RecordError(fmt.Errorf("upstream connection aborted"))
                span.SetStatus(codes.Error, "upstream aborted")
                res = fetchResult{Err: fmt.Errorf("upstream connection aborted")}
                return
            }
            panic(rv) // re-panic on real bugs
        }
    }()

    // ... rest of doFetch unchanged ...
}
```

Key details:
- Named return `(res fetchResult)` so the deferred recover can set the result
- `span.RecordError()` + `span.SetStatus(codes.Error, ...)` so traces show
  the error — without this, trace-to-metric correlation breaks for aborted
  requests
- `panic(rv)` re-panics on anything that isn't `ErrAbortHandler` — real bugs
  (nil dereference, index out of range) still crash the process
- The recover is registered after `span.End()` (runs before it in LIFO), so
  `span.End()` sees the error status set by the recover

### Fix 3: NOT needed — nil URL guard in pool.go

`Target.url` cannot be nil. `newTarget` parses it with `url.Parse` and
returns an error on failure. The panic is `http.ErrAbortHandler`, not a nil
pointer.

### Fix 4: NOT needed — recover in tracing middleware

The tracing middleware is correct. It doesn't cause the panic — it's an
innocent bystander. The OTel SDK handles `span.End()` during panic unwinding.

## Tests

```go
func TestDoFetchErrAbortHandler(t *testing.T) {
    h := &Handler{
        upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            panic(http.ErrAbortHandler)
        }),
        fetchSem:        make(chan struct{}, 1),
        maxResponseBytes: 1 << 20,
    }
    req := httptest.NewRequest("GET", "/", nil)
    res := h.doFetch(req)
    if res.Err == nil {
        t.Fatal("expected error from ErrAbortHandler, got nil")
    }
    if !strings.Contains(res.Err.Error(), "aborted") {
        t.Fatalf("expected aborted error, got %v", res.Err)
    }
}

func TestDoFetchRealPanicPropagates(t *testing.T) {
    h := &Handler{
        upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            panic("real bug")
        }),
        fetchSem:        make(chan struct{}, 1),
        maxResponseBytes: 1 << 20,
    }
    req := httptest.NewRequest("GET", "/", nil)
    defer func() {
        if rv := recover(); rv == nil {
            t.Fatal("expected real panic to propagate, got nothing")
        }
    }()
    h.doFetch(req) // should panic
}

func TestDoFetchSemaphoreReleasedAfterAbort(t *testing.T) {
    sem := make(chan struct{}, 1)
    h := &Handler{
        upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            panic(http.ErrAbortHandler)
        }),
        fetchSem:        sem,
        maxResponseBytes: 1 << 20,
    }
    req := httptest.NewRequest("GET", "/", nil)
    _ = h.doFetch(req)
    // Semaphore should be drainable — the defer must have released it
    select {
    case <-sem:
        // good
    default:
        t.Fatal("fetch semaphore not released after ErrAbortHandler")
    }
}
```

The tests verify:
1. `ErrAbortHandler` becomes an error, not a panic
2. Real panics (nil dereference, etc.) still propagate — the safety property
3. The fetch semaphore is released after an abort (resource leak check)

## Files to change

- `internal/server/listener.go` — `WriteTimeout: 0` in `NewHTTP` and `NewHTTPS` (Fix 1)
- `internal/cache/handler.go` — recover in `doFetch` + named return (Fix 2)
- `internal/cache/handler_test.go` — three new tests

## Why not replace singleflight?

Replacing singleflight with a custom collapser is a bigger change with its
own concurrency risks. The recover + WriteTimeout approach is 8 lines of
code plus a 2-line config change, handles the specific incompatibility, and
preserves singleflight's well-tested deduplication logic.
