# ADR-0017: Static file serving routes

- **Status**: Accepted
- **Date**: 2026-07-04
- **Deciders**: @thylong
- **Phase**: backlog (post-v1.0)

## Context

bouine is a reverse-proxy cache: every route proxies to an upstream pool.
Operators deploying bouine as a CDN edge or sidecar for static content
(images, CSS, JS, HTML) must run a separate origin server (nginx, Caddy)
just to serve files that are already on the same machine. This is
unnecessary operational overhead for single-node and edge deployments.

The requirement: allow a route to serve files from a local directory
directly, without an upstream pool, while still optionally benefiting
from bouine's cache layer for cluster replication and TTL-based eviction.

## Decision

We add a `static` block to `Route` config as an alternative to `pool`.

1. **A route specifies exactly one of `pool` or `static.root`.** Config
   validation rejects routes with both or neither. This keeps the route
   model unambiguous.

2. **Static files are served by a new L4 package `internal/staticfile`.**
   The handler is wired into the L1 router exactly like a cache handler.
   It is an origin — it replaces the upstream pool, not the cache.

3. **Cache is OFF by default for static routes.** The OS page cache
   already provides hot caching in RAM. Adding bouine's hot tier on top
   doubles memory usage for zero latency benefit on a single node.
   Operators opt in to cache wrapping when they want cluster replication
   or TTL-based eviction by setting `cache.enabled: true`.

4. **When cache is enabled, the static handler is the cache handler's
   upstream.** The cache handler treats the static handler identically
   to a remote HTTP origin: on a miss it calls `ServeHTTP`, stores the
   response, and serves future requests from cache. TTL, SWR, SIE,
   eviction, and cluster replication all apply.

5. **Path traversal is prevented by `path.Clean` + `filepath.Rel`
   containment check.** Symlinks in the root are resolved once at
   startup via `filepath.EvalSymlinks`. Per-request symlink evaluation
   is not performed (performance: 5+ syscalls per request).

6. **Content-Type is set from a bundled MIME map**, not the host OS
   MIME database. This ensures consistent Content-Type across all nodes
   in a cluster regardless of OS differences.

7. **Strong ETags use xxhash64 of file content**, cached by path+mtime.
   Weak ETags from mtime+size break in cluster-with-shared-storage
   topologies where different nodes report different mtimes.

8. **Range requests follow RFC 9110 §14.3.2.** Single range → 206.
   Multipart range → collapsed to first range as 206 (server MAY
   collapse per spec). Unsatisfiable range → 416.

9. **Only GET and HEAD are accepted.** All other methods return 405.
   Enforced in the handler, not via route config, so operators cannot
   accidentally allow PUT/DELETE on static files.

10. **`strip_prefix` is reused from `request.strip_prefix`.** No
    separate `static.strip_prefix` field — one mechanism, one place.

11. **Embedded filesystem (`embed.FS`) support is deferred.** Wiring a
    Go `embed.FS` from YAML requires a named registry pattern that is
    over-engineered for v1. Local-disk serving covers the primary use
    case.

### Rejected alternative: serve via `http.FileServer`

`http.FileServer` is the stdlib file server. It was rejected because:
- It supports directory listing (security risk for a CDN).
- It relies on host OS MIME types (inconsistent across cluster nodes).
- It uses weak ETags from mtime+size (breaks shared storage).
- It doesn't enforce a file size cap.
- It doesn't integrate with bouine's metrics or logging.

### Rejected alternative: always cache static files

Caching static files from local disk by default doubles RAM usage (OS
page cache + bouine hot tier) with zero latency benefit. Cache is opt-in.

### Rejected alternative: `embed.FS` support in v1

An `embed.FS` is a Go value declared at compile time. Connecting it to
YAML config requires a named registry (register embed.FS by name in Go,
reference by name in YAML). This adds complexity for a marginal benefit
(files are already on disk; `embed.FS` saves one file open syscall).
Deferred to a follow-up.

## Consequences

### Positive
- Operators can serve static content without a separate origin server.
- Zero impact on the cache hit path (static routes bypass cache by
  default).
- Cluster replication works when cache is enabled — peers without local
  files can serve cached copies.
- Consistent Content-Type and ETag behavior across all nodes.
- Full HTTP semantics: conditional requests, range requests, HEAD.

### Negative / trade-offs
- No per-request symlink evaluation: if an operator creates a symlink
  inside the root after bouine starts, and that symlink escapes root,
  bouine could serve a file outside root. Documented as operator
  responsibility. The `path.Clean` + `filepath.Rel` check catches `..`
  but not symlinks.
- ETag computation on first access costs one full file read for the
  xxhash64. Amortized via `sync.Map` cache keyed by path+mtime.

### Risks
- Large files in cache: a 10 MiB file fills 10 MiB of hot tier. Mitigated
  by `max_file_size` (default 10 MiB) and `cache.max_object_size`.
- MIME map coverage: bundled map covers common web types. Unknown
  extensions fall back to `application/octet-stream`. Operators can add
  custom types via `response.header_set`.

## References

- `internal/staticfile/handler.go` — Handler implementation.
- `internal/staticfile/mime.go` — Bundled MIME map.
- `internal/config/config.go` — `StaticConfig`, `Route.Static`.
- `internal/config/loader.go` — `validateRoute`, `validateStatic`.
- `cmd/bouine/cmd/builder.go` — `buildStaticRoute`.
- RFC 9110 §14.3.2 (Range), §8.8.3 (ETag).
- `docs/plans/static-file-serving.md` — Implementation plan.
- `docs/plans/static-file-serving-review.md` — Linus review findings.
