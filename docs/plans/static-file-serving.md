# Plan: Static File Serving Routes

## Status: Reviewed — updated per Linus review (see static-file-serving-review.md)
## Date: 2026-07-04

---

## 1. Problem

bouine currently requires every route to proxy to an upstream pool. There is
no way to serve static files (images, CSS, JS, HTML) directly from local disk
without standing up a separate origin server. This is overkill for single-node
deployments, edge-of-mesh sidecars, or any scenario where the content is
already on the same machine as the cache.

## 2. Goal

Add a `static` block to `Route` config as an alternative to `pool`. When
present, bouine serves files from a local directory directly, without an
upstream pool. The static handler can optionally flow through the cache layer
for cluster replication, but cache is OFF by default for static routes.

### 2.1 Non-Goals

- Directory listing (security risk, not needed for a CDN).
- SSI / template rendering.
- On-the-fly compression of file contents (deferred — gzip/br is a separate
  middleware concern).
- File watching / auto-reload of disk content (files are read per-request;
  OS page cache is the cache).
- Serving as a general-purpose file server (no PUT/DELETE/WebDAV).
- Embedded filesystem (`embed.FS`) support — this is a build-time concern
  that cannot be wired from YAML. Deferred to a follow-up that introduces a
  named registry pattern.

## 3. Configuration

```yaml
routes:
  - name: assets
    match: { path_prefix: /assets/ }
    static:
      root: /var/www/assets        # absolute path to local directory
      max_file_size: 10485760      # 10 MiB cap per file, default 10 MiB
    request:
      strip_prefix: /assets/       # reuse existing strip_prefix mechanism
    cache:
      enabled: false               # OFF by default for static routes
      ttl_default: 86400s          # only applies if enabled: true

  - name: static-cdn
    match: { path_prefix: /cdn/ }
    static:
      root: /var/www/cdn
    request:
      strip_prefix: /cdn/
    cache:
      enabled: true               # opt in for cluster replication
      ttl_default: 3600s
```

### 3.1 Config struct changes

Add to `internal/config/config.go`:

```go
// StaticConfig configures a route to serve files from a local directory
// instead of proxying to an upstream pool. The directory is resolved and
// symlink-evaluated once at startup; per-request path traversal is
// prevented by path.Clean + filepath.Rel containment check.
type StaticConfig struct {
    Root        string   `yaml:"root,omitempty" json:"root,omitempty"`
    Index       []string `yaml:"index,omitempty" json:"index,omitempty"`
    MaxFileSize ByteSize `yaml:"max_file_size,omitempty" json:"max_file_size,omitempty"`
}

// Route is unchanged except for the new Static field.
type Route struct {
    Name     string        `yaml:"name,omitempty" json:"name,omitempty"`
    Match    RouteMatch    `yaml:"match,omitempty" json:"match,omitempty"`
    Pool     string        `yaml:"pool,omitempty" json:"pool,omitempty"`
    Static   StaticConfig  `yaml:"static,omitempty" json:"static,omitempty"`
    Cache    RouteCache    `yaml:"cache,omitempty" json:"cache,omitempty"`
    Request  RouteRequest  `yaml:"request,omitempty" json:"request,omitempty"`
    Response RouteResponse `yaml:"response,omitempty" json:"response,omitempty"`
}
```

### 3.2 Validation rules

In `internal/config/loader.go`, `validateRoute`:

- A route must specify exactly one of `pool` or `static.root`. Specifying
  both is a validation error. Specifying neither is a validation error.
- `static.root` must be an absolute path.
- `static.max_file_size` defaults to 10 MiB; must be > 0 if set.
- `static.index` entries must not contain `/` (no path traversal via
  index names).
- `request.strip_prefix` validation already exists (must start with `/`).
  Reused for static routes — no new field.

## 4. Implementation

### 4.1 New package: `internal/staticfile` (L4 — Origin layer)

Layer: L4, same as `internal/origin`. A local file source IS an origin —
it replaces the upstream pool. This makes the cache (L3) → origin (L4)
dependency direction architecturally coherent: the cache handler treats
the static handler as its "upstream," exactly like a remote HTTP origin.

```
internal/staticfile/
    doc.go
    handler.go
    handler_test.go
    fuzz_test.go
    mime.go       // bundled MIME map
    mime_test.go
```

#### 4.1.1 `handler.go`

```go
// Package staticfile serves files from a local directory. It is designed
// to be wired into the L1 router as an alternative to an upstream pool,
// and optionally wrapped by the L3 cache handler as its upstream.
//
// Layer: L4 (origin).
package staticfile

// Handler serves static files from a filesystem rooted at a fixed
// directory. It does NOT support directory listing. Path traversal is
// prevented by path.Clean + filepath.Rel containment check. Symlinks
// in the root are resolved once at startup; per-request symlink
// evaluation is NOT performed.
type Handler struct {
    root       string         // symlink-resolved absolute root
    fs         fs.FS          // os.DirFS(root) opened at construction
    indexFiles []string
    maxBytes   int64
    logger     observability.Logger
    metrics    *Metrics
}

// Config configures a static file Handler.
type Config struct {
    Root       string        // absolute path, symlink-evaluated at startup
    IndexFiles []string
    MaxBytes   int64         // default 10 MiB
    Logger     observability.Logger
    Metrics    *Metrics
}

// New opens the root directory, resolves symlinks, and returns a Handler.
// Returns an error if root does not exist, is not a directory, or contains
// a symlink that escapes its parent.
func New(cfg Config) (*Handler, error)
```

**ServeHTTP flow:**

1. Clean `r.URL.Path` with `path.Clean`. This resolves `..` components.
2. Join cleaned path with root. Check with `filepath.Rel(root, joined)` —
   if the relative path starts with `..`, return 403 + increment
   `traversal_blocked` metric.
3. If the cleaned path maps to a directory, try each `indexFiles` entry
   in order. If none match, return 404.
4. Stat the file. If not found, return 404. If directory (no index matched),
   return 404.
5. Check `stat.Size()` against `maxBytes`. If exceeded, return 413 +
   increment `too_large` metric.
6. Set `Content-Length` from `stat.Size()`.
7. Set `Content-Type` from the bundled MIME map (`mime.go`). Fall back to
   `application/octet-stream` for unknown extensions.
8. Compute strong ETag: xxhash64 of file content, computed on first
   access. Format as `"<hex>"`. Cache the ETag in a `sync.Map` keyed by
   path+mtime to avoid rehashing unchanged files.
9. Set `Last-Modified` from `stat.ModTime()`.
10. Handle conditional requests: `If-None-Match` → 304, `If-Modified-Since`
    → 304.
11. Handle range requests (`Range` header): single range → 206 with
    `Content-Range` and `Content-Length` set to the range length.
    Multipart range → serve the first range as 206 (per RFC 9110 §14.3.2,
    a server MAY collapse multipart to single). Unsatisfiable range
    (e.g., bytes=999999- on a 500-byte file) → 416.
12. Open the file and stream the body via `io.Copy` to
    `http.ResponseWriter` using a pooled 64 KiB buffer from `sync.Pool`.
13. On any error mid-stream, log at `warn` level. Do not attempt to
    write an error status to the client if headers are already sent.

**Path traversal defense (no per-request symlink evaluation):**

- Root is symlink-evaluated ONCE at `New()` time via
  `filepath.EvalSymlinks`. If the resolved root escapes its declared
  parent, `New()` returns an error and the daemon refuses to start.
- Per-request defense is `path.Clean` + `filepath.Rel` containment check.
  This is sufficient because the root is fixed and the filesystem below
  it is assumed not to contain escaping symlinks (if it does, that's an
  operator misconfiguration outside bouine's control).

#### 4.1.2 `mime.go`

```go
// init registers a curated set of web MIME types at package load time.
// This ensures consistent Content-Type across all nodes regardless of
// host OS MIME database differences.
func init() {
    for ext, typ := range bundledMIMEs {
        mime.AddExtensionType(ext, typ)
    }
}

var bundledMIMEs = map[string]string{
    ".html": "text/html; charset=utf-8",
    ".htm":  "text/html; charset=utf-8",
    ".css":  "text/css; charset=utf-8",
    ".js":   "application/javascript",
    ".mjs":  "application/javascript",
    ".json": "application/json",
    ".xml":  "application/xml",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif":  "image/gif",
    ".svg":  "image/svg+xml",
    ".ico":  "image/x-icon",
    ".webp": "image/webp",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf":  "font/ttf",
    ".otf":  "font/otf",
    ".wasm": "application/wasm",
    ".txt":  "text/plain; charset=utf-8",
    ".pdf":  "application/pdf",
    ".map":  "application/json",
}
```

This `init` only calls `mime.AddExtensionType` (registering encoders with
the stdlib registry) — it does no I/O or goroutines, per AGENTS.md §4.

### 4.2 Builder changes (`cmd/bouine/cmd/builder.go`)

In `buildRouter`, the current loop skips routes when `rs.pools[rc.Pool]`
is nil. The change:

1. If `rc.Static.Root != ""`:
   - Build a `staticfile.Handler` via `staticfile.New(cfg)`.
   - If startup fails (root doesn't exist, symlink escape), return an
     error from `buildRouter` (propagated to engine startup — the daemon
     refuses to start with a bad static root).
   - Apply `stripPrefixHandler` if `rc.Request.StripPrefix` is set (reuse
     existing helper — same mechanism as proxied routes).
   - If `rc.Cache.Enabled` is explicitly `true`:
     - Wrap in `cache.NewHandler` with the static handler as upstream.
     - Cache config (TTL, SWR, SIE, eviction, cluster replication) applies
       identically to proxied routes.
   - Else: use the static handler directly (no cache wrapper).
   - Register with `router.AddRoute`.
   - Skip pool resolution entirely.
2. Else: existing pool-based logic unchanged.

### 4.3 Router changes (`internal/server/router.go`)

No changes needed. The router already accepts any `http.Handler`.

### 4.4 Cache integration (optional, off by default)

When `cache.enabled: true` is set on a static route, the cache handler
(`internal/cache/handler.go`) wraps the static handler as its `Upstream`.
On a cache miss, the cache handler calls the static handler's
`ServeHTTP`, which reads from disk and writes the response. The cache
handler intercepts the response, stores it, and serves future requests
from cache.

This means cached static files get:
- TTL/SWR/SIE per-route cache config.
- Eviction under memory pressure.
- Cluster replication (full mode broadcasts static file objects to peers).

When cache is OFF (default), the static handler serves directly from disk
on every request. The OS page cache provides the hot caching layer at
zero additional memory cost.

### 4.5 Metrics

New metrics in `internal/staticfile`:

- `bouine_staticfile_requests_total{route, result}` where result is
  `served`, `not_found`, `too_large`, `traversal_blocked`, `method_not_allowed`.
- `bouine_staticfile_bytes_total{route}` — total bytes served.

Cardinality: `route` label only (low cardinality, ≤ number of routes).
No `path` or `content_type` labels.

## 5. Security

- **Path traversal**: `path.Clean` + `filepath.Rel` containment check.
  Root is symlink-evaluated once at startup; per-request symlink
  evaluation is NOT performed (performance).
- **No directory listing**: directories without an index file return 404.
- **File size cap**: `max_file_size` prevents serving arbitrarily large
  files. Default 10 MiB. When cache is enabled, `Cache.MaxObjectSize`
  also applies as a second limit.
- **MIME type**: bundled map registered at init. Never sniff content
  (avoids XSS via content-type confusion). Operators can add
  `X-Content-Type-Options: nosniff` via `response.header_set`.
- **Methods**: static routes accept only GET and HEAD. Other methods
  return 405. Enforced in the handler, not via route config, so operators
  can't accidentally allow PUT.
- **Response headers**: existing `response.header_set` and
  `response.header_remove` apply to static routes (same as proxied routes).
  Operators can set `X-Content-Type-Options: nosniff`, CSP, etc.

## 6. Performance

- **Hit path (cache enabled, cached)**: zero impact. Cached static files
  go through the same cache hit path as any other response. No allocation,
  no disk I/O.
- **Miss path (cache disabled, default)**: single `os.Open` + `io.Copy`
  with a pooled 64 KiB buffer. `Content-Type` lookup is a map read.
  `Content-Length` from stat (no extra syscall — stat already done).
  ETag lookup is a `sync.Map` read (path+mtime key). Full file hash only
  on first access or when mtime changes.
- **No per-request symlink syscalls**: root resolved once at startup.
- **Benchmark**: add `bench/static/` benchmark comparing:
  - Direct file serve (no cache)
  - Cached file serve (second request, cache enabled)
  - vs. `http.FileServer` baseline

## 7. Testing

### 7.1 Unit tests (`internal/staticfile/handler_test.go`)

- Serve a file from a temp directory.
- Path traversal: `../etc/passwd`, `..%2f..%2f`, encoded variants.
- Directory with index file → serves index.
- Directory without index → 404.
- File exceeds max_file_size → 413.
- Conditional GET: If-None-Match → 304, If-Modified-Since → 304.
- Range request: single range → 206. Multipart range → 206 with first
  range (not 416). Unsatisfiable range → 416.
- Content-Length header present and correct.
- HEAD request → headers only, no body.
- POST → 405.
- Strip prefix (via `request.strip_prefix`, not a static-specific field).
- Content-Type by extension (verify bundled MIME map).
- Missing file → 404.
- ETag consistency: same content → same ETag across calls.
- Response header_set/remove applied.

### 7.2 Integration tests (`test/integration/static_test.go`)

- Route with static + cache disabled: every request reads from disk,
  no cache entries created.
- Route with static + cache enabled: first request is miss, second is hit.
- Route with static + cluster full mode: file served on node without
  local files after replication.
- Static route and proxied route coexist in same config.
- Startup fails when static root doesn't exist.
- Startup fails when static root is not a directory.

### 7.3 Fuzz test (`internal/staticfile/fuzz_test.go`)

- Fuzz the path cleaning / traversal defense with random URL paths.

## 8. Documentation

- ADR-0017: Static file serving routes.
- Update `docs/architecture.md` §2.2 module layout to add
  `internal/staticfile` under L4.
- Update `docs/architecture.md` §7 to mention static routes as an L4
  alternative to upstream HTTP proxying.
- Update `docs/runbook/static-files.md` with operational guidance
  (symlink behavior, MIME types, cache trade-offs, cluster replication).
- Update `examples/static-site/config.yaml` to show a self-served
  variant (no upstream pool) alongside the existing CDN-fronting variant.
- Update `internal/config/config.go` godoc for the new `Static` field.

## 9. Rollout

- Feature flag: none needed. The `static` block is additive — existing
  configs without it are unchanged. A route with `static` simply doesn't
  reference a pool.
- Backward compatibility: fully backward compatible. No existing config
  breaks.
- Config validation: routes with neither `pool` nor `static` fail
  validation. Updated validation accepts `static.root` as an alternative
  to `pool`.

## 10. Task breakdown

| # | Task | Files | Est. |
|---|------|-------|------|
| 1 | Add `StaticConfig` to config, update `Validate` | `internal/config/config.go`, `internal/config/loader.go` | 1h |
| 2 | Implement `internal/staticfile/mime.go` (bundled MIME map) | `internal/staticfile/mime.go` | 0.5h |
| 3 | Implement `internal/staticfile/handler.go` | `internal/staticfile/handler.go`, `doc.go` | 3h |
| 4 | Unit tests for handler | `internal/staticfile/handler_test.go` | 2h |
| 5 | Fuzz test for path traversal | `internal/staticfile/fuzz_test.go` | 1h |
| 6 | Wire static routes in builder | `cmd/bouine/cmd/builder.go` | 1h |
| 7 | Metrics | `internal/staticfile/handler.go` | 1h |
| 8 | Integration tests | `test/integration/static_test.go` | 2h |
| 9 | Benchmark | `bench/static/` | 1h |
| 10 | ADR-0017 + docs + example | `docs/decisions/`, `docs/`, `examples/` | 1h |
| 11 | Full verify: lint, test, integration, chaos | — | 1h |

Total: ~14.5h

## 11. Risks

- **No per-request symlink evaluation**: if an operator creates a symlink
  inside the root AFTER bouine starts, and that symlink escapes root,
  bouine could serve a file outside root. This is documented in the
  runbook as an operator responsibility. The `path.Clean` + `filepath.Rel`
  check catches `..` but not symlinks. Mitigation: document clearly, and
  note that the root should be a directory the operator controls.
- **ETag computation on first access**: costs one full file read for the
  xxhash64. This is amortized across all subsequent requests via the
  `sync.Map` cache (path+mtime key). For very large files near the
  `max_file_size` limit, the first request is slower. Acceptable — the
  alternative (mtime+size weak ETag) breaks in cluster-with-shared-storage
  topologies.
- **Large files in cache**: a 10 MiB file fills 10 MiB of hot tier. The
  existing `max_object_size` route cache config controls this. Cache is
  OFF by default, so this only applies when operators explicitly enable it.
- **MIME map coverage**: the bundled map covers common web types. Unknown
  extensions fall back to `application/octet-stream`. Operators can add
  custom types via `response.header_set` for specific routes.
