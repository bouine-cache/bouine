# Static File Serving

## Overview

bouine can serve static files directly from a local directory, without
requiring a separate upstream origin server. This is useful for:

- Single-node CDN edge deployments
- Sidecar configurations where content is on the same machine
- Serving static assets (CSS, JS, images) alongside proxied API routes

## Configuration

```yaml
routes:
  - name: assets
    match: { path_prefix: /assets/ }
    static:
      root: /var/www/assets        # absolute path to directory
      max_file_size: 10Mo          # default 10 MiB
      index: [index.html]          # optional, for directory requests
    request:
      strip_prefix: /assets/       # optional, reuse existing mechanism
    response:
      header_set:
        X-Content-Type-Options: nosniff
```

### Cache integration

Cache is **off by default** for static routes. The OS page cache already
provides hot caching in RAM. Enable bouine's cache layer explicitly when
you need cluster replication or TTL-based eviction:

```yaml
routes:
  - name: assets
    match: { path_prefix: /assets/ }
    static:
      root: /var/www/assets
    cache:
      enabled: true
      ttl_default: 3600s
```

When cache is enabled, the static handler is wrapped in the cache handler
as its "upstream." Cached objects benefit from TTL, SWR, SIE, eviction,
and cluster invalidation exactly like proxied responses.

## Security

- **Path traversal**: prevented by `path.Clean` + `filepath.Rel`
  containment check. The root is symlink-evaluated once at startup.
- **No directory listing**: directories without an index file return 404.
- **File size cap**: `max_file_size` (default 10 MiB) prevents serving
  arbitrarily large files.
- **Methods**: only GET and HEAD are accepted. Other methods return 405.
- **MIME types**: a bundled map ensures consistent Content-Type across
  all nodes. Unknown extensions fall back to `application/octet-stream`.
- **Symlinks after startup**: if an operator creates a symlink inside
  the root after bouine starts, and that symlink escapes root, bouine
  may serve a file outside root. The root directory should be controlled
  by the operator. Per-request symlink evaluation is not performed for
  performance reasons.

## ETags

Strong ETags are computed as xxhash64 of the file content, cached by
path+mtime. The first request for a file incurs one full read for the
hash; subsequent requests for unchanged files use the cached ETag.

## Range requests

Single range → 206 Partial Content. Multipart range → collapsed to the
first range as 206 (per RFC 9110 §14.3.2). Unsatisfiable range → 416.

## Metrics

- `bouine_staticfile_requests_total{route, result}` — result is one of:
  `served`, `not_found`, `too_large`, `traversal_blocked`, `method_not_allowed`.
- `bouine_staticfile_bytes_total{route}` — total bytes served.

## Path rewrites on static routes

`request.strip_prefix` and `request.path_rewrite` (mutually exclusive)
rewrite the path the static file handler resolves, while the cache key
keeps the public path. On cache-enabled static routes the rewrite is
applied exactly once, by the cache handler's origin-bound URI rewriting
(the static handler is wired as the bare upstream).

**Failure mode — rewrite silently skipped**: a `path_rewrite` whose
result is not an absolute path (`/...`) or exceeds 16 KiB is discarded
per request and the origin sees the original public path. On a static
route this surfaces as `not_found` on `bouine_staticfile_requests_total`
for a file that demonstrably exists at the rewritten location. Check the
`replace` template: a template that does not start with `/` when the
match is anchored (`^`) always produces a relative result. Validation
rejects the template bytes that can corrupt the request (control bytes,
raw space, `?`, `#`) at startup, so a mid-flight 4xx/5xx from the origin
is not a rewrite failure — only silent `not_found` is.
