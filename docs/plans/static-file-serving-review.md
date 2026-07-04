# Linus Review: static-file-serving.md

## Verdict: No. — good bones, but several design errors that will bite in production.

---

### BLOCKER 1: `embed: true` in YAML is meaningless

**Location**: §3 config example, §4.1.2

**Problem**: The config says `static.embed: true` but there is no mechanism to
connect a YAML boolean to a Go `embed.FS` value. `embed.FS` is declared in
Go source at compile time (`//go:embed`). You cannot reference it from YAML.
The plan even admits this in §4.1.2: "the caller is responsible for declaring
//go:embed and passing the FS" — but never explains HOW the YAML config
references which embed.FS to use.

**Evidence**: The config has `embed: true` (a boolean) but the handler needs
an `embed.FS` (a Go value). There is no bridge between these two worlds.

**Fix**: Drop `embed: true` from YAML entirely. Embedded FS serving is a
build-time concern, not a runtime config option. If you want embed support,
it should be a Go API: the caller declares the embed.FS in their main package
and registers it via a named registry, then the YAML references the name:
`static.embed: "my-embedded-assets"`. But that's over-engineered for v1.
Ship local-disk serving first. Embed can be a follow-up.

---

### BLOCKER 2: Caching static files from local disk is stupid by default

**Location**: §4.4 "Cache integration"

**Problem**: The plan says cache wrapping is on by default for static routes.
This is wrong. The OS page cache already caches file contents in RAM. Adding
bouine's hot tier (another RAM copy) gives you double RAM usage for zero
latency benefit on a single node. The only scenario where the cache layer
helps is cluster replication (peers without local files).

**Evidence**: A 50 MiB static asset accessed frequently: OS page cache keeps
it in RAM (free). bouine cache also stores it in hot tier (50 MiB of
configured RAM). Two copies, one benefit. The cache layer adds ETag/Vary/TTL
machinery that is irrelevant for local files that don't change.

**Fix**: Default `cache.enabled` to `false` for static routes. Operators
opt in to caching when they want cluster replication or TTL-based eviction.
Document the trade-off explicitly.

---

### BLOCKER 3: Range request semantics are wrong (spec error)

**Location**: §4.1.1 step 9: "multi-range → 416"

**Problem**: RFC 9110 §14.3.2 says a server MAY collapse a multipart range
request into a single 206 response. Returning 416 (Range Not Satisfiable)
means the requested range itself is invalid (e.g., bytes=1000- when file is
500 bytes). A multipart range request is NOT unsatisfiable — it's just a
request the server chose to simplify. Returning 416 breaks every correct
HTTP client.

**Evidence**: RFC 9110 §14.3.2: "A server that supports range requests MAY
ignore or reject a Range header field that consists of more than two
ranges." Ignoring ≠ rejecting with an error status. The correct behavior is
to serve the first range as 206, or serve the full content as 200.

**Fix**: Serve the first range as 206. Or serve the full body as 200. Never
416 for a valid multipart range.

---

### bug 4: Layer assignment is wrong — staticfile is not L1

**Location**: §4.1 "New package: internal/staticfile (L1)"

**Problem**: L1 is defined in architecture.md §2.2 as "HTTP/1, /2, TLS,
route matching." Static file serving is content origin — it replaces an
upstream pool, which is L4. Putting it in L1 violates the layer model
because L1 packages may only depend on L7 and /pkg/api (per §3.1). A static
file handler needs `internal/observability` (L7, OK) but conceptually it's
an origin, not a protocol handler.

**Evidence**: The dependency matrix in §3.1 says `L1 → L7, /pkg/api`. The
staticfile handler depends on `fs.FS`, `embed`, `mime`, `observability` —
all fine for L1. But the AGENTS.md §2 rule says "Never violate the layer
boundaries." The layer is defined by what the code DOES, not what it imports.
Static file serving is origin behavior, not routing behavior.

**Fix**: Put it in L4 (`internal/origin/staticfile/` or
`internal/staticfile` documented as L4). L4 is "origin / upstream pool,
health, hedge, circuit breaker." A local file source IS an origin. This
also makes the "cache handler wraps it as upstream" design in §4.4
architecturally coherent — the cache (L3) talks to an L4 origin, which is
exactly the existing dependency direction.

---

### bug 5: `filepath.EvalSymlinks` on every request is a performance trap

**Location**: §5 "No symlink escape"

**Problem**: `filepath.EvalSymlinks` issues `lstat` + `readlink` syscalls
for every path component. For a path like `/var/www/assets/css/main.css`
that's 5+ syscalls per request, every request, on the miss path. The plan
claims performance focus but doesn't acknowledge this cost.

**Fix**: Evaluate symlinks on the root directory ONCE at startup. If root
contains symlinks, either (a) resolve them and use the resolved path, or
(b) refuse to start with a clear error. Per-request symlink evaluation is
unnecessary if the root is fixed. For subdirectories created after startup,
rely on `path.Clean` + `filepath.Rel` (already in the plan) and skip
per-request `EvalSymlinks`.

---

### bug 6: Missing `Content-Length` on streamed responses

**Location**: §4.1.1 step 10

**Problem**: The plan says "stream via io.Copy" but never mentions setting
`Content-Length`. `io.Copy` to `http.ResponseWriter` without a prior
`Content-Length` header forces the HTTP server into chunked encoding, which
breaks some clients and prevents progress indicators. `http.FileServer`
sets Content-Length from stat. The plan should too.

**Fix**: Stat the file before streaming. Set `Content-Length` from
`stat.Size()`. Then stream. If the stat size doesn't match the actual bytes
(e.g., file changed mid-request), that's a race the OS filesystem already
deals with — the response will be truncated or extended, which is
acceptable.

---

### taste 7: `strip_prefix` in static block duplicates `Request.StripPrefix`

**Location**: §3 config, §3.1 `StaticConfig.StripPrefix`

**Problem**: The `Route` struct already has `Request.StripPrefix` which
strips a prefix before forwarding to upstream. The static block introduces a
SECOND `strip_prefix`. Which applies? Both? In what order? This is confusing.

**Fix**: Reuse `Request.StripPrefix`. The builder already applies it via
`stripPrefixHandler`. Remove `StripPrefix` from `StaticConfig`. One
mechanism, one place.

---

### taste 8: `max_body_bytes` is misnamed

**Location**: §3.1 `StaticConfig.MaxBodyBytes`

**Problem**: It's a file size limit, not a body size limit. "Body" implies
request body in HTTP terminology. The config already has
`max_object_size` and `max_response_bytes` in `RouteCache` — this is a
third name for a size limit, and it's the wrong word.

**Fix**: Rename to `max_file_size`. Or better: reuse `Cache.MaxObjectSize`
since that already limits object size and the static handler goes through
the cache path anyway. If cache is disabled, add `max_file_size` to
`StaticConfig` as the standalone limit.

---

### taste 9: `fallback.redirect` is half-baked feature creep

**Location**: §3, §3.1 `StaticFallback`

**Problem**: The fallback redirect mechanism is underspecified. Where does
the redirect target live? On the same static route? A different route? If
it's a static file on the same route, you get infinite recursion (file not
found → redirect to /custom-404.html → that file IS found → but what if
it's also missing?). The plan doesn't address this.

**Fix**: Ship 404-only fallback in v1. Drop the redirect mechanism. If
operators want custom 404 pages, they can configure a separate catch-all
route with `static.root` pointing to their error pages directory. The
router already does first-match-wins — this works naturally.

---

### bullshit 10: "mime.TypeByExtension" is reliable enough

**Location**: §4.1.1 step 6

**Problem**: The plan says "Set Content-Type from file extension using
mime.TypeByExtension (fall back to application/octet-stream)." This function
reads the host OS MIME database (`/etc/mime.types` on Linux, registry on
Windows). The same file extension may produce different Content-Types on
different nodes in the same cluster. For a cache where node A serves the
file with `text/css` and node B serves it with `application/octet-stream`,
the Vary/ETag logic breaks.

**Fix**: Bundle a static MIME map for the common web file types (html, css,
js, json, xml, png, jpg, gif, svg, woff2, ico, wasm, etc.). Register them
at init via `mime.AddExtensionType`. This ensures consistent behavior across
all nodes. Fall back to `application/octet-stream` only for unknown
extensions.

---

### nit 11: Weak ETag from mtime+size will break in clusters with shared storage

**Location**: §4.1.1 step 7, §11 risks

**Problem**: If two nodes serve the same file from NFS/GlusterFS/shared PVC,
mtimes may differ across nodes (NFS mtime is server-side, but different
mount points may report differently). The plan acknowledges this as a
"risk" but doesn't fix it.

**Fix**: Use a strong ETag: xxhash64 of the file content, computed on first
access and cached. Yes, it costs one full read on the first miss, but it's
correct across all deployment topologies. The mtime+size approach is a
premature optimization that trades correctness for a hash you already have
in your dependency list (`cespare/xxhash/v2`).

---

## Summary of required changes before implementation

1. Drop `embed: true` from YAML. Ship disk-only for v1.
2. Default cache to OFF for static routes.
3. Fix range request semantics (serve first range, never 416 for multipart).
4. Move `internal/staticfile` to L4 (origin layer).
5. Move symlink evaluation to startup, not per-request.
6. Set `Content-Length` from stat before streaming.
7. Remove `StripPrefix` from `StaticConfig` — reuse `Request.StripPrefix`.
8. Rename `max_body_bytes` → `max_file_size` or reuse `Cache.MaxObjectSize`.
9. Drop `fallback.redirect` — ship 404-only.
10. Bundle a static MIME map, don't rely on host OS.
11. Use xxhash64 content hash for ETags, not mtime+size.
