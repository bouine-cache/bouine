# 65 — Edge compression (fill-path): enabling and tuning

Per-route opt-in compression of identity-encoded origin responses on the
fill path (ADR-0075, `docs/plans/edge-compression.md`). The cache-hit path
is untouched: stored variants are served verbatim.

## Enable

```yaml
routes:
  - match: { path_prefix: /products/ }
    pool: storefront
    cache:
      ttl_default: 60s
      compression:
        enabled: true
        content_types: [text/html, text/*, application/json, application/javascript, image/svg+xml, application/xml]
        # encodings: [br, zstd, gzip]   # subset; default all supported
        # min_size: 1024                # bytes; default 1024
        # max_size: 1MiB                # default and hard cap 1 MiB
```

The block must be identical on every cluster node serving the route
(peers fail safe: a mixed-version node treats the object as opaque bytes
plus `Content-Encoding`). Validation rejects the block on
`cache.enabled: false` routes and caps `max_size` at 1 MiB.

## Verify

1. Fill once with `Accept-Encoding: gzip, deflate, br`:
   `curl -sD- -H 'Accept-Encoding: gzip, deflate, br' .../products/x | head`
   — the MISS response carries `Content-Encoding: br` (ties prefer br)
   and a `Content-Length` equal to the compressed size.
2. Repeat: `X-Cache: HIT` serves the stored compressed bytes verbatim.
3. `bouine_cluster_*` peer fetch of the variant from another node HITs
   with the same `Content-Encoding` and no extra origin request.

## Tuning

- `encodings`: drop `br` if brotli quality 4 CPU is too high on the fill
  path; the br bucket then stores nothing compressed (clients still get
  identity from the origin for that bucket).
- `min_size`: raise to skip small bodies where the header overhead
  dominates.
- `max_size`: lower to bound per-fill store-buffer memory under many
  concurrent large fills (the global streaming cap still applies).

## Safety rails (do not bypass)

- Never-compress list is hard-coded: responses with `Set-Cookie` /
  `Set-Login`, requests with `Authorization`, SSE (`text/event-stream`),
  already-encoded responses, and content types outside the allow-list.
  This is the BREACH-class oracle mitigation (threat T25): do NOT enable
  compression on authenticated route trees serving secrets.
- Bodies that expand under compression are stored as identity, never as
  negative-ratio objects.

## Metrics / dashboards

Fill-path compression cost is visible in the gate benchmarks
(`BenchmarkGate_FillCompress_*`, budgets in `bench/run.sh`); regressions
beyond the alloc budgets fail `make bench-gate`. Watch fill p99 on
compression-enabled routes; if it regresses > 2% vs passthrough, lower
`encodings` to `[zstd, gzip]` or disable the route block.
