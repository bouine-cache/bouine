# ADR-0075: Fill-path edge compression (opt-in per route, hit path untouched)

- **Status**: Accepted
- **Date**: 2026-11-17
- **Deciders**: @bouine-core
- **Phase**: compression policy
- **References**: `docs/plans/edge-compression.md`, ADR-0061 (AE bucketing), ADR-0034 (fasthttp), RFC 9110 §8.4, RFC 9111 §3; threat-model T25 (BREACH); `bench/run.sh` BUDGETS

## Context and Problem Statement

Bouine is passthrough-only (architecture §3.3): it stores the encoding the
origin produced and buckets `Accept-Encoding` into `br | zstd | gzip |
identity` (ADR-0061). When the origin does not compress (common for
rendered HTML behind a CMS), every client receives the full identity
bytes, and a large HTML page (tens to hundreds of KiB, up to 1 MiB) costs
real egress and client-side parse time on every miss fill and every hit
serve.

The ask is compression at the edge with a hard constraint: **the
cache-hit path must not regress** — it is gated at `allocs/op == 0` and
`< 5 µs` p50, and any change there breaks the benchmark gate.

## Decision

Add an **opt-in per-route** `cache.compression` block. On a cacheable
**fill** whose origin response is identity-encoded and eligible, bouine
compresses the body to the negotiated AE bucket coding and stores the
compressed variant under the existing variant key. The hit path is
byte-identical to today: stored variants are served verbatim, no
recompression, no decoding, no new allocation.

Key properties:

1. **Fill-only.** Compression runs inside the miss path (streaming tee or
   buffered fill). The hit path gains exactly one nil check per request
   (routes without the block) and zero work otherwise.
2. **Bucket-aligned storage.** The stored encoding always equals the
   negotiated `encodingBucket` of the filling request, so the variant key
   (which already carries the bucket, ADR-0061) matches the stored bytes.
   No key change, no wire change: the object already carries
   `Content-Encoding`, and peer fetch transfers it opaquely.
3. **Streaming for large bodies.** Bodies up to `max_size` (default and
   hard cap 1 MiB) are compressed in 32 KiB chunks through a pooled
   encoder with a sync-flush every 128 KiB, so the filling client still
   sees progressive delivery and only the compressed copy is buffered for
   storage. Bodies above `max_size` are not stored (bounded RAM), though
   the fill may still stream compressed to its client.
4. **Latency-tuned levels.** gzip level 5, zstd `SpeedFastest` (single
   worker, no encoder goroutines), brotli quality 4 (via
   `molecule-man/go-brrr`, already in the module graph through fasthttp).
   Levels are chosen for fill-path latency on up-to-1 MiB bodies, not
   ratio; br q11 is never used.
5. **Fail-safe skips.** Never compress: already-encoded responses,
   `Set-Cookie`/`Set-Login` responses, authorized requests (BREACH, T25),
   SSE, disallowed Content-Types, bodies under `min_size` (default 1024),
   bodies over `max_size`, and inputs where compression expands the output
   (stored as identity instead). Skip paths cost one header check, no
   compression work.

## Alternatives Considered

- **Passthrough only (status quo)**: rejected — large HTML from
  non-compressing origins is the dominant egress cost.
- **`normalize_identity` (store identity, recompress per client on
  egress)**: rejected (architecture §3.3, unchanged) — breaks the zero-alloc
  egress path and multiplies per-variant CPU by the client mix.
- **Compress on the hit path**: impossible without storing identity and
  transcoding per request; rejected for the same reason.
- **New brotli dependency (`andybalholm/brotli`)**: rejected for now —
  `molecule-man/go-brrr` is already an indirect dependency via fasthttp
  and provides a streaming brotli writer; promoting it to direct needs no
  new module in the graph.

## Consequences

- Positive: egress and client parse cost drop on compressible routes;
  hit path unchanged (gate benches must not move); one compressed variant
  per AE bucket, shared cluster-wide via the existing peer fetch.
- Negative: fill p99 gains compression CPU (gated ≤ 2% by
  `BenchmarkGate_FillCompress_*`); RSS bounded by `max_size` per fill plus
  the existing global streaming cap; BREACH-class oracle risk on
  authenticated routes is mitigated by the never-compress gates.
- Config must be identical across cluster nodes serving a route (same rule
  as `verbatim_encoding`); mixed-version nodes treat the object as opaque
  bytes plus a `Content-Encoding` header.
