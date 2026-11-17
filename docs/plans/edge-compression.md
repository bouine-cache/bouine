# Plan: Edge Compression (minimal, fill-path only, large bodies up to 1 MiB)

Introduce response compression at the proxy edge with **zero impact on the
cache-hit path**. Today bouine is passthrough-only (architecture §3.3): it
stores the encoding the origin produced and buckets `Accept-Encoding` into
`br | zstd | gzip | identity` (ADR-0061). This plan adds an opt-in per-route
mode where, on a **miss/fill only**, an uncompressed (`identity`) origin
response is compressed to the negotiated bucket coding before storage.
Bodies of interest here are large HTML, typically 64 KiB – 1 MiB, so
compression is **streaming and chunked**, never a full-body buffer + full
compressed copy in RAM. The hit path stays byte-identical to today: one
stored variant per AE bucket, served verbatim, `allocs/op == 0`.

## 1. Goals and non-goals

Goals:

- Cut egress bytes on compressible responses (big HTML) when the origin
  does not compress.
- No latency regression on hits (hard gate: existing `BenchmarkGate_*` hit
  budgets unchanged, `allocs/op == 0`).
- Bounded added latency on misses; bodies up to 1 MiB compressed inline in
  the fill stream (the fill already waits on the origin transfer).

Non-goals (explicitly deferred):

- `normalize_identity` (store identity, recompress per-client on egress) —
  already rejected in §3.3: breaks the zero-alloc egress path and multiplies
  per-variant CPU. Not part of this plan.
- Recompressing already-compressed origin responses (passthrough stays).
- Compression of SSE / `text/event-stream` (never buffered, never stored).

## 2. Design

### 2.1 Where compression happens

Only on the **fill path** (miss → origin fetch → store). Eligibility is
decided at **response-header time** (status, `Content-Type`, absence of
`Content-Encoding`, route policy — all known before the body), so
`Content-Encoding: <bucket>` is set on the client response upfront. The body
then flows through the existing streaming fill (`streamMissTee` /
`teeStreamToClient`, `internal/cache/stream.go`) with a compressor inserted
in the tee.

Per chunk (32 KiB scratch, existing `streamChunkSize`):

1. Origin chunk → compressor writer (gzip/zstd/br per bucket).
2. Compressor output → (a) client `bufio.Writer` `w`, (b) the store buffer
   (compressed bytes, replacing today's raw tee buffer).
3. Every ~128 KiB of input, a sync-flush (`flate.Writer.Flush`, zstd
   `Flush`, brotli `Flush`) pushes compressed bytes downstream so the
   filling client still sees progressive delivery. Sync-flush cost on ratio
   is < 5% at this granularity.

At end-of-body:

- Store the **compressed** buffer under the existing variant key (the AE
  bucket is already in the key — no key change, no hit-path change), with
  `Content-Encoding: <bucket>` and corrected `Content-Length`.
- Skip the store (response was still streamed compressed to the filling
  client, which is valid HTTP) if: raw size < `min_size`, or compressed ≥
  raw (expansion; keeps the cache clean, no negative-ratio objects).
- Abort buffering mid-stream (keep streaming compressed to the client, stop
  storing) if the compressed buffer exceeds `max_size` or the global
  streaming-bytes cap — same fail-safe pattern as today's
  `maxStreamingBufferBytes` check in `teeStreamToClient`.

Small/buffered fills (client without body-stream support, or bodies under
the buffered threshold) take the existing buffered path: compress the
buffered body in one writer pass at store time, same eligibility rules.

On hits, the stored compressed variant is served verbatim. `Vary:
Accept-Encoding` is honored by the existing variant machinery. Identity
-bucket clients get their own identity variant (separate fill), as today.

Memory bound per fill: 32 KiB chunk scratch (pooled) + one compressed store
buffer ≤ ~`max_size` (HTML compresses 5-10x, so typically 100-200 KiB for a
1 MiB page) + pooled compressor writer. The raw identity body is never held
alongside the compressed copy on the streaming path — the compressor
consumes chunks as they arrive. This is strictly less RAM than today's tee,
which buffers the full raw body up to the existing caps.

### 2.2 Compressor

- Library: `klauspost/compress` — already on the pre-approved allow-list
  (`docs/deps.md`), currently an indirect dep; promote to direct. Provides
  flate (gzip) and zstd. For **br**, `github.com/andybalholm/brotli` is the
  standard but is NOT on the allow-list: adding it requires a deps-reviewer
  sign-off (AGENTS.md §5). Until then the `br` bucket degrades to zstd
  compression (which every br-capable client also accepts — but see §5:
  this stores zstd bytes under the `br` bucket key, WRONG for br-only
  clients; so the safe fallback is: without a br encoder, store under the
  `br` bucket only if the client also accepts zstd/gzip, else skip
  compression for that fill. Decide at implementation; default: gzip +
  zstd now, br once brotli is allow-listed).
- **Pooled writers**: one `sync.Pool` per coding, writer bound to a pooled
  output buffer; `Reset` on put (AGENTS.md §7). Steady-state allocs on the
  compress path ≈ 0 → new `BenchmarkGate_Compress_*` budgets of 0
  allocs/op after warm-up where achievable; the rest budgeted explicitly in
  `bench/run.sh` `BUDGETS`.
- Levels tuned for **latency on 1 MiB bodies**, not ratio:
  - gzip: level 5 (≈15-20 ms/MiB); `BestSpeed` (≈8 ms/MiB, ~15% worse ratio)
    if the fill p99 budget is tight — pick from bench data.
  - zstd: `SpeedFastest` (≈3-5 ms/MiB).
  - br: quality 4-5 (q11 is ~10x slower, never on the fill path).
- **Concurrency**: compression runs inline on the `SetBodyStreamWriter`
  goroutine (per-connection). Rationale: the fill already pays origin RTT +
  body transfer for up to 1 MiB, which dominates single-digit-to-tens-of-ms
  compression. A worker pool would add queueing + handoff latency for no p99
  win. Revisit only if the fill p99 bench regresses > 2%.

### 2.3 Eligibility policy (per route, default off)

New config block, `route_defaults.compression` / per-route override:

```yaml
compression:
  enabled: true                 # default false — opt-in per route
  encodings: [gzip, zstd]       # subset of {br, zstd, gzip}; default all supported
  min_size: 1024                # bytes; smaller bodies are streamed compressed but not stored
  max_size: 1048576             # bytes; 1 MiB default cap — bodies above stream compressed to the client but are not stored
  content_types: [text/html, text/css, text/*, application/json, application/javascript, image/svg+xml, application/xml]
```

Validation in `config.Validate`: encodings ⊆ {br, zstd, gzip}; 0 < min_size <
max_size ≤ 1 MiB (hard cap); content_types non-empty when enabled.
Cluster-wide identical (same rule as `verbatim_encoding` — peer gates fail
safe, miss never wrong body).

Never compress: responses with `Content-Encoding` already set;
`Cache-Control: private/no-store` (not stored anyway); `Set-Cookie`
responses; responses to `Authorization`-bearing requests; SSE
(`text/event-stream`); status codes outside 200/203/206/300/301/404/410
(only cacheable ones are stored anyway).

Security (BREACH, threat T25): the never-compress list above; `private` is
never stored regardless. Document the residual oracle risk in the runbook
when enabling on authenticated routes.

### 2.4 Storage / key / wire (cluster)

- No key change: the AE bucket is already in the variant key. Peer fetch
  (`StoreFromPeer`) transfers the stored compressed object — no protocol
  change; the object already carries `Content-Encoding`, and
  `cluster/peerfetch.go` already caps stored body bytes
  (`maxPeerFetchBytes`), which covers the compressed size.
- `Content-Length` is rewritten to the compressed length at store and at
  serve (the hit path already trusts stored headers).

## 3. Performance plan (the core requirement)

| Path | Budget | Gate |
|---|---|---|
| Hit | unchanged: `< 5 µs` p50, `allocs/op == 0` | existing `BenchmarkGate_*` (must not move) |
| Miss/fill, 1 MiB HTML, compressible | added CPU ≤ 2% p99 vs passthrough; inline, no extra goroutine | new `BenchmarkGate_FillCompress_{Gzip,Zstd,1MiB}` in `BUDGETS` |
| Miss/fill, ineligible | zero added work beyond one header check at response-header time | `BenchmarkGate_FillNoop_*` == miss budget |
| Steady-state RAM | compressed store buffers bounded by `max_size` + global streaming cap (existing counter pattern) | RSS gate ≤ main + 5% |

Benchmarks to add (`bench/`, names per AGENTS.md §8):

- `BenchmarkGate_FillCompress_Gzip1MiB`, `_Zstd1MiB`, `_Gzip64KiB` —
  time-driven, alloc-budgeted, entries required in `bench/run.sh`
  `BUDGETS`; bodies synthesized from repeated HTML-like text (compressible)
  and random bytes (expansion fallback).
- `BenchmarkGate_FillSkip_Encoded/Oversize` — prove the skip path is
  alloc-free.
- Throughput sanity: canonical bench vs Varnish with `gzip` VCL at same body
  sizes.

Correctness gates: `make lint`, `make test` (`-race`), `make conformance`
(cache-tests score must not regress — the suite sends no AE), `make
integration` (3-node: compressed variant fetched from a peer).

## 4. Work breakdown

1. Config schema + `Validate` + docs (`internal/config/config.go` route
   block; ~150 lines + tests).
2. `internal/cache/compress.go`: pooled per-coding writers, level constants,
   `newCompressor(coding) io.WriteCloser`-style wrapper with periodic
   sync-flush, expansion detection. Unit tests: round-trip, flush chunks
   reassemble, pool reuse, no `init` work, no goroutines.
3. Tee integration in `internal/cache/stream.go` (~80 lines): header-time
   eligibility check, wrap `w` + store buffer in a `teeWriter{compressor}`,
   end-of-stream decide store-vs-skip (min_size / expansion / cap), set
   `Content-Encoding` + `Content-Length`. Buffered path hook in
   `handler.go` for non-streaming clients. Hit path untouched.
4. Eligibility predicate — table-driven tests.
5. Benchmarks + `BUDGETS` entries; `benchstat` vs main (N ≥ 10) in the PR
   description, incl. p50/p99 CPU per fill.
6. Tests: unit, handler-level (variant stored compressed and served
   verbatim with correct `Content-Encoding` on hit; identity client gets
   identity; expansion → not stored; > max_size → not stored),
   integration (peer fetch of compressed variant), conformance
   no-regression.
7. Docs: `docs/architecture.md` §3.3 gains the mode, runbook page,
   CHANGELOG. ADR required: `docs/decisions/00XX-edge-compression.md`.

## 5. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Fill p99 regression from CPU on 1 MiB bodies | Medium | Latency-tuned levels (§2.2), bench gate ≤ 2%, per-route disable; br q ≤ 5 |
| Sync-flush hurts ratio or adds latency | Low | flush every 128 KiB only; ratio loss < 5%, measurable in bench |
| Wrong bytes served (encoding mismatch) | Critical | Store-time invariant: `Content-Encoding` written iff we compressed; bucket already in variant key; cross-client tests (br client, identity client) |
| br unavailable in klauspost | Medium | gzip/zstd first; br needs `andybalholm/brotli` allow-list addition; without br encoder, skip compression for br-only buckets (never store zstd bytes under a br-only key) |
| BREACH-class oracle on authenticated routes | Medium | Never-compress list (§2.3); runbook warning |
| Cluster skew (node without feature) | Low | Config identical cluster-wide; old nodes treat the object as opaque bytes + `Content-Encoding` header |
| RAM spike under many concurrent 1 MiB fills | Medium | compressed buffer ≤ max_size each, global `maxStreamingBufferBytes`-style cap stops storing (still streams to client); RSS gate |

## 6. Definition of done

- Per-route opt-in works end-to-end for bodies up to 1 MiB; hit-path
  benchmarks byte-identical (allocs/op == 0).
- `make ci` green, conformance score unchanged, new gate benches within
  budget vs main (benchstat N ≥ 10).
- ≥ 85% coverage on new code, ≥ 95% in `internal/cache` for new files.
- ADR + runbook + CHANGELOG committed.
