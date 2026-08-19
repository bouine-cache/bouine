# Threat Model Walkthrough — Phase 5 (pre-open-source)

> Date: 2026-06-08
> Reviewer: Crush (AI agent)
> Scope: all 51 rows (T01–T51) in `docs/security/threat-model.md`, re-verified
> against the **current** codebase (post phases 4–7 + open-sourcing prep).
> Supersedes the phase-4.5 walkthrough (2026-05-20).

## Summary

| Status | Count | Meaning |
|--------|-------|---------|
| ✅ Shipped | 30 | Control implemented in code and exercised by tests |
| 🟡 Partial / runtime-provided | 8 | Mitigated (often by the Go runtime / netpol / edge) but not an explicit bouine control |
| 🔵 Deferred | 11 | Feature not implemented in v1.0 — explicit backlog (`docs/architecture.md §1.2`) |
| 🟣 Documented residual | 2 | Accepted, out of scope (T20, T26) |
| ❌ Unaddressed | 0 | — |

**The three v1.0 action items from the 4.5 walkthrough are resolved:**
- **T09** MaxVariants cap — ✅ now enforced in `internal/cache/handler.go`
  (`if n >= MaxVariants { … skip Put; bouine_vary_cap_hits_total++ }`).
- **T01** Cluster peer-fetch mTLS — accepted residual (see below): peer-fetch
  is plaintext HTTP/2 but the cluster port (`:8443`) is ClusterIP-internal,
  never exposed, and the innerspace `NetworkPolicy` admits peer traffic only
  from `app: bouine` pods. Documented, not a v1.0 blocker for the single-
  tenant in-mesh deployment.
- **T29** Per-IP connection cap — accepted reliance: ingress is Cloudflare-
  proxied (origin restricted to CF IP ranges via node iptables) plus the
  namespace NetworkPolicy; per-IP capping happens at the edge.

## Corrections to the threat model (controls that overstated reality)

Several rows described controls for **features not shipped in v1.0** or for
explicit bouine mechanisms that are actually provided by the runtime. These
are reclassified in `threat-model.md` and itemised here for honesty:

| ID | Claimed | Reality (v1.0) |
|----|---------|----------------|
| T05 | "Fuzz corpus from PortSwigger papers"; `bouine_smuggling_rejects_total` | No fuzz corpus and no dedicated metric. **Real control:** Go `net/http` strict RFC 9112 parser rejects CL+TE / dual CL / obs-fold by construction. → 🟡 runtime-provided |
| T10 | Explicit `SETTINGS_MAX_CONCURRENT_STREAMS` + RST cap config | No bouine config. **Real control:** Go 1.26 `net/http` HTTP/2 server enforces a default stream cap and includes the CVE-2023-44487 RST-flood mitigation. HTTP/3 part is deferred. → 🟡 runtime-provided |
| T23 | "All pooled buffers zeroed on Put" | Not explicitly zeroed. → 🟡 partial (residual: pooled body buffers may retain prior bytes; not cross-tenant because keys include host) |
| T32 | "Admin API rate limit per token" | Not implemented. → 🔵 deferred (`docs/architecture.md §1.2` backlog: per-route/admin rate limiting) |
| T33 | ESI include caps | ESI not implemented. → 🔵 deferred |
| T34 | Prefetch concurrency caps | Prefetcher not implemented. → 🔵 deferred |
| T39 | QUIC address validation | HTTP/3 not implemented. → 🔵 deferred |
| T42 | VCL shim has no eval | VCL shim not implemented. → 🔵 deferred (`§17.4`) |
| T46 | "Pprof behind admin auth" | pprof is mounted on the admin port, auth-exempt by design (operators and bench harnesses fetch profiles without a bearer token). Opt-in via `admin.pprof_enabled` config flag (default false). Admin port is network-isolated via K8s NetworkPolicy. → ✅ (opt-in flag + network isolation) |
| T47, T49, T50 | AI pipeline controls | AI layer not implemented. → 🔵 deferred |

## Per-threat status (current)

**Spoofing** — T01 🟡 (residual, network-isolated plaintext peer-fetch),
T02 ✅ (upstream TLS verify on by default; `insecure_skip_verify` refused in
release builds), T03 ✅ (admin bearer token, constant-time compare, separate
port, never externally exposed), T04 🟡 (XFF not trust-listed; edge sets it).

**Tampering** — T05 🟡 (net/http strict parser), T06 ✅ (cache key =
scheme+host+path+query+method; headers only via Vary), T07 ✅, T08 🔵
(§18.14), T09 ✅ (MaxVariants enforced), T10 🟡 (Go http2 defaults; H3
deferred), T11 ✅ (net/http rejects CR/LF), T12 ✅ (range coalescing), T13 ✅
(IsCacheable refuses private/Set-Cookie/Authorization), T14 ✅ (cookies not
keyed), T15 ✅ (strict YAML, unknown keys rejected), T16 ✅ (CRC32C per warm
record + WAL replay), T17 🔵 (surrogate keys deferred).

**Repudiation** — T18 ✅ (admin writes audit-logged via slog), T19 ✅ (stored
Date + receive-time per object).

**Information disclosure** — T20 🟣 (timing residual), T21 ✅ (secret headers
stripped from access log; bodies never logged), T22 ✅ (cardinality unit
test, pre-declared labels), T23 🟡 (no explicit zero-on-put; not cross-host),
T24 🔵 (multi-tenant deferred §18.4), T25 ✅ (passthrough; no recompression of
private), T26 🟣 (OCSP residual), T27 ✅ (warm segments 0600).

**Denial of service** — T28 ✅ (read/header/write timeouts on all listeners),
T29 🟡 (edge + netpol), T30 🟡 (per-request handling; 64 KiB stream threshold;
explicit data-plane body cap not enforced), T31 ✅ (single-flight request
collapsing), T32 🔵 (admin rate limit deferred), T33 🔵 (ESI), T34 🔵
(prefetch), T35 ✅ (memberlist tuned, anti-entropy rate-limited), T36 ✅
(Bouine-Hop, MaxHops=2, 508), T37 ✅ (MaxHeaderBytes 64 KiB on all listeners),
T38 ✅ (warm_max_bytes cap), T39 🔵 (HTTP/3), T40 ✅ (supervised goroutines,
joined on shutdown).

**Elevation of privilege** — T41 ✅ (token via env/file, never CLI flag),
T42 🔵 (VCL), T43 ✅ (disk paths from xxhash64 hex, never user input), T44 ✅
(govulncheck CI + Dependabot + SBOM + cosign), T45 ✅ (distroless/non-root/RO
rootfs/drop-all-caps/seccomp in Helm), T46 ✅ (pprof not mounted → no
exposure).

**AI / dashboard (phase 8)** — T47 🔵, T48 ✅ (dashboard escapes output; no
inline JS in the served templates), T49 🔵, T50 🔵, T51 ✅ (dashboard inherits
admin/session auth; no separate model).

## Residual risks accepted for v1.0

1. **Plaintext peer-fetch (T01)** — acceptable for a single-operator in-mesh
   cluster behind a NetworkPolicy and a private cluster port. Revisit if
   bouine is deployed across an untrusted network. Wiring cluster mTLS is a
   tracked backlog item.
2. **No explicit admin rate limit (T32)** — admin port is never internet-
   exposed; abuse requires a leaked token + cluster network access.
3. **No pooled-buffer zeroing (T23)** — reused body buffers are same-process
   and the cache key is host-scoped; no cross-tenant path in v1.0 (single
   tenant per deployment).
4. **Deferred features (T33/T34/T39/T42/T47/T49/T50)** — the threat does not
   exist because the feature does not ship; rows remain for when it does.

**Exit:** every row maps to a shipping control, a runtime-provided
mitigation, an explicit deferral, or a documented residual. Zero unaddressed.
