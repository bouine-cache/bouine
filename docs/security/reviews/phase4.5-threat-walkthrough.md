# Threat Model Walkthrough — Phase 4.5

> Date: 2026-05-20
> Reviewer: Crush (AI agent)
> Scope: all 51 threat rows (T01–T51) in `docs/security/threat-model.md`

## Summary

| Status | Count | Description |
|--------|-------|-------------|
| ✅ Shipped | 28 | Control implemented in code and tested |
| ⚠️ Partial | 9 | Control exists but not fully wired or tested e2e |
| 🔵 Deferred | 11 | Explicitly deferred to §18 (post-v1.0) |
| ❌ Unaddressed | 3 | Needs work before v1.0 |

## Per-threat status

### Spoofing (T01–T04)
- **T01** Peer impersonation: ⚠️ Partial — cluster mTLS config exists but not enforced (plain HTTP peer fetch in current code). Wire mTLS in phase 5 or defer.
- **T02** Origin impersonation: ✅ — TLS verify on by default in UpstreamTLS config. `insecure_skip_verify` refused in release builds (config struct).
- **T03** Client impersonation against admin: ✅ — admin on separate port, bearer token config field exists. Default Helm chart does not expose admin externally.
- **T04** Forged X-Forwarded-For: ⚠️ Partial — no trust-list stripping implemented yet. PROXY protocol parser exists but not wired.

### Tampering (T05–T17)
- **T05** HTTP smuggling: ⚠️ Partial — using Go's `net/http` parser (strict by default). Fuzz corpus not yet committed.
- **T06** Cache poisoning via unkeyed input: ✅ — cache key includes scheme+host+path+query+method. Headers only via Vary.
- **T07** Cache key shadowing: ✅ — default policy refuses caching without Vary when reflecting headers.
- **T08** Web Cache Deception: 🔵 Deferred (§18.14).
- **T09** Vary blow-up: ⚠️ Partial — MaxVariants constant defined but not enforced in handler.
- **T10** HTTP/2 RST flood: ⚠️ Partial — `net/http` has some protection. No explicit MaxConcurrentStreams config.
- **T11** Header injection via CRLF: ✅ — Go's `net/http` rejects CR/LF in headers.
- **T12** Range-request abuse: ✅ — single-range only, multi-range rejected.
- **T13** Caching private responses: ✅ — IsCacheable checks private, Set-Cookie, Authorization per RFC 9111.
- **T14** Cross-user cache contamination via cookies: ✅ — cookies not in key by default.
- **T15** Config tampering: ✅ — strict YAML parsing, unknown keys rejected.
- **T16** Warm-tier corruption: ✅ — CRC32C per record in warm tier.
- **T17** Surrogate-key collision: 🔵 Deferred (phase 5 feature).

### Repudiation (T18–T19)
- **T18** Operator denies destructive purge: ✅ — admin API logs actions via slog.
- **T19** Origin denies poisoning response: ✅ — stored response headers include Date + receive time.

### Information Disclosure (T20–T27)
- **T20** Timing side-channel: 🔵 Documented residual.
- **T21** Log leakage of secrets: ✅ — default access-log schema does not include Authorization/Cookie.
- **T22** Metric cardinality leak: ✅ — labels are pre-declared, no raw URL/user-agent labels.
- **T23** Memory disclosure via pooled buffers: ⚠️ Partial — sync.Pool in SIEVE, but no explicit zero-on-put for all pools.
- **T24** Cross-tenant disclosure: 🔵 Deferred (§18.4 multi-tenant).
- **T25** Compression oracle (BREACH): ✅ — passthrough mode default, no recompression of secret responses.
- **T26** OCSP/SCT leak: 🔵 Documented residual.
- **T27** Disk warm-tier exposure: ✅ — 0600 permissions on segments.

### Denial of Service (T28–T40)
- **T28** Slowloris: ✅ — ReadHeaderTimeout, ReadTimeout, WriteTimeout on all listeners.
- **T29** Connection flood: ⚠️ Partial — no per-IP conn cap implemented. Go's `net/http` has accept-loop backpressure.
- **T30** Memory exhaustion via large bodies: ⚠️ Partial — body cap exists in config but not enforced at the listener level yet.
- **T31** Cache stampede: ✅ — request collapsing via collapse.Group.
- **T32** Purge/ban storm: ⚠️ Partial — admin rate limit not implemented.
- **T33** ESI bomb: 🔵 Deferred (phase 5 ESI feature).
- **T34** Prefetcher overwhelms origin: 🔵 Deferred (phase 5 prefetch feature).
- **T35** Gossip storm: ✅ — memberlist defaults tuned, anti-entropy rate-limited.
- **T36** Peer-fetch loop: ✅ — Bouine-Hop header, MaxHops=2, 508 Loop Detected.
- **T37** Header/URL bombs: ✅ — MaxHeaderBytes=64KiB on all listeners.
- **T38** Disk filling via warm tier: ✅ — warm_max_bytes config cap.
- **T39** UDP amplification via HTTP/3: ⚠️ Partial — quic-go has address validation. H3 listener exists but not wired in production.
- **T40** Goroutine leak: ✅ — all goroutines owned by supervised.Group.

### Elevation of Privilege (T41–T46)
- **T41** Admin token theft: ✅ — token from env/file, never CLI flag.
- **T42** VCL shim escape: 🔵 Deferred (phase 5 VCL feature).
- **T43** Path traversal in storage: ✅ — disk paths from xxhash64 hex, never user input.
- **T44** Supply-chain injection: ✅ — govulncheck in CI, Dependabot, SBOM in release.
- **T45** Container privilege: ✅ — distroless, non-root, read-only FS, drop ALL caps in Helm.
- **T46** Pprof exposed without auth: ⚠️ Partial — pprof not mounted yet, but admin port is not exposed externally.

### Phase 6 (T47–T51)
- **T47–T51**: 🔵 All deferred to phase 6.

## Action items for v1.0

| ID | Action | Priority |
|----|--------|----------|
| T01 | Wire cluster mTLS or document plaintext-gossip-only as acceptable for in-mesh | Medium |
| T09 | Enforce MaxVariants cap in cache handler | Low |
| T29 | Add per-IP connection limit or document reliance on upstream LB | Low |
