# bouine — Threat Model

> Status: **living document**. Last reviewed: **phase 5 (2026-06-08)** — see
> [`reviews/phase5-threat-walkthrough.md`](reviews/phase5-threat-walkthrough.md)
> for the current, code-verified status of every row. That review supersedes
> the per-row "mitigation" prose below where they disagree; in particular,
> controls for not-yet-shipped features (HTTP/3, ESI, prefetch, VCL shim, AI)
> are explicit deferrals, not active controls.
> Scope: bouine v1.0. Phase 8 (AI / dashboard) is covered in a dedicated
> subsection but most controls inherit from v1.0.
> Owners: maintainers listed in `CODEOWNERS`.
> Methodology: **STRIDE** plus CDN/HTTP-cache–specific threat classes
> drawn from OWASP, RFC 9110/9111/9112 errata, and public Varnish/NGINX
> CVE history.

This document enumerates the assets, trust boundaries, attacker classes,
and threats that `bouine` must defend against, together with the controls
implemented (or planned) in each phase. It is referenced from
[`docs/architecture.md`](../architecture.md)
and is binding for security-relevant design decisions.

If a code change touches a threat listed here, the PR description must
state which threat IDs are affected and how the controls remain intact.

---

## 1. Assets

| ID  | Asset                                  | Confidentiality | Integrity | Availability |
|-----|----------------------------------------|-----------------|-----------|--------------|
| A1  | Cached response bodies                 | Low (often public) | High | High |
| A2  | Cache metadata (keys, Vary, TTLs)      | Low             | High      | High |
| A3  | Surrogate-key index & ban list         | Low             | High      | High |
| A4  | TLS private keys (data plane)          | High            | High      | Medium |
| A5  | TLS / mTLS private keys (cluster)      | High            | High      | Medium |
| A6  | Admin bearer tokens                    | High            | High      | Medium |
| A7  | Configuration (YAML, VCL shim source)  | Medium          | High      | High |
| A8  | Access / error logs (may contain IPs)  | Medium          | Medium    | Low  |
| A9  | Process memory (incl. in-flight bodies)| Variable        | High      | High |
| A10 | Local disk warm tier                   | Medium          | High      | Medium |
| A11 | Peer-fetch traffic                     | Medium          | High      | Medium |
| A12 | Origin credentials (mTLS client certs) | High            | High      | Medium |
| A13 | Telemetry stream (metrics/traces)      | Low             | Low       | Low  |

Confidentiality of A1/A2 is "low" by default because caches store
shared, cacheable, public-by-spec content. The moment a response is
private (`Cache-Control: private`, `Authorization`, `Set-Cookie`
without explicit override), it must not be stored — see T13/T14 below.

---

## 2. Trust Boundaries

```
                ┌──────────────────────────────────────────────┐
                │            Untrusted Internet                │
                └───────────────────┬──────────────────────────┘
                                    │  TB1
                       ┌────────────▼─────────────┐
                       │  Data-plane listener     │
                       │  (L1: H1/H2/H3 + TLS)    │
                       └────────────┬─────────────┘
                                    │  TB2 (in-process)
                       ┌────────────▼─────────────┐
                       │  Cache engine + storage  │
                       │  (L2–L4)                 │
                       └─────┬───────────────┬────┘
                             │ TB3           │ TB4
                  ┌──────────▼────┐    ┌─────▼──────────┐
                  │ Origin        │    │ Peer bouine    │
                  │ (mTLS opt)    │    │ (mTLS req)     │
                  └───────────────┘    └────────────────┘

                       ┌──────────────────────────┐
                       │  Operator / CI           │
                       └────────────┬─────────────┘
                                    │  TB5
                       ┌────────────▼─────────────┐
                       │  Admin API (net/http, L7)│
                       │  bearer / mTLS auth      │
                       └──────────────────────────┘

                       ┌──────────────────────────┐
                       │  Local disk / kernel     │  TB6
                       └──────────────────────────┘
```

| ID  | Boundary                              | Notes |
|-----|---------------------------------------|-------|
| TB1 | Internet ↔ data plane                 | Highest-risk boundary. TLS terminates here. |
| TB2 | Listener ↔ engine (in-process)        | Trust = same process. Goroutine-safety boundary. |
| TB3 | bouine ↔ origin                       | TLS verified by default. Optional mTLS. |
| TB4 | bouine ↔ peer bouine                  | mTLS required. Separate CA from data plane. |
| TB5 | Operator ↔ admin API                  | Bearer token or mTLS. Separate port. |
| TB6 | Process ↔ disk / kernel               | OS isolation; encryption-at-rest delegated to volume. |

---

## 3. Attacker Classes

| Class | Description | Capability |
|-------|-------------|------------|
| C1 — External anonymous | Random Internet client | Unlimited request volume, no creds. |
| C2 — Malicious client (authenticated upstream of bouine) | Bot, scraper, abusive user | Can craft any HTTP request, has IP reputation. |
| C3 — Hostile origin | Compromised or misconfigured backend | Can send arbitrary responses. |
| C4 — On-path attacker | Untrusted network between bouine and origin | Read/modify if TLS missing. |
| C5 — Compromised peer | One bouine pod is rooted | Has cluster mTLS cert, can talk gossip + peer-fetch. |
| C6 — Insider / operator misuse | Operator with admin token | Can purge, read config. |
| C7 — Co-tenant on same node | Another container on same K8s node | Can probe localhost, inspect cgroup metrics. |
| C8 — Supply-chain | Malicious dependency, container base, build runner | Code-level access. |

Out of scope: nation-state with physical access, hardware side-channels
on shared CPU (Spectre-class — mitigated by kernel + CPU vendor).

---

## 4. Threats (STRIDE + CDN-specific)

Each threat has an ID (Txx), STRIDE class, attacker class(es), affected
assets, current controls (`✓` = shipped, `→` = roadmap), and residual
risk after controls.

### Spoofing

| ID  | Threat | STRIDE | Attackers | Assets | Controls | Residual |
|-----|--------|--------|-----------|--------|----------|----------|
| T01 | Peer impersonation: rogue pod joins cluster | S | C5,C7 | A2,A11 | ✓ mTLS on cluster listener (TB4), separate CA from data plane, SPIFFE-style SAN check on incoming peer conns. | Low |
| T02 | Origin impersonation: MITM presents wrong cert | S | C4 | A1,A3,A12 | ✓ TLS verify enabled by default (no `InsecureSkipVerify` outside an explicit per-pool `tls.insecure: true` test flag, refused in production builds). Optional SPKI pinning per pool. | Low |
| T03 | Client impersonation against admin API | S | C1,C2 | A6 | ✓ Bearer token (constant-time compare) or mTLS. Rejects insecure HTTP for write methods. Admin port never exposed externally in default Helm chart. | Low |
| T04 | Forged `X-Forwarded-For` / `Forwarded` | S | C1,C2 | A8 | ✓ Trust list of upstream LBs configured per route; otherwise the header is stripped before logging. | Low |

### Tampering

| ID  | Threat | STRIDE | Attackers | Assets | Controls | Residual |
|-----|--------|--------|-----------|--------|----------|----------|
| T05 | **HTTP Request Smuggling** (CL.TE, TE.CL, TE.TE) | T | C1,C2 | A1,A2 | ✓ Reject any request with both `Content-Length` and `Transfer-Encoding`, duplicate `Content-Length`, obs-fold, or non-token `TE` values. Strict RFC 9112 parser. Fuzz corpus from PortSwigger papers. Metric `bouine_smuggling_rejects_total`. | Low |
| T06 | **Cache poisoning via unkeyed input** (Host header, port, scheme, X-Forwarded-Host, X-Forwarded-Scheme, etc.) | T | C1,C2 | A1,A2 | ✓ Cache key includes scheme + canonical host + canonical port + canonical path + canonical query. Headers participate in the key ONLY via `Vary`. Per-route allow-list of forwarded headers may participate via explicit `cache.key.include_headers`. | Low |
| T07 | **Cache key shadowing** via header reflection | T | C1,C2 | A1 | ✓ Default policy refuses to cache responses that reflect arbitrary request headers without `Vary`. Operator-visible warning when this is detected. | Medium |
| T08 | **Web Cache Deception** (path confusion, `/profile.css` returning user data) | T | C1,C2 | A1 | ✓ Default policy: do not cache responses whose `Content-Type` doesn't match the URL extension *when* the response carries `Cache-Control: private` semantics or `Set-Cookie`. Operator may opt out per route. | Medium |
| T09 | **Vary blow-up** (uncontrolled secondary keys) | T,D | C2 | A2,A9 | ✓ Per-route cap on number of stored variants per primary key (`cache.vary.max_variants`, default 64). Excess variants evict the oldest LRU-style with a metric `bouine_vary_eviction_total`. Phase 6 detects pathological Vary patterns. | Medium |
| T10 | **HTTP/2 / HTTP/3 RST flood** (CVE-2023-44487 class) | T,D | C1 | A9 | ✓ `SETTINGS_MAX_CONCURRENT_STREAMS` enforced, RST rate per connection capped, oversized cancellation triggers connection close, metric exposed. HTTP/3 equivalent: STOP_SENDING / RESET_STREAM caps. | Low |
| T11 | Header injection via CRLF in upstream response | T | C3 | A1,A8 | ✓ Strict header parsing per RFC 9110 §5.5; CR/LF/NUL rejected; metric exposed. | Low |
| T12 | Range-request abuse to amplify origin bandwidth | T,D | C1,C2 | A9 | ✓ Range requests are coalesced via request collapsing; ranges outside cached body trigger a single origin fetch, never N. Per-route max Range count per request (default 10). | Low |
| T13 | Caching responses that should be private (`Set-Cookie`, `Authorization`) | T,I | C3 (misconfig) | A1 | ✓ Engine refuses by default; opt-in requires explicit `cache.cookies.allow_set_cookie: true` or `cache.allow_authorized: true`. Both emit boot-time warnings. | Low |
| T14 | Cross-user cache contamination via cookies | T,I | C1,C2 | A1 | ✓ Cookies do not participate in cache key by default. Per-route allow-list of cookie names (`cache.cookies.key: [sid_country]`) may opt specific cookies in; their values are canonicalized and bounded. | Low |
| T15 | Config tampering during load (env-var injection) | T | C6,C8 | A7 | ✓ Config schema validation; envvar interpolation only on values that pass through `${VAR}` syntax explicitly; no shell expansion. Reject unknown keys (`strict: true`). | Low |
| T16 | Warm-tier corruption (bit-rot, partial write) | T | C7,kernel | A1,A10 | ✓ Per-segment CRC32C footer; corrupt segments are quarantined on open; WAL replay rebuilds index. | Low |
| T17 | Surrogate-key collision (predictable key namespace) | T | C1,C2,C3 | A3 | ✓ Surrogate-key matches are exact-string, never regex; ban list operates on hashed namespaces; ban requests over the admin API require auth. | Low |

### Repudiation

| ID  | Threat | STRIDE | Attackers | Assets | Controls | Residual |
|-----|--------|--------|-----------|--------|----------|----------|
| T18 | Operator denies issuing a destructive purge | R | C6 | A3,A8 | ✓ Admin API logs every write action with: token-ID hash, source IP, full predicate, count of affected objects, monotonic seq. Logs are append-only to stdout (operator forwards to SIEM). | Medium |
| T19 | Origin denies serving a poisoning response | R | C3 | A8 | ✓ Cache stores the origin's `Date` + receive-time + response hash for every miss; ban evidence trail can reproduce the headers we received. | Medium |

### Information disclosure

| ID  | Threat | STRIDE | Attackers | Assets | Controls | Residual |
|-----|--------|--------|-----------|--------|----------|----------|
| T20 | Timing side-channel reveals cache hit vs miss | I | C1,C2 | A1 | Documented residual. Application-level mitigations (e.g., randomized origin latency) are out of scope; bouine exposes a per-route `cache.timing_padding: bool` that adds 0–`max-age`/100 jitter when set. | Medium |
| T21 | Log leakage of `Authorization` / `Cookie` / body | I | C6,C8 | A6,A8 | ✓ Default access-log schema strips secret headers; full-header log is opt-in per route with a config warning. Bodies never logged. | Low |
| T22 | Metric cardinality leak (e.g., raw URL as label) | I,D | C1 | A8,A9 | ✓ Cardinality budget enforced by unit test; labels are pre-declared allow-list. Reviewers reject high-cardinality labels (see `AGENTS.md §9`). | Low |
| T23 | Memory disclosure via reused pooled buffers | I | C1,C2 | A1,A9 | ✓ All pooled buffers (`sync.Pool`) zeroed on Put. Fuzz tests exercise concurrent put/get/get. | Low |
| T24 | Cross-tenant disclosure (when multiple vhosts share storage) | I | C1,C2 | A1 | ✓ Cache key includes scheme + host. Per-route ACL stage runs before lookup. Multi-tenant scoping beyond vhost is deferred (see `docs/architecture.md §1.2`). | Medium |
| T25 | Compression-oracle attacks (BREACH / CRIME) | I | C2,C4 | A1 | ✓ Default policy: store responses in the encoding the origin produced; never recompress secret responses (responses with `Cache-Control: private` would not be stored anyway). HTTP/2 `HPACK` and HTTP/3 `QPACK` settings disable dynamic-table sensitive headers. | Low |
| T26 | OCSP / SCT / cert-transparency information leak | I | C4 | A4 | Documented residual. Stapling is enabled where the origin supplies it; bouine does not fetch OCSP itself to avoid leaking traffic patterns. | Medium |
| T27 | Disk warm-tier exposure on shared host | I | C7 | A10 | ✓ Permissions `0600` on segments; encryption-at-rest delegated to the cloud volume (see `docs/architecture.md §1.2`). | Medium |

### Denial of Service

| ID  | Threat | STRIDE | Attackers | Assets | Controls | Residual |
|-----|--------|--------|-----------|--------|----------|----------|
| T28 | Slowloris / slow-body (H1 + H2 streams + H3) | D | C1,C2 | A9 | ✓ Per-conn read/write deadlines, per-stream deadline, max idle, bound on slow-body byte rate. | Low |
| T29 | Connection flood | D | C1 | A9 | ✓ Per-IP conn cap (configurable), per-listener total cap, accept loop backpressure. SO_REUSEPORT in front of multiple worker procs is supported. | Medium |
| T30 | Memory exhaustion via large bodies | D | C1,C3 | A9 | ✓ Per-request body cap (default 100 MiB); larger streams pass-through without caching; configurable per route. Body never fully buffered above 64 KiB. | Low |
| T31 | Cache stampede on cold key | D | C1,C2 | A9 (origin) | ✓ Request collapsing single-flight per key; bounded collapse window; jittered TTLs on origin-driven `Cache-Control: max-age`. | Low |
| T32 | Purge / ban storm exhausting CPU | D | C6 (misuse), C1 (via leaked token) | A2,A9 | ✓ Admin API rate limit per token; bans compiled once and indexed by surrogate key first, regex last; purge broadcast is gossiped at bounded rate. | Medium |
| T33 | ESI bomb (recursive includes) | D | C3 | A9 | ✓ Max include depth (default 5), per-request include count cap (default 64), cycle detection by URL hash, per-request CPU budget. | Low |
| T34 | Prefetcher overwhelms origin | D | C6 (misconfig), C3 | A9 (origin) | ✓ Per-pool prefetch concurrency cap, jitter, opt-in time-of-day windows; respects `429` and `Retry-After`. | Low |
| T35 | Cluster-wide gossip storm during partition recovery | D | network | A11 | ✓ Memberlist defaults tuned; suspicion timeout + indirect probes; anti-entropy reconciler is rate-limited; backpressure on hinted-handoff queue. | Medium |
| T36 | Peer-fetch loop (A→B→A) | D | C5,bug | A9,A11 | ✓ `Bouine-Hop` header, hop limit (default 2), monotonic request ID for loop detection, drop with 508 on excess. | Low |
| T37 | Header / URL bombs (huge header value or query string) | D | C1,C2 | A9 | ✓ Header total ≤ 64 KiB, per-header ≤ 8 KiB, header count ≤ 100, URL ≤ 8 KiB, query string ≤ 4 KiB. Configurable per listener. | Low |
| T38 | Disk filling via uncapped warm tier writes | D | C1 | A10 | ✓ Hard cap `storage.warm_max_bytes`; eviction blocks ingest beyond cap; metric + alert thresholds. | Low |
| T39 | UDP amplification via HTTP/3 (QUIC) | D | C1 | A9 | ✓ Address validation (Retry token) on every new connection until amplification limit (RFC 9000 §8) is satisfied; metric exposed. | Low |
| T40 | Goroutine leak via abandoned upstream conn | D | C3 (slow), bug | A9 | ✓ All goroutines owned, joined on shutdown; CI test detects leaks via `goleak`. | Low |

### Elevation of privilege

| ID  | Threat | STRIDE | Attackers | Assets | Controls | Residual |
|-----|--------|--------|-----------|--------|----------|----------|
| T41 | Admin token theft → cluster-wide purge | E | C1,C8 | A3,A6 | ✓ Tokens short-lived (operator policy); rotation supported by reading from file; admin write endpoints (purge/ban/refresh) check token equality in constant time. Cobra never accepts a token via flag (env var / file only). | Medium |
| T42 | VCL shim escape to host code | E | C6 (malicious VCL) | process | ✓ Shim is a translator to the native config tree; no embedded interpreter, no eval; no inline-C support (out of scope in `docs/architecture.md §1.2`). | Low |
| T43 | Path traversal in storage layout | E | C1,C2 | A10 | ✓ Disk paths derived from `xxhash64(key)` hex, never from user input. Static analysis bans `os.Open` with user input. | Low |
| T44 | Supply-chain code injection via dependency | E | C8 | process | ✓ Dependency allow-list (`docs/deps.md`), `govulncheck` in CI, signed container images (cosign), SBOM (syft), Dependabot reviews. | Medium |
| T45 | Container privilege | E | C7,C8 | process | ✓ Distroless base, non-root UID, read-only root FS, `securityContext` defaults in Helm chart (`runAsNonRoot`, `allowPrivilegeEscalation: false`, drop ALL caps, seccomp `RuntimeDefault`). | Low |
| T46 | Pprof exposed without auth → arbitrary heap dump | E,I | C1 | A9 | ✓ Pprof mounted on admin listener only, behind same auth; admin listener not bound externally in default Helm. | Low |

---

## 5. Phase-6 (AI / dashboard) specifics

| ID  | Threat | Controls |
|-----|--------|----------|
| T47 | AI ingestion pipeline DoS by replaying large logs | ✓ Hard sample-rate cap; analyzer runs on a separate goroutine pool with a CPU + memory budget. Backpressure drops samples before it stalls. |
| T48 | Dashboard XSS via reflected route names | ✓ Strict CSP, no inline JS, escape on server, contract tests for output encoding. |
| T49 | "Apply suggestion" → unintended config push | ✓ Two-step: suggestions emit a config diff; apply requires the same auth as direct admin API; dry-run preview mandatory in default UI. |
| T50 | Model file tampering | ✓ ONNX model file hash pinned in config; load fails on mismatch. |
| T51 | Dashboard auth bypass | ✓ Inherits admin API auth; no separate session model. |

---

## 6. Out-of-scope threats (delegated)

These are intentionally not addressed inside bouine. They are listed so
operators know where to put the control.

- **WAF / OWASP-class request filtering** — delegated to a sidecar
  (e.g. Coraza, modsec) or an upstream LB.
- **DDoS scrubbing at L3/L4** — delegated to the cloud edge / CDN in
  front of bouine.
- **End-user authentication / authorization on the data plane** —
  deferred (see `docs/architecture.md §1.2`). Today bouine forwards the auth headers
  and trusts the origin to enforce.
- **Encryption-at-rest of the warm tier** — delegated to the cloud
  volume (LUKS, EBS encryption, etc.).
- **Per-tenant isolation beyond virtual host** — deferred. Operators
  who need strong tenant isolation run one bouine deployment per
  tenant.
- **Rate-limiting on the data plane** — deferred (see `docs/architecture.md §1.2`).
  Backpressure exists for connections and slow bodies; per-route
  request-rate limiting is not yet a feature.

---

## 7. Controls inventory (cross-cutting)

| Control                          | Phase | Owner package |
|----------------------------------|-------|---------------|
| TLS termination + cert reload    | 1     | `internal/listener/tls` |
| Strict H1/H2/H3 framing parsers  | 1     | `internal/listener/{h1,h2,h3}` |
| Header / URL / body size caps    | 1     | `internal/pipeline` |
| Cache key normalization          | 3     | `internal/cache/key` |
| Vary blow-up cap                 | 3     | `internal/cache/vary` |
| Request collapsing               | 3     | `internal/pipeline/collapse` |
| Cluster mTLS + protocol version  | 4     | `internal/cluster/peer` |
| Anti-entropy rate limit          | 4     | `internal/cluster/antientropy` |
| Admin auth (bearer / mTLS)       | 1+    | `internal/admin/auth` |
| Admin write audit log            | 4     | `internal/admin/audit` |
| Surrogate-key indexer            | 5     | `internal/cache/surrogate` |
| ESI safety limits                | 5     | `internal/esi` |
| Dependency / SBOM / cosign       | 0     | CI |
| Container hardening              | 0     | Helm chart |
| Cardinality budget tests         | 0+    | `internal/observability` |
| Smuggling fuzz corpus            | 1     | `internal/listener/h1` |

---

## 8. Review & maintenance

- This document is reviewed at the start and end of every phase.
- Every new dependency triggers a row in `docs/deps.md` AND a fresh
  pass through §4 to check for new attacker capability.
- Every CVE in `golang.org/x/net/http2` or
  `memberlist` triggers a review of the relevant threat rows.
- PRs touching a threat row must update this document in the same
  change, otherwise CI fails (a doc-coverage check verifies every Txx
  cited in code comments still exists here).
