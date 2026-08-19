# Security Policy

`bouine` sits on the critical path of HTTP traffic. Security issues are
treated as the highest-priority work; they are not negotiable against
features or performance.

## Security Requirements

This section describes what users can and cannot expect from bouine in
terms of security. It is the authoritative reference for the security
guarantees the software is intended to provide.

### What bouine provides

- **TLS on the data plane** — minimum TLS 1.2, prefer 1.3, with a
  pinned cipher suite list and SNI required. See `AGENTS.md §6`.
- **TLS / mTLS for cluster traffic** — peer-to-peer communication uses
  mutual TLS. See `docs/security/threat-model.md` TB4.
- **Admin authentication** — the admin API requires a bearer token
  (constant-time compare) or mTLS. Write methods refuse insecure
  transports by default. Tokens are read from env vars or files, never
  from CLI flags.
- **HTTP smuggling defenses** — ambiguous framing
  (`Content-Length` + `Transfer-Encoding`, duplicate `Content-Length`,
  obs-fold) is rejected with `400` and a metric increment. Fuzz-tested
  via a committed corpus.
- **Header hygiene** — hop-by-hop headers (`Connection`, `Keep-Alive`,
  `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`, `Proxy-*`) are
  stripped per RFC 9110 §7.6.1 and never forwarded blindly.
- **Private response protection** — responses with `Cache-Control:
  private`, `Authorization`, or `Set-Cookie` (without explicit
  override) are never stored in the cache.
- **Path traversal prevention** — storage paths are derived from xxhash64
  of cache keys, never from user-controlled strings. Static file serving
  is fuzz-tested against path traversal.
- **Resource limits** — connection limits, in-flight request caps,
  storage admission control, request-collapsing latch caps, and bounded
  queues prevent unbounded memory growth. Every parser has a byte cap
  (headers <= 64 KiB, URLs <= 8 KiB, max 100 headers).
- **Secret-free logging** — `Authorization`, `Cookie`, `Set-Cookie`,
  custom auth headers, and request/response bodies are never logged by
  default. Operators may opt in per route.
- **Signed releases** — container images and release artifacts are
  signed with cosign (keyless, via GitHub OIDC). SBOM (SPDX) is
  generated for every release.
- **Vulnerability scanning** — `govulncheck` runs in CI and on
  pre-push. `gitleaks` scans every commit for leaked credentials.
  `Trivy` scans released images for HIGH/CRITICAL vulnerabilities.
- **No plaintext secrets in code or logs** — enforced by `gosec` and
  `gitleaks` in CI.

### What bouine does NOT provide (security non-goals)

- **Data-plane authentication / authorization** — bouine does not
  authenticate end users or enforce authorization on the data plane.
  It forwards `Authorization` / `Cookie` headers and lets the origin
  enforce. `auth_request`-style external auth is planned for v1.1 (see
  [`ROADMAP.md`](ROADMAP.md)).
- **Rate limiting** — per-route request-rate limiting is not shipped in
  v1.0. Connection and slow-body backpressure exist, but token-bucket
  per-route limiting is deferred.
- **WAF / DDoS scrubbing** — delegated to a sidecar or Layer-7 load
  balancer.
- **Encryption at rest of the warm tier** — delegated to the cloud
  volume (EBS/PD/Azure Disk). bouine does not encrypt disk storage.
- **Multi-tenant isolation beyond virtual host** — operators with
  strong tenancy needs should run one deployment per tenant.
- **Built-in ACME / certificate issuance** — bouine reloads certs from
  disk; ACME is delegated to cert-manager or a sidecar.

The full threat model with STRIDE analysis, assets, trust boundaries,
and per-threat controls lives in
[`docs/security/threat-model.md`](docs/security/threat-model.md).

---

## Supported Versions

Until v1.0, only `main` and the most recent tagged pre-release receive
security fixes. After v1.0 ships, the policy below applies.

| Version       | Status                | Security fixes |
|---------------|-----------------------|----------------|
| `main`        | active development    | yes            |
| v1.x (latest) | supported             | yes            |
| v1.x (prev)   | supported for 6 months| yes            |
| < latest minor| not supported         | no             |

If you operate a `bouine` deployment older than 6 months behind the
latest minor, please upgrade before reporting issues unless you can
reproduce on a supported version.

## Security Contacts

- **Email**: [bouine@pm.me](mailto:bouine@pm.me)
- **GitHub**: [@bouine-cache/maintainers](https://github.com/orgs/bouine-cache/teams/maintainers)
- **Private reporting**: [GitHub Private Vulnerability Reporting](https://github.com/bouine-cache/bouine/security/advisories/new)

## Reporting a Vulnerability

**Do not open a public GitHub issue for security reports.**

Report privately via GitHub's
[Private vulnerability reporting](https://github.com/bouine-cache/bouine/security/advisories/new)
— it creates a tracked, embargoed advisory and is the only supported
channel. You may also email the security team at
**bouine@pm.me**; GitHub PVR is preferred so that reports and fixes stay
in one place.

Please include:

- A description of the issue and its impact.
- The affected version (commit SHA if `main`).
- A minimal reproduction, ideally a script or `curl` invocation.
- Whether the issue is already public anywhere.
- Whether you would like credit and under what name.

We will acknowledge receipt within **3 business days** and aim to ship
a fix or an embargoed mitigation within **30 days** for high-severity
issues, **90 days** for medium-severity issues. We will keep you in the
loop with at least weekly updates during an embargo.

## Scope

In scope:

- The `bouine` binary, including all listeners (HTTP/1.1, HTTP/2),
  cache engine, storage, clustering, admin API, and CLI.
- Default configuration shipped under `examples/` and the Helm chart.
- The official container image.
- The Go SDK (`pkg/bouineapi`).

Out of scope:

- Misconfiguration that is documented as unsafe (e.g.
  `tls.insecure_skip_verify` in non-release builds, opting in to
  `cache.cookies.allow_set_cookie`, exposing the admin port to the
  Internet against the chart defaults).
- Third-party dependencies — please report upstream. We will track and
  bump.
- Denial-of-service achievable only with operator credentials (auth
  abuse).
- Findings against features explicitly listed as deferred in
  [`docs/architecture.md §1.2`](docs/architecture.md#12-non-goals)
  (data-plane auth, rate limiting, multi-tenant isolation beyond vhost,
  etc.).

## Severity Guidance

We use a CVSS-like internal rubric. Examples:

- **Critical**: cache poisoning across users; remote code execution;
  cluster mTLS bypass; admin auth bypass.
- **High**: HTTP request smuggling; cross-route data leak; persistent
  cache corruption.
- **Medium**: information disclosure with limited blast radius;
  denial-of-service requiring sustained traffic; logic bugs in `Vary`
  / `Cache-Control` parsing without exploitable impact.
- **Low**: cosmetic timing side-channels, log-only issues, hardening
  improvements.

The full threat model with controls and residual risk lives in
[`docs/security/threat-model.md`](docs/security/threat-model.md).

## Coordinated Disclosure

We follow a standard 90-day embargo, extendable by mutual agreement if a
fix is non-trivial. Once a fix lands in a tagged release:

- A GHSA advisory is published.
- A `CHANGELOG.md` entry is added under the `### Security` section.
- The threat model is updated with the new control.
- We request a CVE if the issue is exploitable in default
  configurations.

Reporters are credited in the advisory and in `CHANGELOG.md` unless
they ask otherwise.

## Vulnerability Response Process

When a vulnerability report is received, the maintainers follow this
process:

1. **Triage (within 3 business days).** A maintainer acknowledges
   receipt via the GitHub PVR advisory or email, confirms the report is
   in scope (see [Scope](#scope) above), and assigns a preliminary
   severity using the [Severity Guidance](#severity-guidance) rubric.
   If the report is out of scope, the reporter is told why.

2. **Assess and reproduce (within 1 week of triage).** A maintainer
   attempts to reproduce the issue on a supported version. If
   reproducible, the severity is confirmed; if not, the report is
   closed with an explanation. If the issue is already public, the
   embargo is lifted and a fix is prioritized immediately.

3. **Develop a fix.** The fix is developed on a private branch or via
   the GitHub PVR advisory's draft. At least one other maintainer
   reviews the fix. The fix includes:
   - Code change with tests.
   - Threat model update (`docs/security/threat-model.md`) if the
     issue maps to a new or existing threat row.
   - CHANGELOG entry under `### Security`.

4. **Release.** A tagged release is cut containing the fix. The
   release is signed with cosign and includes an SBOM. For critical
   issues, an out-of-band release is issued; for medium/low issues, the
   fix may batch into the next scheduled release.

5. **Publish advisory.** Once the release is available:
   - The GitHub Security Advisory (GHSA) is published.
   - A CVE is requested if the issue is exploitable in default
     configurations.
   - The reporter is credited unless they requested anonymity.

6. **Post-release review.** Within one week of the release, maintainers
   review whether existing controls, fuzz corpora, or CI gates should be
   strengthened to prevent similar issues. Action items are tracked as
   GitHub issues.

Throughout the embargo, the reporter receives at least weekly status
updates. If a fix cannot be delivered within the target timeframe
(30 days for high-severity, 90 days for medium-severity), the
maintainers notify the reporter with a revised timeline and rationale.

## Hardening Checklist for Operators

These are the most common misconfigurations we see in HTTP caches. Even
if you never report an issue, please review:

- [ ] Admin port is NOT exposed to the Internet. Default Helm chart
      binds it to cluster-internal only.
- [ ] Admin bearer token is rotated regularly and stored as a Kubernetes
      `Secret`, never on the CLI flag.
- [ ] Upstream TLS verification is enabled (`tls.enabled: true`,
      `insecure_skip_verify: false`).
- [ ] Warm-tier volume has encryption-at-rest enabled at the cloud
      provider layer.
- [ ] `enable_0rtt` is `false` unless your application has documented
      idempotency contracts for the routes that opt in.
- [ ] Container runs as non-root with `readOnlyRootFilesystem: true`
      and dropped capabilities.
- [ ] You have monitoring on `bouine_smuggling_rejects_total`,
      `bouine_vary_eviction_total`, `bouine_cluster_protocol_mismatch_total`.

Thank you for helping keep `bouine` and its users safe.
