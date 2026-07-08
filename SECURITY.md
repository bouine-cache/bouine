# Security Policy

`bouine` sits on the critical path of HTTP traffic. Security issues are
treated as the highest-priority work; they are not negotiable against
features or performance.

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

## Reporting a Vulnerability

**Do not open a public GitHub issue for security reports.**

Report privately via GitHub's
[Private vulnerability reporting](https://github.com/bouine-cache/bouine/security/advisories/new)
— it creates a tracked, embargoed advisory and is the only supported
channel. (No security email alias is published; GitHub PVR keeps reports
and fixes in one place.)

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
  [`PLAN.md §18`](PLAN.md#18-out-of-scope--future-roadmap-post-v10)
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
