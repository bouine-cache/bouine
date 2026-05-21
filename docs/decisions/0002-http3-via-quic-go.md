# ADR-0002: HTTP/3 via `quic-go/quic-go`

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 1 (pre-flight)

## Context

`PLAN.md §1.1` requires HTTP/3 (QUIC) on the data plane. The Go
ecosystem has exactly two viable production options today:

- [`quic-go/quic-go`](https://github.com/quic-go/quic-go) — the
  reference implementation used by Caddy, Hugo, Cloudflared, and many
  others. Mature, RFC 9000/9001/9114 compliant, datagram support,
  0-RTT, BBR/CUBIC congestion control.
- `golang.org/x/net/quic` — early experimental tree, not API-stable,
  no HTTP/3 layer of its own.

`quic-go` is already implied by `PLAN.md §2.1` ("HTTP/3 via
`quic-go/http3`"). This ADR makes the choice formal.

## Decision

We use `github.com/quic-go/quic-go` for QUIC and
`github.com/quic-go/quic-go/http3` for HTTP/3.

- Pinned in `go.mod` once the listener PR lands.
- Listed on the dependency allow-list (`docs/deps.md`).
- Wrapped by `internal/listener/h3` so the public surface stays narrow
  and `quic-go` upgrades are localized.

We accept `quic-go`'s API-stability posture: the library follows
semver, ships frequent patch releases, and has occasionally made
breaking changes in major versions. The wrapper insulates the rest of
the codebase.

## Consequences

### Positive
- Battle-tested HTTP/3 stack; no need to reinvent QUIC.
- Built-in primitives we want anyway: 0-RTT (off by default per
  `PLAN.md §17.3`), datagram, retry/address validation (threat T39).
- Active maintenance, security advisories upstreamed.

### Negative / trade-offs
- Large dependency footprint relative to stdlib.
- Tied to upstream's CGO-free crypto choice (BoringSSL / Go stdlib).
- Major-version bumps may require wrapper updates.

### Risks
- Upstream CVEs require fast patch rollouts. Mitigated by
  `govulncheck` + Dependabot grouping (see
  `.github/dependabot.yml` `quic-stack` group).

## Alternatives considered

- **`golang.org/x/net/quic`** — too early. No HTTP/3 layer; would
  require building our own. Revisit in a future minor release.
- **Roll our own QUIC** — outright rejected; thousands of person-hours
  of cryptanalysis + interop testing live in `quic-go`.

## References

- `PLAN.md §1.1`, `§2.1`, `§17.3`
- `docs/security/threat-model.md` T10, T39
- https://github.com/quic-go/quic-go
