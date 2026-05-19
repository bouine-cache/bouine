# ADR-0003: Hand-rolled PROXY protocol parser

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 1 (pre-flight)

## Context

bouine must accept the [PROXY protocol](https://www.haproxy.org/download/2.8/doc/proxy-protocol.txt)
v1 and v2 on the data-plane listeners. This is required when an
upstream L4 load balancer (AWS NLB, GCP TCP LB, HAProxy) terminates
the client TCP connection and forwards the real client address out of
band.

The protocol is small: v1 is a single CRLF-terminated ASCII line; v2 is
a fixed-length binary header (≤ 232 bytes typical, hard upper bound by
spec). Parsing fits in ~150 lines of Go.

Available libraries:

- [`pires/go-proxyproto`](https://github.com/pires/go-proxyproto) —
  MIT, ~2 KLOC, active. Reasonable quality but uses `interface{}` in
  hot paths and allocates on every accept.
- [`mwitkow/go-conntrack`](https://github.com/mwitkow/go-conntrack) —
  unrelated, doesn't fit.

PROXY protocol parsing runs on the L1 listener hot path. It MUST be
zero-allocation post-warmup (`AGENTS.md §7`).

## Decision

We hand-roll the PROXY protocol parser inside `internal/listener/proxyproto`.

- Parse from a `*bufio.Reader` peek-buffer so the underlying
  `net.Conn` is not consumed if the prefix is absent.
- Allocate **once** per connection: a stack-sized header struct, no
  interfaces, no slices for the v1 path.
- Strict by default — unknown family/transport in v2 fails the
  handshake, rather than letting the connection proceed with garbage.
- Fuzz-tested via `go test -fuzz` with corpus seeded from the
  HAProxy reference test vectors.

The implementation lives in L1 and exports a tiny interface
(`Read(io.Reader) (Header, error)`) consumed by every listener.

## Consequences

### Positive
- Zero alloc on the hot path (mandated by performance budget).
- No external dependency; less supply-chain surface (threat T44).
- Easy to audit; ~150 LOC + tests.
- Tight control over strictness, error messages, and metrics.

### Negative / trade-offs
- We own bugs and CVEs in the parser. Mitigated by fuzz tests +
  authoritative test vectors from the HAProxy spec.
- Small ongoing maintenance cost.

### Risks
- Protocol edge cases (TLV extensions, AF_UNIX). Mitigated by
  starting with the strict subset bouine actually needs and rejecting
  unknown extensions; we can relax later.

## Alternatives considered

- **`pires/go-proxyproto`** — adds a dependency and allocates. Could
  reconsider in v1.1 if the maintenance cost of our parser proves
  excessive.
- **No PROXY protocol support** — rejected; PROXY is table stakes
  behind every cloud L4 LB.

## References

- HAProxy PROXY protocol spec (versions 1 + 2)
- `PLAN.md §7`
- `docs/security/threat-model.md` T04, T37, T44
- `AGENTS.md §5` (dependency policy), `§7` (perf budget)
