# ADR-0064: amd64 release artifacts build at the GOAMD64=v1 baseline

- **Status**: Accepted
- **Date**: 2026-10-17
- **Renumbered**: 2026-10-23, from ADR-0060 — duplicate number; ADR-0060
  (profile-guided optimization) keeps it, as its number is baked into
  build-workflow, runbook and released changelog entries. Released
  changelog entries citing "ADR-0060" in a build-baseline / GOAMD64
  context (e.g. the [0.5.30] GOAMD64=v1 entry) resolve to this ADR;
  released entries are append-only and cannot be edited in place
- **Deciders**: @chrisdupin
- **Phase**: phase 6
- **Amends**: fc70d762 (build: compile linux release artifacts at GOAMD64=v3 and GOARM64=v8.1)

## Context

fc70d762 built the container image and linux release binaries at
`GOAMD64=v3`, keeping the bench-gate runner at baseline so the gate
never measured what shipped. The 0.5.29 release soaks then collapsed at
the default `peer_fetch_concurrency: 4` where 0.5.28 held, and the
stress-cluster bisect convicted the v3 codegen with a two-sided A/B on
identical source and shape:

- GOAMD64=v3 alone reproduces the failure: ~45% peer-RPC hold-time p50
  and erratic 2-4x p99 tails at identical CPU per request, tipping the
  4-slot queueing knee into shed-runaway and origin-ejection collapse.
- GOAMD64=v1 on the 0.5.29 code restores 0.5.28's service signature and
  passes the identical shape at 99.82%.

The extra hold-time is off-CPU wall time (CPU-seconds per request are
equal), which also rules out AVX2 turbo-license downclocking. The
claimed v3 gains never materialize natively: fasthttp's and xxhash's
SIMD paths are runtime CPU-dispatched, and the motivating numbers came
from QEMU emulation.

Full evidence, arm artifacts and census data live in the
bouine-stress-test repo,
`results/0529rc1-soak-banttl5m-3h-3n-6k/INVESTIGATION.md` (update (5)).

## Decision

- amd64 release artifacts (container image, release binaries, nightly
  binaries) build at the `GOAMD64=v1` baseline.
- `GOARM64=v8.1` stays: natively measured gains (LSE atomics on the
  atomic-heavy hit path), unrelated mechanism.
- The bench-gate runner and shipped artifacts must stay at the same
  microarchitecture level. Changing it requires a stress-cluster
  release-soak A/B (3h, pfc=4 knee shape), not microbenchmarks and not
  emulation.

## Consequences

### Positive

- The pfc=4 knee collapse is removed at the root; peer-RPC hold-time
  returns to ~1.6ms, doubling per-slot peer-fetch capacity at small
  concurrency settings.

### Negative / trade-offs

- Theoretical v3 wins on vectorizable compute are given up on amd64;
  none was ever demonstrated natively, and the proxy's peer path is
  latency-bound, not vector-bound.

### Risks

- `GOARM64=v8.1` has not been validated by a linux/arm64 release soak
  (darwin/arm64 benchstat only); revisit if an arm64 soak ever shows
  this class of tail regression.

## Alternatives considered

- `GOAMD64=v2`: no 3h soak evidence for it; v1 is the only level with a
  clean 5-for-5 record.
- Fix the application code and keep v3: the failure reproduces on
  unmodified 0.5.28 code, so there is no code bug to fix; the +45%
  hold-time alone disqualifies v3 for a latency-bound proxy.
- Raise the `peer_fetch_concurrency` default only: shipped as well
  (4 to 16), but it treats the amplifier, not the cause.

## References

- Harness investigation (full evidence): bouine-stress-test repo,
  `results/0529rc1-soak-banttl5m-3h-3n-6k/INVESTIGATION.md`, update (5).
- fc70d762 (the v3 bump); 2d4ba9a5 (pfc default 4 to 16).
