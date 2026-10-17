# ADR-0060: amd64 release artifacts build at the GOAMD64=v1 baseline

- **Status**: Accepted
- **Date**: 2026-10-17
- **Deciders**: @chrisdupin
- **Phase**: phase 6
- **Amends**: fc70d762 (build: compile linux release artifacts at
  GOAMD64=v3 and GOARM64=v8.1)

## Context

fc70d762 raised the container image and the linux release binaries from
`GOAMD64=v1` to `GOAMD64=v3` (alongside `GOARM64=v8.1`, which is kept).
The claimed amd64 upside (cache geomean −9.3% et al.) was measured under
QEMU TCG emulation, and the bench-gate runner deliberately stayed at
baseline so gate comparisons stayed apples-to-apples — which meant the
gate never measured what actually shipped.

The 0.5.29-rc1/rc2 release soaks (prod-hot-only-banttl5m, 6k req/s,
3 nodes, 3h, code-default `peer_fetch_concurrency: 4`) then failed
5-for-5 with a peer-fetch slot saturation collapse that 0.5.28 never
showed (5-for-5 healthy), same node, same day, sequential A/B. Demand
and CPU composition were ruled out in the harness investigation; the
bisect that followed pinned the build:

1. Image provenance, read from the shipped binaries: 0.5.28 and rc1
   are both go1.27.2; the golang base-image digest bump changed the
   toolchain not at all; client_golang's histogram `observe()` is
   byte-identical across the bump; the only runtime dependency deltas
   are trivial. The build delta is `GOAMD64` v1→v3 plus 20 application
   commits.
2. A `GOAMD64=v3` rebuild of plain v0.5.28 source (verified
   binary-identical to CI builds except buildinfo strings) reproduces
   the regression's full signature with zero application commits:
   peer-RPC hold-time p50 1.6 → 2.3ms (+45%) at identical CPU per
   request (447µs both), erratic 2-4× p99 tails, a deeper acquireSlot
   queue, and a maturity tip at ~T+50m into the shed-runaway / origin
   ejection collapse at the 3h shape. The shipped v1 build holds the
   identical shape indefinitely.
3. The mechanism is not CPU cycles: CPU-seconds per request are equal,
   so the extra hold-time is off-CPU wall time, consistent with the
   investigation's "same work, 2× of it" observation. AVX2 turbo
   licensing is ruled out by the same equality (a frequency penalty
   would inflate CPU-seconds per request).
4. The claimed SIMD upside was wrong in kind: fasthttp's and xxhash's
   SIMD paths are runtime CPU-feature-dispatched; `GOAMD64` does not
   enable them.

## Decision

- amd64 release artifacts (container image, release binaries, nightly
  binaries) build at the `GOAMD64=v1` baseline.
- `GOARM64=v8.1` stays: its gains were measured natively (Apple M5
  benchstat, LSE atomics on the atomic-heavy hit path), and its
  mechanism is unrelated to the amd64 regression.
- The bench-gate runner and shipped artifacts must stay at the same
  microarchitecture level. Changing the level requires a stress-cluster
  release-soak A/B (3h, pfc=4 knee shape), not microbenchmarks and not
  emulation.

## Consequences

### Positive

- The 0.5.29 knee regression (pfc=4 collapse class) is removed at the
  root; validated by a 3h pfc=4 arm on HEAD built at v1
  (`bisect-headv1-3h-3n-6k`).
- Peer-RPC hold-time p50 returns to ~1.6ms, restoring ~2× per-slot
  peer-fetch capacity at small `peer_fetch_concurrency` settings.
- Gate-vs-shipped parity is restored on amd64.

### Negative / trade-offs

- The theoretical v3 wins on pure-compute workloads are given up on
  amd64; no shipped benchmark ever demonstrated one natively, and the
  proxy's peer path is latency-bound, not vector-bound.
- Pre-Haswell (<2013) amd64 fleets gain nothing from either level; v1
  runs everywhere by construction.

### Risks

- None known on amd64. `GOARM64=v8.1` remains unvalidated by a
  linux/arm64 stress soak (darwin/arm64 benchstat only); revisit if an
  arm64 release soak ever shows the same class of tail regression.

## Alternatives considered

- `GOAMD64=v2`: untested on the stress cluster; rejected because v1 is
  the only level with 5-for-5 healthy 3h soak evidence.
- Fix the application code and keep v3: the collapse reproduces on
  v0.5.28 code, so there is no code-side bug to fix; v3's +45% p50
  alone halves per-slot capacity at any configured concurrency.
- Raise the `peer_fetch_concurrency` default only: shipped as well
  (4 → 16), but it treats the amplifier, not the cause; the hold-time
  regression would persist at any configured concurrency.

## References

- Harness investigation (full evidence, arms and census data):
  `results/0529rc1-soak-banttl5m-3h-3n-6k/INVESTIGATION.md` in the
  bouine-stress-test repo, updates 2026-10-17 (5) onward.
- fc70d762 (the v3 bump), 96e619bc (outbound TFO, closed by PR #800),
  2d4ba9a5 (pfc default 4 → 16).
