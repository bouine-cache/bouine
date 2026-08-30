# ADR-0041: Profile-Guided Optimization via committed default.pgo

- **Status**: Accepted
- **Date**: 2026-08-30
- **Deciders**: @thylong
- **Phase**: cross-phase (build & release)
- **Consulted**: —
- **Informed**: —
- **Supersedes**: —

## Context

bouine's performance budget (AGENTS.md §7) is defined against Varnish on
the canonical benchmark, and the hit path is the strictest CPU path in
the system (< 5 µs p50, 0 allocs/op). The hot path is interface-heavy
(`FastPathHandler`, `cache.Cachaner`, middleware chain, `Parser.Serve`
loop), which is precisely the shape where the Go compiler's PGO passes
gain the most: hot-call devirtualization and inlining are driven by
observed call frequency.

Deployment environments vary widely — some operators run hit-heavy
(95 %+), others miss-heavy with cold origins. A profile captured from a
single workload would bias the binary toward that workload. CPU
profiles, however, are additive under `go tool pprof` merge, so a
composite profile can represent the union of traffic shapes.

There is no existing automation to capture CPU profiles under load:
the nightly load-test captures heap/goroutine profiles only, and the
`/debug/pprof/*` endpoints are already wired behind
`admin.pprof_enabled` (internal/admin/pprofwrapper) but disabled in the
load-test config.

Go's PGO mechanism is automatic: a gzipped protobuf CPU profile named
`default.pgo` in the **main package's directory** (`cmd/bouine/`) is
picked up by every build of that package (no flags), affecting `make
build`, the Dockerfile build, and `.github/workflows/release.yml`
alike. A `default.pgo` at the module root is silently ignored for
main-package builds; the profile must live in `cmd/bouine/`.

## Decision

We adopt PGO with a **composite profile refreshed per release**, merged
from three traffic legs:

1. `bench/pgo/run.sh` (wired as `make pgo-capture / pgo-merge /
   pgo-refresh / pgo-verify`) boots the single-node load-test topology
   (origin + bouine only) and captures a CPU profile per leg via the
   admin `/debug/pprof/profile` endpoint:
   - **hit leg** — mirrors §3.2 hit-only (30k RPS, `const_rate.js`),
   - **miss leg** — mirrors §3.3 miss-storm (8k RPS),
   - **mixed leg** — reuses §3.6 `k6.js` verbatim (10k RPS, 60/15/10
     split).
2. Legs are merged (`go tool pprof -proto`) and installed as
   `cmd/bouine/default.pgo`, next to the main package. The file is
   **committed** and tracked; `bench/pgo/.stack/` intermediates are
   gitignored.
3. **Refresh on every release-prepare PR**: `release-pgo.yml` runs
   `make pgo-refresh` and pushes the regenerated profile onto any
   open PR whose title matches `chore(release): prepare vX.Y.Z` (the
   project's release-PR convention, cf. commit d104531). The release PR
   therefore always carries a profile captured from the release
   candidate's own code.
4. `admin.pprof_enabled: true` in `bench/loadtest/config/bouine.yaml`
   enables the endpoints in the load-test topology only; production
   defaults remain off.
5. The bench gate (`bench/run.sh gate`) is unchanged: PGO does not
   alter allocs/op, and ns/op comparison stays the province of
   `benchstat` vs `bench/results/baseline.txt`.

## Consequences

### Positive
- 2–7% expected CPU reduction on the hit path (Go team's published
  PGO range is 2–14%; bouine's hand-tuned fast path tempers this) at
  zero runtime cost.
- Every shipped binary — local `make build`, Docker image, release
  binaries — benefits with no build-flag changes.
- The composite profile covers both hit-heavy and miss-heavy
  deployments; no workload is privileged.
- The refresh rides the existing release-prepare PR flow; no maintainer
  action required beyond reviewing the updated binary blob.

### Negative / trade-offs
- `default.pgo` is an opaque binary blob in the tree (small, ~tens of
  KiB); diffs are unreviewable line-by-line, so review relies on the
  workflow provenance (capture logs + sanity check).
- Build time increases slightly on profile-affected packages (PGO
  disables the midstack inlining cache for hot functions).
- `bench/results/baseline.txt` comparisons across the PGO adoption
  boundary are not apples-to-apples; the baseline must be regenerated
  once after this ADR lands (§7 evidence discipline).

### Risks
- **Staleness**: a profile that no longer matches the code drifts toward
  no-op or, rarely, mild pessimism (≤ ~2%, from I-cache pressure). The
  per-release refresh bounds staleness to one release cycle.
- **Thin profiles**: a too-short capture window (< ~100 samples) cannot
  steer inlining; `pgo-refresh` fails the run in that case instead of
  committing a useless profile.
- **Self-hosted runner variance**: profiles are captured on the
  `[self-hosted, ci]` runner; a mis-behaving host could skew weights.
  The three-leg merge and the 100-sample floor bound the damage.

## Alternatives considered

- **Per-architecture profiles**: Go applies one `default.pgo` per
  module; per-arch profiles would require build-tag gymnastics and
  multi-runner captures for marginal gain (the compiler normalizes
  samples to hot/cold, which is arch-portable). Rejected.
- **`GOAMD64` micro-tuning instead**: orthogonal (codegen level, not
  call-frequency driven) and already possible via env; does not need
  an ADR or automation. Not a substitute.
- **Nightly-only refresh**: profile would refresh even when no release
  follows, producing churn on `main` with no user-visible binary. The
  release-PR hook is cheaper and always timely. Rejected.
- **No PGO**: forgo 2–7% on the CPU-bound axis where bouine competes
  with Varnish. Rejected given the automation already exists.

## References

- AGENTS.md §7 (performance rules), §14 (build/CI), §17 (PR checklist)
- `bench/pgo/run.sh`, `bench/loadtest/config/bouine.yaml`
- `.github/workflows/release-pgo.yml`
- Go PGO user guide: https://go.dev/doc/pgo/
- `go tool pprof -proto` merging: https://go.dev/cmd/pprof
