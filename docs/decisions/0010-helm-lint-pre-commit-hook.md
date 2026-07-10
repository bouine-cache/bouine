# ADR-0010: Helm chart lint in pre-commit and pre-push hooks

- **Status**: Accepted
- **Date**: 2026-06-08
- **Deciders**: @thylong
- **Phase**: phase 6

## Context

Phase 6.1 publishes the Helm chart at `deploy/helm/bouine` to a GitHub
Pages Helm repository on every release tag (`.github/workflows/chart-release.yml`),
where Artifact Hub indexes it. A chart that fails `helm lint` would break
the release workflow and ship a broken artifact to users.

`AGENTS.md §14.4` mandates that the prek configuration mirror the
CI gates so problems are caught locally in seconds, and that **adding or
modifying a prek hook requires an ADR**. The chart had no local
lint gate; regressions to `Chart.yaml`, `values.yaml`, or the templates
were only caught after a tag push.

## Decision

We add a `helm-lint` hook to `.pre-commit-config.yaml` that runs
`helm lint deploy/helm/bouine`. It is scoped with `files: ^deploy/helm/bouine/`
so it only fires when chart files change, and registered on both the
`pre-commit` and `pre-push` stages so a broken chart cannot be committed
or pushed.

## Consequences

### Positive
- Chart breakage is caught locally before commit, mirroring the
  release-time `helm lint` and matching the §14.4 "local mirror of CI"
  rule.
- Scoped `files` filter keeps the hook off commits that do not touch the
  chart, preserving the 30 s pre-commit budget (`helm lint` runs in well
  under a second).

### Negative / trade-offs
- Contributors who modify the chart must have the `helm` binary on
  `PATH`; the hook fails if it is absent. This is acceptable because
  anyone editing a Helm chart is expected to have Helm installed.

### Risks
- A future Helm major could change `helm lint` behavior. Mitigated by
  CI re-running `prek run --all-files` and by pinning Helm in the
  release workflow via `azure/setup-helm`.

## Alternatives considered

- **CI-only lint**: rejected — violates the §14.4 principle that hooks
  are the local mirror of CI, and lets broken charts reach a tag push.
- **Conditional skip when `helm` is missing**: rejected for chart-editing
  contributors — a silently-skipped lint defeats the gate. The `files`
  filter already exempts everyone not touching the chart.

## References

- `AGENTS.md §14.4` (prek hooks; ADR requirement for new hooks).
- `.github/workflows/chart-release.yml` (release-time chart publishing).
- ADR-0006 (unify on `net/http`) — prior phase-level tooling decision.
