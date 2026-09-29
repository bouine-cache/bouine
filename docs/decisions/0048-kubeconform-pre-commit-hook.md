# ADR-0048: kubeconform validation of rendered Helm templates

- **Status**: Accepted
- **Date**: 2026-09-01
- **Deciders**: @chridupin-33
- **Phase**: phase 6

## Context

The HPA template (`templates/hpa.yaml`) has rendered
`behavior.scaleDown.stabilizationSeconds` since chart 0.1.2
(commit `eb9c3ab`) through 0.5.3 — a field that does not exist in
the `autoscaling/v2` API (the correct field is
`stabilizationWindowSeconds`). `helm lint` does not validate rendered
manifests against Kubernetes schemas, and the default values have
`autoscaling.enabled: false`, so the template never rendered in any
existing check. The result passed every local and CI gate and only
surfaced at apply time in a downstream consumer's CD lint
(kubeconform strict schemas).

`AGENTS.md §14.4` mandates that the prek configuration mirror the CI
gates and that adding a prek hook requires an ADR.

## Decision

We add a `helm-kubeconform` prek hook that:

1. Renders the chart with `helm template` including
   `--set autoscaling.enabled=true`, so the HPA template is always
   exercised regardless of defaults.
2. Pipes the output through `kubeconform -strict` using the
   `master-standalone-strict` schema location, which rejects unknown
   fields such as the one that caused this regression.
3. Renders a second variant with `--set serviceMonitor.enabled=true`
   so the ServiceMonitor template is exercised, and adds the
   [datree CRDs-catalog](https://github.com/datreeio/CRDs-catalog) as a
   second `-schema-location` so the rendered ServiceMonitor is
   validated against the real `monitoring.coreos.com/v1` CRD — not
   silently skipped as an unknown kind. This was added after PR #738
   (commit `b0b1eac`) shipped `scrapeNativeHistograms`,
   `scrapeProtocols`, and `scrapeClassicHistograms` inside
   `endpoints[]` (a `ServiceMonitorSpec` field, not an `Endpoint`
   field) past every gate because the ServiceMonitor was neither
   rendered nor schema-covered.

It is scoped with `files: ^deploy/helm/bouine/` so it only fires when
chart files change, and registered on both the `pre-commit` and
`pre-push` stages. This complements the existing `helm-lint` hook
(ADR-0010): `helm lint` catches chart-structure errors, kubeconform
catches rendered-output schema errors.

kubeconform is installed by `make setup-dev` and by the CI `prek` job
before `prek run --all-files`, so the gate is enforced everywhere the
hooks run. The hook skips with a warning when the binary is absent
(e.g. clones that predate `make setup-dev`) so contributors on such
environments are not blocked — but that fallback must never become the
norm: a schema gate that silently skips is a gate that does not exist.

## Consequences

- Rendered-manifest schema regressions are caught locally in seconds,
  before commit, instead of at cluster apply time.
- `helm template` with autoscaling enabled becomes part of the standard
  verification for any chart change.
- Future template additions to conditionally-rendered resources
  (PDB, NetworkPolicy, Ingress, ServiceMonitor, ...) should be added to
  the hook's `--set` list when introduced, so each template renders in
  at least one gate.
- The datree CRDs-catalog is archived (2024) but still served via
  raw.githubusercontent.com. Its schemas are frozen at their last
  state, which is fine for stable CRDs like ServiceMonitor v1. A
  future chart feature relying on a brand-new or rapidly-evolving CRD
  may require vendoring that CRD's schema pinned to a specific release.
- The first run on a fresh machine or CI runner needs network access
  to download the ServiceMonitor schema (~100 KB). Subsequent runs use
  the `.kubeconform-cache/` directory (gitignored) and are fully
  offline. The self-hosted CI runner must allow outbound HTTPS to
  `raw.githubusercontent.com`.
- `-ignore-missing-schemas` is kept: kinds covered by a schema
  location are validated strictly; kinds without any schema (e.g.
  future CRDs not in the catalog) still skip silently rather than
  blocking all commits. Dropping the flag was considered and rejected
  as too noisy until catalog coverage is comprehensive.
