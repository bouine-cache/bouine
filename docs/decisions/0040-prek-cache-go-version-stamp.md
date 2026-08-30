# ADR-0040: Re-enable the CI prek cache with a GO_VERSION_STAMP guard

- **Status**: Accepted
- **Date**: 2026-08-30
- **Deciders**: @thylong
- **Phase**: cross-cutting (CI)

## Context

The `prek` CI job runs `prek run --all-files` on a self-hosted runner.
prek installs and caches hook environments — most expensively the
`golangci-lint` binary built from source by the
`golangci/golangci-lint` hook. The `j178/prek-action` step caches these
environments between runs, keyed by the hash of
`.pre-commit-config.yaml` (plus OS/arch/python location; see the
action's `src/cache.ts`).

Commit 024819b disabled that cache (`cache: false`) because the key
does not include the Go version: after bumping the toolchain from
1.26 to 1.27, the cache restored a golangci-lint binary built with
Go 1.26, which hard-fails on 1.27 code with:

    Go language version (go1.26) used to build golangci-lint is
    lower than the targeted Go version (1.27.0)

Every Go toolchain bump since would have reproduced this failure
while the cache stayed warm — hence the blanket disable. The cost is
a full hook-environment rebuild (golangci-lint compile) on every CI
run.

## Decision

Re-enable the prek cache, and close the version-blindness hole:

1. **GO_VERSION_STAMP**: a `# GO_VERSION_STAMP: <version>` comment at
   the top of `.pre-commit-config.yaml` carrying the exact `go` version
   from `go.mod`. Because prek-action's cache key hashes this file,
   the stamp makes the cache invalidate on every toolchain bump — the
   stale binary can never be restored again.

2. **go-version-stamp hook** (local, pre-commit stage): runs
   `scripts/check-go-version-stamp.sh` on changes to `go.mod` or the
   hook config, failing the commit if the stamp drifted from
   `go.mod`. Drift is therefore impossible to merge; the cache key
   stays trustworthy.

3. **`make bump-go-stamp`**: one-shot Makefile target that updates the
   stamp from `go.mod`, so the Go-bump workflow is one command.

4. **`cache: true`** on the prek-action step, with a comment in
   `ci.yml` pointing at the stamp dependency so the next person
   disabling it knows what they would break.

## Consequences

- CI prek runs restore cached hook environments again; the
  golangci-lint compile cost is paid only when hooks or the Go
  version change.
- A Go bump now requires `make bump-go-stamp` (enforced by the hook,
  so forgetting fails locally in seconds rather than in CI).
- The stamp lives in a YAML comment, so it adds no runtime behavior —
  it exists purely as cache-key entropy and as a machine-checked
  contract.
- If prek-action ever adds Go-version awareness to its own cache key
  natively, the stamp can be retired; the hook keeps the file honest
  either way.
