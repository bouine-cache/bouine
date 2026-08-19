# ADR-0032: Changelog automation

- **Status**: Accepted
- **Date**: 2026-08-18
- **Deciders**: @thylong
- **Phase**: tooling
- **Related**: ADR-0028 (testify assertions — same review-discipline pattern)
- **Consulted**: (none)
- **Informed**: (none)

## Context

The `CHANGELOG.md` file fell behind by two releases (v0.4.0, v0.4.1)
because there was no mechanism reminding contributors to update it and
no automated promotion at release time. The file follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) with an
`[Unreleased]` section, but that section was not being curated
continuously.

The repo already uses Conventional Commits and auto-generates GitHub
release notes from commit subjects, but the curated `CHANGELOG.md` is
the human-readable summary that operators read. It needs to stay in sync
without becoming a manual burden.

`AGENTS.md` §10 requires an ADR for "anything touching the VCL shim's
supported surface" and process changes that affect every contributor.
Adding a pre-push hook and a release-time CI job qualifies.

## Decision

We adopt the **hybrid** approach (Keep a Changelog recommended workflow):

1. **Continuous curation via a pre-push warning hook.** A local prek
   hook (`changelog-reminder`, `scripts/changelog-reminder.sh`) runs at
   the `pre-push` stage. It inspects the commits being pushed; if any
   match `feat|fix|perf|refactor` and `CHANGELOG.md` was not modified in
   the same range, it prints a warning to stderr. It **always exits 0**
   -- it is advisory, not a block. This keeps the signal-to-noise ratio
   high: `chore`, `ci`, `test`, `docs`, and `build` commits do not
   trigger the warning.

2. **Automated promotion at release time.** A GitHub Actions workflow
   (`changelog-promote.yml`) triggers after the Release workflow
   completes successfully on a tag. It runs
   `scripts/changelog-promote.py`, which:
   - Replaces the `## [Unreleased]` heading with
     `## [version] - date`.
   - Inserts a fresh, empty `## [Unreleased]` section above it.
   - Updates the `[Unreleased]` comparison link to point at the new tag.
   - Adds a `[version]` link entry.
   The workflow then commits the change and opens a PR against `main`.

### Why pre-push, not pre-commit

The `pre-commit` stage fires before the commit message is written, so it
cannot inspect commit subjects to filter by type. `pre-push` fires after
commits are created and can read the full range with `git log`. The
tradeoff is that the warning comes later (at push time, not commit
time), but this is acceptable -- the warning is advisory, and the
promotion job is the real enforcement.

### Why warning, not block

A hard block would force contributors to add empty or meaningless
changelog entries just to push. The warning nudges without blocking,
and the promotion job handles the mechanical part. If drift persists,
the hook can be upgraded to a block without changing the script
structure.

### Why a PR, not a direct push to main

The promotion PR creates a reviewable artifact. If the `[Unreleased]`
section is empty or malformed, a human can fix it before merge. Direct
pushes to `main` from CI bypass review and are harder to audit.

## Consequences

### Positive
- Contributors are reminded to update `CHANGELOG.md` without being
  blocked.
- The `[Unreleased]` section is promoted automatically at release time
  -- no manual cutting.
- The PR-based flow keeps a human in the loop for the final changelog
  entry.
- The warning hook is fast (one `git log` + one `git diff --name-only`)
  and stays within the pre-push budget.

### Negative / trade-offs
- The warning is advisory; a contributor can ignore it. Mitigation: if
  drift recurs, upgrade to a hard block.
- The promotion PR must be merged before the next release. If it sits
  open, the next release's promotion will fail (the version already
  exists, or the `[Unreleased]` section is stale). Mitigation: the
  promotion script exits non-zero on these conditions, making the
  failure visible.
- Python3 is required for the promotion script. All GitHub-hosted
  runners have it pre-installed. The script has no external
  dependencies.

### Risks
- If a release is cut from a branch other than `main`, the promotion PR
  (which targets `main`) may not reflect the correct base. Mitigation:
  the workflow checks out the tag ref, so the `CHANGELOG.md` content is
  from the released commit.
- The `changelog-reminder` hook uses `@{u}` (upstream tracking branch)
  to determine the push range. On a new branch with no upstream, it
  falls back to `origin/main`. If neither exists, it exits silently.

## Alternatives considered

- **Auto-generate from Conventional Commits (convco / auto-changelog).**
  Rejected: entries would be raw commit subjects, losing the curated
  tone that operators rely on. The current CHANGELOG has multi-line
  context that no commit subject can capture.

- **Release-drafter.** Rejected: maintains a draft in GitHub Releases,
  not in the file. Would create two sources of truth.

- **CI gate that fails release PRs without a CHANGELOG diff.** Rejected
  as the sole mechanism: only fires near release time, too late to build
  the habit. Kept as a future escalation path if the warning hook is
  insufficient.

- **Direct push to main from CI.** Rejected: bypasses review. The PR
  flow is safer and creates an audit trail.

## References

- [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
- `scripts/changelog-reminder.sh` -- pre-push warning hook
- `scripts/changelog-promote.py` -- release-time promotion script
- `.github/workflows/changelog-promote.yml` -- promotion workflow
- `.pre-commit-config.yaml` -- prek hook registration
