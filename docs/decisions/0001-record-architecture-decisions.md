# ADR-0001: Record architecture decisions

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 0

## Context

bouine is a multi-phase project (`docs/architecture.md`) with strict layering and
non-trivial cross-cutting concerns (performance, RFC 9111 conformance,
security, observability). Decisions taken now will be revisited and
challenged by future contributors — human and AI. We need a durable
record of *why* a choice was made, not just *what* the code does.

`AGENTS.md §10` already mandates that ADRs are written for dependency
additions, protocol/wire-format changes, eviction-algorithm changes,
cluster-protocol changes, VCL-shim surface changes, and prek
hook changes. This ADR formalizes the practice itself so the process is
authoritative.

## Decision

We use [MADR](https://adr.github.io/madr/) under `docs/decisions/` to
record architecture decisions.

- ADRs are stored as Markdown, numbered `NNNN-short-title.md`.
- ADR template lives at `docs/decisions/adr-template.md`.
- ADRs are immutable. To revisit, write a new ADR that supersedes the
  old one and update the old `Status` field accordingly.
- ADRs ship in the same PR as the change they document.
- The index in `docs/decisions/README.md` is kept current; CI may add a
  lint pass for it later.

## Consequences

### Positive
- New contributors can read the *why* without archaeology.
- AI agents have a structured place to consult before making
  architectural changes (see `AGENTS.md §16`).
- The threat model and `docs/architecture.md` link to ADRs for context.

### Negative / trade-offs
- Slight friction on PRs that introduce architectural changes.
- Risk of ADR rot: ADRs become outdated if not superseded properly.
  Mitigated by requiring `Status: Superseded` updates in the same PR
  that supersedes them.

### Risks
- None notable for v1.0.

## Alternatives considered

- **No ADRs, rely on commit messages.** Rejected: commit messages are
  not indexed, not discoverable, and authors don't typically describe
  trade-offs at the depth ADRs require.
- **GitHub Discussions.** Rejected: not versioned with the code, can be
  deleted or edited, and not visible to AI agents inspecting the repo.
- **Confluence / wiki.** Rejected: lives outside the repo, hard to keep
  in sync with code changes.

## References

- [ADR GitHub Organization](https://adr.github.io/)
- [MADR](https://adr.github.io/madr/)
- `AGENTS.md §10`
- `docs/architecture.md`
