# bouine — Project Governance

This document describes how `bouine` makes decisions, who holds key roles,
and how disputes are resolved. It is the authoritative governance reference;
[`AGENTS.md`](AGENTS.md) governs *how* contributors work day-to-day, and
[`docs/architecture.md`](docs/architecture.md) governs *what* is built.

---

## 1. Model

bouine uses a **maintainer collective** governance model. A small group
of maintainers shares decision-making authority. There is no single
benevolent dictator; decisions are made by consensus among maintainers,
with the lead maintainer as tie-breaker when consensus cannot be reached.

This model was chosen because:

- The project spans multiple specialized areas (HTTP parsing, caching,
  storage, clustering, observability) where no single person has deep
  expertise in all of them.
- A collective reduces the bus factor risk and ensures continuity.
- It keeps the review burden distributed.

---

## 2. Roles

The current list of maintainers and their GitHub handles is visible on
the [`@bouine-cache/maintainers`](https://github.com/orgs/bouine-cache/teams/maintainers)
team page. The `CODEOWNERS` file maps each area of the codebase to the
team, ensuring the right reviewers are auto-requested.

| Role | Who | Responsibilities | Key tasks |
|------|-----|-------------------|-----------|
| **Lead maintainer** | Current: [`@thylong`](https://github.com/thylong) (Théotime Lévêque) | Final tie-breaker, release cadence, roadmap arbitration, security response coordination. | Arbitrate disputes that stall at maintainer level; cut releases and tag versions; coordinate vulnerability response with security contacts; approve roadmap changes. |
| **Maintainer** | Members of [`@bouine-cache/maintainers`](https://github.com/orgs/bouine-cache/teams/maintainers) | Review and merge PRs, approve architectural changes, triage issues, mentor contributors. | Review PRs against the [PR checklist](CONTRIBUTING.md#pull-request-checklist); ensure CI is green before merging; approve ADRs (two approvals required for architectural changes); triage and label incoming issues; guide new contributors; enforce the [Code of Conduct](CODE_OF_CONDUCT.md). |
| **Contributor** | Anyone with a merged PR | Propose changes, participate in discussions, fix bugs. | Open issues for bugs and feature requests; submit PRs following [CONTRIBUTING.md](CONTRIBUTING.md); sign commits with `Signed-off-by` (DCO); participate in design discussions and ADR reviews. |
| **AI agent** | Automated contributors (e.g. Crush, Claude Code) | Execute well-scoped coding tasks under human supervision. | Follow [`AGENTS.md`](AGENTS.md) strictly; cannot merge, cannot approve PRs, cannot make policy or governance decisions; all AI-generated PRs require a human maintainer for review and merge. |

### 2.1 Becoming a maintainer

A contributor may be nominated as a maintainer by an existing maintainer
after demonstrating sustained, high-quality contributions across multiple
areas. Nominations are discussed privately among maintainers and require
unanimous agreement. A new maintainer is added to the
`@bouine-cache/maintainers` GitHub team and is given write access to the
repository.

### 2.2 Removing a maintainer

A maintainer may step down at any time. A maintainer who is inactive
(no commits, reviews, or discussions) for 6 months may be moved to
emeritus status by consensus of the remaining maintainers. Emeritus
maintainers retain credit but lose write access.

---

## 3. Decision-making

### 3.1 Day-to-day changes

Standard PRs (bug fixes, documentation, non-breaking improvements) require
**at least one maintainer review and approval** before merge. The reviewer
is responsible for checking the [PR checklist](CONTRIBUTING.md#pull-request-checklist)
and ensuring CI is green.

### 3.2 Architectural changes

Changes that affect the layer model, public API, wire format, eviction
algorithm, cluster protocol, or the VCL shim require:

1. An **ADR** (Architecture Decision Record) under
   [`docs/decisions/`](docs/decisions/) describing the problem, options,
   and rationale.
2. **Two maintainer approvals** (including the lead maintainer or the
   area owner from `CODEOWNERS`).
3. A 48-hour waiting period for community feedback after the ADR is
   opened, unless the change is a security fix.

### 3.3 Roadmap changes

The roadmap lives in [`ROADMAP.md`](ROADMAP.md).
Adding, removing, or reordering a phase requires maintainer consensus.
The lead maintainer arbitrates if maintainers disagree.

### 3.4 Dispute resolution

1. **Discuss first.** Disagreements are resolved in the PR, the linked
   issue, or a GitHub Discussion. Focus on technical merit, citing RFC
   clauses, benchmarks, or design docs.
2. **Escalate to maintainers.** If the discussion stalls, any maintainer
   can request a maintainer-level review by mentioning
   `@bouine-cache/maintainers`.
3. **Lead maintainer decides.** If maintainers cannot reach consensus
   within one week, the lead maintainer makes the final decision and
   documents the rationale in the PR or ADR.

---

## 4. Access continuity

bouine has a **bus factor of 2**. The
[`@bouine-cache/maintainers`](https://github.com/orgs/bouine-cache/teams/maintainers)
team currently has two members with admin access to the repository
and the GitHub organization: [`@thylong`](https://github.com/thylong)
and [`@chridupin-33`](https://github.com/chridupin-33). Both are
familiar with the codebase architecture, the release pipeline, and the
security response process. The loss of either one leaves the other
fully capable of continuing the project without interruption.

No critical capability depends on a single person. If any one
individual becomes unavailable (death, incapacitation, or departure),
the remaining admin(s) can perform all of the following within one
week of confirmation:

- **Create and close issues** — both admins have triage and admin
  permissions on the repository.
- **Accept proposed changes (merge PRs)** — both admins can approve and
  merge pull requests. The `main` branch is protected (direct pushes
  blocked, CI must pass, at least one maintainer approval required),
  but any admin can override or adjust branch protection if needed.
- **Release versions of software** — release credentials are stored in
  GitHub Secrets (not on any individual's machine), accessible to all
  organization admins:
  - Docker Hub push credentials (for container images).
  - Cosign keyless signing via GitHub Actions OIDC (no private key to
    manage; tied to the GitHub workflow, not an individual).
  - GitHub Actions release environment.
  - Artifact Hub Helm chart publishing.
  A new release is cut by tagging a version (`git tag v1.x.y`) and
  pushing the tag, which triggers the `release.yml` workflow
  automatically.
- **Respond to security reports** — all maintainers are security
  contacts and can receive private vulnerability reports via GitHub
  Private Vulnerability Reporting.
- **Manage the GitHub organization** — both admins can add or remove
  team members, adjust repository settings, and rotate secrets.

The project has no external infrastructure dependencies (no separate
DNS, no standalone CI runner, no external package registry beyond
Docker Hub and Artifact Hub) that would require credentials outside
GitHub. This eliminates the need for a lockbox or will; all access is
through the GitHub organization, which has two admins.

---

## 5. Security governance

Security-related decisions follow the process in
[`SECURITY.md`](SECURITY.md). The lead maintainer coordinates
vulnerability response. All maintainers are security contacts and can
receive private vulnerability reports via GitHub Private Vulnerability
Reporting. See also
[`docs/security/threat-model.md`](docs/security/threat-model.md).

---

## 6. Changes to this document

Changes to `GOVERNANCE.md` require:

1. A PR with the proposed change.
2. **Two maintainer approvals** (including the lead maintainer).
3. A 7-day comment period for community feedback.

The file is listed in [`CODEOWNERS`](CODEOWNERS) and auto-requests
maintainer review on any change.
