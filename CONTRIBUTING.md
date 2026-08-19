# Contributing to bouine

Thanks for considering a contribution. `bouine` is built to be a
production-grade HTTP cache, so the contribution bar is intentionally
high. This document describes the rules for humans; AI agents follow
[`AGENTS.md`](AGENTS.md), which is the authoritative how-to for all
contributors.

> If a rule here conflicts with `AGENTS.md`, `AGENTS.md` wins for
> *how*, and [`docs/architecture.md`](docs/architecture.md) wins for *what*.

---

## Quick links

- Roadmap: [`ROADMAP.md`](ROADMAP.md)
- Architecture: [`docs/architecture.md`](docs/architecture.md)
- Governance: [`GOVERNANCE.md`](GOVERNANCE.md)
- Working agreement (binding for all contributors): [`AGENTS.md`](AGENTS.md)
- Security policy and reporting: [`SECURITY.md`](SECURITY.md)
- Code of Conduct: [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- Threat model: [`docs/security/threat-model.md`](docs/security/threat-model.md)
- Decision records: [`docs/decisions/`](docs/decisions/)

---

## Before you start

1. Read [`ROADMAP.md`](ROADMAP.md) so you know where the project is
   heading. We do not accept features that are not on the roadmap.
   Open a discussion issue first if you want to argue for inclusion.
2. Read [`AGENTS.md`](AGENTS.md) end to end. The non-negotiable rules
   in §2 apply to every contributor, AI or human.
3. Search existing issues and PRs to avoid duplicate work.

---

## Setting up

```bash
git clone https://github.com/bouine-cache/bouine.git
cd bouine
make setup-dev    # installs golangci-lint, govulncheck, gitleaks, templ, hooks, and verifies the build
make build        # builds ./bin/bouine
make test         # go test -race ./...
```

You need:

- Go 1.26.x (toolchain pinned in `go.mod`).
- `prek` (`brew install prek`, `pip install prek`, or `uv tool install prek`).
- Docker (for integration and conformance tests).

`make setup-dev` installs all remaining development tools
(`golangci-lint`, `govulncheck`, `gitleaks`, `templ`), downloads Go
module dependencies, installs prek hooks, and verifies the build and
short tests pass. `make hooks` is **mandatory**. Bypassing prek
(`--no-verify`, `SKIP=`) is forbidden by [`AGENTS.md §2.11`](AGENTS.md).
CI re-runs `prek run --all-files` as its first stage; bypassed local
hooks still fail the build.

---

## The working loop

For any non-trivial change:

1. **Orient** — re-read the relevant sections of `ROADMAP.md` and
   `AGENTS.md`. Identify which architectural layer(s) you are touching
   (see [`AGENTS.md §3`](AGENTS.md)).
2. **Plan** — open or comment on an issue describing the approach.
   List the tests that will prove correctness.
3. **Implement** — smallest reasonable change, follow neighboring
   patterns, add tests as you go.
4. **Verify** — `make ci` minimum. If you touched L1–L6, run
   `make bench-all` and include the `benchstat` diff in the PR. If you
   touched cache logic, run `make conformance`. If you touched
   cluster code, run `make integration`.
5. **Document** — update godoc, runbooks, and ADRs as needed.
6. **PR** — small, focused, with the checklist below filled in.

---

## Pull request checklist

Mirrors [`AGENTS.md §17`](AGENTS.md). All boxes must be checked before
review.

- [ ] `prek run --all-files` passes locally.
- [ ] Layer dependencies respected (`depguard` clean).
- [ ] `make ci` is green locally.
- [ ] Tests added or updated; coverage not reduced.
- [ ] If you touched the hot path: zero-alloc benchmark proves it.
- [ ] If you touched cache logic: `cache-tests` score not regressed.
- [ ] If you touched config: `config.Validate` updated and the new field
      documented in the config reference.
- [ ] If you touched a public API: SDK types updated, semver impact
      noted in the PR description.
- [ ] If you touched a threat row: `docs/security/threat-model.md`
      updated in the same PR.
- [ ] Godoc updated.
- [ ] ADR added under `docs/decisions/` if the change is architectural.
- [ ] No secrets, no PII, no production hostnames committed.
- [ ] `HANDOFF.md` removed if you created one.

---

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/).
Allowed prefixes:

`feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf`, `build`, `ci`.

Examples:

```
git commit -s -m "feat(cache): implement stale-while-revalidate state machine"
git commit -s -m "fix(listener): reject obs-fold in HTTP/1.1 headers"
git commit -s -m "perf(storage): eliminate alloc on hot-tier lookup"
git commit -s -m "docs(threat-model): add T36 peer-fetch loop mitigation"
```

The `-s` flag adds the `Signed-off-by:` trailer required by the DCO
(see [below](#developer-certificate-of-origin-dco)).

The `commit-msg` prek hooks validate the header prefix and enforce
the DCO sign-off. Release notes are generated from these prefixes.

---

## Coding standards (summary)

The authoritative list is in [`AGENTS.md §4`](AGENTS.md). Highlights:

- `gofmt -s` + `goimports`, 100-column soft limit.
- `golangci-lint` must pass with the mandatory linter set
  (see [`AGENTS.md §14.4`](AGENTS.md)).
- No `panic` outside `main`. No `init()` doing work.
- No global mutable state. Inject clock, randomness, metrics, config.
- `context.Context` is the first argument of every I/O function.
- No bare goroutines — use `errgroup` or the supervised pool.
- Add comments to explain *why*, not *what*. Cite RFC clauses when
  spec-driven.

---

## Performance discipline

Performance is a feature. See [`AGENTS.md §7`](AGENTS.md). Highlights:

- Hit path: `< 5 µs` CPU at p50, `allocs/op = 0` after warm-up.
- Hot loops never `fmt.Sprintf`, `errors.New`, or grow maps.
- PRs touching L1–L6 must include a `benchstat` diff vs `main`.
- CI gates: `≤ 2%` p99 regression, `≤ 5%` memory regression, zero new
  hot-path allocations.

---

## Testing discipline

See [`AGENTS.md §8`](AGENTS.md). Highlights:

- Unit tests live alongside code, table-driven, `-race` on.
- Coverage gate: `≥ 85%` per package, `≥ 95%` for `internal/cache` and
  `internal/storage`.
- Use the injected clock — never `time.Now()` in tests.
- No sleeps. Use deterministic synchronization.
- Flaky tests are quarantined within one business day.

---

## Security

If you find a vulnerability, **do not open a public issue**.
Follow [`SECURITY.md`](SECURITY.md).

If your change touches a threat row in
[`docs/security/threat-model.md`](docs/security/threat-model.md), update
the document in the same PR. CI fails otherwise.

---

## Documentation

- Every exported identifier has a godoc.
- Every layer has a `doc.go`.
- Operator-facing changes update `docs/runbook/`.
- Architectural decisions get an ADR under `docs/decisions/`.
- Diagrams use Mermaid in Markdown; no binary images.

---

## New contributors

Welcome! If you're looking for a place to start:

1. Browse issues labeled [`good first issue`](https://github.com/bouine-cache/bouine/labels/good%20first%20issue)
   — these are self-contained tasks that don't require deep cache or cluster
   knowledge.
2. Read the [architecture reference](docs/architecture.md) for a high-level
   overview of the layer model.
3. Check the [codebase guide](https://bouine.org/docs/contributing/codebase/)
   for a map of packages and their responsibilities.

Good first issues are tagged by maintainers. If you find an untagged issue
that looks approachable, ask in the issue or open a GitHub Discussion.

---

## Reporting issues

Use the issue templates under `.github/ISSUE_TEMPLATE/`. Include:

- bouine version (`bouine version`).
- Go version and OS.
- Minimal config to reproduce.
- Expected and actual behavior.
- Logs at `debug` level if relevant.
- If clustering is involved, the output of `bouine cluster peers` from
  each node.

---

## License

By contributing, you agree that your contributions are licensed under
the [Apache License 2.0](LICENSE). This is an **inbound = outbound**
model: you license your contribution under the same terms the project
distributes.

Source files in this repository do **not** carry per-file license headers.
The project-level LICENSE file is sufficient; contributors are not expected
to add headers to their files. If a third-party file is imported, its own
license applies.

---

## Developer Certificate of Origin (DCO)

Every commit **must** include a `Signed-off-by:` trailer to certify that
the contributor has the right to submit the work under the project's
license. This is the [Developer Certificate of Origin](https://developercertificate.org/),
a lightweight alternative to a CLA.

### How to sign off

The easiest way is to pass `-s` (or `--signoff`) to `git commit`:

```bash
git commit -s -m "feat(cache): implement stale-while-revalidate state machine"
```

This automatically adds a trailer like:

```
Signed-off-by: Your Name <you@example.com>
```

If you forgot to sign off, amend the commit:

```bash
git commit --amend -s --no-edit
```

### What you are certifying

By adding `Signed-off-by`, you certify the following:

> Developer Certificate of Origin
> Version 1.1
>
> Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
>
> Everyone is permitted to copy and distribute verbatim copies of this
> license document, but changing it is not allowed.
>
> Developer's Certificate of Origin 1.1
>
> By making a contribution to this project, I certify that:
>
> (a) The contribution was created in whole or in part by me and I
>     have the right to submit it under the open source license
>     indicated in the file; or
>
> (b) The contribution is based upon previous work that, to the best
>     of my knowledge, is covered under an appropriate open source
>     license and I have the right under that license to submit that
>     work with modifications, whether created in whole or in part
>     by me, under the same open source license (unless I am
>     permitted to submit under a different license), as indicated
>     in the file; or
>
> (c) The contribution was provided directly to me by some other
>     person who certified (a), (b) or (c) and I have not modified
>     it.
>
> (d) I understand and agree that this project and the contribution
>     are public and that a record of the contribution (including all
>     personal information I submit with it, including my sign-off) is
>     maintained indefinitely and may be redistributed consistent with
>     this project or the open source license(s) involved.

The full text is available at <https://developercertificate.org/>.

### Enforcement

A `commit-msg` prek hook (`scripts/check-dco.sh`) rejects commits
without a valid `Signed-off-by:` trailer. CI re-runs this check on
every push, so bypassing the local hook will still fail the build.
