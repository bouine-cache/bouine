# AGENTS.md — Working Agreement for AI Agents on `bouine`

This file is the operating manual for any AI agent (single or in a swarm)
contributing to `bouine`. It is binding. **Read [`docs/architecture.md`](docs/architecture.md) first**, then this
file, then start work. If anything in this file conflicts with `docs/architecture.md`,
`docs/architecture.md` wins for *what* to build; this file wins for *how* to build it.

> One-line summary: build a horizontally-scalable, observability-first HTTP/1.1-only
> reverse-proxy cache in Go 1.27 that matches Varnish on
> [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests),
> never regresses on benchmarks, and stays maintainable across many phases
> and many contributors.

---

## 0. Table of Contents

1. Mission & Success Criteria
2. Non-Negotiable Rules
3. Layered Architecture Rules
4. Coding Standards (Go 1.27)
5. Dependency Policy
6. Security Rules
7. Performance Rules
8. Testing Rules
9. Observability Rules
10. Documentation Rules
11. Concurrency & Memory Discipline
12. Error Handling & Logging
13. Configuration & Compatibility
14. Build, CI & Release
15. Multi-Agent Coordination
16. Working Loop (mandatory per task)
17. Pull Request Checklist
18. Escalation & Stop Conditions
19. Glossary

---

## 1. Mission & Success Criteria

Every change must move the project toward **all** of:

- **Correctness** — RFC 9111 score on `http-tests/cache-tests` ≥ Varnish, never
  regress.
- **Performance** — canonical benchmark within 10% of Varnish RPS on the same
  hardware; CI gates ≤ 2% p99 regression and zero added allocations on the
  hit path.
- **Operability** — runs on Kubernetes out of the box, scales horizontally,
  exposes metrics/traces/logs/pprof.
- **Maintainability** — strict layering, small interfaces, no global state,
  ≥ 85% test coverage per package (≥ 95% in `cache` and `storage`).
- **Security** — no unsanitized header injection, no path traversal, no
  unbounded memory growth, no plaintext secrets in code or logs.

If a change cannot defend itself on at least correctness + one other axis,
it does not belong.

---

## 2. Non-Negotiable Rules

These rules override autonomy. Break them and the change must be reverted.

1. **Never violate the layer boundaries** defined in `docs/architecture.md §2`. A package
   may only depend on packages in lower layers, through declared interfaces.
   A reverse import (e.g. `storage` importing `cache`) fails `depguard` in CI.
2. **One HTTP stack only: `fasthttp`** — admin and data plane, H1 only
   (HTTP/2 and HTTP/3 are not supported). See ADR-0034.
3. **Never add a global variable** for mutable state. The daemon is a single
   `Engine` struct. Configuration, clocks, randomness, and metrics are
   injected.
4. **Never allocate on the cache-hit path** after warm-up. PRs touching the
   hit path must include an `allocs/op == 0` benchmark assertion.
5. **Never weaken the cache-tests score.** A PR that regresses any test must
   either fix the regression or be rejected.
6. **Never bypass the benchmark gate.** `bench/` results are required for
   any change that touches `internal/{server,cache,storage,origin,cluster}`.
7. **Never commit secrets, tokens, customer data, or production hostnames.**
   Use `testdata/` fixtures with synthetic values.
8. **Never push to remote** unless the user explicitly says so. Don't open
   PRs unprompted.
9. **Never silently approximate RFC 9111.** If the spec is ambiguous, cite
   the clause in a code comment and add a test that pins the chosen
   behavior. Same rule applies to the VCL shim.
10. **Never expand scope.** If `docs/architecture.md` doesn't list a feature for the
    current phase, you may not add it. Open a discussion issue instead.
11. **Never commit with `prek` disabled or skipped.** Hooks must run
    and pass locally before any commit. `git commit --no-verify`, `SKIP=`,
    `PREK_ALLOW_NO_CONFIG=1`, and equivalent escape hatches are
    forbidden. See §14.4.

---

## 3. Layered Architecture Rules

Layers are numbered L1 (closest to the wire) through L8 (AI / dashboard).
See `docs/architecture.md §2`.

### 3.1 Allowed dependencies

```
L8 → L7, L6, /pkg/api
L7 → /pkg/api, third-party telemetry libs only
L6 → L7, L5, L3, L2 (read-only), /pkg/api
L5 → L7, L3, L2, /pkg/api
L4 → L7, /pkg/api
L3 → L7, L2, L4, /pkg/api
L2 → L7, /pkg/api
L1 → L7, /pkg/api
```

- `pkg/api` and `pkg/bouineapi` are leaves; they import nothing from
  `internal/`.
- `internal/vcl` lowers to the same config tree consumed by `internal/config`;
  it must not call any other layer at runtime.
- Cross-layer calls go through **interfaces declared in the consumer
  package**, never in the implementor. Example: `cache` declares
  `cache.Store`; `storage` implements it. This keeps consumers free of
  implementation-specific imports.

### 3.2 Enforcement

- A `lint/depguard.yaml` config encodes the matrix above and fails CI on
  violation.
- Every layer has a `doc.go` describing inputs, outputs, invariants.
- Public types in `internal/*` are marked `// Stable.` or `// Unstable.`; the
  former cannot change shape without a migration note.

---

## 4. Coding Standards (Go 1.27)

- **Toolchain pinned** in `go.mod` (`go 1.27.X`). Never bump unilaterally.
- **Formatting**: `gofmt -s` (enforced by prek; see `.pre-commit-config.yaml`).
  Lines wrap at 100 columns except generated code.
- **templ**: dashboard HTML lives in `internal/dashboard/templates/*.templ`.
  After editing any `.templ` file run `go generate ./internal/dashboard/templates/`
  (or `templ generate`). Commit both the `.templ` source and the generated
  `_templ.go` file. Never edit `_templ.go` by hand — it is overwritten on
  the next `go generate`. The `templ` binary (v0.3.x) must be on `PATH`;
  install once with `go install github.com/a-h/templ/cmd/templ@latest` or
  download the release binary from GitHub.
- **Linters**: `golangci-lint`. The enforced linter set and thresholds
  live in `.golangci.yaml` (single source of truth — do not restate the
  list here; restated lists drift). Key floors: `gosec` zero findings,
  complexity ≤ 15, functions ≤ 80 lines.
- **Never run `golangci-lint run --fix` directly**: the `fieldalignment`
  auto-fix strips ALL comments from struct fields. Use `make lint-fix`
  (disables `fieldalignment` for the fix pass); reorder fields manually.
- **Naming**: idiomatic Go — short receivers (1–2 chars), no Hungarian
  notation, exported identifiers documented with a sentence starting with
  the identifier name.
- **No init functions** that do work. `init` is reserved for registering
  encoders/decoders with stdlib registries; no I/O, no goroutines.
- **No `panic` outside main**. Use typed errors. Recover only at HTTP
  handler boundaries.
- **No reflection on the hot path.** Pre-compute keyed accessors.
- **Generics**: allowed when they shrink code without hurting readability.
  Forbidden in the hit path if they cause shape-driven allocation.
- **Comments**: explain *why*. The code says *what*. Cite RFC clauses by
  number when behavior is spec-driven.

---

## 5. Dependency Policy

- **Stdlib first.** A dependency must justify itself in the PR description.
- Allow-list maintained in `docs/deps.md`. New entries require a reviewer
  with the `deps` label.
- Vendor required? No — `go mod` only, but `go.sum` is sacred. CI runs
  `go mod tidy && git diff --exit-code`.
- Banned by default: ORMs, runtime DI containers, log libraries that aren't
  `slog`-compatible, HTTP servers other than `fasthttp`
  (ADR-0034), any LGPL / AGPL code.
- Pre-approved core:
  `hashicorp/memberlist`, `cespare/xxhash/v2`, `klauspost/compress`,
  `prometheus/client_golang`, `go.opentelemetry.io/otel`, `spf13/cobra`,
  `stretchr/testify` (tests only), `golang.org/x/{sync,sys}`,
  `github.com/valyala/fasthttp` (ADR-0034).
- `govulncheck` runs in CI and gates merge.

---

## 6. Security Rules

- **Headers**: strip hop-by-hop headers per RFC 9110 §7.6.1. Never copy
  `Connection`, `Keep-Alive`, `TE`, `Trailer`, `Transfer-Encoding`,
  `Upgrade`, `Proxy-*` blindly.
- **Smuggling defenses**: reject ambiguous framing (`Content-Length` +
  `Transfer-Encoding`, duplicate `Content-Length`, obs-fold) with `400`
  and a metric increment. Tested via fuzz corpus.
- **Auth on admin**: bearer token (constant-time compare) or mTLS. Default
  is to refuse insecure transports for write methods. Token lives in an
  env var or a file path; never on the CLI.
- **TLS**: minimum TLS 1.2, prefer 1.3. Cipher suite list pinned. SNI
  required.
- **Input limits**: every parser has a byte cap. Headers ≤ 64 KiB total,
  per-header ≤ 8 KiB, max 100 headers per message. URLs ≤ 8 KiB. Body
  caps configurable but always enforced.
- **Path traversal**: storage paths derived from cache keys are hashed
  (xxhash64 → hex), never built from user-controlled strings.
- **Resource exhaustion**: connection limits, in-flight request caps,
  storage admission control, request-collapsing latch caps. Every queue
  has a bounded size.
- **Logging hygiene**: never log `Authorization`, `Cookie`, `Set-Cookie`,
  custom auth headers, or request/response bodies by default. Operators
  may opt in per route.
- **Crypto**: only stdlib `crypto/*`. No hand-rolled primitives.
- **`gosec`** must report zero findings; suppressions require a comment
  citing the threat model.

---

## 7. Performance Rules

- **Hit path budget**: < 5 µs CPU per request at p50, allocs/op = 0,
  bytes/op = 0 after warm-up.
- **Hot loops** must not contain: `fmt.Sprintf`, `errors.New`, map growth,
  interface boxing of concrete value types.
- **Buffers**: pool 4 KiB and 64 KiB sizes via `sync.Pool`. Always reset on
  put.
- **Hashing**: xxhash64 for keys. Never use `crypto/sha*` on the hit path.
- **Goroutines**: the h1 reactor (ADR-0041) owns the connection/
  goroutine model — one event-loop goroutine multiplexes many
  connections; never reintroduce goroutine-per-connection. Background
  work uses bounded worker pools sized at boot.
- **Atomics over mutexes** where contention is observed. Document why with
  a benchmark in the PR.
- **Profiling**: every perf-sensitive PR includes `go test -bench`,
  `go test -cpuprofile`, `go test -memprofile` outputs in the description.
- **Benchmark suite** (`/bench`) is authoritative. Acceptance gates:
  - canonical RPS:  ≥ previous main − 2%.
  - canonical p99:  ≤ previous main + 2%.
  - allocs/op on hit path: = previous main exactly.
  - memory RSS at steady state: ≤ previous main + 5%.
- **Streaming**: bodies larger than 64 KiB stream end-to-end; never
  buffered in RAM.

---

## 8. Testing Rules

- **Test policy (formal, mandatory):** When major new functionality is
  added to the software, tests for that functionality MUST be added to
  an automated test suite in the same pull request. A PR that adds
  major functionality without tests will be rejected by reviewers and
  will fail the CI gate (`prek run --all-files` runs
  `go test -race -short`). This policy applies to all new features,
  behavior changes, and bug fixes. "Major new functionality" is defined
  as any change that adds a new user-visible capability, a new
  configuration option, a new API endpoint, or a new code path in the
  cache, storage, cluster, or server layers.
- **Unit tests** ship in the same package, `_test.go`. Table-driven.
  `-race` always on in CI.
- **Coverage targets** per package: ≥ 85% default, ≥ 95% for
  `internal/cache` and `internal/storage`. CI reports coverage (Codecov)
  but does not yet fail on regression; targets are enforced by review
  until a per-package gate is wired into CI.
- **Fuzz tests** for: header parsing, `Cache-Control` tokenizer, `Vary`
  canonicalization, URL normalization. At least one corpus
  per fuzzer committed under `testdata/fuzz/`. Fuzzing runs nightly,
  not per-PR.
- **Conformance**: `test/cachetests` runs the upstream
  `http-tests/cache-tests` harness against a real `bouine` instance. CI
  publishes the score as a JSON badge and blocks regressions.
- **Integration**: `test/integration` boots a 3-node bouine cluster + an
  origin in-process. Scenarios listed in `docs/architecture.md §12`.
- **Chaos**: kill a peer, drop packets, slow the disk. Lives under
  `test/chaos`. TODO: settle the run cadence — CI currently runs it on
  every PR (`ci.yml`), the original intent was nightly-only.
- **Benchmarks**: `bench/` runs on a pinned self-hosted runner with CPU
  affinity. `benchstat` compares HEAD vs `main`, N ≥ 10. Benchmark
  naming convention:
  - `BenchmarkGate_*` — hot-path, alloc-budgeted, time-driven. Selected
    and enforced by `make bench-gate`. Every `BenchmarkGate_*` must have
    an entry in the `BUDGETS` map in `bench/run.sh`; the gate fails if a
    benchmark runs without a budget (drift) or a budget exists without a
    benchmark (stale).
  - `BenchmarkSingle_*` — single-shot, not time-driven. Must skip itself
    under time-driven benchtime (skip on second `b.Loop()` iteration).
    Run manually with `-benchtime=1x -count=10`. Never appears in
    `make bench-gate`; self-skips in `make bench-all`.
  - `Benchmark*` (no prefix) — regular time-driven benchmarks. Run in
    `make bench-all` only.
- **No flaky tests.** A test that flakes twice in a week is quarantined
  (`-skip` with a tracking issue) within one business day.
- **Determinism**: no `time.Now()` and no sleeps in tests; use the
  injected clock or synchronization primitives. No random ports; use
  `:0` and read back.
- **Assertions**: use `testify` (`require` + `assert` only). See
  ADR-0028 for the require-vs-assert convention and the `time.Time`
  comparison exception. `suite`/`mock`/`httpmock` are not approved.

---

## 9. Observability Rules

Observability is a product feature, not an afterthought.

- **Metrics** — Prometheus, namespaced `bouine_*`. Every layer exports RED
  (rate, errors, duration) and the relevant USE (utilization, saturation,
  errors). Labels: `route`, `method`, `cache_result`, `upstream_pool`.
  Cardinality budget: ≤ 10 k unique label combinations per metric at
  steady state; reviewers reject high-cardinality labels (raw URL, user
  agent, IP).
- **Traces** — OpenTelemetry, one span per layer. Span names are stable
  strings (no interpolation). Attributes use OTEL semantic conventions
  where they exist.
- **Logs** — `slog` JSON. Levels: `error` (action required), `warn`
  (degraded), `info` (lifecycle), `debug` (developer). No `printf` debug
  in committed code.
- **Access log** — sampled by default (1:100), always-on for errors. Fields:
  ts, request-id, route, method, status, cache_result, upstream_pool,
  bytes_in, bytes_out, dur_ns, peer_hops.
- **Self-check endpoints**: `/healthz`, `/readyz`, `/v1/debug/cachecheck`,
  `/debug/pprof/*`. All bound to the admin port; never to the data port.
- **Cardinality tests** — a unit test inspects registered metrics and
  enforces the budget.

---

## 10. Documentation Rules

- Every exported identifier has a godoc.
- Every layer has a `doc.go` with: purpose, public surface, invariants,
  performance notes.
- `docs/runbook/*.md` is updated whenever a new failure mode or operator
  action is introduced.
- `docs/decisions/` holds ADRs in MADR format. Required for: dependency
  additions, protocol/wire-format changes, eviction-algorithm changes,
  cluster protocol changes, anything touching the VCL shim's supported
  surface.
- README stays a quickstart. Deep content lives under `docs/`.
- Diagrams use Mermaid in Markdown; no binary images committed.
- TODO (doc-lint): add a CI check that fails when any file path, `make`
  target, ADR number, or `§` reference cited in this file no longer
  resolves. Until it exists, references here are verified by review only.

---

## 11. Concurrency & Memory Discipline

- **Context plumbing**: every public function that does I/O or can block
  takes `context.Context` as the first argument. Cancellation is honored
  within 10 ms.
- **No bare goroutines.** Use `errgroup.Group`, a worker pool, or
  `internal/runtime/supervised` (introduced in phase 1). Every goroutine
  has an owner that joins it during shutdown.
- **Shutdown**: every component implements
  `Close(ctx context.Context) error`. Closes drain in-flight work within
  the context deadline.
- **Channels**: bounded buffers, documented direction, owner closes,
  consumer never closes.
- **Locks**: prefer `sync.RWMutex` only when read-heavy is *proven* by
  bench; otherwise `sync.Mutex`. Always defer-unlock in the same function
  it was taken. Never hold a lock across an I/O call.
- **Atomics**: use `sync/atomic` typed variants (`atomic.Int64`, etc.). No
  raw `uintptr` tricks.
- **Race detector**: CI runs `go test -race` on Linux amd64 + arm64.

---

## 12. Error Handling & Logging

- Errors wrap with `%w`. Sentinel errors live in the package that
  produces them (`var ErrNotFound = errors.New(...)`).
- Public errors are typed (`*cache.MissError`), private may be sentinels.
- Never log + return — pick one. The boundary handler logs; lower layers
  return.
- HTTP error mapping happens at one place per surface (data plane in
  `internal/server/router.go`, admin in `internal/admin/server.go`).
- Stack traces only on `error` level. PII never appears in errors.

---

## 13. Configuration & Compatibility

- The config schema lives in `internal/config/config.go` (struct tags) and
  is validated by `config.Validate` in `internal/config/loader.go`. There is
  no JSON-Schema emitter; the Go structs are the contract.
- Config changes that break compatibility require a major version bump and
  a migration guide.
- VCL shim is deferred (`internal/vcl` does not exist yet); when it lands
  it will support the subset defined in `docs/architecture.md` §16 item 4.
  Unsupported constructs are reported, never silently ignored.
- The Go SDK (`pkg/bouineapi`) follows semver. Wire types in `pkg/api` are
  additive — add fields, never remove or rename in the same major.
- Feature flags for experimental work live under `experimental:` in
  config and default to off.

---

## 14. Build, CI & Release

### 14.1 Commands

Run `make help` for the full, always-current list. Targets with contract
semantics:

```
make bench-gate      # gating benchmarks; alloc budgets enforced (BUDGETS in bench/run.sh)
make ci              # lint + test-short + build + hooks-run — the local CI gate
make hooks           # install prek hooks (required before committing; see §14.4)
```

> There is no config JSON-schema generator or in-repo docs-site build:
> config is validated in Go (`config.Validate`), and the documentation
> site lives in the separate `bouine-documentation` repo. Do not
> reference `make schema` or `make docs`. (`make schema-sync` /
> `schema-check` are unrelated: they sync the embedded Kubernetes Helm
> chart schema.)

### 14.2 CI pipeline (stages)

1. `lint` + `govulncheck`. (`go vet` is a subset of `golangci-lint`'s
   `govet` linter, already enabled in `.golangci.yaml`.)
2. `unit` (with `-race`, per-OS matrix linux/amd64, linux/arm64,
   darwin/arm64).
3. `coverage` report (Codecov; targets enforced by review, see §8).
4. `conformance` (`cache-tests`), publish score.
5. `bench-gate` on self-hosted runner, `benchstat` diff vs the committed
   baseline (`bench/run.sh`).
6. `integration` (3-node cluster) + `chaos`.
7. `release` (tagged refs only): SBOM (`syft`), provenance, signed
   container image (`cosign`).

Fuzzing is NOT a per-PR stage: it runs nightly
(`.github/workflows/nightly.yml`).

### 14.3 Branching & releases

- Trunk-based development on `main`. Short-lived branches.
- Conventional Commits. PR titles must match.
- Tags follow `vMAJOR.MINOR.PATCH`. Release notes generated from commits.
- Container images: distroless, non-root, multi-arch (amd64+arm64).

### 14.4 prek hooks (mandatory)

`bouine` uses [`prek`](https://github.com/j178/prek) (a fast, drop-in
replacement for pre-commit) to enforce the minimum quality bar on every
commit. The hooks are the local mirror of the CI gates; they exist so
problems are caught in seconds, not minutes.

**Setup is required**, not optional:

```
brew install prek                 # or: pip install prek, uv tool install prek
prek install                       # installs the git hook
prek install --hook-type commit-msg   # Conventional Commits check
make hooks                         # one-shot equivalent of the two above
```

The `.pre-commit-config.yaml` at the repo root is the single source of
truth for what runs and how; prek reads it natively. It MUST keep
registering the following capabilities — mechanisms and tool versions
live in the config file, not here (restating them is how this section
drifted):

- **Config validation** — YAML/JSON/TOML checks.
- **File hygiene** — EOF fixer, trailing whitespace, line endings,
  merge-conflict markers, large-file cap.
- **Go formatting** — `gofmt -s` on every commit.
- **Go tests** — `go test -race -count=1 -short ./...` on commit; the
  full `go test -race ./...` on the `pre-push` stage. Long benchmarks,
  fuzz, integration, and conformance suites stay in CI / `make` targets.
- **Lint** — golangci-lint on changed lines only; linter set and
  thresholds live in `.golangci.yaml`.
- **Go module hygiene** — `go mod tidy` must produce no diff.
- **govulncheck** — `pre-push` stage only (too slow for every commit,
  but must pass before code leaves the laptop).
- **Secret scan** — refuses commits containing credentials, tokens, or
  production hostnames.
- **Conventional Commits** — commit-msg validation against the
  `feat|fix|chore|docs|refactor|test|perf|build|ci` prefix list
  (release notes are generated from commits).
- **DCO sign-off** — a `Signed-off-by:` trailer on every commit; see
  `CONTRIBUTING.md § "Developer Certificate of Origin (DCO)"`. Use
  `git commit -s`. CI also runs a separate `dco` job that checks all
  commits in a PR.

**Operational rules:**

- The hook config is versioned. Bumping hook versions follows the same
  PR review process as code; pin every revision (`rev: v1.2.3`).
- CI re-runs `prek run --all-files` as its first lint stage so a
  bypassed local hook still fails the build.
- Hooks must remain fast: the `pre-commit` stage budget is **30 s** on a
  laptop. Anything slower moves to `pre-push` or CI. (prek's built-in
  Rust hooks and parallel execution help stay within this budget.)
- New hooks require an ADR in `docs/decisions/` justifying the cost and
  scope.
- Bypassing hooks (`--no-verify`, `SKIP=`) is forbidden (see §2.11). If a
  hook is genuinely broken, fix it or remove it via PR — don't route
  around it.

---

## 15. Multi-Agent Coordination

When more than one agent is working concurrently:

1. **Claim before you code.** Open or update an entry in
   `docs/agents/work-queue.md` with: agent ID, target package(s),
   intended phase task, ETA. One agent per package at a time.
2. **Stay in your lane.** Cross-package changes require posting a
   coordination note in `docs/agents/coordination.md` and waiting for
   acknowledgement (or 24 h, whichever comes first).
3. **Interfaces first.** When a feature touches multiple packages, one
   agent lands the interface and a stub; others depend on the interface,
   not the in-progress implementation.
4. **Small PRs.** Target ≤ 400 changed lines (excluding generated code
   and testdata). Big features land behind a feature flag.
5. **Never rebase someone else's branch.** Coordinate handoffs via
   merges + notes.
6. **Conflict resolution**: the agent that landed the interface owns
   conflicts on that interface. The data-plane on-call rule applies:
   regressions to L1–L3 take priority over any other work.
7. **Single source of truth**: `docs/architecture.md` for *what*, `AGENTS.md` for
   *how*, `docs/decisions/` for *why*. If they disagree, escalate to the
   user — don't pick.
8. **Hand-off protocol**: when stopping mid-task, leave a `HANDOFF.md`
   at the repo root with: state, next concrete steps, known risks,
   commands run, commands not yet run. Delete it when the task is done.

---

## 16. Working Loop (mandatory per task)

For every task an agent starts, execute this loop. No shortcuts.

1. **Orient**
   - Re-read `docs/architecture.md` sections relevant to the task.
   - Re-read this file's sections relevant to the task.
   - `git status` + `git log -n 20` for recent context.
   - Search for existing implementations (`grep`, `rg`, LSP).
2. **Plan**
   - Write a TODO list using the `todos` tool if the task has > 2 steps.
   - Identify the layers touched and verify dependency direction.
   - Identify the tests that will prove correctness *before* writing code.
3. **Implement**
   - Smallest reasonable change.
   - Follow patterns from neighboring code.
   - Add tests as you go, not at the end.
4. **Verify** — required gates by change type:

   | Change touches | Required gates |
   |---|---|
   | Anything | `make lint`, `make test` |
   | `internal/{server,cache,storage,origin,cluster}` | + `make bench-gate`, compare to baseline |
   | `internal/cache` logic | + `make conformance` |
   | Clustering, origin, or server behavior | + `make integration` locally |
   | Config schema | `config.Validate` updated, new field documented |
   | Docs only | none |

   `make integration` and `make chaos` run in CI on every PR. `make
   bench-all` is for deep analysis, not a gate. A task with any failing
   gate is not done — fix it or explain why the failure is unrelated
   before reporting completion.
5. **Document**
   - Update godoc.
   - Update `docs/runbook` and `docs/decisions` when warranted.
   - Update `docs/architecture.md` only if scope/exit-criteria actually changed and the
     user approved the change.
6. **Report**
   - Concise summary (≤ 4 lines unless complex).
   - Include `file:line` references.
   - List anything skipped or deferred.

---

## 17. Pull Request Checklist

Before declaring a change ready:

- [ ] `prek run --all-files` passes locally.
- [ ] Layer dependencies respected (`depguard` clean).
- [ ] `make ci` green locally.
- [ ] Tests added/updated; coverage not reduced.
- [ ] If hot path: zero-alloc benchmark proves it.
- [ ] If cache logic: `cache-tests` score not regressed.
- [ ] If dashboard templates changed: `go generate ./internal/dashboard/templates/`
  committed alongside `_templ.go` files.
- [ ] If config: `config.Validate` updated and the new field documented.
- [ ] If public API: SDK types updated, semver impact noted.
- [ ] Godoc + changelog entry.
- [ ] ADR added if architectural.
- [ ] No secrets, no PII, no production hostnames.
- [ ] `HANDOFF.md` removed if you created one.

---

## 18. Escalation & Stop Conditions

Stop and ask the user, with a concrete proposal, when:

- A required dependency would be added that's not on the allow-list.
- A change would alter the layer model or break a public type.
- A test failure on `main` blocks all forward progress.
- A security finding has no clear remediation that fits within the phase.
- A benchmark regression looks unavoidable for a feature on the roadmap.
- A spec ambiguity in RFC 9111 has two defensible interpretations.

**Do not** stop for:

- "This task is large." — break it down.
- "I'm not sure which style to use." — read neighboring code.
- "Tests are slow." — run the subset, iterate, run the full suite at end.

When you stop, write what you tried, what failed, what you propose. Never
write "I need more info" without listing the items and acceptable
substitutes.

---

## 19. Glossary

- **Hit path** — code executed when a request finds a usable cached
  response and returns without origin fetch. The fastest path in the
  system; performance gates are strictest here.
- **Miss path** — request not in cache; fetches from peer or origin.
- **SWR / SIE** — `stale-while-revalidate` / `stale-if-error` (RFC 5861).
- **Surrogate key** — opaque label attached to a response for grouped
  invalidation.
- **Ban** — predicate-based invalidation, evaluated lazily on lookup.
- **Purge** — exact-key invalidation, immediate.
- **Refresh** — soft purge, triggers revalidation on next access.
- **Peer fetch** — owner-first cluster lookup before falling back to origin.
- **Hop limit** — max number of peers a single request may traverse before
  going to origin.
- **Hit-path budget** — performance envelope for the hit path
  (see §7).
- **VCL shim** — deferred subset translator from Varnish Configuration
  Language to the bouine config tree (see `docs/architecture.md` §16
  item 4).

---

*Last updated alongside `docs/architecture.md`. When you change one, check the other.*
