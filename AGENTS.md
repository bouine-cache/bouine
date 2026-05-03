# AGENTS.md — Working Agreement for AI Agents on `bouine`

This file is the operating manual for any AI agent (single or in a swarm)
contributing to `bouine`. It is binding. **Read `PLAN.md` first**, then this
file, then start work. If anything in this file conflicts with `PLAN.md`,
`PLAN.md` wins for *what* to build; this file wins for *how* to build it.

> One-line summary: build a horizontally-scalable, observability-first HTTP/1.1+2+3
> reverse-proxy cache in Go 1.26 that matches Varnish on
> [`http-tests/cache-tests`](https://github.com/http-tests/cache-tests),
> never regresses on benchmarks, and stays maintainable across many phases
> and many contributors.

---

## 0. Table of Contents

1. Mission & Success Criteria
2. Non-Negotiable Rules
3. Layered Architecture Rules
4. Coding Standards (Go 1.26)
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
18. Anti-Patterns & Common Mistakes
19. Escalation & Stop Conditions
20. Glossary

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

1. **Never violate the layer boundaries** defined in `PLAN.md §2`. A package
   may only depend on packages in lower layers, through declared interfaces.
   A reverse import (e.g. `storage` importing `cache`) is a build error.
2. **One HTTP stack only: `net/http`.** The admin
   surface uses `net/http.ServeMux` — never a third-party HTTP
   framework. The data plane uses `net/http` (H1+H2). See ADR-0006.
3. **Never add a global variable** for mutable state. The daemon is a single
   `Engine` struct. Configuration, clocks, randomness, and metrics are
   injected.
4. **Never allocate on the cache-hit path** after warm-up. PRs touching the
   hit path must include an `allocs/op == 0` benchmark assertion.
5. **Never weaken the cache-tests score.** A PR that regresses any test must
   either fix the regression or be rejected.
6. **Never bypass the benchmark gate.** `bench/` results are required for
   any change that touches `internal/{listener,pipeline,cache,storage,origin,cluster}`.
7. **Never commit secrets, tokens, customer data, or production hostnames.**
   Use `testdata/` fixtures with synthetic values.
8. **Never push to remote** unless the user explicitly says so. Don't open
   PRs unprompted.
9. **Never silently approximate RFC 9111.** If the spec is ambiguous, cite
   the clause in a code comment and add a test that pins the chosen
   behavior. Same rule applies to the VCL shim.
10. **Never expand scope.** If `PLAN.md` doesn't list a feature for the
    current phase, you may not add it. Open a discussion issue instead.
11. **Never commit with `pre-commit` disabled or skipped.** Hooks must run
    and pass locally before any commit. `git commit --no-verify`, `SKIP=`,
    `PRE_COMMIT_ALLOW_NO_CONFIG=1`, and equivalent escape hatches are
    forbidden. See §14.4.

---

## 3. Layered Architecture Rules

Layers are numbered L1 (closest to the wire) through L9 (AI / dashboard).
See `PLAN.md §2`.

### 3.1 Allowed dependencies

```
L9 → L8, L7, /pkg/api
L8 → /pkg/api, third-party telemetry libs only
L7 → L8, L6, L4, L3 (read-only), /pkg/api
L6 → L8, L4, L3, /pkg/api
L5 → L8, /pkg/api
L4 → L8, L3, L5, /pkg/api
L3 → L8, /pkg/api
L2 → L8, L4, L5, /pkg/api
L1 → L8, L2, /pkg/api
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

## 4. Coding Standards (Go 1.26)

- **Toolchain pinned** in `go.mod` (`go 1.26.X`). Never bump unilaterally.
- **Formatting**: `gofmt -s` and `goimports`. Lines wrap at 100 columns
  except generated code.
- **templ**: dashboard HTML lives in `internal/dashboard/templates/*.templ`.
  After editing any `.templ` file run `go generate ./internal/dashboard/templates/`
  (or `templ generate`). Commit both the `.templ` source and the generated
  `_templ.go` file. Never edit `_templ.go` by hand — it is overwritten on
  the next `go generate`. The `templ` binary (v0.3.x) must be on `PATH`;
  install once with `go install github.com/a-h/templ/cmd/templ@latest` or
  download the release binary from GitHub.
- **Linters**: `golangci-lint` with `govet`, `staticcheck`, `errcheck`,
  `gocritic`, `revive`, `bodyclose`, `contextcheck`, `noctx`, `nilerr`,
  `forbidigo` (banning `fmt.Println`, `panic`, `os.Exit` outside `main`),
  `gosec`, `gocyclo` (complexity ≤ 15), `funlen` (≤ 80 lines), `lll`.
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
  `slog`-compatible, HTTP servers other than `net/http`,
  any LGPL / AGPL code.
- Pre-approved core:
  `hashicorp/memberlist`, `cespare/xxhash/v2`, `klauspost/compress`,
  `prometheus/client_golang`, `go.opentelemetry.io/otel`, `spf13/cobra`,
  `stretchr/testify` (tests only), `golang.org/x/{net,sync,sys}`.
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
- **Goroutines**: max two per request (reader + writer). Background work
  uses bounded worker pools sized at boot.
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

- **Unit tests** ship in the same package, `_test.go`. Table-driven.
  `-race` always on in CI.
- **Coverage gates** per package: ≥ 85% default, ≥ 95% for
  `internal/cache` and `internal/storage`. CI reports per-package coverage
  and fails on regression.
- **Fuzz tests** for: header parsing, `Cache-Control` tokenizer, `Vary`
  canonicalization, URL normalization, VCL parser. At least one corpus
  per fuzzer committed under `testdata/fuzz/`.
- **Conformance**: `test/cachetests` runs the upstream
  `http-tests/cache-tests` harness against a real `bouine` instance. CI
  publishes the score as a JSON badge and blocks regressions.
- **Integration**: `test/integration` boots a 3-node bouine cluster + an
  origin via `docker compose`. Scenarios listed in `PLAN.md §12.3`.
- **Chaos**: kill a peer, drop packets, slow the disk. Lives under
  `test/chaos`, runs nightly, not on every PR.
- **Benchmarks**: `bench/` runs on a pinned self-hosted runner with CPU
  affinity. `benchstat` compares HEAD vs `main`, N ≥ 10.
- **No flaky tests.** A test that flakes twice in a week is quarantined
  (`-skip` with a tracking issue) within one business day.
- **Determinism**: no `time.Now()` in tests; use the injected clock. No
  random ports; use `:0` and read back.

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
  bytes_in, bytes_out, dur_ns, peer_hop.
- **Self-check endpoints**: `/healthz`, `/readyz`, `/debug/cachecheck`,
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
  it was taken.
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
  `internal/pipeline/errors.go`, admin in `internal/admin/errors.go`).
- Stack traces only on `error` level. PII never appears in errors.

---

## 13. Configuration & Compatibility

- YAML config schema lives in `internal/config/schema.go`. JSON Schema
  emitted by `bouine config schema` is the contract.
- Config changes that break compatibility require a major version bump and
  a migration guide.
- VCL shim supports the subset defined in `PLAN.md §17.4`. Unsupported
  constructs are reported, never silently ignored.
- The Go SDK (`pkg/bouineapi`) follows semver. Wire types in `pkg/api` are
  additive — add fields, never remove or rename in the same major.
- Feature flags for experimental work live under `experimental:` in
  config and default to off.

---

## 14. Build, CI & Release

### 14.1 Commands

```
make build           # binary to ./bin/bouine
make test            # go test -race ./...
make lint            # golangci-lint run
make vet             # go vet ./...
make fuzz            # short fuzz pass on registered targets
make bench           # bench harness, writes bench/results/
make benchstat       # compare HEAD to main
make conformance     # run http-tests/cache-tests, write report
make integration     # docker compose up; run scenarios
make ci              # all of the above in the order CI uses
make docs            # build docs site (phase 4+)
make schema          # regenerate JSON schema and SDK types
make templ           # go generate ./internal/dashboard/templates/ (requires templ CLI)
make hooks           # install pre-commit hooks into .git/hooks
```

### 14.2 CI pipeline (stages)

1. `vet` + `lint` + `govulncheck`.
2. `unit` (with `-race`, per-OS matrix linux/amd64, linux/arm64,
   darwin/arm64).
3. `coverage` gate.
4. `fuzz` (short, time-boxed; long-running nightly).
5. `conformance` (`cache-tests`), publish score.
6. `bench` on self-hosted runner, `benchstat` diff vs `main`.
7. `integration` (3-node cluster).
8. `release` (tagged refs only): SBOM (`syft`), provenance, signed
   container image (`cosign`).

### 14.3 Branching & releases

- Trunk-based development on `main`. Short-lived branches.
- Conventional Commits. PR titles must match.
- Tags follow `vMAJOR.MINOR.PATCH`. Release notes generated from commits.
- Container images: distroless, non-root, multi-arch (amd64+arm64).

### 14.4 Pre-commit hooks (mandatory)

`bouine` uses [`pre-commit`](https://pre-commit.com) to enforce the
minimum quality bar on every commit. The hooks are the local mirror of
the CI gates; they exist so problems are caught in seconds, not minutes.

**Setup is required**, not optional:

```
pip install pre-commit            # or: brew install pre-commit
pre-commit install                # installs the git hook
pre-commit install --hook-type commit-msg   # Conventional Commits check
make hooks                        # one-shot equivalent of the two above
```

The `.pre-commit-config.yaml` at the repo root is the single source of
truth. It MUST register at minimum:

- **YAML validation** — `check-yaml` from `pre-commit/pre-commit-hooks`
  for every `*.yaml` / `*.yml` file (config, Helm, GitHub Actions,
  fixtures). Multi-document files allowed where needed.
- **Generic file hygiene** — `end-of-file-fixer`, `trailing-whitespace`,
  `mixed-line-ending`, `check-merge-conflict`, `check-added-large-files`
  (cap 1 MiB; testdata fixtures excluded explicitly).
- **Go formatting** — `gofmt -s` and `goimports` via the
  `golangci/golangci-lint` hook in `--fix` mode, or `dnephin/pre-commit-golang`
  for `gofmt`/`goimports` standalone.
- **Go tests** — a local hook running
  `go test -race -count=1 -short ./...`. The `-short` flag is what keeps
  the hook usable; long benchmarks, fuzz, integration, and conformance
  suites stay in CI / `make` targets. The full `go test -race ./...` runs
  on the `pre-push` stage.
- **golangci-lint** — invoked via the official hook with
  `--new-from-rev=HEAD~` on commit (changed lines only) and
  `--config=.golangci.yaml`. The enabled linter set MUST include at
  least:
  - `govet`
  - `staticcheck`
  - `errcheck`
  - `gosec`
  - `bodyclose`
  - `contextcheck`
  - `noctx`
  - `nilerr`
  - `gocritic`
  - `revive`
  - `forbidigo` (bans `fmt.Println`, `panic`, `os.Exit` outside `main`)
  - `gocyclo` (complexity ≤ 15)
  - `funlen` (≤ 80 lines)
  - `depguard` (enforces §3.1 layer matrix)
  - `unparam`
  - `ineffassign`
  - `unused`
  - `misspell`
- **Go module hygiene** — a local hook running `go mod tidy` and failing
  if it produced a diff.
- **govulncheck** — runs on `pre-push` stage only (too slow for every
  commit, but must pass before code leaves the laptop).
- **Secret scan** — `gitleaks` hook with the repo config; refuses
  commits containing credentials, tokens, or production hostnames.
- **Conventional Commits** — a `commit-msg` stage hook validating the
  message header against the `feat|fix|chore|docs|refactor|test|perf|build|ci`
  prefix list. Required because release notes are generated from commits.

**Operational rules:**

- The hook config is versioned. Bumping hook versions follows the same
  PR review process as code; pin every revision (`rev: v1.2.3`).
- CI re-runs `pre-commit run --all-files` as its first lint stage so a
  bypassed local hook still fails the build.
- Hooks must remain fast: the `pre-commit` stage budget is **30 s** on a
  laptop. Anything slower moves to `pre-push` or CI.
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
   regressions to L1–L4 take priority over any other work.
7. **Single source of truth**: `PLAN.md` for *what*, `AGENTS.md` for
   *how*, `docs/decisions/` for *why*. If they disagree, escalate to the
   user — don't pick.
8. **Hand-off protocol**: when stopping mid-task, leave a `HANDOFF.md`
   at the repo root with: state, next concrete steps, known risks,
   commands run, commands not yet run. Delete it when the task is done.

---

## 16. Working Loop (mandatory per task)

For every task an agent starts, execute this loop. No shortcuts.

1. **Orient**
   - Re-read `PLAN.md` sections relevant to the task.
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
4. **Verify**
   - `make lint test` minimum.
   - If touching L1–L6: `make bench` and compare to `main`.
   - If touching `cache`: `make conformance`.
   - If touching cluster: `make integration`.
5. **Document**
   - Update godoc.
   - Update `docs/runbook` and `docs/decisions` when warranted.
   - Update `PLAN.md` only if scope/exit-criteria actually changed and the
     user approved the change.
6. **Report**
   - Concise summary (≤ 4 lines unless complex).
   - Include `file:line` references.
   - List anything skipped or deferred.

---

## 17. Pull Request Checklist

Before declaring a change ready:

- [ ] `pre-commit run --all-files` passes locally.
- [ ] Layer dependencies respected (`depguard` clean).
- [ ] `make ci` green locally.
- [ ] Tests added/updated; coverage not reduced.
- [ ] If hot path: zero-alloc benchmark proves it.
- [ ] If cache logic: `cache-tests` score not regressed.
- [ ] If dashboard templates changed: `go generate ./internal/dashboard/templates/`
  committed alongside `_templ.go` files.
- [ ] If config: JSON schema regenerated.
- [ ] If public API: SDK types updated, semver impact noted.
- [ ] Godoc + changelog entry.
- [ ] ADR added if architectural.
- [ ] No secrets, no PII, no production hostnames.
- [ ] `HANDOFF.md` removed if you created one.

---

## 18. Anti-Patterns & Common Mistakes

- ❌ Putting cache logic inside an HTTP handler.
- ❌ Importing a third-party HTTP framework for admin endpoints.
- ❌ Using `time.Now()` directly instead of an injected clock.
- ❌ `panic` for control flow.
- ❌ Logging a request body or `Authorization` header for "debugging".
- ❌ Adding a label like `url` or `user_agent` to a Prometheus counter.
- ❌ A new dependency without an entry in `docs/deps.md`.
- ❌ "TODO: handle error" — handle it or return it.
- ❌ `git commit --no-verify` or `SKIP=...`. Fix the hook instead.
- ❌ Sleeps in tests. Use deterministic clocks or synchronization
  primitives.
- ❌ Holding a lock across an I/O call.
- ❌ Buffering an entire response body into memory.
- ❌ A goroutine without an owner or a way to stop it.
- ❌ Catching an error and returning `nil` with a log.
- ❌ Spec deviation ("close enough to RFC 9111").
- ❌ Touching `PLAN.md` without explicit user approval.

---

## 19. Escalation & Stop Conditions

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

## 20. Glossary

- **Hit path** — code executed when a request finds a usable cached
  response and returns without origin fetch. The fastest path in the
  system; performance gates are strictest here.
- **Miss path** — request not in cache; fetches from peer or origin.
- **Revalidation** — conditional request to origin to refresh freshness.
- **SWR / SIE** — `stale-while-revalidate` / `stale-if-error` (RFC 5861).
- **Surrogate key** — opaque label attached to a response for grouped
  invalidation.
- **Ban** — predicate-based invalidation, evaluated lazily on lookup.
- **Purge** — exact-key invalidation, immediate.
- **Refresh** — soft purge, triggers revalidation on next access.
- **Hot tier (L0)** — sharded in-RAM map of cache objects.
- **Warm tier (L1)** — mmap-backed segmented disk storage.
- **Peer fetch** — owner-first cluster lookup before falling back to origin.
- **Hop limit** — max number of peers a single request may traverse before
  going to origin.
- **Hedged request** — duplicate fetch sent after p99 latency; first to
  return wins.
- **Request collapsing** — single-flight serialization of identical
  in-flight misses.
- **Anti-entropy** — periodic reconciliation of cluster state to repair
  drift.
- **VCL shim** — subset translator from Varnish Configuration Language to
  the bouine config tree (see `PLAN.md §17.4`).
- **Hit-path budget** — performance envelope for the hit path
  (see §7).

---

*Last updated alongside `PLAN.md`. When you change one, check the other.*
