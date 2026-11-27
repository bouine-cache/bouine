# Plan: CLI named remotes + K8s API-server proxy access (issue #953, option C)

Implements the amended design in issue #953. Client-side multi-deployment
admin surface (`remotes` file, `--remote`, `-l/--selector`, fan-out over
`purge`/`ban`/`refresh`, `--dry-run`, per-remote outcomes + exit code), plus
the **option C access transport**: remotes addressed as
`kube://<namespace>/<admin-svc>:<port>` reached through the Kubernetes
API-server `services/proxy` subresource, eliminating the manual
`kubectl port-forward` pre-step.

Scope guardrails (from the issue): no wire protocol change, no daemon config
schema change, no new admin endpoint, no change to `internal/{cache,storage,
origin,cluster,server}`; hit path untouched; single-remote output
byte-for-byte identical.

## 0. Decision record

- Access transport: **option C** (API-server `services/proxy`), chosen over
  built-in port-forward (option B). Rationale: single gateway, per-user
  API-server audit, no new reachable listener, multi-cluster fan-out from
  kubeconfig contexts. Cost accepted: `k8s.io/client-go` in the CLI module
  (measured +11-12 MB on the 46.4 MB binary, 78 k8s packages, 10 modules),
  gated by ADR-0077 with a `deps` label.
- Admin-listener TLS is **not** a prerequisite under option C (operator→API
  server hop is TLS; API-server→pod hop is intra-cluster). It remains a
  separate hardening item. Plaintext-write refusal applies to `http://`
  remotes only.
- Naming: `remote` (not `target` — taken by `StatusTarget`).
- Credential rules: `token-env`/`token-file` (0600, refusal not warning) per
  remote; ambient `$BOUINE_TOKEN` never borrowed by a named remote; duplicate
  token values across remotes trigger a warning.

## 1. Work breakdown (PR split)

### PR 1 — SDK: remotes model, fan-out, transport indirection + deps ADR

- `pkg/bouineapi`: `Remote`, `Outcome[T]`, `Fanout[T]` (sequential, file
  order, never aborts); `New` normalizes a bare `host:port` to `http://`
  (documented behavior change); scheme-aware BaseURL.
- Client transport indirection: direct HTTP (fasthttp, as today) vs
  kube-apiserver-proxy transport. The proxy transport loads kubeconfig via
  `clientcmd`, builds API-server URLs
  `/api/v1/namespaces/<ns>/services/<svc>:<port>/proxy/<path>`, and issues
  `GET /version` through the proxy first, refusing `kube://` remotes against
  daemons older than the proxy-strip version with a clear error.
- ADR-0077 (`docs/decisions/0077-cli-kube-client-go.md`): client-go admission
  (deps label), measured binary-size table (46.4 MB → ~57-58 MB), rejected
  alternatives (kubectl subprocess, hand-rolled kubeconfig, separate module).
- ADR-0076 (`docs/decisions/0076-cli-remotes.md`): the remotes design,
  resolution matrices, credential rules.
- Tests: unit for `Fanout` ordering/never-abort, `New` normalization, proxy
  URL construction, version gate. Golden single-remote output tests.
- Gates: `make lint`, `make test`, `go mod tidy` diff-clean, `govulncheck`
  on the 10 new modules (cost recorded in PR description per AGENTS.md §7).
  `pkg/bouineapi` is Stable: additions only → minor bump.

### PR 2 — Daemon: admin proxy-prefix strip + observability

- `internal/admin/server.go`: one outermost wrapper in the
  `fullHandler()`/`minimalHandler()` constructors stripping the exact leading
  segment `/proxy/` or bare `/proxy` from `ctx.Path()`, before
  `authMiddleware`, the dashboard branch, `routeHandler`, and the minimal
  handler switch. Always-on no-op without the prefix. No config field.
- Access log gains `via=direct|kube-apiserver-proxy` attribute (one label).
- Bearer token still required end-to-end; no `X-Remote-*` trust. No change to
  `authMiddleware`.
- Tests: table test over every route class with/without `/proxy`; 401 without
  token through proxy; 413 oversized POST through proxy; 429 rate-limited
  through proxy; boot-phase proxied `/healthz` via minimal handler; wire
  `binaryVersion` assertion stays 3.
- Gates: `make lint`, `make test -race`, `make ci`. Not on the hit path; no
  `bench-gate`/`conformance` trigger beyond standard CI.

### PR 3 — Helm chart: opt-in proxy RBAC + NetworkPolicy knob

- New `deploy/helm/bouine/templates/rbac.yaml`, rendered only when
  `adminProxy.rbac.create: true`: Role + RoleBinding granting
  `services/proxy` on `resourceNames: ["<fullname>-admin"]` (exact string,
  pinned by helm unittest), verbs `get, create`.
- `networkPolicy.adminApiServerCIDRs` values knob adding an `ipBlock` ingress
  rule for the admin port; runbook documents the fallback (node CIDR, or
  disable just the admin-port rule) where API-server egress IPs are unstable.
- Helm unittests in `templates/tests/`: Role renders only when enabled;
  resourceNames resolves exactly; NetworkPolicy contains the CIDR block when
  set. `helm upgrade` with defaults produces no diff.
- Gates: `make lint` (chart lint/hooks), helm unittest suite.

### PR 4 — CLI: remotes file, resolution, `bouine remote`, transport selection

- `--remotes-file` > `$BOUINE_REMOTES_FILE` > `$XDG_CONFIG_HOME/bouine/remotes.yaml`.
  YAML via existing `gopkg.in/yaml.v3`. `name`+`server` required, unique
  names; `token-env`/`token-file` at most one; 0600-or-refused; literal
  `token:` unsupported; duplicate resolved token values warn.
- Resolution matrices (endpoint + token) exactly as in the issue, including
  all conflict errors and "ambient token never reaches a named remote".
- `bouine remote` / `use` / `delete` (no `add`); `use` writes back to the
  source file with round-trip fidelity tests (comments + key order
  preserved via `yaml.Node`).
- Transport selection per remote: bare `host:port` or `http(s)://` → direct;
  `kube://ns/svc:port` → API-server proxy.
- Tests: resolution matrix table, file-handling table (missing/empty/
  malformed/duplicate/unknown/wrong-perms), transport tests, round-trip
  tests.
- Gates: `make lint`, `make test -race`.

### PR 5 — CLI: selector fan-out, `--dry-run`, output/exit codes, integration

- `-l/--selector k=v,...` ANDed, zero matches = error, no `--all`; fan-out
  only with `--selector`; scope: `purge`, `ban`, `refresh` only (status/
  explain/keys/config/cluster stay single-remote, reasons recorded in the
  issue). Sequential file order; failures don't abort; `--dry-run` prints
  plan and exits 0.
- Output: single-remote unchanged (per-command golden tests); multi-remote
  text `name<TAB>result` per line, JSON bare array `[{remote, ok, error?,
  result?}]`, failures also to stderr; exit 0 iff all succeeded.
- Plaintext-write refusal for non-loopback `http://` remotes (loopback =
  literal `127.0.0.0/8`, `::1` after scheme strip; DNS names out of scope v1),
  with per-remote `allow-insecure: true` (preferred over a flag).
- Integration (`test/integration`): two in-process nodes; `ban --selector`
  with one stopped → per-remote outcomes + exit 1; mixed-version case: new
  CLI via a fake API-server proxy against an old admin handler → version-gate
  error; wire `binaryVersion` == 3 assertion.
- Gates: `make lint`, `make test -race`, `make integration` locally.

### PR 6 — Docs, threat model, changelog

- Fix `docs/architecture.md` §11 command list (drop unregistered
  `stats`/`bench`/`cachecheck`/`cluster join`, add `explain`, add new flags);
  README CLI section + kube-proxy recipe; `docs/runbook/20-purge-ban.md`
  "several deployments" section incl. API-server proxy access + NetworkPolicy
  fallback + `services/proxy` RBAC grant; shell completion; CHANGELOG
  `[Unreleased]`; fix `.github/ISSUE_TEMPLATE/feature_request.yml:56`
  dangling "§18".
- `docs/security/threat-model.md`: correct T03 ("rejects insecure HTTP for
  write methods" — false today; becomes true for CLI-side plaintext writes,
  server-side refusal remains unimplemented) and A07/T41 ("token never via
  CLI flag" — false, `conn.go` has `--token`); add row for API-server proxy
  abuse → mitigated by resourceNames-scoped Role + audit log + end-to-end
  bearer; bump T41 impact note for `--selector` blast radius.

## 2. Upgrade / rollout risk mitigation (version bump containing this work)

1. No wire change (`binaryVersion = 3` strict check untouched; asserted in
   tests); mixed-version clusters safe by construction + integration test.
2. No daemon config schema change; helm additions default-off → `helm
   upgrade` no-op unless opted in. No major bump, no migration guide (AGENTS.md §13).
3. Rollout reuses runbook `30-rolling-restart.md` (RollingUpdate, PDB
   minAvailable 2, preStop `/drain`, grace ≥ 40s, `/readyz`) and gates
   pre-prod on `bench/loadtest/scenarios/4.5_rolling_update/run.sh` (k6 1k
   RPS during rolling restart, 0% 5xx). Monitor 5xx logs during rollout.
4. Client/daemon matrix: old CLI → new daemon byte-identical (golden tests);
   new CLI → old daemon via `kube://` refused by the explicit `/version` gate.
5. SDK semver minor (additive only, no wire type changes).
6. Rollback: `helm rollback` removes additive resources; CLI downgrade is a
   static binary swap; no storage/WAL/cache-format change; no feature flag —
   daemon behavior activates only when the operator grants `services/proxy`.
7. Observability during rollout: API-server audit `services/proxy` calls on
   the admin svc, new `via=kube-apiserver-proxy` counter, existing
   generation-skew alert. Rollback triggers: any 5xx during rollout; any
   proxied write erroring against an old daemon.

## 3. Definition of done

- All six PRs landed; `make ci` green on each; issue acceptance conditions
  (roadmap bullet, ADR-0076 + ADR-0077, tests incl. integration) met.
- CLI binary size delta recorded in the ADR-0077 PR description with
  `go version -m` output; `govulncheck` delta recorded.
- Threat-model rows corrected in the same PR series, not deferred.
EOF
echo WRITTEN && wc -l /Users/theotime.leveque/development/bouine-cache/bouine/docs/plans/cli-remotes-kube-proxy.md
