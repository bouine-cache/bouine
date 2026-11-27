# ADR-0076: CLI named remotes and multi-target fan-out

- **Status**: Accepted
- **Date**: 2026-11-27
- **Deciders**: @bouine-core
- **Phase**: post-v1.0 operator surface (issue #953)
- **References**: issue #953 (design + review addenda), ADR-0077 (transport dependency), `cmd/bouine/cmd/remotes.go`, `pkg/bouineapi/remotes.go`, `docs/plans/cli-remotes-kube-proxy.md`

## Context and Problem Statement

Every admin-plane subcommand talks to exactly one endpoint
(`--server`/`$BOUINE_SERVER`). Operators running several independent
deployments (per region, per tenant, blue/green, edge in front of a
shield) write shell loops with duplicated addresses and tokens, hide
failures behind `|| true`, and have no record of which deployment was
hit. ROADMAP.md's Deferred list already records "one deployment per
tenant today".

## Decision

Add a user-scoped remotes file (`--remotes-file` > `$BOUINE_REMOTES_FILE`
> `$XDG_CONFIG_HOME/bouine/remotes.yaml`) naming deployments with a
server, a credential reference (`token-env` or a 0600 `token-file`;
literal tokens are unsupported), and labels.

- Selection: `--remote NAME`, or `-l/--selector k=v,...` (ANDed,
  zero matches is an error, no `--all`). `--server`/`$BOUINE_SERVER`
  stay the ad-hoc escape hatch and win over the file; the three are
  mutually exclusive (ambiguous = error).
- Token rule: an ambient `--token`/`$BOUINE_TOKEN` is never borrowed by
  a named remote; combining them with `--remote`/`--selector` is an
  error. Duplicate resolved token values across selected remotes print
  a warning.
- Fan-out (only when `--selector` matched >1 remote, scope:
  `purge`/`ban`/`refresh`): sequential in file order, never aborts on a
  failed remote; text prints `name<TAB>result` per line, JSON a bare
  array `[{remote, ok, error?, result?}]`, failures also to stderr;
  exit 0 iff all succeeded. `--dry-run` prints the plan and exits 0.
- Single-remote output is byte-for-byte unchanged (per-command golden
  tests), including JSON shape.
- Naming: "remote", not "target" — `StatusTarget` already means origin
  pool member in the same binary.
- Plaintext writes to non-loopback `http://` remotes are refused
  (loopback = literal `127.0.0.0/8`, `::1`; DNS names out of scope v1)
  unless the remote sets `allow-insecure: true`.

## Consequences

- Positive: operators name deployments once; invalidations fan out with
  truthful per-target outcomes and exit codes; credential blast radius
  is bounded by per-remote credentials and the no-ambient-token rule.
- Negative: none on the daemon wire or config schema; `pkg/bouineapi`
  gains `Remote`/`Outcome`/`Fanout`/`RoundTripper` additively (minor
  semver bump). `ban` fan-out is cumulative and lazy, so `--dry-run` is
  documented as the first step.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| `--tier edge\|shield` flag | Encodes one topology into a generic tool; label selector covers it. |
| Built-in port-forward / kube discovery | See ADR-0077; transport goes through the API-server proxy instead. |
| Server-side "fleet" endpoint | Cross-deployment trust problem; client-side fan-out adds no server attack surface. |
| `--max-concurrency` | Ordering across tiers is correctness; CLI cannot infer the graph. |
| Full k8s selector language | Half a parser with no second use case. |
| Reusing the `target` noun | Taken by origin pool members (`bouineapi.StatusTarget`). |
