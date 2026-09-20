# ADR-0050: internal/config is a shared kernel; eviction enum moves out of pkg/api

- **Status**: Accepted
- **Date**: 2026-09-20
- **Deciders**: @chridupin-33
- **Phase**: phase 2
- **Consulted**: —

## Context

`internal/config` had no declared position in the layer matrix
(AGENTS.md §3.1, `lint/depguard.yaml`), so each layer's allow-list
decided per-package whether it could import it. `internal/storage` (L2)
was not allowed to. When typed string enums were introduced for config
values (PRs #714/#715), that asymmetry forced the `EvictionAlgorithm`
enum out of `internal/config` and into `pkg/api` — a semver-frozen,
wire-stable package — with `internal/config` aliasing it, purely so
`internal/storage` could reach the type.

That placement was wrong for three reasons:

1. `pkg/api` exists for wire-stable types shared between the admin HTTP
   surface, the Go SDK (`pkg/bouineapi`), and the dashboard. The
   eviction algorithm appears in none of them: no wire type, no admin
   response, no SDK call references it. Parking it there put a
   config-vocabulary type under semver "wire-stable" rules, where
   renaming a value would count as a breaking *wire* change for a wire
   that does not exist.
2. It forced a type alias (`config.EvictionAlgorithm =
   api.EvictionAlgorithm`) — indirection with no purpose other than
   satisfying an import restriction.
3. The domain explanation of the cachaner policy was duplicated in four
   packages, guaranteeing drift.

`ClusterMode` and `TLSVersion` never had this problem because their
consumers (cluster, cmd, dashboard) already import `internal/config`.

## Decision

- `internal/config` is a **shared kernel**: every layer may import it
  directly. Like `internal/observability`, it must remain a leaf — it
  may not import any other `internal/*` package.
- Every layer's depguard allow-list (`.golangci.yaml` and
  `lint/depguard.yaml`) explicitly allows `internal/config`.
- `EvictionAlgorithm` and its constants move back to
  `internal/config` as the single owner. `internal/storage` imports
  `config.EvictionAlgorithm` directly. `pkg/api/eviction.go` is
  deleted; `pkg/api` returns to containing only wire-stable types.
- Shared enums that only config consumers need (`ClusterMode`,
  `TLSVersion`) stay declared in `internal/config`, same as
  `EvictionAlgorithm`. One mechanism for all of them.

## Consequences

- No alias indirection and no enum in the public API surface; renaming
  a config enum value is an internal change, not a semver event.
- The canonical definition of config vocabulary types lives in one
  package; doc comments on the enum are single-sourced.
- Risk: `internal/config` becoming a god-import for layers it does not
  belong to. Mitigated by its leaf rule: config imports nothing from
  `internal/*` except itself (enforced by the `config-leaf` depguard
  rule), so no import cycle can form through it.
- Cross-layer *interfaces* are unaffected; this decision concerns
  shared value types only.

## Alternatives considered

- **Tiny shared-enum leaf package** (e.g. `pkg/cacheenum`): avoids
  touching the layer model but adds a package for three string types
  and still splits config vocabulary across two homes.
- **Keep the alias in `pkg/api`** (status quo ante): keeps the
  asymmetry between `EvictionAlgorithm` (aliased through the public
  API) and `ClusterMode`/`TLSVersion` (plain config types), and
  encumbers a wire-stable package with a non-wire type.
