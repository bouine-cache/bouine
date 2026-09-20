# ADR-0050: Resolved config layer; /v1/config serves effective values

- **Status**: Accepted
- **Date**: 2026-09-20
- **Deciders**: @chris.dupin
- **Phase**: config rework (follows the structured-validation PR, #714)

## Context

The effective configuration exists nowhere as a single source of
truth. Defaults are scattered across three layers:

1. `config.Defaults()` (loader) — listener, TLS floor, cluster mode,
   admin drain.
2. `config.Validate` / `Parse` — GOMEMLIMIT derivations
   (hot_max_bytes 75%, warm_max_entries 15%, max_streaming_buffer_bytes
   7%).
3. Consumer packages — `origin.NewPool` re-applies connect defaults
   (dial 10s, keepalive 30s, 64 conns/host, 90s idle, 30s response
   header); `cache.NewHandler` re-applies handler defaults (4 MiB
   response cap, 32 fetch concurrency, 100ms fetch wait, 64 MiB
   streaming buffer); `cmd/bouine/cmd` re-implements the per-tier
   eviction fallback and the per-route fetch-timeout inheritance.

Three "optional" conventions coexist (nil pointer, zero-means-default,
negative-means-disable), and `GET /v1/config` returned the raw config
tree — an operator reading it could not tell which values the process
actually runs with, and the cmd builders re-derived the same fallbacks
independently of what the config layer documents.

This is the third PR of the config rework program: structured
validation (landed), typed enums (landed), resolved layer (this ADR).

## Decision

We materialize a fully resolved configuration once, after validation,
in the config package: `Config.Resolve() *config.Resolved`.

- Every zero-means-default knob the cmd builders read is replaced by
  its effective value; negative disable sentinels (-1) become explicit
  bools (`WarmSyncDisabled`, `WALSyncPerEntry`, `CompactionDisabled`,
  `CompactStartImmediate`, `CheckpointingDisabled`); nil pointers are
  dereferenced to concrete values (`AllowSetCookie`); per-tier eviction
  overrides collapse to concrete algorithms; the per-route fetch
  timeout inherits the pool's resolved response-header timeout.
- The cmd builders (`buildStore`, `buildPools`, `buildRouter`,
  `buildStaticRoute`) map from resolved values instead of
  re-implementing fallback logic. The builder is a translation, not a
  policy layer.
- `GET /v1/config` serves the resolved tree. The raw tree is no longer
  exposed. **This is a breaking change to the admin API response
  shape** (fields renamed to resolved counterparts, sentinel values
  replaced by bools). It is acceptable without a major version bump
  under AGENTS.md §13 because the *YAML config schema is unchanged*;
  the admin endpoint is an operator surface, and its consumers are
  humans and the dashboard, not the Go SDK (`pkg/bouineapi` never
  modeled the response).
- Resolved is secret-free by construction: admin/Cloudflare tokens and
  TLS/CA cert-key paths have no field in the resolved tree, replacing
  the previous copy-then-zero approach (`sanitizedConfig`) that could
  drift when a secret field was added.
- The consumer-side zero-defaults in `origin` and `cache` remain in
  place for now (defense in depth; their unit tests construct raw
  configs). The resolved layer mirrors their documented defaults, with
  each mirrored constant naming its consumer counterpart. A follow-up
  can strip them per package once every construction path routes
  through resolved values.

`TestResolvedCompleteness` (config package) and the builder tests pin
that every consumer-read field is materialized; `TestResolve_NoSecretsInJSON`
pins the secret contract.

## Consequences

### Positive
- One place answers "what will the process actually run with" — for
  operators (/v1/config), the dashboard, and the builders.
- Secrets cannot leak through /v1/config by construction; adding a
  secret field to the raw config does not silently expose it.
- Builder fallback logic (eviction override chain, fetch-timeout
  inheritance, refresh-margin computation) has a single implementation
  in the config layer, testable without booting an engine.
- Disable sentinels become explicit bools at the consumption boundary.

### Negative / trade-offs
- /v1/config response shape changes; anything scripting against the
  raw tree must be updated (breaking admin API, not wire/config
  schema).
- Consumer defaults are mirrored in two places until the follow-up
  strips the consumer-side copies; drift is possible (mirrored
  constants name their counterparts and are pinned by tests).

### Risks
- A new consumer field added without a resolved counterpart silently
  falls back to its zero value. Mitigated by `TestResolvedCompleteness`
  and review checklist: builder reads must map from resolved values.

## Alternatives considered

- Keep /v1/config raw and add a separate /v1/config/resolved endpoint:
  two sources of truth, one of them still wrong (raw); rejected.
- Strip consumer-side defaults in the same PR: touches
  `internal/cache` and `internal/origin` plus their large test
  surfaces; violates the small-PR rule (AGENTS.md §15.4). Registered
  as follow-up.
- Keep sanitizedConfig (copy + zero): drift-prone opt-out list; the
  resolved tree's allow-list shape is the safer default.

## References

- Predecessors: structured validation (path-anchored FieldError),
  typed enums (`EvictionAlgorithm`, `ClusterMode`, `TLSVersion`).
- AGENTS.md §13 (compat), §3 (layering — config stays a leaf, so
  consumer defaults are mirrored, not imported).
- `docs/plans/config-rework.md` (local-only working plan).
