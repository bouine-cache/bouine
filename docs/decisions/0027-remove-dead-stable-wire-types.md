# ADR-0027: Remove dead Stable wire types from pkg/api

- **Status**: Accepted
- **Date**: 2026-08-04
- **Deciders**: @thylong
- **Phase**: 2

## Context

ADR-0025 removed the `full` cluster mode and ~2000 lines of
replication transport, handlers, tests, dashboard UI, and insights
rules. The removal was incomplete: three `Stable`-tagged public types
in `pkg/api/cluster.go` survived even though nothing produces or
consumes them.

- `ReplicationEvent` — the struct full-mode replication broadcast.
  Zero non-test references in the tree. Its own godoc said "no longer
  populated or broadcast."
- `GossipTypeReplication` — the gossip type constant for replication
  events. Zero references outside the const block.
- `PurgeEvent.Type` and `BanEvent.Type` — string fields the broadcaster
  set to `GossipTypePurge` / `GossipTypeBan` but no consumer read. The
  binary cluster codec (`internal/cluster/codec.go`) dispatches on a
  `msgType` byte in the frame header, not on field presence, and never
  serializes the `Type` field. The JSON gossip path
  (`handleJSONGossip`) decodes into an anonymous `struct{Type string}`
  only to log "unrecognized" — it never decodes into `PurgeEvent` or
  `BanEvent`.

AGENTS.md §13 forbids removing wire types in the same major version:
"wire types in `pkg/api` are additive — add fields, never remove or
rename in the same major." So the conservative option was to mark these
`// Deprecated:` and defer removal to the next major.

The project owner chose to remove them now and bump the major version
rather than carry dead `Stable` surface whose own godoc documented them
as dead. This ADR records that decision and the breakage so consumers
have a migration reference.

## Decision

Remove the dead `Stable` types from `pkg/api` in the next major bump:

- Delete `ReplicationEvent` entirely.
- Delete `GossipTypePurge`, `GossipTypeBan`, and `GossipTypeReplication`
  constants. After removing the `Type` fields, `GossipTypePurge` and
  `GossipTypeBan` have no remaining references either — the entire
  const block goes.
- Remove the `Type string` field from `PurgeEvent` and `BanEvent`.
- Stop setting `Type` in `internal/cluster/broadcast.go`
  (`BroadcastPurge`, `BroadcastBan`).

This is a breaking change to the `Stable` surface of `pkg/api` and
requires a major version bump per AGENTS.md §13.

## Consequences

### Positive

- No dead `Stable` types whose godoc contradicts the code.
- `PurgeEvent` / `BanEvent` shrink by one string field each — no wire
  impact on the binary codec (which never serialized `Type`), no
  behavioral impact on the cluster.
- Removes the misleading doc comment claiming "every gossip message
  carries a 'type' field" — the binary codec carries a `msgType` byte,
  not a string field.

### Negative / trade-offs

- **Breaking change.** Any external consumer depending on
  `api.ReplicationEvent`, `api.GossipType*`, or the `Type` field on
  `PurgeEvent` / `BanEvent` fails to compile against the new major.
  This is intentional and documented here.
- The `Client.HTTPClient` field godoc in `pkg/bouineapi` is updated
  separately to reflect the new default-timeout behavior; that change
  is additive, not breaking.

### Risks

- A consumer that JSON-decodes cluster gossip messages into their own
  mirror struct with a `Type` field will see it empty: the broadcaster
  no longer sets it and the binary codec never serialized it. This is
  not a supported wire path — the documented transport is the binary
  codec — but is called out here for completeness.
- Consumers that reference `api.PurgeEvent{}.Type` or
  `api.BanEvent{}.Type` directly fail to compile (the field is gone,
  not zero-valued). This is the intended breaking surface.

## Alternatives considered

1. **Deprecate now, remove in next major.** Add `// Deprecated:`
   godoc, stop populating `Type` in the broadcaster, file this ADR
   scheduling removal for the next major. Rejected by the project
   owner: carrying dead `Stable` surface whose own godoc says "no
   longer populated" is worse than the one-time migration cost.

2. **Wire the types up.** ADR-0025 removed the feature these types
   served; there is nothing to wire them to. Not a real option.

## References

- ADR-0025: Remove full cluster mode (root cause of the dead types).
- AGENTS.md §13: Configuration & Compatibility (semver / wire-type
  rules).
- Issue #304: `fix(pkg): SDK unbounded error body + no timeout default;
  pkg/api dead Stable types`.
