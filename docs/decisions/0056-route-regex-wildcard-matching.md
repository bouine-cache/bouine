# ADR-0056: Regex path and wildcard host route matching (RouteMatch expressiveness)

- **Status**: Accepted
- **Date**: 2026-10-06
- **Deciders**: @bouine-core
- **Phase**: route table
- **References**: issue #772; ADR-0047 (traffic-class label — the shadow-report pattern reused); `http-tests/cache-tests` (no impact: default surface unchanged)

## Context and Problem Statement

bouine's route table must mirror caching policies currently expressed as
rules at the CDN edge when those policies move into bouine (layered
deployment `client → edge → bouine → origin`). The edge's rule language
matches on regex paths (campaign landing pages under
`^/{locale}/l/campaign-.*` with a short TTL override) and wildcard hosts
(an entire `*.staging.` subdomain tree with caching disabled).

`RouteMatch` supported exact host + raw `path_prefix` + exact methods
only, so those policy classes required route explosion: one route per
per-PR host, one per market variant, one per campaign page family.

## Decision Drivers

- **Expressiveness**: cover wildcard-host and regex-path policy classes
  without route explosion.
- **Conformance**: routes without the new fields behave byte-identically
  (AGENTS.md §2.5).
- **Performance**: route resolution runs before every request, fast path
  included; prefix-only tables must keep their zero-alloc hit gates
  (AGENTS.md §2.4, §7).
- **Operability**: dead config (a later route an earlier one fully
  covers) must be surfaced, not silently ignored — the same contract the
  traffic-class shadow report already ships (ADR-0047).

## Decision

Extend `RouteMatch` with two optional fields; `path_prefix` semantics
are unchanged.

1. **`match.host: "*.suffix"`** — a single leading `*.` wildcard compiles
   to a suffix match anchored on the label boundary (the traffic-class
   classifier's semantics): `*.example.com` matches any host strictly
   longer than `.example.com` — `foo.example.com`,
   `a.b.example.com` — never `example.com` itself. Exact hosts keep
   case-insensitive equality. Any other `*` placement (mid-string,
   trailing, bare `*`) is rejected by `config.Validate`: wildcards stay
   cheap (`strings`-level comparison on the cold classify path) and
   unambiguous. Regex hosts were considered and rejected — wildcards
   cover the real cases at a fraction of the cost.

2. **`match.path: "^…$"`** — an RE2 pattern matched against the request
   path (query excluded). Go's `regexp` is linear-time (no ReDoS from an
   operator-supplied pattern — the same choice `request.path_rewrite`
   already made), compiled **once** at startup (`Router.AddRouteSpec`), never
   per request. `path` and `path_prefix` are mutually exclusive in
   `config.Validate`; the pattern must be anchored at both ends (an
   unanchored pattern silently matches suffixes the operator did not
   intend), size-capped at `MaxRoutePathPatternBytes` (512 B), and free
   of raw control bytes (dead config: parsed request paths never carry
   them).

3. **Precedence and shadow reporting** — declaration order, first match
   wins, unchanged. The router now reports, at boot (Error log, boot
   proceeds), each later route **fully** covered by an earlier one: host
   (absent ⊇ exact-in-suffix ⊇ narrower suffix), path (absent ⊇
   prefix-extension ⊇ byte-identical regex), and methods (absent ⊇
   superset) must all be covered. The detection is deliberately
   conservative — a regex against any other predicate is undecidable in
   general and stays silent, because a false finding would send
   operators chasing a ghost while a missed one costs nothing (the
   ordering semantics are unchanged either way).

### Performance evidence

- `BenchmarkGate_RoutedFastPath_Hit` (prefix-only table): unchanged,
  0 allocs/op.
- `BenchmarkGate_RoutedFastPath_Hit_WildcardHost` (new gate, budget 0):
  length-bounded `EqualFold` on the Host tail, 0 allocs/op — measured
  ~88 ns/op on Apple M5.
- `Benchmark_RoutedFastPath_Hit_RegexPath` (regular, ungated benchmark):
  pre-compiled RE2 evaluation, measured 0 allocs/op steady state
  (~130–200 ns/op, machine-pooled executor). It is deliberately **not**
  a gate: the zero-alloc guarantee is pinned for the prefix/wildcard
  forms only; arbitrary operator patterns are not covered by a
  per-pattern alloc budget. Operators wanting the hard guarantee keep
  prefix-only tables.

### Cardinality

One `route` label value per configured route: a regex route is one
route, not N — the feature *reduces* label and config cardinality
versus route explosion (AGENTS.md §9 budget).

## Alternatives Considered

- **Regex hosts** — rejected: real policies need label-boundary suffix
  matching, which a wildcard expresses at a fraction of the matching
  cost and with zero validation ambiguity.
- **Rejecting shadowed routes at `config.Validate`** — rejected: boot
  must proceed (the traffic-class precedent, ADR-0047); a hard failure
  would turn a metrics-only misordering into an outage on upgrade.
- **Full regex-subset shadow analysis** — rejected: undecidable in
  general; a conservative detector that never lies is worth more than
  an eager one that sometimes does.

## Consequences

- `Router.AddRouteSpec(RouteSpec)` joins `AddRoute` as the registration
  surface; `AddRoute` keeps the exact-host/prefix-only form for its
  existing call sites, both funnel into one `addRoute` so label
  derivation, host compilation, and shadow detection cannot diverge.
- `MatchByHostPath` (admin purge/refresh key building) and
  `RoutedFastPath.TryHit` resolve extended predicates through the same
  `matchRoute` — one authority, no drift between planes.
- Route names auto-derive as `host:path` with the regex shown verbatim
  when `path` is used; the dashboard route tables and config insights
  display the effective predicate (`RouteMatch.PathLabel`).
- Positive: policy classes that needed N routes collapse to one.
- Negative: route tables mixing regex and prefix routes cannot be
  fully shadow-checked (documented silence, not a correctness gap).
