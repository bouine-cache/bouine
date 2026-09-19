# ADR-0047: `traffic_class` metric label for host-matched traffic populations

- **Status**: Accepted
- **Date**: 2026-09-19
- **Deciders**: @bouine-core
- **Phase**: observability
- **References**: issue #707, AGENTS.md §9 (cardinality budget), ADR-0034 (fasthttp H1 stack), ADR-0041 (H1 reactor)

## Context

A single bouine instance can carry multiple distinct traffic
populations toward the same microservices — e.g. client-side traffic
(browsers hitting a public host) and server-side traffic (internal
front-apps hitting an internal hostname). The data-plane metrics
(`bouine_requests_total`, `bouine_request_duration_seconds`,
`bouine_response_bytes_total`) cannot tell the populations apart, so
per-population caching analyses (hit ratio, latency percentiles,
bandwidth saved) are impossible on the bouine side (issue #707).

The distinguishing input is the requested Host. The two obvious
mechanisms both fail:

- **Raw `host` label** — the Host is user-controlled input. Routes
  with an empty `host:` (match-all) accept any Host header, so a
  hostile or buggy client mints unbounded label series, violating the
  §9 cardinality budget. On the H1 fast path the Host string can alias
  the connection read buffer, and the reactor metrics ring requires
  all retained strings to be stable handler-owned values — copying the
  Host per hit would allocate on the hit path (AGENTS.md §2.4).
- **Splitting fleets** — the same fleet carries both populations; the
  split must live inside one instance's metrics. Splitting deployments
  is in fact the decision this data should inform, not a prerequisite
  for it.

## Decision

We add an operator-declared, closed set of **traffic classes** in a
new `metrics` config section, and an unconditional **`traffic_class`**
label on the three data-plane request metric families.

1. **Config** (`metrics.traffic_classes`, validated by
   `config.Validate`): up to 8 classes, each with up to 64 host
   patterns — exact host, single leading `*.`, or single trailing
   `.*`/`*`; anything else is a config error, as is a bare `*`
   (matches every host, so under first-match precedence every later
   class would be dead config). Names must match
   `^[a-z][a-z0-9_]{0,31}$`, be unique, and not be the reserved
   `unclassified`. Declaration order is precedence. Matching is
   case-insensitive with the port stripped, identical to route
   matching.

2. **Label** (`traffic_class`, always present): values come
   exclusively from the configured set plus the `unclassified`
   fallback. The label is spoof-proof by construction — the request
   Host can only select among config-owned strings, the same guarantee
   pattern as `upstream_pool` (pinned by
   `TestTrafficClass_LabelSpaceClosed`).

3. **One classifier, three call sites** (`internal/server`):
   the slow path stamps the router's `X-Bouine-Traffic-Class`
   UserValue (no-route 404s classified too — Host is known before
   routing); the H1 fast path stamps
   `api.FastPathResponse.TrafficClass` inside `RoutedFastPath.TryHit`
   (a stable config-owned string, satisfying the reactor ring's
   retain-safety contract); `RecordHit` and the reactor's
   `hitMetricsRecord` carry the field to the metrics hook. Access logs
   gain a `traffic_class` attribute.

4. **Slot tables**: `poolMetrics` gains a traffic-class axis
   (`metricClassSlots = 9`: unclassified + the 8-class cap), filled
   lazily exactly like pools; `PreResolveTrafficClasses` joins
   `PreResolveRoutes` and the builder passes both the same config
   slice (pinned by `TestTrafficClass_ClassifierSubSetOfPreResolve`).
   Unknown classes at record time fall back to slot 0, mirroring the
   `_default` pool fallback.

5. **Zero hit-path cost**: patterns are lowercased once at compile
   time; the request Host is never lowercased (`strings.ToLower`
   allocates on any case change — the exact hazard that rules out the
   raw-host label). Classification is map lookups and
   length-bounded case-insensitive comparisons; gated by
   `BenchmarkGate_RoutedFastPath_Hit_TrafficClass` and its
   mixed-case variant (0 allocs/op).

## Consequences

### Positive
- Per-population PromQL becomes possible with no deployment change:
  hit ratio, p95, and saved bandwidth per class (see the issue for
  the queries).
- One stable metric shape across all deployments — the label is always
  present, so dashboards and alerts never handle an absent label.
- `unclassified` doubles as a self-documenting misconfiguration signal
  (a new frontend host nobody classified yet).

### Negative / trade-offs
- **Series-identity reset**: adding a label changes every series'
  identity; on upgrade, `rate()` shows one gap window at the deploy
  boundary. Noted in the upgrade runbook.
- **Cardinality**: the class axis multiplies the per-family ceilings by
  `1 + #classes` (opt-in: no configured classes ⇒ no multiplication;
  idle classes cost zero series, lazy fill).

### Risks
- **`bouine_requests_total` exceeds the §9 10 000 line** whenever
  pools × (1 + #classes) > 57 (33 pools at 3 slots is already 17 325).
  We accept a documented exception on three grounds: the overage is
  opt-in, the label set is closed (config-owned, spoof-pinned), and
  the operator guideline is arithmetic — keep
  pools × (1 + #classes) ≤ 57, or apply the documented
  `metric_relabel_configs` drop pattern (native-histogram runbook).
  The histogram family stays within budget at the full class cap.
- **Cluster drift**: nodes with different class configs classify the
  same request differently. Peer fetches forward the Host, so both
  nodes classify identically; only the metric labels can differ, never
  routing or caching (this is observability-only, by design).
- **Semantics creep**: bouine attaches no meaning to class names —
  `csr`/`ssr` is a deployment convention. Future policy uses
  (per-class admission, shed priority, HPA scaling) are new ADRs.

## Alternatives considered

- **Raw `host` label** — rejected: unbounded, user-controlled label
  space; hit-path allocation hazard (above).
- **Two deployments / namespaces** — rejected: the same fleet carries
  both populations; the split must be observable in one place to be
  the data that informs any later split.
- **Per-class origin metrics** — deferred: requires plumbing the class
  into L3; the data-plane families answer the question asked.
- **`X-Forwarded-Host` as the classification input** — deferred follow
  -up knob: classification keys off the Host bouine receives today;
  deployments whose proxy rewrites Host must write patterns against the
  rewritten value.

## References

- AGENTS.md §9 — cardinality budget and the exception recorded here.
- docs/runbook/native-histogram.md — series arithmetic with the class
  axis and the `metric_relabel_configs` drop pattern.
- docs/plans/traffic-class-metric-label.md — the design plan (rev 2).
