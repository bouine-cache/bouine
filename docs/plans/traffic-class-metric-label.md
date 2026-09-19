# Plan: traffic_class Metric Label — Differentiate CSR from SSR Traffic

Status: Draft (revision 2 — post design review + plan review fixes)
Date: 2026-09-19
Depends on: none (adds a config section + a metrics label)

## 1. Problem Statement

Backmarket runs bouine (bouine-L2) behind doorman. The same instance receives
two call populations toward the same µservices:

- **CSR**: a client browser calls `www.backmarket.<tld>` (via doorman) — the
  doorman caching ratio was measured poor because Cloudflare and tiered
  caching absorb it.
- **SSR**: front-apps call `doorman.doorman-<env>-<continent>.svc.cluster.local`
  — caching ratio measured good.

Today `bouine_requests_total` and `bouine_request_duration_seconds` cannot
distinguish the two populations, so the team cannot reproduce on bouine the
per-rendering-mode caching analysis they used last year to conclude that
Cloudflare was enough for CSR and doorman caching was only useful for SSR.

Origin conversation (internal paste, 2026-09-19):

> what can be done to stay generic on bouine might be to have a new label
> on metric, with the Host for which the request is made:
> client browser targets www.backmarket.[tld] (CSR)
> front-apps targets doorman.doorman-<env>-<continent>.svc.cluster.local (SSR)

### 1.1 Rejected alternatives

- **Raw `host` label** — Host is user-controlled input. Routes with an empty
  `host:` (match-all; the expected bouine-L2 config) accept any Host header,
  so a hostile or buggy client mints unbounded label series, violating the
  AGENTS.md §9 cardinality budget. On the H1 fast path the Host string can
  alias the connection read buffer, and the reactor metrics ring
  (internal/server/h1parser/reactor_metrics.go) requires all retained strings
  to be stable handler-owned values — copying the Host per hit would
  allocate on the hit path (AGENTS.md §2.4).
- **Two namespaces (bouine-L1 / bouine-L2)** — rejected in the origin
  conversation: the same L2 fleet carries both CSR and SSR traffic, so the
  split must live inside one instance's metrics. Splitting fleets is also
  the decision this metric should inform, not a prerequisite for it.

## 2. Design

### 2.1 Config: operator-declared traffic classes

```yaml
metrics:
  # Declaration order is precedence: the first class whose host pattern
  # matches wins. Max 8 classes (validated), each with max 64 host patterns.
  traffic_classes:
    - name: csr
      hosts:
        - "www.backmarket.fr"
        - "www.backmarket.de"
    - name: ssr
      hosts:
        - "*.svc.cluster.local"
        - "www.backmarket.*"
```

- New `metrics` top-level config section (additive; absent = no classes).
- `name`: Prometheus label value. Validation: `^[a-z][a-z0-9_]{0,31}$`,
  unique across classes, reserved values `unclassified` forbidden.
- `hosts`: glob patterns — single leading `*.` (suffix match), single
  trailing `.*` / `*` (prefix match), or exact host. Matching is
  case-insensitive with the port stripped, identical to route matching
  (Router.stripHostPort / strings.EqualFold). A pattern containing `*`
  anywhere else is a config error. A pattern that is exactly `*` is
  also a config error: it matches every host, so under first-match
  precedence every later class would be dead config. `unclassified`
  is the catch-all — it must not be re-invented as a pattern.

### 2.2 Label: `traffic_class`

Unconditional (always present) on three data-plane metric families, with
closed label values sourced exclusively from config (never request input):

| Metric | New label | Fallback value |
|---|---|---|
| `bouine_requests_total` | `traffic_class` | `unclassified` |
| `bouine_request_duration_seconds` | `traffic_class` | `unclassified` |
| `bouine_response_bytes_total` | `traffic_class` | `unclassified` |

- **Always present**: one stable metric shape for all deployments, queries
  never need absent-label handling. Accepted trade-off: a one-time series
  identity reset on upgrade for every deployment (label set change ⇒ new
  series identity; `rate()` gaps once at the deploy boundary). CHANGELOG and
  the upgrade runbook note it.
- **`unclassified`**: requests whose Host matches no class. Non-zero rates
  on it are a self-documenting misconfiguration signal (a new frontend host
  nobody classified yet), and it absorbs no-route 404s and admin/peer-plane
  noise.
- Origin-side metrics (`bouine_origin_*`) are **out of scope**; follow-up if
  per-mode origin load is needed (requires plumbing the class into L3).
- Native histograms: `traffic_class` multiplies the sparse series
  identically to the classic `_bucket` series. Update
  docs/runbook/native-histogram.md budget arithmetic.

### 2.3 Classification engine: one resolver, three call sites

Single `TrafficClassifier` type in `internal/server` (L1 owns route
resolution semantics; config passes through `config.Validate`):

```go
type TrafficClassifier struct {
    classes []trafficClass // pre-compiled: exact map + suffix list + prefix list
}
func (c *TrafficClassifier) Classify(host string) string // "unclassified" when nil/no match
```

- Pre-computed at boot from config; per-request work is map lookups and
  length-bounded suffix/prefix comparison over the port-stripped Host,
  case-insensitive via `strings.EqualFold` on the candidate slices —
  the same comparison `matchRoute` already runs for route hosts
  (router.go:117). Patterns are lowercased once at compile time; the
  request Host is never lowercased — `strings.ToLower` allocates
  whenever any rune changes case, so an adversarial mixed-case Host
  would allocate on the hit path (the exact hazard §1.1 cites to
  reject the raw-host label). Zero allocations.
- Spoof-proof by construction: label values are config-owned strings, the
  request Host can only select among them — the same guarantee pattern as
  `upstream_pool`.

Call sites (all three must land for the label to be trustworthy):

1. **Slow path (fasthttp middleware)** — the router resolves the class
   during route matching (it already stringifies `ctx.Host()` in
   `matchRoute`, Router.ServeRequest → matchRoute) and sets a third
   UserValue alongside `XBouinePool`; the metrics middleware's
   `attribution()` reads it. When the router has no classifier (no
   classes configured) the UserValue is absent and the middleware falls
   back to `unclassified`, matching the `_default` pool fallback
   pattern. No-route 404s are classified too (Host is known before
   routing).
2. **H1 fast path (blocking + reactor)** — `RoutedFastPath.TryHit` resolves
   the class in the same pass (it has the Host in `req.Host`) and stamps it
   on `api.FastPathResponse.TrafficClass` as a stable config-owned string
   (satisfies the reactor ring retain-safety contract,
   reactor_metrics.go:15-18; zero-alloc: it is the handler's field, not a
   request-derived copy). `api.FastPathMetrics.RecordHit` (pkg/api, marked
   Unstable) gains a `trafficClass string` parameter;
   `hitMetricsRecord`/`metricsDrainer.hook` in h1parser gain the field;
   `DataPlaneMetrics.RecordHit` consumes it.
3. **Static routes** — the fasthttp path of staticfile handlers attributes
   via the router UserValue (call site 1); the staticfile Handler's own
   metrics (`static.requests_total`, dashboard-only family with its own
   label contract) do not carry the label in this round.

### 2.4 Slot tables and pre-resolution

`poolMetrics` (internal/observability/dataplane.go) gains a traffic-class
axis sized by the configured class list (index 0 = `unclassified`, always
present):

```go
type poolMetrics struct {
    requestsTotal   [metricStatusSlots][metricResultSlots][metricSourceSlots][metricClassSlots]atomic.Pointer[prometheus.Counter]
    requestDuration [metricStatusClassSlots][metricResultSlots][metricClassSlots]atomic.Pointer[prometheus.Observer]
    responseBytes   [metricResultSlots][metricSourceSlots][metricClassSlots]atomic.Pointer[prometheus.Counter]
}
```

- `metricClassSlots = 9` (unclassified + max 8 configured classes) — static
  array bound, config validation caps at 8, unused slots cost nothing
  (lazy fill, same as pools).
- `PreResolveRoutes(poolNames)` is joined by
  `PreResolveTrafficClasses(classNames []string)`; builder.go calls both.
- A class name at record time that is not in the pre-resolved table maps
  to slot 0 (`unclassified`), mirroring the `_default` pool fallback of
  the poolID lookup (dataplane.go:986). Both structures are built from
  the same config slice so divergence "cannot happen" — which is
  exactly why the fallback is specified here and pinned by a builder
  unit test asserting classifier outputs ⊆ `PreResolveTrafficClasses`
  inputs.
- Array growth per pool: requestsTotal 175 → 1575 slots,
  requestDuration 30 → 270, responseBytes 25 → 225 — 230 → 2070
  slots, 9×. Idle cost stays zero series: with no classes configured
  the only class slot is `unclassified` (1× multiplier, lazy fill), so
  steady-state series multiply only by the classes actually
  configured and observed.
- Slot-table memory cost: 2070 pointers ≈ 16 KB per pool, ≈ 550 KB
  for 33 pools — bounded, one-time at boot.
- Cardinality budget test (cardinality_budget_test.go): today it
  counts only `bouine_request_duration_seconds` — the cheapest
  family, the one without the source axis — so as written it cannot
  catch a violation on the other two families. It must count all
  three families, drive 3 class slots, and assert the observed
  series equal the closed-form product (pools × classSlots ×
  per-family axes): with a closed label set the count is derivable,
  so any excess is a leak, not arithmetic.

### 2.5 Series budget arithmetic (AGENTS.md §9)

Per-family worst case at 33 pools (incl. `_default`) × classSlots,
where classSlots = 1 + #configured classes (≤ 9 at the cap):

- `bouine_requests_total` — 7 status × 5 results × 5 sources = 175 per
  (pool, class). **The binding constraint.** Today, no classes:
  5 775 (58% of the 10 000 budget). Any configured class breaks the
  line: 11 550 at classSlots 2, 17 325 for csr+ssr (classSlots 3),
  51 975 at the cap. Over budget whenever
  pools × (1 + #classes) > 57 — lowering the 8-class cap cannot fix
  this family (any class ≥ 1 breaks it); that was checked, not
  assumed.
- `bouine_request_duration_seconds` — 6 status classes × 5 results =
  30 per (pool, class): 8 910 at max classSlots, within the 10 000
  budget; over the < 5 000 target → same mitigation as today,
  `metric_relabel_configs` drop rules (documented pattern,
  docs/runbook/native-histogram.md) and the native histogram path.
- `bouine_response_bytes_total` — 5 results × 5 sources = 25 per
  (pool, class): 7 425 at max, within budget.

**Decision (maintainer sign-off, revision 2):** keep the label on all
three families and record an explicit exception to the §9 10 000
line in ADR-0047 for `bouine_requests_total`, on three grounds: the
overage is opt-in (no configured classes ⇒ no multiplication), the
label set is closed (config-owned values, no request-derived series
— pinned by the label-space spoofing test), and the runbook carries
the operator guideline: pools × (1 + #classes) ≤ 57 keeps
`bouine_requests_total` ≤ 10 000; deployments above the line reduce
classes, split fleets, or apply `metric_relabel_configs` drops.

### 2.6 Access log

`buildFastHTTPAccessLogAttrs` (dataplane.go) adds `"traffic_class", class`
next to `"cache_status"`. Zero Prometheus cost; enables per-mode log queries
before the metric ships and cross-checks the label in incident forensics.

## 3. Implementation Checklist

- [ ] ADR-0047 `traffic-class-metric-label` (MADR; metrics schema change,
      AGENTS.md §10): closed-set config-sourced label, rejection of raw
      host, always-present semantics, series reset trade-off, and the
      explicit §9 cardinality exception for `bouine_requests_total`
      with the pools × (1 + #classes) ≤ 57 guideline (§2.5).
- [ ] `internal/config`: `MetricsConfig.TrafficClasses` + validation
      (name regex, uniqueness, reserved `unclassified`, ≤8 classes,
      ≤64 patterns/class, glob form, bare-`*` catch-all rejected) +
      loader tests.
- [ ] `internal/server/router.go`: `TrafficClassifier` type, host-pattern
      compiler, `Classify`, wire into matchRoute/ServeRequest UserValue
      alongside pool attribution; unit tests for exact/suffix/prefix/
      port-stripping/case/fallthrough.
- [ ] `pkg/api/fastpath.go`: `FastPathResponse.TrafficClass` field;
      `FastPathMetrics.RecordHit` gains `trafficClass` parameter. This
      is a breaking shape change to the `// Unstable.` interface
      (fastpath.go:315) — permitted by the Unstable marker, not an
      additive change; make no semver-additive claim for it.
- [ ] `internal/server/routedfastpath.go`: stamp class from the route
      match pass (config-owned string).
- [ ] `internal/cache/fastpath.go`: propagate `TrafficClass` through
      `buildFastPathResponse` / `responseFromComposedHead` from the
      handler's classifier-resolved value.
- [ ] `internal/server/h1parser`: `hitMetricsRecord`, `metricsDrainer.hook`,
      parser + reactor pushHit/inline call gain the field.
- [ ] `internal/observability/dataplane.go`: label on the three families,
      class axis in `poolMetrics`, `PreResolveTrafficClasses`,
      `recordFastHTTPMetrics` / `RecordHit` plumbing, access-log attr.
- [ ] `cmd/bouine/cmd/builder.go`: build classifier from config, pass to
      router + fast paths; call `PreResolveTrafficClasses`.
- [ ] Tests: label-space spoofing pin (label_space_test.go — spoofed Host
      must not mint labels), cardinality budget counting all three
      families with classes (cardinality_budget_test.go — currently
      counts only the histogram), fast-path attribution e2e
      (cmd fastpath_attribution_test.go pattern), router classification
      table tests, config validation table tests, h1parser ring record
      tests, builder test asserting classifier outputs ⊆
      PreResolveTrafficClasses inputs.
- [ ] Benchmarks: `BenchmarkGate_*` entries for the middleware and
      fast-path hit with the class axis (alloc budget unchanged — 0
      allocs/op), with a mixed-case Host variant: a ToLower-based
      implementation allocates only on non-lowercase input, so an
      all-lowercase benchmark would hide it; add to `bench/run.sh`
      BUDGETS.
- [ ] Docs: CHANGELOG (Unreleased, note series-identity reset),
      native-histogram runbook multiplier, upgrade runbook note,
      SLO doc pointer (per-class hit-ratio queries now possible).
- [ ] Gates: `make lint`, `make test`, `make bench-gate`,
      `make integration` (server layer touched),
      `prek run --all-files`.

## 4. Blind Spots / Open Questions

1. **Host rewrite at doorman (blocking question for the rollout)**:
   classification keys off the Host bouine receives. If doorman rewrites
   `Host` when proxying to bouine-L2, Backmarket must write class patterns
   against the rewritten value (per Maxou: front-apps target
   `doorman.doorman-<env>-<continent>.svc.cluster.local`, which suggests
   doorman preserves the downstream Host header — but verify). Support
   for `X-Forwarded-Host` as the classification input is a possible
   follow-up knob if it does not.
2. **Class semantics doc**: the label is generic (`traffic_class`); the
   csr/ssr meaning is a Backmarket deployment convention. The ADR should
   say explicitly that bouine attaches no semantics to class names —
   future policy uses (per-class admission, shed priority, HPA scaling)
   would be new ADRs.
3. **Cardinality follow-through**: the runbook must restate the
   sparse-series budget with the class axis, and beyond the histogram
   target it must carry the §2.5 exception: `bouine_requests_total`
   crosses the hard 10k line at pools × (1 + #classes) > 57, so the
   runbook documents the guideline and the `metric_relabel_configs`
   drop pattern as the mitigation (documented pattern).
4. **Peer traffic**: peer fetches carry the forwarded Host, so they
   classify identically on both nodes; no extra plumbing, but the ADR
   should pin this as intended behavior.
5. **Cluster mode**: eventual vs strong mode does not affect
   classification (Host is always available at request ingress).
6. **Dashboard**: out of scope this round (maintainer decision); the
   in-repo dashboards' `label_values(bouine_requests_total, ...)` queries
   are unaffected by an added label.

## 5. Queries This Enables

```promql
# Hit ratio per rendering mode (the doorman-analysis replica):
sum by (traffic_class) (rate(bouine_requests_total{cache_result="HIT"}[5m]))
/ sum by (traffic_class) (rate(bouine_requests_total[5m]))

# p95 per mode:
histogram_quantile(0.95,
  sum by (le, traffic_class) (rate(bouine_request_duration_seconds_bucket[5m])))

# Origin bandwidth saved per mode:
sum by (traffic_class) (rate(bouine_response_bytes_total{source!="",source!="origin"}[5m]))
```
