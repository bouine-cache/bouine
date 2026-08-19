# ROADMAP.md — bouine Roadmap (2026)

This document describes what bouine intends to do — and not do — over
the next 12 months. It is a direction guide, not a commitment; the
purpose is to help users and contributors understand where the project
is heading.

Phases 0–6 (core caching, clustering, negative caching, jittered TTLs,
soft-purge, Go SDK) are **complete and tagged**. See
[`CHANGELOG.md`](CHANGELOG.md) for shipped releases and
[`docs/architecture.md`](docs/architecture.md) for the architecture
reference.

---

## 1. Multilayer caching with orchestrated invalidation

**Goal:** Support hierarchical cache topologies (cache-of-caches) where
bouine nodes form tiers (edge → regional → origin-shield) with
propagating invalidation.

- Parent-fetch protocol with loop detection via `Via` / `X-Bouine-Tier`
  headers.
- Orchestrated purge and ban propagation across tiers — an invalidation
  at the edge cascades to parent tiers, and vice versa.
- Per-tier TTL and freshness overrides so upstream tiers can serve
  longer freshness than edges.
- Backward-compatible: single-tier deployments are unaffected.

**Non-goals for v1:** Cross-cluster federation and regional tiering
beyond two levels.

**Target:** v1.2

---

## 2. Authentication mechanisms (auth_request)

**Goal:** Add an `auth_request`-style external authentication module,
similar to NGINX's `auth_request` directive, so bouine can delegate
request authorization to an external service before serving from cache.

- Per-route `auth_request` config pointing to an external auth endpoint.
- Subrequest is issued on cache miss and on first request per session;
  cached responses are served without re-authenticating until TTL expiry.
- Support for forwarding auth result headers to the origin and injecting
  them into the cached response.
- Pluggable auth providers: external HTTP, JWT validation, OAuth
  introspection (v1.1+ for JWT/OAuth).

**Non-goals for v1:** bouine is not an identity provider. It delegates
authentication; it does not issue tokens or manage sessions.

**Target:** v1.1

---

## 3. Migration to fasthttp with HTTP/2 (C10k mitigation)

**Goal:** Replace `net/http` on the data plane with
[`valyala/fasthttp`](https://github.com/valyala/fasthttp) to address the
C10k problem and reduce per-connection overhead at high concurrency.

- Evaluate `fasthttp` HTTP/2 support (or a compatible HTTP/2 layer) to
  ensure no protocol regression vs. `net/http`.
- Preserve RFC 9111 compliance and the `cache-tests` conformance score.
- Maintain the zero-alloc hit path; benchmark against the current
  `net/http` baseline to prove the C10k improvement.
- Admin surface stays on `net/http.ServeMux` (not affected by C10k).

**Non-goals:** The admin server is not migrated. HTTP/3 remains deferred.

**Target:** v1.1–v1.2 (research spike in v1.1, implementation in v1.2
if benchmarks justify the switch)

---

## 4. Additional eviction algorithms

**Goal:** Expand the eviction policy catalog beyond the current LRU and
Sieve implementations.

- **LFU (Least Frequently Used)** — track access frequency with a
  Count-Min Sketch to stay memory-efficient.
- **W-TinyLFU** — windowed TinyLFU for workload-adaptive eviction.
- **SLRU (Segmented LRU)** — protect recently-promoted objects from
  being evicted by a scan burst.
- Pluggable eviction interface already exists (`internal/cache/evictor`);
  each algorithm is a new implementation of the interface.
- Per-route eviction policy selection so high-churn routes can use a
  different algorithm than stable-content routes.

**Non-goals:** Machine-learning-based eviction is deferred to the
AI traffic analysis phase.

**Target:** v1.1 (LFU, SLRU), v1.2 (W-TinyLFU)

---

## 5. Detailed performance benchmarks vs. other projects

**Goal:** Publish a comprehensive, reproducible benchmark suite
comparing bouine against Varnish, NGINX, Envoy, and Traefik on the same
hardware with the same upstream.

- Single-node scenarios: throughput ramp, cache-hit-only baseline,
  cache-miss storm, working-set overflow, mixed realistic traffic.
- Multi-node scenarios: horizontal scaling (1–10 nodes), gossip
  convergence under load, rolling updates under traffic.
- Stress scenarios: connection exhaustion (1k → 50k concurrent), large
  body streaming, Vary blow-up.
- All results published with percentile tables, HDR histograms, and
  reproducible scripts under `bench/loadtest/`.
- Automated nightly runs on a pinned self-hosted runner; results
  published to `bench/loadtest/results/`.

**Target:** v1.0 (initial matrix), ongoing (quarterly refresh)

---

## 6. Blog and content

**Goal:** Open a blog on [bouine.org](https://bouine.org) to share
tips, tricks, operational lessons, and technical deep dives related to
bouine and HTTP caching in general.

- Initial content plan:
  - "Why we built bouine" — origin story and design philosophy.
  - "RFC 9111 in practice" — how cache directives map to bouine config.
  - "Zero-alloc hit path" — engineering deep dive with benchmarks.
  - "Migrating from Varnish to bouine" — walkthrough with real configs.
  - "Cluster gossip under load" — how membership and peer-fetch work.
  - "Tuning eviction for your workload" — choosing between LRU, Sieve,
    LFU, and SLRU.
- Community contributions welcome; posts are reviewed by maintainers.
- Source content lives in the `bouine-documentation` repository;
  published via the existing docs site pipeline.

**Non-goals:** The blog is not a marketing channel. It is technical
content for operators and contributors.

**Target:** v1.0 (launch with 2–3 posts), ongoing (monthly cadence)

---

## Deferred (post-v1.2)

Features deliberately deferred beyond the 12-month horizon:

- **HTTP/3** — deferred until demand materializes; `quic-go` removed.
- **VCL shim** — parser + lowering pass for a VCL 4.1 subset. Will be
  revisited when Varnish migration demand justifies the effort.
- **gRPC caching** — passthrough only; no caching of gRPC responses.
- **WebSocket caching** — never; passthrough only.
- **Forward-proxy mode** — bouine is a reverse cache only.
- **Multi-tenant routing scopes** — operators run one deployment per
  tenant today; scoped multi-tenancy is a future candidate.
- **AI traffic analysis** — streaming analytics layer with ML-assisted
  cache suggestions; post-v1.2 when the core is stable.
- **ESI-lite** — `<esi:include>` support; deferred unless demand grows.
- **Backup/restore of ban list** — v1.1 candidate, may slip.

---

## Definition of Done (v1.0)

- [x] Hit-path allocs/op = 0 on the canonical benchmark.
- [x] `cache-tests` score >= Varnish (current: 93.7%).
- [x] Canonical benchmark within 10% of Varnish RPS, CI-gated.
- [x] No orphaned / unwired production packages.
- [x] Threat model zero unaddressed `Txx` without a deferral.
- [x] Helm chart published.
- [x] Migration guides (NGINX, Varnish) in `docs/migration/`.
- [ ] Phase 7 simplification complete (`engine.go` <= 300 LOC).
- [ ] Benchmark baseline refreshed.
- [ ] Initial competitive benchmark matrix published.
- [ ] Blog launched with first posts.
