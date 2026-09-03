# Agent work queue

This file is the live coordination log between AI agents working on
`bouine`. Per `AGENTS.md §15`, an agent **must** claim an entry here
before starting non-trivial work, and one agent owns a package at a
time.

Format (newest at the top):

```markdown
- [STATUS] agent-id — packages — task — started: YYYY-MM-DD HH:MM UTC — ETA: …
```

`STATUS` is one of: `WIP`, `BLOCKED`, `DONE`, `ABANDONED`.

When you finish a claim, move it to the **Recently completed** section
at the bottom. Entries older than 30 days may be pruned.

## Active claims

- [WIP] crush — release 0.5.7 (changelog curation + promotion, chart bump) — started: 2026-09-03 — ETA: same day

## fasthttp migration — phase claims

Reference: [Issue #521](https://github.com/bouine-cache/bouine/issues/521) — full migration plan.

| Phase | Description | Agent | Status | Target packages |
|---|---|---|---|---|
| 0 | Foundation (deps, interfaces, transport pkg, OTel carrier) | unassigned | not started | `pkg/api`, `internal/transport`, `internal/observability/tracing` |
| 1 | Data-plane server + h1parser rewrite + TCP_NODELAY | unassigned | not started | `internal/server`, `internal/server/h1parser` |
| 2 | Cache handler rewrite | unassigned | not started | `internal/cache` |
| 3 | Zero-copy origin response capture | unassigned | not started | `internal/cache` (response capture path) |
| 4 | Streaming origin responses | unassigned | not started | `internal/cache` (streaming, headerGuard elimination) |
| 5 | Origin / upstream | unassigned | not started | `internal/origin` |
| 6 | Cluster + peer pipelining | unassigned | not started | `internal/cluster` |
| 7 | Admin / observability / dashboard | unassigned | not started | `internal/admin`, `internal/observability`, `internal/dashboard`, `internal/staticfile`, `internal/cloudflare` |
| 8 | SDK / header / config | unassigned | not started | `pkg/bouineapi`, `pkg/header`, `web/dashboard`, `internal/config` |
| 9 | Engine wiring | unassigned | not started | `cmd/bouine/cmd` |
| 10 | Tests / CI | unassigned | not started | all `_test.go` files, `.golangci.yaml`, `bench/`, `test/` |
| 11 | Docs / cleanup / GC tuning | unassigned | not started | `docs/`, `AGENTS.md`, runbook |

**Parallelization:** Phases 5-8 can run in parallel after Phase 2 lands the `fasthttp.RequestHandler` interface. Phases 3-4 depend on Phase 5 but can overlap with 6-8. One agent per package at a time. PR size limit: 400 changed lines (AGENTS.md §15.4).

## Recently completed

- [DONE] crush — internal/cache, internal/server/h1parser, internal/origin, pkg/header — SSE support: hinted dispatch, live unbuffered streaming, idle read/write deadlines, per-event flush, tests (ADR-0042) — 2026-09-03

- [DONE] crush — internal/observability, internal/cache, pkg/header — MISS-path batch 2: middleware byte classification, ToMap skip, non-interning SetEntryRaw, sharded singleflight, cacheKey logging gate (PR pending) — 2026-08-29

- [DONE] crush — internal/cache (miss-path fetch), internal/origin (PoolFastClient) — MISS-path alloc reduction: SwapBody body ownership + deadline-based fetch timeout (PR #556) — 2026-08-29

- [DONE] crush — pre-flight — phase 1 prep (config, supervised, tlsutil, metrics, pkg/api, integration skeleton, ADRs 0002-0005) — 2026-05-19
- [DONE] crush — repo-bootstrap — phase 0 scaffolding (toolchain, hooks, Cobra entry, Fiber `/healthz`) — 2026-05-19
