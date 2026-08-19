# ADR-0006: Drop Fiber, unify admin on net/http

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 1

## Context

The original design (`docs/architecture.md §2.1`) used Fiber v3 for the admin
control-plane surface. Fiber wraps `valyala/fasthttp`, which is a
complete reimplementation of HTTP/1.1 parsing, buffer management, and
connection pooling — separate from Go's `net/http`.

This gave the daemon **three distinct HTTP stacks**:

1. `net/http` — data plane: HTTP/1.1, HTTP/2 (via `golang.org/x/net/http2`).
2. `quic-go/http3` — data plane: HTTP/3.
3. `valyala/fasthttp` (via Fiber) — admin plane only.

The admin surface serves ≤ 10 RPS of operator traffic (`/healthz`,
`/readyz`, `/metrics`, `/v1/purge`). It is not on the hot path and
does not benefit from fasthttp's allocation model.

Meanwhile Fiber's dependency footprint was:

- `github.com/gofiber/fiber/v3`
- `github.com/gofiber/utils/v2`
- `github.com/gofiber/schema`
- `github.com/valyala/fasthttp`
- `github.com/valyala/bytebufferpool`
- `github.com/tinylib/msgp`
- `github.com/philhofer/fwd`

Seven dependencies — each tracked by Dependabot, each a potential CVE
surface — for a handful of admin endpoints.

Fiber also required `adaptor.HTTPHandler(...)` to bridge `net/http`
handlers (Prometheus, pprof) into Fiber, adding runtime overhead and
a mental-model tax.

Since Go 1.22, `net/http.ServeMux` supports method + pattern routing
(`"GET /healthz"`) which eliminates the last ergonomic reason to use
Fiber for this surface.

## Decision

Drop Fiber entirely. Rewrite `internal/admin` on `net/http.ServeMux`.

The daemon now runs on **two** HTTP implementations:

1. `net/http` — admin plane AND data plane (H1 + H2).
2. `quic-go/http3` — data plane only (H3).

Both share `http.Handler`. The admin server is a plain `*http.Server`
on its own port, using the same lifecycle pattern as the data-plane
listeners (ADR-0004).

## Consequences

### Positive
- 7 fewer dependencies; smaller SBOM and govulncheck surface.
- One HTTP handler model for the entire daemon. Middleware (access log,
  metrics, auth) is plain `func(http.Handler) http.Handler` everywhere.
- Tests use `httptest.NewRecorder` + `handler.ServeHTTP` uniformly.
- No `adaptor` bridge for Prometheus or pprof — they're already
  `http.Handler`.
- The admin server now supports HTTP/2 (via `net/http`) automatically
  if TLS is added later — Fiber could not.
- One fewer HTTP stack to profile, debug, and keep in mental context.

### Negative / trade-offs
- Fiber's middleware ecosystem (CORS, rate-limit, recover, etc.) is
  no longer available. Mitigated: we'll hand-write the small admin
  middleware we need (auth, recover) — they are trivial in `net/http`.
- Fiber's `ctx.JSON(...)` one-liner is replaced by a small `writeJSON`
  helper — 4 lines of code.

### Risks
- None notable. The admin surface is simple; `net/http.ServeMux` is
  battle-tested.

## Alternatives considered

- **Keep Fiber, accept the third stack.** Rejected: maintenance cost
  outweighs ergonomic benefit for 5 endpoints.
- **Use a lightweight router (chi, gorilla/mux).** Rejected: Go 1.22+
  `ServeMux` has method + pattern matching; no external router needed.
- **Use Fiber for the data plane too.** Rejected: Fiber/fasthttp
  cannot speak HTTP/2 or HTTP/3 (AGENTS.md §2 non-negotiable).

## References

- `AGENTS.md §2` (Fiber never on data plane)
- `docs/architecture.md §2.1` (updated to remove Fiber)
- ADR-0004 (per-listener http.Server model)
- Go 1.22 release notes: enhanced ServeMux routing
