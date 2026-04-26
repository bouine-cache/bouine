# Dependency allow-list

This document is the source of truth for third-party Go dependencies
allowed in the bouine module. New entries require an ADR (see
`docs/decisions/`) and a PR reviewer with the `deps` label
(`AGENTS.md §5`).

A dependency must justify itself on at least one of:

- Solves a problem the stdlib does not.
- Standard, battle-tested implementation that would take meaningful
  effort to reimplement (and would not be more correct if we did).
- Pre-approved core in `AGENTS.md §5`.

Banned by default: ORMs, runtime DI containers, log libraries that
aren't `slog`-compatible, HTTP servers other than
`net/http`/`fiber`/`quic-go`, any LGPL / AGPL code.

## Allow-list

| Module                                          | License    | Used by                  | Reason |
|-------------------------------------------------|------------|--------------------------|--------|
| `github.com/spf13/cobra`                        | Apache-2.0 | `cmd/bouine`             | CLI framework chosen in `PLAN.md`. |
| `github.com/spf13/pflag`                        | BSD-3      | (transitive)             | Cobra dependency. |
| `github.com/hashicorp/memberlist`               | MPL-2.0    | `internal/cluster`       | Gossip membership (ADR-0007). Pre-approved `AGENTS.md §5`. |
| `github.com/hashicorp/go-msgpack/v2`            | MIT        | (transitive)             | memberlist serialisation. |
| `github.com/hashicorp/go-sockaddr`              | MPL-2.0    | (transitive)             | memberlist network addressing. |
| `github.com/hashicorp/go-metrics`               | MIT        | (transitive)             | memberlist telemetry. |
| `github.com/hashicorp/errwrap`                  | MPL-2.0    | (transitive)             | memberlist error wrapping. |
| `github.com/hashicorp/go-multierror`            | MPL-2.0    | (transitive)             | memberlist multi-error. |
| `github.com/hashicorp/go-immutable-radix`       | MPL-2.0    | (transitive)             | memberlist internal. |
| `github.com/hashicorp/golang-lru`               | MPL-2.0    | (transitive)             | memberlist internal LRU. |
| `github.com/miekg/dns`                          | BSD-3      | (transitive)             | memberlist DNS lookup. |
| `github.com/sean-/seed`                         | MIT        | (transitive)             | memberlist RNG seed. |
| `github.com/quic-go/quic-go`                    | MIT        | `internal/listener`      | HTTP/3 listener (ADR-0002). |
| `github.com/prometheus/client_golang`           | Apache-2.0 | `internal/observability` | Prometheus metrics + handler. Pre-approved in `AGENTS.md §5`. |
| `github.com/prometheus/client_model`            | Apache-2.0 | (transitive)             | client_golang dependency. |
| `github.com/prometheus/common`                  | Apache-2.0 | (transitive)             | client_golang dependency. |
| `github.com/prometheus/procfs`                  | Apache-2.0 | (transitive)             | Process collector. |
| `github.com/beorn7/perks`                       | MIT        | (transitive)             | Histogram quantile algorithm. |
| `github.com/cespare/xxhash/v2`                  | MIT        | (transitive)             | Pulled by Prometheus; will be a direct dep in phase 2 (cache keys). |
| `github.com/munnerz/goautoneg`                  | BSD-3      | (transitive)             | Prometheus content negotiation. |
| `github.com/inconshreveable/mousetrap`          | Apache-2.0 | (transitive)             | Cobra dependency. |
| `google.golang.org/protobuf`                    | BSD-3      | (transitive)             | Prometheus expfmt. |
| `golang.org/x/sync`                             | BSD-3      | `internal/runtime/supervised` | errgroup. Pre-approved in `AGENTS.md §5`. |
| `golang.org/x/crypto`                           | BSD-3      | (transitive)             | Standard extended crypto. |
| `golang.org/x/net`                              | BSD-3      | `internal/listener`      | HTTP/2 server configuration. h2c was replaced by native Go 1.24+ Protocols API. |
| `golang.org/x/sys`                              | BSD-3      | (transitive)             | Low-level syscalls. |
| `golang.org/x/text`                             | BSD-3      | (transitive)             | Unicode handling. |
| `gopkg.in/yaml.v3`                              | MIT + Apache-2.0 | `internal/config`   | YAML config parsing. Standard for Go config files. |
| `github.com/fsnotify/fsnotify`                  | BSD-3      | `internal/config`        | Filesystem change notifications for config hot-reload. |

> **Fiber removed in ADR-0006.** `gofiber/fiber/v3`, `gofiber/utils`,
> `gofiber/schema`, `valyala/fasthttp`, `valyala/bytebufferpool`,
> `tinylib/msgp`, `philhofer/fwd`, `andybalholm/brotli`,
> `klauspost/compress`, `mattn/go-colorable`, `mattn/go-isatty`,
> `google/uuid` — all dropped. The admin surface now runs on
> `net/http.ServeMux`.

### Planned additions (per phase)

The following are pre-approved in `AGENTS.md §5` but will only be
added when the corresponding phase code lands. Adding them earlier
without code requires a justification in the PR.

| Module                                       | Phase | Purpose |
|----------------------------------------------|-------|---------|
| `github.com/quic-go/quic-go`                 | 1     | HTTP/3 listener (see ADR-0002). |
| `github.com/hashicorp/memberlist`            | 4     | Gossip membership. |
| `go.opentelemetry.io/otel`                   | 1     | Tracing. |
| `github.com/stretchr/testify`                | any   | Test assertions (tests only). |
| `golang.org/x/sync`                          | 1     | errgroup, semaphore. |
| `golang.org/x/vuln`                          | 0     | govulncheck (tool, not module dep). |

## Review cadence

- Every dependency is reviewed on bump (Dependabot opens PRs weekly).
- The full allow-list is reviewed at the end of each phase.
- A CVE in any entry above triggers a security review pass against the
  threat model.
| `github.com/a-h/templ`                          | MIT        | `internal/dashboard/templates` | Type-safe HTML templating for the operator dashboard. |
| `github.com/fsnotify/fsnotify`                  | BSD-3      | `internal/config`              | File-system watcher for SIGHUP + fsnotify hot reload. |
| `gopkg.in/yaml.v3`                              | MIT + Apache-2.0 | `internal/config`    | YAML config parsing. Chosen for strict mode and good error messages. |
| `go.opentelemetry.io/otel`                      | Apache-2.0 | `internal/observability/tracing` | OTel API; no-op by default, wired to OTLP exporter via config. |
| `go.opentelemetry.io/otel/trace`                | Apache-2.0 | `internal/observability/tracing` | OTel trace types. |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp` | Apache-2.0 | (planned) | OTLP/HTTP exporter for Jaeger/Tempo. Pending C1. |
