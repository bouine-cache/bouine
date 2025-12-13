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
| `github.com/gofiber/fiber/v3`                   | MIT        | `internal/admin`         | Control plane (admin API). Never on the data plane (`AGENTS.md §2.2`). |
| `github.com/gofiber/utils/v2`                   | MIT        | (transitive)             | Fiber dependency. |
| `github.com/gofiber/schema`                     | MIT        | (transitive)             | Fiber dependency. |
| `github.com/valyala/fasthttp`                   | MIT        | (transitive)             | Fiber dependency. |
| `github.com/valyala/bytebufferpool`             | MIT        | (transitive)             | fasthttp dependency. |
| `github.com/klauspost/compress`                 | BSD-3      | (transitive)             | Fiber/fasthttp dependency; will be reused directly when compression policy lands (phase 3). |
| `github.com/andybalholm/brotli`                 | MIT        | (transitive)             | Fiber dependency. |
| `github.com/mattn/go-colorable`                 | MIT        | (transitive)             | Fiber dependency. |
| `github.com/mattn/go-isatty`                    | MIT        | (transitive)             | Fiber dependency. |
| `github.com/google/uuid`                        | BSD-3      | (transitive)             | Fiber dependency. |
| `github.com/inconshreveable/mousetrap`          | Apache-2.0 | (transitive)             | Cobra dependency. |
| `github.com/philhofer/fwd`                      | MIT        | (transitive)             | tinylib/msgp dependency. |
| `github.com/tinylib/msgp`                       | MIT        | (transitive)             | Fiber dependency. |
| `github.com/prometheus/client_golang`           | Apache-2.0 | `internal/observability` | Prometheus metrics + handler. Pre-approved in `AGENTS.md §5`. |
| `github.com/prometheus/client_model`            | Apache-2.0 | (transitive)             | client_golang dependency. |
| `github.com/prometheus/common`                  | Apache-2.0 | (transitive)             | client_golang dependency. |
| `github.com/prometheus/procfs`                  | Apache-2.0 | (transitive)             | Process collector. |
| `github.com/beorn7/perks`                       | MIT        | (transitive)             | Histogram quantile algorithm. |
| `github.com/cespare/xxhash/v2`                  | MIT        | (transitive)             | Pulled by Prometheus; will be a direct dep in phase 2 (cache keys). |
| `github.com/munnerz/goautoneg`                  | BSD-3      | (transitive)             | Prometheus content negotiation. |
| `google.golang.org/protobuf`                    | BSD-3      | (transitive)             | Prometheus expfmt. |
| `golang.org/x/sync`                             | BSD-3      | `internal/runtime/supervised` | errgroup. Pre-approved in `AGENTS.md §5`. |
| `golang.org/x/crypto`                           | BSD-3      | (transitive)             | Standard extended crypto. |
| `golang.org/x/net`                              | BSD-3      | (transitive, future)     | H2 server hooks; will be a direct dep in phase 1. |
| `golang.org/x/sys`                              | BSD-3      | (transitive)             | Low-level syscalls. |
| `golang.org/x/text`                             | BSD-3      | (transitive)             | Unicode handling. |
| `gopkg.in/yaml.v3`                              | MIT + Apache-2.0 | `internal/config`   | YAML config parsing. Standard for Go config files. |

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
