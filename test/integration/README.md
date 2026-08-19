# Integration harness

This directory holds the bouine integration test harness. Scenarios
boot a small docker-compose stack (echo origin + bouine) and drive
traffic through it.

> Status: **skeleton**. Tests are gated by the `integration` build tag
> and ignored by `make test`. They run from `make integration`, which
> lands in phase 4 (`docs/architecture.md §15`). Until then this directory pins the
> structure so phase-1 listener PRs can drop scenarios in without
> bikeshedding the layout.

## Layout

```
test/integration/
├── README.md
├── docker-compose.yaml      # bouine + origin services
├── origin/
│   └── main.go              # echo origin used by every scenario
├── driver/
│   └── driver.go            # shared Go driver (build, boot, teardown)
├── h1_test.go               # phase 1
├── h2_test.go               # phase 1
├── h3_test.go               # phase 1
├── purge_test.go            # phase 4
├── cluster_test.go          # phase 4
└── chaos_test.go            # phase 4.5
```

## Running

```bash
make integration                   # full suite
go test -tags=integration ./...    # by hand
```

Every test file MUST start with:

```go
//go:build integration
// +build integration
```

so it is excluded from the default `go test ./...`.

## Conventions

- Tests bring up the stack fresh per package; no shared state.
- Random ports — never hardcode ports above the bouine admin default.
- Use `internal/testutil/tlsutil.WriteCertFiles` to materialize the
  TLS cert files the bouine binary will load.
- Failures dump bouine + origin logs to the test output (see
  `driver.Stack.Dump`).

## Phase-1 expectations

The phase-1 exit criterion in `docs/architecture.md §15` is:

> `bouine serve` proxies traffic on all 3 protocols; integration tests
> show parity with `curl --http1.1/--http2/--http3`.

The matching scenarios live in `h1_test.go`, `h2_test.go`, `h3_test.go`.
Each fires GET / HEAD / POST through bouine and against the origin
directly, then asserts headers, body, and status match.
