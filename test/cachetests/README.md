# Cache-tests conformance harness

This directory wires the
[`http-tests/cache-tests`](https://github.com/http-tests/cache-tests)
suite against a running `bouine` instance. The harness:

1. Builds `bouine` via `make build`.
2. Clones (or updates) the cache-tests repo into `.cache-tests/`.
3. Starts the cache-tests Node.js origin server on port 8000.
4. Starts `bouine` proxying to the origin on port 8080.
5. Runs the cache-tests CLI against `bouine`.
6. Saves JSON results to `results/bouine.json`.
7. Prints a pass/fail/setup-failure summary.

## Prerequisites

- Go 1.26+ (to build bouine)
- Node.js 18+ and npm (for the cache-tests harness)

## Running

```bash
make conformance           # from repo root
bash test/cachetests/run.sh  # directly
```

## Results

Results are saved as `results/bouine.json` (gitignored). CI uploads
them as an artifact on every run so the score is visible in the
Actions tab.

The JSON format matches what `cache-tests.fyi` expects. Key fields
per test:

- `result: true` — pass (✅)
- `result: false` — conformance failure (⛔️)
- `result: "setup_error"` — harness setup failure (🔹)

## CI integration

The `conformance` job in `.github/workflows/ci.yml` runs after the
build job. It:

- Sets up Go + Node.js.
- Runs `make conformance`.
- Uploads `results/` as an artifact regardless of outcome.

The job does **not** fail CI on test failures (we're working toward
Varnish parity, not there yet). It does fail on script-level errors
(bouine won't start, origin won't start, etc.).

## Important: PUT must be proxied

The cache-tests harness uses `PUT` requests to synchronize state
between client and origin. bouine must pass `PUT` through to the
origin without caching it. The cache handler already does this —
`PUT` is an invalidating method and is forwarded directly.

## Exit criteria (`docs/architecture.md §15` phase 3)

> ≥ Varnish score on cache-tests.

The score is tracked as a CI artifact. Regressions are flagged
manually until the automated regression gate lands in phase 4.5.
