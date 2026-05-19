# Cache-tests conformance harness

This directory wires
[`http-tests/cache-tests`](https://github.com/http-tests/cache-tests)
against a running `bouine` instance. The test runner boots bouine + an
origin, then executes the upstream harness and records the score.

> Status: **stub**. The structure is pinned so CI can reference it.
> Implementation lands alongside the first cache-tests PR.

## Running

```bash
make conformance
```

## Layout

```
test/cachetests/
├── README.md
├── run.sh          # boots bouine + origin, runs the harness, reports
└── results/        # CI writes JSON score reports here (gitignored)
```

## Exit criteria (PLAN.md §15 phase 3)

> ≥ Varnish score on cache-tests; canonical bench within 10 % of
> Varnish RPS on the same hardware.

The score is published as a JSON badge in CI and blocks regression.
