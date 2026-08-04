# ADR-0028: Use testify for test assertions

- **Status**: Accepted
- **Date**: 2026-08-04
- **Deciders**: @thylong
- **Phase**: 2

## Context

The test suite used hand-rolled assertion helpers (`t.Fatalf` with
`%v want %v` format strings) and four bespoke comparison helpers
(`assertPurgeEqual`, `assertBanEqual`, and `assertMetricExists`-style
duplicates). The bespoke helpers drift from the fields they compare:
`internal/cluster/codec_test.go:151-186` reimplemented `Equal`
*bespoke-style* for `time.Time` (it used `.Equal()` for instant
comparison, which was correct but inconsistent with whole-struct
comparison — the loc-pointer trap described below was avoided by
not using whole-struct `Equal` in the first place).

`testify` is listed under "Planned additions" in `docs/deps.md`, which
`AGENTS.md §5` treats as pre-approved for test-only use. This ADR
records *why and how* it is adopted so the allow-list entry has the
required justification.

## Decision

Adopt `github.com/stretchr/testify` (`require` + `assert` packages
only) for test assertions across all `*_test.go` files.

### require-vs-assert convention

- **`require` for preconditions.** Use `require.*` when failure makes
  every subsequent assertion meaningless: decode/setup/parse before the
  thing under test, nilness before a deref, table-row setup inside
  `t.Run`. `require` calls `t.FailNow` and aborts the test.
- **`assert` for observations.** Use `assert.*` for the actual
  observations the test verifies, so one test reports multiple
  independent failures instead of stopping at the first.
- Inside `t.Run` subtests: `require` for table setup, `assert` for the
  per-case expectation, so one bad row doesn't abort the whole table.
- **Never call `require` inside a goroutine that doesn't own `t`.** It
  calls `t.FailNow` which must run on the test's goroutine. Convert
  such sites to `assert` (non-fatal) or channel the result back to the
  main goroutine for `require`.

### time.Time comparison exception

`require.Equal` uses `reflect.DeepEqual`, which compares
`time.Time.loc *Location` by pointer. Decoders that rebuild timestamps
via `time.Unix(0, nano)` yield `loc = time.Local`; test fixtures built
with `time.Date(..., time.UTC)` have `loc = time.UTC`. UTC ≠ Local →
`require.Equal` on a whole struct containing `time.Time` **fails** on
instant-equal times. This is not nanosecond drift — `UnixNano()`
round-trips nanos exactly — it is location-pointer inequality.

**Rule:** for any struct containing `time.Time`, do **not** use
whole-struct `require.Equal`. Normalize both sides to UTC first
(`got.T = got.T.UTC(); want.T = want.T.UTC()`) then `require.Equal`, or
keep a thin helper asserting the time field with
`require.True(t, got.T.Equal(want.T))` alongside `require.Equal` for
the non-time fields. Prefer normalization — the decoder's `time.Local`
is an implementation detail tests shouldn't observe.

### Sentinel error identity

`require.ErrorIs` unwraps; `!=` does not. Default to
`require.Equal(t, sentinel, err)` to preserve exact identity. Reserve
`require.ErrorIs` for cases where production code already wraps the
sentinel; audit each call site during migration, do not assume.

### Banned sub-packages

Only `require` and `assert` are approved. `suite`, `mock`, and
`httpmock` are **not** adopted — the deps approval is for assertions
only. Test scaffolding (`t.Parallel`, `t.Run`, `t.Helper`, `t.Skip`,
`t.TempDir`, `t.Cleanup`) stays as-is; testify is additive.

## Consequences

### Positive

- Deletes 4 hand-rolled assertion helpers that mimic testify poorly.
- Enables `testifylint` in a follow-up PR to catch the class of bug
  where `assert.Equal(t, got, want)` is called with `*testing.T` from a
  non-test goroutine, and to enforce require-vs-assert discipline
  mechanically.
- Auto-diffs on failure for struct comparisons, replacing hand-written
  `%d want %d` format strings that drift from the fields being
  compared.

### Negative / trade-offs

- One new test-only dependency (MIT). No runtime impact — testify is
  not linked into the daemon binary.
- Diff noise in CI logs for large struct comparisons: mitigate with
  `assert.Contains` or `assert.True(t, bytes.Equal(...))` for large
  blobs.

### Risks

- `time.Time` location-pointer inequality (handled per the rule above).
- Sentinel identity weakening if `ErrorIs` is used where `!=` was
  intended (handled by defaulting to `Equal`).

## Alternatives considered

1. **Keep hand-rolled helpers.** Rejected: they drift from the fields
   they compare and the `codec_test.go` one was actively wrong about
   `time.Time` semantics.
2. **Adopt `gotestyourself/assert` instead.** Rejected: smaller
   ecosystem, no `testifylint` equivalent, less familiar to
   contributors.
3. **Adopt `suite`/`mock` as well.** Rejected: `suite` adds a
   global-ish test runner pattern that conflicts with `t.Parallel`
   subtests; `mock` encourages brittle call-sequence assertions. The
   assertion-only subset is the value.

## References

- `docs/deps.md` — allow-list entry for testify.
- `AGENTS.md §5` — Dependency Policy (pre-approved core).
- `AGENTS.md §8` — Testing Rules.
