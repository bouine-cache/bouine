# ADR-0036: Header type migration — local canonicalHeaderKey

Date: 2026-08-22

## Status

Accepted

## Context

The `pkg/header` package used `http.CanonicalHeaderKey` from `net/http`
for case-insensitive header key canonicalization. With ADR-0034
dropping `net/http` as the HTTP stack, importing `net/http` solely for
header key canonicalization introduces an unnecessary dependency.

## Decision

Implement a local `canonicalHeaderKey` function that produces the same
output as `net/http.CanonicalHeaderKey`: uppercase the first letter of
each dash-separated word, lowercase the rest. This is a 15-line function
with no external dependencies.

`FromHTTP` and `WriteTo` (which take `http.Header`) are retained with
`nolint:depguard` because the cache handler still uses `http.Handler` as
its upstream interface. `FromFastHTTP` and `WriteToFastHTTP` are added
for fasthttp-native callers.

## Consequences

- `pkg/header` no longer depends on `net/http` for canonicalization.
- Complete: the `FromHTTP`/`WriteTo` net/http shims were removed once the
  cache handler migrated to fasthttp-native types; the fasthttp-native
  `FromFastHTTP`/`WriteToFastHTTP` pair is the only remaining surface.
- Header key canonicalization is identical to `net/http` — verified by
  tests that compare against the same canonicalization rules.
