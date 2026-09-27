# Plan: Accept-Encoding Bucketing for Variant Keys

Status: Draft
Date: 2026-09-26
Depends on: ADR-0046 (include_headers union), ADR-0049 (variant metadata)
Target package: `internal/cache` (primary), `cmd/bouine/cmd` (config wiring)

## 1. Problem Statement

`docs/architecture.md` §3.3 claims responses are keyed by bucketing
`Accept-Encoding` into `br | zstd | gzip | identity`. The claim is false.
What exists is token sorting:

- `internal/cache/key.go:424` (`buildVaryKeyInto`) sorts comma-separated
  tokens for `Accept-Encoding`/`Accept-Language`/`Accept`/`Accept-Charset`
  (`isListValuedVaryField`, key.go:435).
- `internal/cache/vary.go:211` (`normalizeHeaderValue`) does the same sort
  (lowercase + sort) for the fast-path variant-key core.

Sorting collapses ordering differences only. Measured with a scratch probe
against `VariantKey` (`internal/cache/vary.go:78`):

```
8 realistic browser Accept-Encoding strings -> 6 distinct cache variants
  variant a5e7276…: [gzip, deflate, br] [br, gzip, deflate] [deflate, gzip, br]
  variant 9bdfde0…: [gzip, deflate, br, zstd]
  variant 4555e6…: [gzip, deflate, br, zstd, identity]
  variant 40b7cab…: [gzip]
  variant 8218fd6…: [gzip, br]
  variant 4c4be45…: [identity]
```

Every distinct token *set* is a distinct stored variant for the same
resource. The consequences:

- **Origin load**: the first request per (URL × AE-set) is a MISS and a
  full origin fetch, even when the origin would have produced the same
  bytes (see §2.1 — most origins only offer gzip/br).
- **Hit-ratio loss**: an object stored under `gzip, deflate, br` cannot
  serve a request asking `gzip` even though every real origin answer for
  `gzip` is identical to the answer for `gzip, deflate, br`.
- **Vary-cap exhaustion**: each extra variant consumes one of the
  `MaxVariants = 64` slots per primary key (`vary.go:26`); a client
  population with 6 AE dialects burns 6 slots where 2 suffice.
- **SIEVE/cachaner dilution**: the same body duplicated 6 ways evicts
  6 entries' worth of hot-tier budget.

The RFC basis for fixing this beyond plain efficiency: RFC 9111 §4.1
explicitly permits normalizing a Vary-nominated header "in a way that is
known to have identical semantics" — q-value-aware encoding selection is
the canonical example (the RFC itself cites reordering and
case-normalization; encoding negotiation is the same class).

### 1.1 Why bucket, not canonicalize the whole header

### 1.1 Why bucket, not canonicalize the whole header

The correct bucketing for a shared cache is **negotiation outcome**, not
token set: which single encoding would this cache ask the origin for?
Every browser in practical circulation sends one of:

| Accept-Encoding | bucket |
|---|---|
| `gzip, deflate, br, zstd` | `br` |
| `gzip, deflate, br` | `br` |
| `br, gzip, deflate` | `br` |
| `gzip, br` | `br` |
| `gzip, deflate` | `gzip` |
| `gzip` | `gzip` |
| `identity` / empty / absent | `identity` |

Rule: pick the highest-preference coding (by q-value, ties broken
br > zstd > gzip — the sharing-maximizing order, so the two dominant
browser spellings, zstd-bearing and zstd-less, share the `br` bucket)
among the three compressions bouine keys on; an absent/empty AE hashes
to `identity`. Q-value `0` excludes.
**Deliberately NOT bucketed:** `deflate`, `identity`, and any other
token (see §1.2). Honest accounting of the probe's 8 strings: they
collapse to **3** buckets (`br` for all five br-capable spellings,
`gzip`, `identity`) — the zstd-bearing spellings no longer fork a
fourth variant. Variant count per resource is capped at the number of
distinct negotiated codings (≤ 4), independent of spelling zoo.

(The initial implementation shipped with zstd-preference ties and the
prose claimed 8→2; both were corrected post-estimate — the 2 ignored
the correctly-separate gzip and identity buckets, and zstd-preference
preserved the zstd/br fork that bucketing exists to collapse.)

### 1.2 Review correction: unknown codings are NOT "ignored"

The original draft said bucketing picks "the highest-preference
encoding the bucket set supports" and unknown codings are ignored.
That rule is wrong, and the failure mode is worth writing down because
it dictates the pairing rule in §2.1:

`Accept-Encoding: gzip, deflate` → bucket `gzip`. A deflate-only
origin (no gzip support) receives the *rewritten* header (§3.3) and
serves `Content-Encoding: deflate` bytes — which then live under the
`gzip` bucket and are served verbatim to a gzip-only client. Wrong
bytes, unfixable at egress.

The correct model is the one Varnish's builtin VCL uses for
`Accept-Encoding`: the bucket selects which *single canonical token*
bouine asks the origin for, and the bucket set is exactly the set of
codings bouine is willing to re-serve verbatim. If the origin cannot
produce that coding it falls back to `identity` — every client in the
bucket then gets identity bytes, consistently. `deflate` is dropped
from the bucket set (per-coding variant explosion is pointless for a
universally-compressible population: gzip/br/zstd cover all real
browsers, and a deflate-only origin served identity is correct, just
uncompressed).

This is also why the pairing rule (§2.1) is not an optional second
half: bucketing the key without rewriting the outbound header is
*incorrect*, not merely suboptimal.

## 2. Constraints

### 2.1 The origin-side pairing hazard

The cache key must reflect what the origin would send, not what the
client asked for. If bouine keys `gzip` and `gzip, deflate, br` requests to
the same `br` bucket but forwards each client's raw `Accept-Encoding` to
the origin, it stores the `gzip` answer under the `br` bucket and then
serves gzip bytes to a br-capable client. **A correct bucketing therefore
has two halves**:

1. Variant key = bucket(AE).
2. Origin-bound `Accept-Encoding` = a single canonical token derived from
   the bucket (the header rewrite half).

For origins that ignore AE entirely this is moot. For origins that
negotiate, half 2 is what makes half 1 correct. Since the response is
stored under the bucket of the *rewritten* request, and egress serves the
stored body verbatim with its stored `Content-Encoding`, a br-bucket
request always receives br bytes.

### 2.2 Cluster mixed-version hazard (the ADR-0046 class)

`BuildVaryKey`'s hex travels on the wire as
`PeerFetchRequest.VaryKey` (pkg/api/cluster.go:110), and the owner answers
`404` on mismatch (`internal/cluster/peerfetch.go:909`). Two nodes running
different bucketing code produce different VaryKey hex for the same
request: mixed-version nodes simply never share variants — a
**availability** degradation, never a wrong-body swap (the server-side
assertion gate rejects). This mirrors the `include_headers` mixed-config
risk documented in ADR-0046 and is why §6.4 prescribes the rollout order.

### 2.3 The hash-input builders (review-corrected count)

ADR-0046 described six hash-input constructions; the 2026-09 survey
of the current tree finds the live set is **two normalizers consumed
by four construction sites**:

- `normalizeHeaderValue` (vary.go:211) — called from
  `variantKeyCore` (vary.go:150) and `variantKeySlow` (vary.go:198).
- `normaliseListHeader`/`isListValuedVaryField` (key.go:424/435) —
  the `BuildVaryKey`/`buildVaryKeyInto` path (key.go:404), used by
  the peer gates (handler.go:1347, fastpath.go:333).

(`variantKeyFromRaw` no longer exists — fastpath.go:1000 documents it
as superseded by `VariantKeyFromRaw`, which routes through
`variantKeyCore`.) The dispatch fix therefore touches exactly **two
normalizer call paths**, but the parity-test obligation is unchanged:
every construction path must agree, or the cross-variant body-swap bug
class (ADR-0046) is back.

ADR-0046's core lesson: the variant-key hash input is constructed in six
places (`VariantKey`, `VariantKeyFast`, `variantKeySlow`,
`variantKeyFromRaw`-era inline copies, `BuildVaryKey`, and the
`variantKeyCore` generic). All six call **one** of two normalizers
(`normalizeHeaderValue` in vary.go:211 or the `isListValuedVaryField` +
`normaliseListHeader` branch in key.go:424). The fix must therefore live
**inside the two normalizers**, not in any single call site. Changing a
call site instead of the normalizer is precisely how the cross-variant
body-swap bug class is born (ADR-0046 "Alternatives considered").

### 2.4 Zero-alloc hit path

`normalizeHeaderValue` runs on the hit path (inside `variantKeyCore`) and
is covered by gate budgets: `[FastPath_PeerHitVary]=7` allocs,
`[Handler_CacheMiss_Cacheable]=18`. The bucketing must be zero-allocation
in the common case: stack scanning of the header value, no
`strings.Split`, no map, no `[]byte` → `string` conversions. The slow
buffer-overflow paths already exist (`variantKeySlow`) and stay.

### 2.5 What must NOT change

- The primary key (`BuildKey`) — untouched; bucketing applies only to the
  Vary-nominated `Accept-Encoding` value.
- `VaryValue` (the stored field list) — untouched; it names headers, not
  values.
- The `VaryKey` wire format — still the `BuildVaryKey` hex of the
  normalized values. Cluster compatibility is preserved by rollout order
  (§6.4), not format change.
- `isCacheBlocked`/`IsCacheable*` — cacheability is orthogonal to keying.
- `Content-Encoding`-based variant *validation* on egress — none exists
  today and adding one is out of scope (§7).

## 3. Design

### 3.1 New function: `encodingBucket`

```go
// encodingBucket reduces an Accept-Encoding header value to a single
// negotiated content coding: "br", "zstd", "gzip", or "identity".
// Zero-allocation: scans the value in place on the stack.
// Selection: highest q-value wins among {br, zstd, gzip}; ties broken
// br > zstd > gzip (sharing-maximizing; see §1.1). q=0 excludes.
// deflate/identity/unknown tokens contribute nothing (§1.2).
// Empty input -> "identity".
func encodingBucket(v string) string
```

Location: `internal/cache/vary.go`, beside `normalizeHeaderValue`.
Semantics (all decisions citable):

- RFC 9110 §12.5.3 defines the grammar; per RFC 9110 §8.4.1 the server
  chooses one coding. Bucket = the coding bouine will *ask the origin
  for* (§3.3), restricted to the three compressions bouine is willing
  to re-serve verbatim plus `identity`.
- No acceptable coding present (AE absent, empty, or all q=0) →
  `identity`. `identity;q=0` is treated like any other excluded token:
  bouine does not generate 406 responses (cite this deviation in a
  comment; an operator needing strict 406 semantics can use
  `exclude_headers`).
- Missing `q` = `q=1` (RFC 9110 §5.2.1 weight default; cite).
- Malformed input (unparseable q, stray tokens) → bucket from the
  well-formed prefix; fully malformed → `identity`. Never error, never
  allocate.

### 3.2 Normalizer dispatch

`normalizeHeaderValue` (vary.go:211) and the
`isListValuedVaryField`/`normaliseListHeader` pair (key.go:424/437)
gain the same single branch:

```go
if field is "accept-encoding" {
    return encodingBucket(value)
}
// existing sort path for accept-language / accept / accept-charset
```

Both call sites pass the **field name** today, so the normalizers need no
signature change — `variantKeyCore` (vary.go:150) and
`variantKeySlow` (vary.go:198) call `normalizeHeaderValue` with the field
name in scope; `buildVaryKeyInto` (key.go:424) already branches on
`isListValuedVaryField(f)`. Add `isAcceptEncodingField(f)` beside
`isListValuedVaryField` (both lowercase, already trimmed at both sites).

One subtlety: `normalizeHeaderValue` currently receives only the *value*.
The dispatch belongs at the two **callers** (vary.go:150, vary.go:198)
which know the field name — keep `normalizeHeaderValue` unchanged for
non-AE fields and route AE through `encodingBucket` at the call site. This
preserves its contract for any future caller and keeps the diff minimal.
`buildVaryKeyInto` (key.go:424) does the same inline.

### 3.3 The origin-bound header rewrite

New method on `Handler` (miss/revalidate/bypass/refresh paths only —
never the hit path):

```go
// rewriteOutboundAE replaces the client's Accept-Encoding on
// origin-bound requests with the canonical single-coding token for the
// request's AE bucket, so the stored variant always matches the bucket
// its key claims (see plan §2.1).
func rewriteOutboundAE(h *fasthttp.RequestHeader, bucket string)
```

Wired at the request-construction sites (candidate set; sites 4 and 7
are skipped per the rule below, leaving six rewrite points — the table
keeps all candidates so the skip decision is on the record):

| Site | Function | File:line | Rewrites? |
|---|---|---|---|
| 1 | `doBackgroundRefresh` | handler.go:1066 | yes |
| 2 | `revalidate` | handler.go:1983 | yes |
| 3 | `doBackgroundRevalidate` | handler.go:2195 | yes |
| 4 | `invalidateAndProxy` | handler.go:2438 | no — not a fill path |
| 5 | `doShedRefill` | handler.go:2714 | yes |
| 6 | `doFetchFast` | handler.go:2864 | yes (primary fill) |
| 7 | `streamMiss` (streaming fetch) | stream.go:102 | no — never caches |
| 8 | `doBackgroundRefresh`'s registry replay | refresh_registry.go:48 | yes — stored AE value drives it |

The rewrite is unconditional on cache-enabled routes when
`encoding_policy: bucket` (the default): every origin-bound request
gets `Accept-Encoding: <bucket-token>` — `zstd`, `br`, `gzip`, or the
header removed entirely for `identity` (an explicit
`Accept-Encoding: identity` is also acceptable; removal is chosen
because some origins treat a bare `identity` as "client explicitly
refuses compression" and add `Vary: Accept-Encoding` noise). We cannot
know at miss time whether the origin will declare AE in `Vary`, so the
safe rule is to always normalize — a response stored without AE in its
Vary never fragments on AE anyway.

`invalidateAndProxy` (site 4) is a method-invalidating proxy path, not a
fill path — its response is not stored, so the rewrite is a no-op there
and is skipped (the table entry documents the decision).

Bypass routes (`cache.enabled: false`) never rewrite — verbatim proxying
is their contract. The SSE stream path (`stream.go:102`, site 7) never
caches and never keys on AE; skipped for the same reason.

### 3.4 Egress unchanged

Stored `Content-Encoding`/`Content-Length` are served verbatim from the
stored headers (serializeHead path). A `br`-bucket client that could only
do gzip cannot exist: its request would have hashed to the `gzip` bucket.
A client whose AE is absent hashes to `identity` and gets the identity
variant — same as today.

## 4. Configuration

New field on `RouteKey` (internal/config/config.go:692):

```yaml
routes:
  - match: { host: api.example.com }
    cache:
      key:
        encoding_policy: bucket   # bucket (default) | verbatim
```

- `verbatim` restores today's sort-only behavior for operators who need
  byte-exact AE forwarding to the origin (e.g., origin-side content
  negotiation with custom codings, or an origin that varies response
  *bodies* by full AE string — a pathological but real case).
- Validation: enum check in `config.Validate`; the field is documented in
  the struct tag comments; identical across cluster nodes (same hazard
  class and runbook note as `include_headers`, ADR-0046).
- `KeyPolicy` gains an `encodingPolicy` field (keypolicy.go, after
  `includeHeaders`); `NewKeyPolicy` signature extends by one param —
  update all call sites found in the survey: `cmd/bouine/cmd/builder.go`
  (`buildKeyPolicy`), `cmd/bouine/cmd/engine.go` (cacheCheck), plus
  test call sites (strip_query_test.go, vary_test.go, fuzz_test.go,
  keypolicy_test.go).
- CacheCheck (`/v1/debug/cachecheck`) output shows the effective bucket
  for a request so operators can verify pre- and post-upgrade keys.

## 5. Tests

Per AGENTS.md §8, tests land in the same PR as the feature.

### 5.1 Unit — `internal/cache`

- `encodingBucket` table-driven test (new `vary_test.go` cases):
  the 8 probe strings → expected buckets; q-value cases
  (`gzip;q=1, br;q=0.5` → gzip; `br;q=0` exclusion; `zstd;q=1, br;q=1`
  → zstd tie-break); malformed inputs; empty string; case-insensitivity
  (`GZIP`, `Br`).
- `VariantKey`/`BuildVaryKey` equivalence: all AE inputs mapping to one
  bucket produce the **same** key; different buckets produce different
  keys; **parity across all six builders** — extend
  `TestEvaluateCrossPathParity`'s sibling (`key_test.go:71`-era parity
  cases) with AE-bucket rows covering `VariantKey`,
  `VariantKeyFast` (via `fastVarySrc`), `variantKeySlow` (long-Vary
  overflow input), and `BuildVaryKey`. A mismatch here is the
  cross-variant body-swap bug class (ADR-0046).
- Zero-alloc: `BenchmarkGate_VaryKey_AcceptEncodingBucket` (new, added to
  `BUDGETS` with `=0`). Inputs: the 8 probe strings; a 300-byte AE value
  (overflow-to-slow-path case, budgeted separately if it allocates —
  it must not on the fast path).
- End-to-end miss/hit handler test: origin echoes its received AE in the
  body; request with `Accept-Encoding: gzip, br` misses (rewritten to
  `br` at origin), subsequent `Accept-Encoding: br, gzip` request hits
  with the *same* body — proving both halves of §2.1.
- `exclude_headers` interaction: `exclude_headers: [accept-encoding]`
  still removes AE from the key entirely (pre-existing behavior; bucket
  must not resurrect it). Test both orders of precedence.
- `encoding_policy: verbatim` restores sort behavior (config →
  KeyPolicy → key comparison).

### 5.2 Integration

`test/integration` origin route (`driver/origin.go:117` `/vary` already
varies on AE):

- New scenario `cluster_encoding_bucket_test.go`: 3-node strong cluster;
  node A fills with `gzip, deflate, br`; node B serves a `br, gzip`
  request via peer fetch with a HIT (assert origin request count delta
  is zero) — this is the cluster-level proof that mixed AE dialects
  share one variant.
- Mixed-config negative test (mirroring the existing vary-leak tests,
  `cluster_vary_leak_test.go`): a node configured `verbatim` while
  peers run `bucket` must produce peer-fetch misses (never a wrong
  body) — pinning the §2.2 fail-safe.

### 5.3 Conformance

`make conformance` must not regress (score 93.7%). The cache-tests
harness does not exercise AE-bucket semantics (upstream tests use
`Vary: Accept-*` on *Language*/*Accept* for keying, and their AE cases
are about `Content-Encoding` response handling) — but the run is
mandatory per AGENTS.md §16 for `internal/cache` changes, and a
regression here would indicate the normalizer dispatch leaked beyond AE.

## 6. Rollout

### 6.0 Review correction: one PR, not two

The original draft's step 1 ("land bucketing alone — feature inert
until the rewrite") was incoherent: the variant key changes the moment
bucketing is in the normalizers, so it is never inert — keys change
but the origin still receives raw AE, which is precisely the §1.2
wrong-bytes hazard, live, for the entire window between the two PRs.
Both halves land in one PR. (The collision counter from the original
§6.2 is also dropped: the fill request's bucket and the stored
VaryKey derive from the same input, so the counter was tautological —
the e2e test in §5.1 proves the pairing invariant instead.)

### 6.1 Bench gates

Per AGENTS.md §7/§16: touch `internal/cache` → `make bench-gate`,
`benchstat` vs baseline. Expected outcomes:

- No new allocs on `FastPath_PeerHitVary` (budget 7) — the AE branch is
  a comparison and a stack scan before the existing sort work, which is
  then skipped for AE (net CPU neutral or better).
- New `VaryKey_AcceptEncodingBucket` budget entry lands in the same PR
  so `bench/run.sh` never runs a gate benchmark without a budget
  (ADR-0040 drift rule).

### 6.2 Metrics & observability

- No new counter. The originally proposed
  `bouine_encoding_bucket_collisions_total` was tautological (see
  §6.0) — dropped in review. The pairing invariant is proven by the
  e2e handler test (§5.1), not observed at runtime.
- `X-Cache-Source` unchanged.

### 6.3 Docs

- `docs/architecture.md` §3.3: replace the aspirational text with the
  implemented behavior + `encoding_policy` knob + the
  origin-header-rewrite pairing rule.
- ADR-0051 "Bucket Accept-Encoding in the variant key" — decision,
  the six-builder constraint, the pairing hazard, mixed-cluster
  behavior, `verbatim` escape hatch. Follows MADR format; referenced
  from §3.3.
- `docs/runbook`: new entry for "peer-fetch variant mismatches after a
  mixed-version deploy" (symptom: hit-ratio drop on cluster; cause:
  §2.2; action: complete the rolling restart — do not roll back half-way).
- `CHANGELOG.md` under `[Unreleased]` → `### Changed` (behavior change)
  + `### Fixed` (doc/code drift: §3.3 claim now true).

### 6.4 Cluster upgrade note

Rolling restart required before mixed nodes can share AE variants;
within one version window the peer gate rejects (fail-safe miss, never
a wrong body — CL-2 SLO degradation possible during the window,
acceptable per §2.2 and worth the runbook line, not a blocker).

### 6.5 Post-land verification

- Watch `bouine_requests_total{cache_result="MISS"}` on an AE-heavy
  route: expect a step down (fewer distinct variants = fewer first-miss
  fills). This is the hit-ratio win being observed, not assumed.
- `MaxVariants` cap hits (`vary_cap_hits_total`, insight rule
  `ruleCacheVaryExplosion`) should step down on routes where AE was the
  fragmenting header.

## 7. Explicit non-goals

- **Response recompression** (normalize_identity mode from §3.3 of the
  architecture doc): storing one representation and recompressing on
  egress is a different project; rejected here because it breaks the
  zero-alloc egress path and needs its own ADR + benchmarks.
- **`Accept` / `Accept-Language` bucketing**: higher cardinality, far
  harder to define a "negotiation outcome" bucket, and the sort already
  handles the common ordering variance. The dispatch table
  (`isAcceptEncodingField`) leaves room; nothing more.
- **Stale-variant cleanup**: after upgrade, previously stored
  verbatim-keyed variants remain until TTL/eviction. They are simply
  unreachable under new keys — the old entries expire naturally; a
  mass purge would spike origin load (the ADR-0045 lesson).
- **`If-Range`/304 pairing changes**: out of scope.

## 8. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Normalizer dispatch missed in one of the two call paths | Medium | Critical (body-swap) | Parity tests across every construction path (§5.1); ADR-0046 precedent review in PR |
| Deflate-only origin serves `Content-Encoding: deflate` under a `gzip` bucket (§1.2) | Low (post-correction) | Critical (wrong bytes) | Outbound rewrite to a single canonical token (§3.3): a deflate-only origin answers identity for the whole bucket, consistently; `encoding_policy: verbatim` escape hatch |
| Mixed-version cluster hit-ratio dip | Certain (during window) | Medium | Fail-safe peer gate (no wrong bytes); runbook + release note; CL-2 SLO tolerance |
| Alloc regression on hit path | Low | Blocks merge | New gate benchmark with budget 0; benchstat gate in CI |
| Conformance regression | Low | Blocks merge | `make conformance` gate (§5.3) — verified the upstream suite sends no `Accept-Encoding` and varies on none of its tests, so the score cannot move via AE |
| Operators relying on verbatim AE at origin | Medium | Medium | `encoding_policy` config; migration note in changelog |

## 9. Work items (ordered)

1. `encodingBucket` in `internal/cache/vary.go` + table tests.
2. Dispatch at the two normalizer call paths + parity tests across
   every construction path.
3. Gate benchmark + `BUDGETS` entry + alloc verification.
4. `rewriteOutboundAE` + wiring at the six rewrite points +
   handler e2e tests.
5. `encoding_policy` config field + `KeyPolicy` + `config.Validate` +
   docs in config comments + cacheCheck display.
6. Integration tests (fill-via-A, hit-via-B; verbatim-mismatch
   fail-safe).
7. Docs: architecture §3.3 rewrite, ADR-0051, runbook entry,
   changelog entries.
8. Gates: `make lint`, `make test`, `make bench-gate`, `make conformance`,
   `make integration` (§16 table for `internal/cache`).

All items land in **one PR** (§6.0).
