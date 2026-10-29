# ADR-0072: Cookie-value cache keying — cache.key.cookie_values (Cloudflare cookie.include parity)

- **Status**: Accepted
- **Date**: 2026-10-29
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: RFC 6265 §4.1.1, §4.2, §5.2, §5.4; RFC 9111 §4.1; RFC 9110 §12.5.5; ADR-0046 (include_headers); ADR-0052/ADR-0054 (identity refusals, unchanged); ADR-0063 (route_defaults merge precedent); ADR-0067 (client-state headers); ADR-0071 (config-derived Vary); issues #768 (cookie_presence), #870; [Cloudflare cache rules — custom cache key `cookie.include` / `cookie.check_presence`](https://developers.cloudflare.com/cache/how-to/cache-rules/settings/)

## Context and Problem Statement

Issue #768 shipped cookie **presence** keying (`cache.key.cookie_presence`): a listed cookie contributes a present/absent bit. That covers origins that render differently when a cookie *exists* (consent, login state). The remaining SSR shape renders differently per cookie **value** — locale, currency, experiment-variant cookies — and today has no expression: `bypass_on_cookie*` disables the cache entirely, keying nothing fragments nothing but risks wrong bodies when the origin reads the value.

The reference deployment (Cloudflare) exposes exactly this via the cache key's cookie section: `cookie.include` (value in key), `cookie.check_presence` (bit in key). A bouine route should be translatable to and from that configuration with identical cache partitioning — same URL + same listed values ⇒ same entry; any listed-value difference ⇒ different entry.

`cache.key.include_headers: [Cookie]` technically hashes the raw header, but it is the wrong tool (ADR-0054 already rejected it for this shape): every distinct cookie *string* — analytics IDs included — becomes a variant, and the value rides the key material twice for routes that also want presence bits.

## Decision Drivers

- **Cloudflare parity**: equivalent configurations must produce equivalent partitioning (same semantics, different config format — an operator migrating a cache rule must be able to reason that the bouine route behaves identically).
- **Cardinality**: unlike presence bits (bounded 2^N), distinct values are unbounded. The knob must cap the blast radius at config time and at storage time.
- **PII**: cookie values are user data. They may exist only in transient hash inputs — never stored, never logged, never in metrics labels.
- **No identity relaxation**: ADR-0052/0054 refusals (cookied requests never share in-flight fetches; client-state response headers never cross users, ADR-0067) are load-bearing and must not be weakened by "the variant is keyed anyway" reasoning.
- **Performance**: value keying is a per-route opt-in cost; every other route must pay zero (§7 hit-path budget, BenchmarkGate zero-cost proof).

## Decision

Add `cache.key.cookie_values: [names]` (capped at **4** names) to `RouteKey`, and `route_defaults.cache.key.cookie_values` following the #771/#763 list-merge precedent (unset inherits wholesale, own list replaces — never a union, explicit `[]` opts out; every route kind inherits, the policy is consumed by every cache handler).

### Keying mechanism — the presence machinery, second field

A second reserved synthetic Vary field `x-bouine-cookie-values` unions into the stored VaryValue exactly like `x-bouine-cookie-presence` and `include_headers` (effectiveVary, ADR-0071 derivedVary), so store/lookup pairing, peer gates (VaryKey assertions), 304 revalidation, cold-flight identity (flightHeaders), and refresh replay (registry saves Cookie) all hash the same dimension through the same single code paths.

The hash contribution is the **reduced pair string**: `"name=value"` pairs positional over the sorted normalized name list, joined by `"; "`. Canonicalization is pinned for cluster agreement:

- names matched case-insensitively; values case-sensitive, trimmed of OWS (RFC 6265 §5.2), DQUOTEs kept (§5.2 rule 6);
- every Cookie line participates (the §4.2 "; "-joined value — `CookieAll`, never a single `Peek`: the multi-line bug class #768's review fixed);
- duplicate listed name: **last** occurrence wins (§5.4 ordering — browsers send longer-path cookies first);
- absent or empty-valued listed cookie ⇒ the empty value — one variant, the include_headers absent-header convention (RFC 9111 Vary semantics);
- no Cookie header at all ⇒ the constant-empty contribution (the presence convention);
- no truncation — determinism beats byte-budget; oversized reductions fall to the existing `variantKeySlow` path.

A value can never forge a pair boundary: RFC 6265 cookie-values exclude `;` and `,` (§4.1 cookie-octet), so the `"; "`-joined reduction is unambiguous.

### Validation (config.Validate)

- RFC 6265 §4.1.1 name tokens, case-insensitive uniqueness, cap 4 (stricter than presence's 16: values multiply the variant space, presence only bit-doubles — the cap rejects the config before a warm cache reveals the hit-ratio collapse).
- No overlap with `cookie_presence` (a name keyed by value and presence is ambiguous — the value subsumes the bit).
- Mutually exclusive with `bypass_on_cookie` / `bypass_on_cookie_names` (dead config: a bypassed request never reaches the key; same rule as presence).
- `include_headers` may not list `"cookie"` while either cookie list is configured (double-keying the same header two ways; the lists are the sanctioned single path — threat-model T06).

### Cloudflare mapping

| Cloudflare (`cache_key.custom_key.cookie`) | bouine (`cache.key`) |
|---|---|
| `include: [c1, c2]` | `cookie_values: [c1, c2]` |
| `check_presence: [c1]` | `cookie_presence: [c1]` |
| unlisted cookies ignored | same |
| `__cf*` cookies forbidden from the key | N/A (bouine sets no first-party cookies) |

Documented deltas: Cloudflare does not specify duplicate/multi-line cookie handling — bouine pins it above so cluster nodes always agree; Cloudflare hashes the value into an opaque key, bouine hashes the xxhash64 reduction — equivalent partitioning, never byte-equal keys (no interop requirement).

### Safety

- **Cardinality**: `max_variants` remains the storage backstop — a Put over the cap is refused (never a wrong variant served) and `vary_cap_hits_total` increments; the dashboard's existing too-many-variants insight surfaces sustained refusals. The 4-name cap bounds the config-time dimension count, not the value space — a session-ID-valued cookie here is an operator error the runbook calls out.
- **PII**: the raw value exists only inside the transient variant-key hash input (stack buffer → xxhash → discarded). The stored VaryValue carries only the synthetic field *name*. Pinned by a log-capture test (no value or fragment in any log output) and fuzzed invariants.
- **Identity refusals unchanged**: cookied requests still never share in-flight fetches (ADR-0052/0054 `collapseDenied`), `Set-Cookie`/`Set-Login` storage blocking unchanged (ADR-0012/0067). Value keying makes stored variants per-value, it does not make cookied users shareable.
- **Cluster**: identical policy across nodes is the same hazard class as `include_headers` — a mismatched node computes different VaryKeys and the peer gates fail safe (miss, never a wrong body; pinned by `TestCookieValues_PeerGateFailSafe`).

### Performance

The reduction allocates once (the joined pair string; pair values ride a stack array). Budgets mirror the presence tiers as ceilings — measured and gated at `VaryKey_CookieValues`=1, `VaryKey_CookieValuesFast`=2, `VaryKey_CookieValuesRaw`=4, and `VaryKey_NoCookieKeyingZeroCost`=2 proving flag-off routes pay nothing (`variantKeyCore` adds one guarded case; the field is absent from their Vary).

## Consequences

- **Positive**: per-value SSR content becomes cacheable with Cloudflare-parity semantics; presence and value keying compose on one route (two independent synthetic fields); all cluster machinery inherited from #768 unchanged; zero cost for non-keyed routes.
- **Negative**: hit ratio on a misconfigured route (high-cardinality value like a session ID) degrades to near-zero until `max_variants` refuses stores — visible via `vary_cap_hits_total` and the dashboard insight, diagnosable per the runbook; the 4-name cap is a new per-knob bound to remember (mirrored in code comments at both enforcement sites).
- **Neutral**: `include_headers: [Cookie]` stays legal on routes with no cookie lists (its pre-existing semantics); the two-knob rejection only fires when both are configured.

## Notes

The alternative "reuse include_headers for values" was rejected: it keys the whole raw header (unlisted analytics cookies fragment variants), offers no name-scoping, no cap, and no place to hang the Cloudflare mapping. The alternative "key a hash of the value, not the value" is exactly what the design does — the reduction *is* the canonical pre-hash form; hashing again would add nothing.

ADR-0054's stance ("values are PII and cardinality — presence only") is not reversed but *scoped*: values join the key only under an explicit, capped, name-scoped opt-in, exactly the trade Cloudflare's `cookie.include` makes available.
