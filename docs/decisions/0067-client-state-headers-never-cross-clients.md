# ADR-0067: Client-state response headers are never served to a second client

- **Status**: Accepted
- **Date**: 2026-10-25
- **Deciders**: @bouine-core
- **Phase**: cache key policy
- **References**: ADR-0012 (Set-Cookie storage block), ADR-0052 (never collapse authorized or unsafe-method requests), ADR-0054 (bypass-on-cookie), ADR-0057 (flight keys on declared variation dimensions), ADR-0066 (fill flights); RFC 6265 §4.1; RFC 9110 §5.2; WICG Sign-In Cookies proposal (`Set-Login`); `internal/cache/cacheable.go` (`carriesClientState`), `internal/cache/stream.go`, `internal/cache/handler.go`, `internal/cache/clientstate_test.go`

## Context and Problem Statement

ADR-0012 blocks *storage* of responses carrying `Set-Cookie` by default and
strips the header from the stored copy under `allow_set_cookie`. But a
shared cache has a second serving surface: **in-flight collapse**. ADR-0057
byte-handoff flights (warm and declared-dimension misses) hand the leader's
*entire* buffered response — headers included — to every follower, and the
`revalKeySuffix`/`collapsedFetch` singleflights hand the shared fetch result
to every caller. The response's origin `Set-Cookie` was never checked on
those paths: two anonymous concurrent requests against an origin that
rotates session cookies on every response (a standard session-fixation
defense) meant the follower received the **leader's session cookie** — a
cross-client leak ADR-0012's storage gate could not see.

`Set-Login` (WICG Sign-In Cookies: `logged-in` / `logged-out`, the
requester's login state in a credentialed fetch) had no gate at all: a
`Set-Login: logged-in` response with explicit freshness was **stored
unstripped** and replayed to every subsequent client, and could be handed
in-flight like any other header. nginx never had the in-flight half of this
problem structurally (`proxy_cache_lock` is purely store-mediated: waiters
serve from cache or go upstream, never from the leader's in-flight bytes);
bouine's byte-handoff is more aggressive and needed the gate to match.

## Decision

**A response carrying a client-state header — `Set-Cookie` or `Set-Login` —
is served only to the client whose request produced it.** Never stored
unstripped, never handed to another caller through any collapse surface:

1. **Storage (extends ADR-0012 to both headers).** Default: a response
   carrying either header is not stored, even with explicit freshness
   (`isBlockedByClientState` + the handler gates in
   `isResponseCacheable`/`isBlockedByDirectives`/`writeAndMaybeStore`/
   `doBackgroundRevalidate`/`doBackgroundRefresh`/`doShedRefill`/
   `maybeStorePostResponseFast`). Under `allow_set_cookie: true`: storable
   with explicit freshness, but `buildObject` always strips both headers —
   stricter than nginx's opt-in (`proxy_ignore_headers Set-Cookie` caches
   *with* the cookie). A 304 never merges either header into the stored
   object (`MergeHeaders304`).

2. **In-flight sharing.** When the leader's response carries either header,
   the flight publishes `ErrStreamUnshareable` instead of the bytes:
   - **byte-handoff flights** (warm, declared-dimension): followers are
     released at header time and fetch their own — the same release the
     streaming path already used for SSE (ADR-0042).
   - **`collapsedFetch` / `collapsedRevalidateBg`** (stayin-alive warm
     flights, foreground revalidations): x/sync cannot distinguish the
     leader from a follower (both see `shared=true` when the value was
     handed to others), so **every shared caller discards the shared
     result and re-issues its own fetch** (a 200 revalidation answer
     only; a 304 never carries client state after rule 1). A solo caller
     keeps its own response.
   - **fill flights (ADR-0066)** are untouched by construction: followers
     never receive leader bytes. The settlement lifecycle is unchanged —
     `inflight.stored` still closes after the leader's store attempt —
     so under the default gate the follower re-enters to its own fetch,
     and under `allow_set_cookie` it serves the stripped stored copy.

3. **No new configuration.** `allow_set_cookie` governs both headers: it
   is the existing "cache responses carrying client state, stripped"
   opt-in. Adding a second knob for `Set-Login` would offer operators a
   way to replay login state across users that no deployment wants.

## Consequences

- **The cross-client leak closes on every surface**: a client's
  `Set-Cookie` / `Set-Login` reaches only the client whose request
  produced it — in-flight and from the store. Pinned by
  `internal/cache/clientstate_test.go` across fill flights, byte-handoff
  flights (buffered and tee publish sites), revalidation flights, the
  `allow_set_cookie` stripped-copy serve, and the POST store-to-GET-key
  gate.
- **Cost**: one extra origin fetch per shared caller when a client-state
  origin meets concurrent anonymous misses on one key — the same cost
  ADR-0052 accepts for requests that cannot be proven shareable, bounded
  by the fetch semaphore and shed machinery. The check itself is two
  zero-allocation header lookups on the miss path.
- **Fill-flight sequential equivalence is preserved for client-state
  responses**: a follower receives exactly what a request arriving after
  the fill would receive (own fetch under the default gate; the stripped
  stored copy under the opt-in).
- **`Set-Login` responses stop being cacheable by default** — a behavior
  change for operators who (unknowingly) cached them; the exposure being
  removed is another user's login state replayed to anonymous clients.
- **Open question (not decided here)**: whether *other* per-client
  response headers should join the client-state set (e.g. future
  session-bearing headers). The set is deliberately explicit, not
  heuristic: a shared cache guessing which headers are personal is how
  false negatives (leaks) and false positives (lost caching) both happen.
