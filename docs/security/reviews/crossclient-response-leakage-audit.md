# Cross-client response leakage audit (defensive)

Date: 2026-10-27 · Scope: every path where a response produced for client A can
reach client B (cache poisoning, wrong request collapsing, memory/connection
aliasing) · Mode: read-only audit + failing regression tests for confirmed
findings · Method: direct code reading of `internal/{cache,cluster,origin,server,transport,admin,config,observability}`,
subagent deep-dives (cluster, memory/connections, keys/Vary, admin/observability)
with spot-verified conclusions, and four regression tests run under `-race`.

**Regression tests**: AUD-01/AUD-04 landed with their fixes
(`swr_identity_regression_test.go`, `key_origin_parity_regression_test.go`,
PRs #850/#851); AUD-02/AUD-03 are committed as proven-failing tests
quarantined behind `t.Skip` with issue references
(`crossclient_poison_regression_test.go`, issues #848/#849) — see §4.

---

## 1. Pipeline map

```
conn (fp_conn.go, 1 goroutine/conn)
 └─ h1parser.parseBuffer ── smuggling defenses (CL+TE, dup-CL, obs-fold → 400+close)
     ├─ H1 FAST PATH (fastpath.go): key from RAW path (buildKeyFromRaw) → TryHit
     │    └─ onStale → TriggerBgRevalidateFromFastPath (materializes RawRequest)
     └─ FALL-THROUGH → cache.Handler.ServeRequest (handler.go:1520)
         ├─ isInvalidating? → invalidateAndProxy (POST/PUT/DELETE/QUERY/TRACE…)
         ├─ rewriteRequestCtx → requestBypass (bypass_on_cookie[_names], UA) → handleBypass
         ├─ lookup (handler.go:2052): primaryKey → Vary resolver → variant key
         ├─ evaluateFast → Hit/StaleHit → serveObject / serveRange / conditional-304
         │    └─ StaleHit+SWR → triggerBgRevalidate (FULL client ri replayed!)
         ├─ Miss → handleCacheMiss → peer-fetch (owner) → fetchAndStore
         │    ├─ fetchFlightKey (collapse.go): byte-handoff vs FILL flight
         │    │    byte-handoff: follower waits on done → writeBufferedResult
         │    │    fill flight:  follower waits on stored → re-enters ServeRequest
         │    └─ streamMiss (stream.go): cacheability verdict → publish → store
         └─ Revalidate → revalidate (conditional, reval-suffixed singleflight)
Store gate: isResponseCacheableBytes / isCacheBlocked (no-store, private, Vary:*,
  Set-Cookie/Set-Login, request-Authorization w/o public/must-revalidate/s-maxage)
Collapse gate: collapseDenied (unsafe method, Authorization, Cookie)
Background: doBackgroundRevalidate (SWR), doBackgroundRefresh (registry,
  Vary-only header snapshot), doShedRefill — all replay ri headers to origin
```

---

## 2. Findings

Severity: Critical = confirmed cross-client delivery of identity data or
attacker-triggerable shared-key poisoning. High = same outcome, needs config
or specific (but realistic) conditions. Medium = wrong content/status without
identity leak. Low = hardening / missing pins.

| ID | Case | Sev | Status | Location | Scenario | Proof | Proposed fix |
|---|---|---|---|---|---|---|---|
| AUD-01 | F1, A3, C11 | **High** | **CONFIRMED** | `handler.go:3314-3317` (`doBackgroundRevalidate` replays full `ri.Header`), trigger at `handler.go:1611-1613` / `2093-2101` | A logged-in client hitting a stale entry triggers the SWR background revalidation, which forwards **that client's session Cookie** to the origin; the personalized 200 (no Set-Cookie, explicitly cacheable) is stored under the **shared** key and served to every later client as a HIT | `TestSWRRevalidation_CookiedStaleHitPoisonsSharedEntry` — fails: anonymous client B's HIT body is `user:sid=A-secret-canary` | Mirror the refresh registry's identity-minimal snapshot (`refresh_registry.go Register` saves Vary-relevant headers only): strip Cookie/Authorization/custom identity headers from SWR/refill replays, or refuse the SWR trigger for identity-bearing requests (serve stale, let the next anonymous request refresh) |
| AUD-02 | E6, A10 | Medium | **CONFIRMED** | `stream.go:1207-1214` (`publishFillVerdict` publishes 304s), follower at `handler.go:2713-2740` (`writeBufferedResult`); also `collapsedFetch` (`handler.go:2398-2434`) → `writeAndMaybeStore` | On a warm byte-handoff flight (stored `no-cache` object w/o validators ⇒ every request is a Miss with the object in scope), a **conditional leader** settles as 304 and the verdict is handed to **unconditional followers** — a bodiless 304 for a request that sent no validators (RFC 9110 §15.4.5) | `TestWarmFlight_Leader304ServedToUnconditionalFollower` — fails 3/3 runs: follower status=304, body="", `fetches=1` (it parked; only the leader's conditional reached origin) | Treat a 304 as inherently leader-conditional: `publishFillVerdict` should publish `ErrStreamUnshareable` for 304s (fill followers already re-enter safely), and `collapsedFetch`/`collapsedRevalidateBg` should re-fetch when a shared flight settled as 304 that the follower did not request |
| AUD-03 | A11 | Medium | **CONFIRMED** | No 206 guard in `isResponseCacheableBytes` (`stream.go:529-567`); `Range` only handled on the hit path (`handler.go:1603-1609`); `Range` not part of the key | A Range request on a miss is forwarded verbatim; the origin's 206 partial with explicit freshness passes the store gate and is stored under the **primary** key; every later unconditional client is served the partial 206 as a HIT (truncated body + foreign Content-Range) | `TestRangeMiss_StoresPartial206ServedToUnconditionalClients` — fails: client B gets HIT 206 `01234` instead of 200 `0123456789` | Strip `Range` on miss fills (fetch the full representation, then serve ranges from it, matching the hit path), or refuse to store 206s from miss fills |
| AUD-04 | B1 | **High** | **CONFIRMED** | Slow-path key from fasthttp-normalized `ctx.Path()` (`handler.go:1473` → `BuildKeyFast`), raw target forwarded to origin (`originURI`, `handler.go:802-808`); fast path hashes the RAW path (`h1parser/parser.go:524-563`, `fastpath.go:920`, only `//` collapsed at `fastpath.go:958-975`) | `/a/../secret` and `/secret` (also `/%2e%2e/`, `%2F`) hash to the **same slow-path key** while the origin receives the raw spelling: the response fetched under the odd spelling is served to every client using the clean one (classic normalization-differential poisoning); the H1 fast path disagrees with the slow path (parity divergence + `bouine keys find`/purge compute a third form via `url.Parse`) | `TestKey_DotSegmentSpellingPoisonsNormalizedKey` — fails: client B's HIT body carries the `/a/../secret` spelling's content | Hash the same bytes the origin receives (raw target), or apply ONE canonicalization identically at key time, origin-forward time, and admin key computation; validate request-target form (see AUD-13); pin fast/slow/admin key parity for dot-segment spellings |
| AUD-05 | H2, H3 | **High** | CONFIRMED (code) | `cluster/peerfetch.go:1130-1160` (`PeerPutHandler`: decodes and `store.Put`s any object; no store-gate re-check, no `maxObjectSize`, no ownership check); `admin/server.go:924-932` (`/v1/peer/*` token-exempt); `config.go:384-387` (peer TLS optional — "Leave empty for plain HTTP") | The write-to-owner RPC is an **unauthenticated, unvalidated arbitrary-object store** on the admin listener (default plaintext): any network actor can push any body under any key (including a Vary resolver with attacker-chosen `VaryValue`/`PairedKey`), which then propagates fleet-wide via peer-fetch. Also: the receiving node applies none of the sender's store gates, so config drift (e.g. `allow_set_cookie` true on one node) leaks personalized responses cluster-wide | Code reading (peer routes exempt from auth middleware; `Handle` stores unconditionally); no regression test written (needs the multi-node harness — see §5) | Require auth on peer routes (shared cluster secret or mTLS mandatory when `listen.cluster` is set); on receive, re-run the store gates (`isCacheBlocked`, `skipStoreReason`, `maxObjectSize`) and verify this node owns `obj.Key` |
| AUD-06 | A4, A2, C8, C11 | Medium | LIKELY | `collapse.go:80-81` (gate = Authorization/Cookie/unsafe only); `cacheable.go:292-296` (store gate = request Authorization only); verbatim header copy (`handler.go:3675-3677` etc.) | With zero config, `X-API-Key`, `X-Auth-Token`, `X-Session-Id`, `X-Tenant-Id`, `X-Forwarded-User`, `Proxy-Authorization` (and any other custom identity header) are **unkeyed and un-gated**: a request carrying them collapses with anonymous callers and its response is stored under the anonymous key — cross-user poisoning on any origin that personalizes on them. Same class: an origin reflecting `Origin`/`X-Forwarded-Host` without declaring `Vary` gets one shared entry | Code; `include_headers` (`config.go:949-967`) exists but is documented as a Vary-union, not as "declare your identity headers"; no warning anywhere | Document `include_headers` as the identity-declaration surface with a security warning; consider a default strip/ignore list for well-known auth header names on origin-bound copies, and a boot-time warning when a route has no identity handling |
| AUD-07 | C6 | Medium | LIKELY | Zero code matches for `Clear-Site-Data` repo-wide; not in `carriesClientState` (`cacheable.go:349-351`) | A logout response carrying `Clear-Site-Data` (e.g. 200 + Last-Modified, heuristically cacheable) is stored and the header replayed to every subsequent client — one user's logout executes against other users' browsers | Code (grep) + `isHeuristicStatus` includes 200 | Add `Clear-Site-Data` to the client-state family (ADR-0067): never store, never share in-flight |
| AUD-08 | D2 | Medium | LIKELY | `handler.go:3802-3825` (`buildLocationKey`): `Location`/`Content-Location` resolved with no same-origin check, purged + broadcast | An unauthenticated `POST` (proxied, not auth-gated) whose response carries `Location: http://any-host/x` force-evicts `http://any-host/x` on every node — cross-host forced invalidation (DoS on hit ratio for any origin the fleet serves) | Code | Restrict Location/Content-Location invalidation to same-host (or same-route-pool) targets; many caches do; RFC 9111 §4.4 only says the URIs "in" the fields are affected |
| AUD-09 | A8 | Medium | LIKELY | `handler.go:3675-3677` (+ sibling fill sites): all client headers copied verbatim to origin; keying ignores `X-Forwarded-Host`, `X-Forwarded-Proto`, XFF entries unless the route opts into `request.forwarded` | On routes without `request.forwarded`, a client-supplied `X-Forwarded-Host: attacker.example` reaches the origin unkeyed — an origin rendering absolute links/redirects by that header returns attacker-influenced content stored under the clean key (canonical unkeyed-header poisoning) | Code; `forwarded.go` SETS (overwrites) the headers only when configured | Default-strip `X-Forwarded-*` (and `Forwarded`) on origin-bound copies when `request.forwarded` is not enabled for the route |
| AUD-10 | J5, J7 | Low | CONFIRMED | `cmd/bouine/cmd/engine.go:420-426` (auto-generated admin token logged in plaintext at Warn); `admin/server.go:952-955` (empty token ⇒ every non-exempt route unauthenticated) | Anyone with log read access gets the admin token (purge/ban/config-reload); a `Server` constructed with `Config{}` has an open admin plane | Code | Log only a token hint/hash; refuse to start (or bind loopback only) with an empty token |
| AUD-11 | A13 | Low | LIKELY | `stream.go:160-164` (request body copied to origin-bound request); body not in key or flight key | Fat GET (body on GET) influences the origin response but is unkeyed; a bodied GET's fill is stored under the bodyless key. RFC 9110 §8.6/§9.3.1 leave GET body semantics undefined (Varnish also ignores it) | Code | Hardening: strip GET/HEAD bodies on origin-bound copies, or document the posture |
| AUD-12 | A9, C13 | Low | HARDENING | No response-side check that the origin's `Content-Encoding` matches the negotiated AE bucket (`vary.go:358-361` documents the assumption) | A non-conforming origin returning `br` bytes for an identity-bucket fill stores br once; every AE population sharing the primary key (no `Vary: Accept-Encoding` route) receives it | Code | Verify `Content-Encoding` against the negotiated bucket at store time; fall back to identity/ refusal on mismatch |
| AUD-13 | B5, B4 | Low | LIKELY | `h1parser/parser.go:524-563` accepts any request-target form; `fastpath.go:437-455` does not require a path starting `/`; no IDN/punycode host canonicalization | Absolute-form (`GET http://h/x`) is keyed with the whole URI as path on the fast path vs normalized on the slow path; fragments kept by the h1parser vs stripped by fasthttp | Code | Validate origin-form (or normalize absolute-form into parts before keying); reject fragments; consider punycode host canonicalization |
| AUD-14 | I4, I7 | Low | HARDENING | No in-repo enforcement of origin-connection disposal after errors; reuse correctness rests on fasthttp v1.77 internals (errors close the conn; `StreamBody` conns pooled only after full consumption) | A fasthttp regression would silently turn into response-queue poisoning on the origin pool | Subagent code reading + `origin/pool.go`, `transport.go` | Add an upgrade-pinning test (partial-read/timeout ⇒ connection closed, never reused) |
| AUD-15 | C12, D3 | Low | HARDENING | `buildObject` strips only Set-Cookie/Set-Login/X-Cache; `maybeStorePostResponseFast` (`handler.go:3771-3800`) keeps RFC 7234 §3 POST-response storage for Varnish parity | Origin-internal headers (`X-Accel-*`, `Server-Timing`, debug headers) are stored and replayed downstream; a POST response with `max-age` + matching `Content-Location` is stored under the GET key | Code | Optional strip list for origin-internal headers; consider a config gate for POST-response storage |

**SAFE-VERIFIED highlights** (the load-bearing invariants that hold, with
evidence): store gate RFC 9111 §3.5 for Authorization (`cacheable.go:292-296`,
`stream.go:580-584`); Set-Cookie/Set-Login never stored by default and always
stripped (`handler.go:4444-4445`, `clientstate_test.go`); collapse refusal for
Authorization/Cookie/unsafe methods (`collapse.go:64-82`, pinned by
`collapse_test.go`, `fillflight_test.go`); fill flights never hand off bytes —
followers re-enter the full pipeline with sequential-equivalence pinned
(ADR-0066, `TestFetchAndStore_FillFollowerSequentialEquivalence`); truncated
bodies never shared or stored (`settleTeeFill` `bodyComplete`, pinned by
`TestFetchAndStore_FillFollowerTerminatesForEachVerdict`); HEAD/GET flight and
store separation (issue #752 fixes); 128-bit full-key storage/collapse/memo
(`storage/collision_test.go`); Vary normalization, multi-line join, `*`
refusal, drift purge, peer variant gates; smuggling defenses before any cache
logic (`h1parser/smuggling_test.go`); `RequestCtx`-buffer materialization
discipline for every escaping goroutine (`bg_revalidate_test.go`); store deep
copies; data-plane logging carries no header values.

---

## 3. Coverage matrix

| Case | Status | Evidence |
|---|---|---|
| A1 Authorization | SAFE-VERIFIED | store gate `cacheable.go:292-296`, bytes path `stream.go:580-584`; collapse `collapse.go:80`; tests `collapse_test.go:175-260` |
| A2 Proxy-Authorization | **FINDING** (AUD-06) | unkeyed, un-gated, forwarded verbatim |
| A3 Cookie | **FINDING** (AUD-01; posture documented) | collapse refused `collapse.go:81` (`TestFetchAndStore_NoCrossCookieCollapse`); storage NOT gated by design (`docs/architecture.md §3.4`, RFC-conformant, `bypass_on_cookie` opt-in); SWR background replay is the leak amplifier |
| A4 custom auth headers | **FINDING** (AUD-06) | zero-config: unkeyed + un-gated |
| A5 credentials in URL | SAFE-VERIFIED | full sorted query in key (`key.go:364-391`); signed URLs distinct; userinfo/absolute-form → AUD-13 |
| A6 mTLS client identity | N/A | no client-cert identity forwarding feature; a client-sent `X-Client-Cert` is ordinary unkeyed header (AUD-06 class) |
| A7 XFF / CF-* / True-Client-IP | SAFE-VERIFIED | `forwarded.go` (T06/T04); zero CF-*/True-Client-IP code matches; spoofed XFF entries only forwarded, never keyed |
| A8 unkeyed routing headers | **FINDING** (AUD-09) | verbatim copy `handler.go:3675-3677`; Host itself keyed |
| A9 Accept/AE/AL/UA | SAFE-VERIFIED (residual AUD-12) | AE/AL bucketing ADR-0061 + `encodingbucket_pairing_test.go`; UA never keyed (ADR-0055 bypass opt-in) |
| A10 conditionals | SAFE-VERIFIED except AUD-02 | hit-path 304 vs client's own validators (`tryConditional304Fast`); vary-miss validator reuse has cross-requester proofs (`serveResolver304`, `handler_test.go:200-290`) |
| A11 Range | **FINDING** (AUD-03) | hit path safe (`range.go`); miss path forwards Range, stores 206 under primary key |
| A12 request CC directives | SAFE-VERIFIED | no-store→Bypass `engine.go:72`; no-cache→Revalidate/Miss `engine.go:141-146`; only-if-cached→504 `handler.go:2238`; fill followers re-enter all gates |
| A13 fat GET / QUERY | **FINDING** (AUD-11, Low) | QUERY is invalidating (never cached); GET body forwarded unkeyed |
| A14 Connection header tricks | SAFE-VERIFIED | smuggling checks pre-cache (`parser.go:650-656`); cache and origin read the same ctx header set (no divergence window); `stripConnectionListedHeaders` on serve |
| A15 header shape | SAFE-VERIFIED | dup-CL/CL+TE/obs-fold rejected (`h1parser/smuggling_test.go`, 400+close); duplicate Cookie lines handled (`requestInfoFromRaw` joins, `requestBypass` PeekAll); caps reject, not truncate |
| B1 path normalization | **FINDING** (AUD-04) | key normalized vs raw origin target; fast path raw |
| B2 web cache deception | SAFE / N-A | no extension- or path-based TTL rules exist; `ttl_override` is an explicit operator regex gated to status<400 (ADR-0059) |
| B3 query handling | SAFE-VERIFIED | sorted full query; `dedup`/`strip_params` per-route opt-in only |
| B4 fragment | **FINDING** (AUD-13 family, Low) | h1parser keeps `#` in path/query; fasthttp strips on slow path |
| B5 absolute-form / Host | **FINDING** (AUD-13) | no target-form validation; host lowercase+port-strip keyed; no IDN punycode |
| B6 key hashing | SAFE-VERIFIED | full `[16]byte` keys in hot store, inflight table, unstorable memo; `storage/collision_test.go` |
| B7 cross-version key drift | HARDENING | `include_headers`/`verbatim_encoding` cross-node sameness is documented operator duty (`§3.2/§3.3`); no runtime fingerprint/enforcement |
| C1 Set-Cookie | SAFE-VERIFIED | default refusal (ADR-0012), always stripped at store (`handler.go:4444`), qualified `no-cache="Set-Cookie"` fields stripped on serve (`stripNoCacheFields`, test `handler_test.go:3434-3438`) |
| C2 Set-Login | SAFE-VERIFIED | ADR-0067 family (`clientStateInMap`/`clientStateInFastHTTP`) |
| C3 CC parsing/precedence | SAFE-VERIFIED | RFC 9213 precedence `cacheable.go:35-62`; multi-line join §5.2; must-understand escape; Pragma; validator-bearing no-cache storable (§5.2.2.4, cited); ttl_override error gate (ADR-0059) |
| C4 Vary | SAFE-VERIFIED | normalization + multi-line join + `*` refusal + drift purge (ADR-0058); peer variant gates (`servePeerHit`, `varyKeyMatches`) |
| C5 401/407 + WWW-Authenticate | SAFE-VERIFIED (default) | not heuristic (`cacheable.go:370-378`), not in default negative table (`pkg/api/status_ttl.go:23`); residual: explicit-freshness 401 IS storable (RFC MAY) — hardening note |
| C6 Clear-Site-Data | **FINDING** (AUD-07) | zero handling repo-wide |
| C7 redirects with identity | SAFE-VERIFIED | Set-Cookie gate blocks login redirects; 302/303 need explicit freshness; Location target keyed by its own full URL |
| C8 CORS | origin-dependent (AUD-06 class) | bouine keys only on declared Vary; OPTIONS has its own method key segment (`key.go:77-81`) |
| C9 per-client statuses | SAFE-VERIFIED | 429/403 not heuristic, not default-negative; shed 503 never stored; negative TTL is operator-scoped |
| C10 206/304/1xx | **FINDING** (AUD-03 for 206); 304/1xx SAFE | 304 never stored from fills (`cacheable.go:74-76`); 1xx skipped by fasthttp parser |
| C11 body personalization | **FINDING** (AUD-01/06) | request-side gate covers Authorization (store) + Cookie (collapse only); documented posture |
| C12 internal origin headers | HARDENING (AUD-15) | stored + replayed |
| C13 encoding/CL/TE mismatch | SAFE-VERIFIED (residual AUD-12) | CL rewritten from stored body (`handler.go:4451-4456`); TE stripped by client; AE bucket pairing |
| D1 unsafe methods | SAFE-VERIFIED | `isInvalidatingBytes` dispatch; collapse gate; invalidate on 2xx/3xx only |
| D2 Location invalidation | **FINDING** (AUD-08) | no same-origin check |
| D3 POST-response storage | HARDENING (AUD-15) | kept for Varnish parity, gated on explicit freshness + Content-Location + no client-state (`handler.go:3771-3800`) |
| D4 HEAD | SAFE-VERIFIED | `headFlightKey`/`headRevalKeySuffix`, `remapHeadToGet`, HEAD store guard (`handler.go:3415-3417`), §4.3.5 freshening with validator gate |
| D5 OPTIONS/TRACE/CONNECT/WebDAV | SAFE-VERIFIED | OPTIONS safe + own key; everything else invalidating → proxied, never cached |
| D6 method spelling/override | SAFE-VERIFIED | lowercase `get` ≠ GET → invalidating → proxied; no method-override headers honored |
| E1 collapse key | SAFE-VERIFIED | method + key + declared dims; identity + unsafe excluded (`collapse.go`, ADR-0052/0054/0057) |
| E2 Vary unknown at collapse | SAFE-VERIFIED | cold include-free → fill flight (no bytes); declared dims hashed as variant key |
| E3 unstorable leader | SAFE-VERIFIED | fill followers re-enter; unstorable memo only removes parking; sequential-equivalence suite |
| E4 streaming fan-out before verdict | SAFE-VERIFIED | publish strictly after cacheability verdict; SSE/overflow released at header time via unshareable sentinel |
| E5 unstorable memo | SAFE-VERIFIED | directive-only, flight-eligible-only arming (`stream.go:923-925`, `unstorable_test.go:533+`), bounded, fail-open to collapse |
| E6 leader conditional/range | **FINDING** (AUD-02; range via AUD-03) | 304 published to byte-handoff followers |
| E7 leader abort/timeout/truncation | SAFE-VERIFIED | `bodyComplete=false` → unshareable + no store; channels pinned (`fillflight_test.go:342+`) |
| E8 leader errors | SAFE (policy) | error verdicts → follower own-fetch (except shed 503, documented); generic errors shared on warm flights only |
| E9 latch concurrency | SAFE-VERIFIED | sharded mutex table, atomic followers, res-set-before-close (happens-before via channel close) |
| E10 hit-for-pass markers | N/A | no hit-for-pass objects; memo is control-plane only |
| E11 staggered waves | SAFE-VERIFIED | each re-entry runs full ServeRequest gates; stagger delays own-fetch only |
| F1 SWR refresh identity | **FINDING** (AUD-01) | full ri.Header replay incl. Cookie; Authorization only §3.5-gated |
| F2 stale-if-error serve | SAFE-VERIFIED | serves the originally-storable object through per-request gates |
| F3 304 merge hygiene | SAFE-VERIFIED | `MergeHeaders304` excludes Set-Cookie (pinned `conditional_test.go:113-122`) |
| F4 admin refresh identity | SAFE-VERIFIED | refresh = soft purge (TTL→0); no origin fetch with identity |
| G1–G5 includes/ESI | N/A | no fragment assembly implemented (`include_headers` is keying only) |
| H1 peer-fetch headers | SAFE-VERIFIED | RPC carries only key + vary assertion (`peerfetch.go:648-679`) |
| H2 peer-put | **FINDING** (AUD-05) | unauthenticated + unvalidated store |
| H3 verdict divergence | **FINDING** (AUD-05) | receiver applies none of the gates |
| H4 pipelined correlation | SAFE-VERIFIED | FIFO per-conn worker, conns discarded on error; `seqtracker.go` is event dedup, not transport |
| H5 purge propagation lag | documented model | best-effort 2s fan-out + gossip; eventual consistency by design |
| H6 migration metadata | N/A / SAFE | no migration mechanism; peer codec v7 carries VaryKey/VaryValue/PairedKey |
| I1 ctx-slice escapes | SAFE-VERIFIED | materialization discipline + `bg_revalidate_test.go:31,117` |
| I2 pool reuse | SAFE-VERIFIED | owned clones before publish; release ordering; `fastpath_body_lifetime_test.go:111` |
| I3 store aliasing | SAFE-VERIFIED | hot-tier deep copy + exact-size bodies |
| I4 origin conn reuse | SAFE-VERIFIED (caveat AUD-14) | delegated to fasthttp; no in-repo pin |
| I5 smuggling / ordering | SAFE-VERIFIED | defenses pre-cache; per-conn parse→serve→parse |
| I6 goroutine capture | SAFE-VERIFIED | params not loop vars; semaphores + WaitGroups |
| J1 zero-config defaults | SAFE-VERIFIED | Set-Cookie blocked; Authorization §3.5-gated; no include_headers; Cookie posture = AUD-01/06 caveat |
| J2 VCL shim | N/A | deferred (`internal/vcl` does not exist) |
| J3 route matcher vs key builder | covered by AUD-04 | same path-normalization divergence family |
| J4 Cloudflare | SAFE-VERIFIED | outbound CF API invalidation only; no inbound CF-* trust |
| J5 logging hygiene | data plane SAFE-VERIFIED; **FINDING** AUD-10 (admin token log) | `dataplane.go:1039-1052` (no header values), `origin_headers.go:61-68` |

---

## 4. Regression tests written

All four were written as failing tests against `main` (run 2026-10-27,
`-race -count=3`; each failure message is the finding's evidence).

**Fixed and landed with their fixes:**

- AUD-01 → `internal/cache/swr_identity_regression_test.go` —
  `TestSWRRevalidation_CookiedStaleHitDoesNotPoisonSharedEntry` (synctest
  bubble, canary `sid=A-secret-canary`), plus the Authorization half, the
  cookie-presence-route exemption, and the gate's truth table. Issue #846,
  fix PR #850.
- AUD-04 → `internal/cache/key_origin_parity_regression_test.go` —
  `TestOriginTarget_NormalizedToKeyIdentity` (the poisoning scenario),
  query-verbatim pin, admin/data-plane key parity, and the
  `pathNeedsNormalization` truth table. Issue #847, fix PR #851.

**Proven but not yet fixed — quarantined behind `t.Skip` with issue
references** (`internal/cache/crossclient_poison_regression_test.go`),
each expected to fail on current `main` until its fix lands; remove the
Skip line and the test must then pass unchanged:

- AUD-02 → `TestWarmFlight_Leader304ServedToUnconditionalFollower` — gated
  origin forces the park; asserts follower status/`fetches` count. Issue #848.
- AUD-03 → `TestRangeMiss_StoresPartial206ServedToUnconditionalClients`. Issue #849.

AUD-05 needs the `test/integration` multi-node harness (peer-put auth
+ gate re-check) — not written here because the fix direction (token vs
mandatory mTLS) is an operator decision (see §5).

## 5. Open questions (product decisions)

1. **Cookie posture** (AUD-01/AUD-03/AUD-06): bouine is RFC 9111-conformant
   storing responses to cookied requests, but Varnish's built-in VCL *passes*
   on Cookie/Authorization by default. Should `bypass_on_cookie` become the
   default (breaking the cache-tests cookie case), or should the SWR/refresh
   replay be identity-minimal only? Recommended minimum: identity-minimal
   background replays.
2. **Peer-plane authentication** (AUD-05): shared cluster token vs mandatory
   mTLS when `listen.cluster` is set — decide before fixing.
3. **Range on miss** (AUD-03): strip-and-refetch-full (matches hit path) vs
   separate range cache entries.
4. **Key canonicalization** (AUD-04): raw-target hashing changes every stored
   key (cache invalidation on upgrade) — needs a migration note and cluster
   same-version rollout.
5. **401 with explicit freshness** (C5 residual): RFC permits storage;
   WWW-Authenticate nonce replay across clients may be worth an operator knob.
