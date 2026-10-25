# 63 — Cache-key introspection (keys find / top / recent)

Query what the cache is doing without scanning it
(`GET /v1/keys/find`, `GET /v1/keys/top`, `GET /v1/keys/recent`,
`bouine keys find/top/recent`, issue #816).

---

## Why a query surface, not a listing

bouine deliberately has no object enumeration, for the same reason
Varnish does not: a storage scan yields hashed keys, not URLs — the
XXH128 key is irreversible — and a hot-tier walk is O(millions of
entries). The keys surface inverts the problem:

- **URL→key is deterministic**: the key for a URL is computed with the
  same key policy the data plane uses (the matched route's
  first-match-wins policy, or the default host-ful policy), so
  "look up this URL" is a single map probe, never a scan.
- **key→URL is impossible**: that is the design, not a gap.

`bouine keys top` / `recent` answer the other operator question —
"what am I missing on lately?" — from the URL ring sampler, which
records sampled **non-HIT** traffic per URL prefix (first three path
segments, capped at 512 prefixes, `url_ring_sample_rate`). The
listing is a request-traffic view, not a storage inventory.

## Procedure

```bash
export BOUINE_SERVER=127.0.0.1:9000 BOUINE_TOKEN=<token>

bouine keys find http://example.com/pages/p?x=1
bouine keys find http://example.com/pages/p?x=1 --peer   # cluster: ask the ring owner
bouine keys top --limit 20                               # most-requested prefixes
bouine keys top --class miss                             # only prefixes missing
bouine keys recent                                        # latest-active prefixes
```

`find` reports: the computed key hex, the matched route, the query
policy folded into the key, the ring owner (cluster mode), and this
node's state for the key — tier (`hot`/`warm`), size, TTL, age, hit
counters, the stored Vary header plus the tracked variant count,
protection flags (`backed to warm`, `protected warm copy`,
`keep-grace`), and lazy-ban status. Absence is data too:
`absent (no_entry)`.

## Interpreting the output

- **`local: hot, ... backed to warm`** — the object also has a warm
  copy; hot eviction will demote, not delete.
- **`BANNED: matches an active ban`** — the object sits in a tier but
  the next lookup evicts it. On the hot tier `Ban`'s eager scan
  usually removes entries immediately; a banned-but-present entry
  is the normal state on the warm arm.
- **`N tracked variants` + `vary: ...`** — the URL resolves to a
  variant resolver. A URL alone cannot select a variant (that needs
  request headers such as `Accept-Encoding`), so find reports the
  resolver plus its live fan-out.
- **`owner state:`** (with `--peer`) — the ring owner's answer. A
  failed peer query is reported (`owner state: query failed: ...`)
  and never masks the local answer.

## Semantics and caveats

- **Node-local.** `find`, `top`, and `recent` reflect the pod the
  admin request landed on. On Kubernetes, port-forward a named pod
  (see README); the ClusterIP admin Service routes each connection
  to a random pod.
- **Probe is side-effect-free.** The storage probe never counts a
  hit, never marks SIEVE visited, never promotes a warm object to
  hot, and never evicts a lazy-banned entry — the observation cannot
  perturb what it observes.
- **`--peer` is best-effort.** The owner query rides the peer admin
  channel with the fetch RPC budget (500 ms) and the per-address
  breaker; an owner in breaker cooldown reports an error instead of
  hanging.
- **No `--scan`.** v1 ships without a storage enumeration mode
  (explicitly deferred by #816); if you need bulk key export, use the
  snapshot/restore tooling instead.
