# 60 — Accept-Encoding bucketing: upgrade and mixed-version window

What changed in ADR-0051, what to expect during the rollout, and how to
diagnose a cluster that looks like it lost hit ratio.

---

## What changed

`Accept-Encoding` participates in the variant key as a negotiation
bucket (`zstd | br | gzip | identity`) instead of the sorted raw
header, and origin-bound requests carry the canonical bucket token
(`Accept-Encoding: br`) instead of the client's dialect. Consequences:

- All clients that negotiate the same best coding share one stored
  variant. Six realistic browser dialects collapse to two variants.
- **All existing AE variant keys change on upgrade.** Previously stored
  variants are unreachable under the new keys and expire naturally by
  TTL/eviction. Do NOT mass-purge: the re-fill spike is the exact
  origin-load cliff ADR-0045 documented. Expect a transient miss-rate
  step on `Vary: Accept-Encoding` routes for one TTL cycle.
- Nodes on opposite sides of the upgrade compute different variant
  keys for the same request. The peer-fetch assertion gate
  (`peerfetch.go`) rejects mismatches with a miss — never a wrong body
  — so a mixed-version cluster temporarily cannot share AE variants.

## Procedure

1. Roll the StatefulSet as usual (see runbook 30). No config change is
   required — bucketing is the default.
2. Complete the rollout. Do not stop halfway and do not roll back
   partway; both leave the cluster mixed indefinitely.
3. Watch for one TTL cycle on AE-varied routes:
   - `bouine_requests_total{cache_result="MISS"}` — an initial step up
     (old keys unreachable), then a step DOWN below the pre-upgrade
     baseline (fewer distinct variants = fewer first-miss fills).
   - `vary_cap_hits_total` — should step down or disappear on routes
     where AE was the fragmenting header.
   - `bouine_peer_fetch_hits_total` — dips during the mixed window,
     recovers to baseline once every node is on the new build.

## Diagnosis

| Symptom | Cause | Action |
|---|---|---|
| Hit ratio low > 1 TTL after rollout completes | Nodes still mixed-version (rollout stalled, or a canary left behind) | `bouine_cluster_protocol_mismatch_total` and per-node buildinfo via `GET /v1/config`; finish the rollout |
| Hit ratio low only on routes with `Vary: Accept-Encoding` and an origin that keys on the raw AE string | Rare origin behavior (body varies by full AE string) | Set `cache.key.verbatim_encoding: true` on that route, cluster-wide (all nodes, same PR) |
| Peer-fetch variant-mismatch counters climbing with no rollout in progress | Two nodes configured with different `verbatim_encoding` values | Align the setting across all nodes serving the route — same hazard class as `include_headers` mismatches |

## Escape hatch

`cache.key.verbatim_encoding: true` restores pre-bucketing keying
and verbatim origin forwarding. It re-fragments the variant space, so
use it only for the origin-behavior case above, and set it on every
node serving the route in one change.
