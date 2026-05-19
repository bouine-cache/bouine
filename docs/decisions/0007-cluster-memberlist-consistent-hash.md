# ADR-0007: Cluster design — memberlist gossip + consistent hash

- **Status**: Accepted
- **Date**: 2026-05-19
- **Deciders**: @thylong
- **Phase**: phase 4 (pre-flight)

## Context

Phase 4 requires horizontal scaling: multiple bouine pods must share
the same cache namespace, forward misses to the owning peer before
going to origin, and propagate purges cluster-wide within 1 s p99.

Three coordination approaches were considered:

1. **External coordinator** (etcd, Consul, ZooKeeper) — adds an
   operational dependency, a new failure domain, and latency on every
   membership change.
2. **Gossip without a hash ring** — simple but requires broadcast
   storms to locate owners; O(N) lookup, hard to bound.
3. **Gossip for membership + consistent hash for routing** — O(1)
   key → owner mapping, no external coordinator, well-understood
   failure characteristics.

The Varnish approach (`vmod_directors` + manual topology) and the
Squid approach (ICP/HTCP) were both rejected as over-engineered for
the K8s StatefulSet deployment target.

## Decision

**L6 (cluster) uses `hashicorp/memberlist` for gossip and a
consistent hash ring with bounded loads for request routing.**

Specifically:

- `hashicorp/memberlist` handles: node join/leave/failure detection,
  SWIM protocol health probing, user-data broadcasting. Proven by
  Consul, Serf, and Nomad in production at scale.
- Consistent hash with bounded loads (Google 2017 paper) avoids hot
  spots: each real node has 256 virtual nodes; the load factor cap
  is configurable (default 1.25×).
- Peer-fetch is HTTP/2 over mTLS on a dedicated port (`:8443`). The
  cluster CA is separate from the data-plane CA.
- Protocol framing: every peer-fetch and gossip metadata message
  starts with `magic(4) + version(uint16)` — see ADR-0005 cluster
  wire protocol versioning.
- K8s bootstrap: headless `Service` DNS gives stable peer addresses;
  pods self-discover by resolving the headless service SRV records.
  No external coordinator, no seed list required in practice.

## Consequences

### Positive
- No external dependency (etcd, Consul not required).
- `hashicorp/memberlist` is pre-approved in `AGENTS.md §5` and
  battle-tested in production systems.
- Consistent hash provides O(1) owner lookup and bounded key
  movement on scale-out.
- mTLS peer fetch is a natural fit for K8s with cert-manager.

### Negative / trade-offs
- memberlist's SWIM probing generates ~constant gossip traffic (tuned
  to ~2 kB/s per node at 10-node clusters — negligible).
- Bounded-load hash requires storing virtual node → real node
  mapping in memory (~O(256 × N) entries, trivially small).
- Adding a cluster port (`:8443`) requires a NetworkPolicy rule.

### Risks
- Gossip partitions under aggressive network policies. Mitigated by
  the SWIM indirect-probe mechanism and a repair timeout gate.

## References

- `PLAN.md §5` (Clustering)
- `PLAN.md §5.5` (Wire protocol versioning)
- `docs/security/threat-model.md` T01, T35, T36
- [memberlist](https://github.com/hashicorp/memberlist)
- [Consistent Hashing with Bounded Loads](https://arxiv.org/abs/1608.01350)
