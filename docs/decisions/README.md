# Architecture Decision Records

This directory holds the project's [Architecture Decision Records](https://adr.github.io/)
in [MADR](https://adr.github.io/madr/) format.

An ADR is **required** when a change:

- Adds or removes a dependency on the allow-list.
- Changes a protocol or wire format.
- Changes the eviction algorithm or cache state machine in ways
  observable to operators.
- Changes the cluster protocol.
- Touches the VCL shim's supported surface.
- Adds or modifies a pre-commit hook.

ADRs are immutable once accepted. To revisit a decision, write a new
ADR that supersedes the old one and update the old one's `Status`
field to `Superseded by ADR-NNNN`.

## Process

1. Copy `adr-template.md` to `NNNN-short-title.md`, where `NNNN` is the
   next four-digit number.
2. Fill in the sections.
3. Open a PR. The ADR ships in the same PR as the change it documents.
4. On merge, the ADR is permanent.

## Index

| Number | Title                                              | Status   |
|--------|----------------------------------------------------|----------|
| 0001   | Record architecture decisions                      | Accepted |
| 0002   | HTTP/3 via quic-go                                 | Accepted |
| 0003   | Hand-rolled PROXY protocol parser                  | Accepted |
| 0004   | One *http.Server per listener                      | Accepted |
| 0005   | Round-robin upstream selection via atomic counter  | Accepted |
| 0006   | Drop Fiber, unify admin on net/http                | Accepted |
| 0007   | Cluster design — memberlist gossip + consistent hash | Accepted |
| 0008   | Cluster mode: local cache + gossip invalidation      | Accepted |
| 0009   | Cache state-machine hardening (RFC 9111 conformance) | Accepted |
| 0010   | Helm chart lint in pre-commit and pre-push hooks     | Accepted |
| 0011   | Per-route TTL override decoupled from upstream Cache-Control | Accepted |
| 0012   | Block caching of Set-Cookie responses by default     | Accepted |
| 0013   | ttl_default makes no-freshness responses cacheable   | Accepted |
| 0014   | Anti-entropy reconciliation for full cluster mode    | Accepted |
| 0015   | Binary wire format for cluster key replication       | Accepted |
| 0016   | Refresh-Before-Expiry Per Route                      | Accepted |
| 0017   | Static file serving routes                           | Accepted |
| 0018   | Backfill cooldown for anti-entropy reconciler        | Accepted |
| 0019   | Churn detection for anti-entropy backfill            | Accepted |
| 0020   | Hot-to-warm sync                                     | Accepted |
| 0021   | Refresh popularity gate                               | Accepted |
| 0022   | Refresh persist cycles                               | Accepted |
| 0023   | Warm-tier eviction (SIEVE)                            | Accepted |
