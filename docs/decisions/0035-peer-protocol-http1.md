# ADR-0035: Peer wire protocol is HTTP/1.1 over mTLS

Date: 2026-08-22

## Status

Accepted

## Context

ADR-0034 dropped HTTP/2 and HTTP/3 support. The cluster peer-fetch and
broadcast protocol previously used HTTP/2 multiplexing over mTLS
connections (`ForceAttemptHTTP2: true` on the peer transport).

Without HTTP/2, peer communication falls back to HTTP/1.1 keep-alive
with connection pooling. This increases the number of TCP connections
per peer (one per concurrent request instead of a single multiplexed
stream), but fasthttp's connection pool mitigates this via
`MaxConnsPerHost` and keep-alive reuse.

## Decision

The peer wire protocol is HTTP/1.1 over mTLS. `transport.Client` wraps
`fasthttp.Client` with context-aware `Do()`. The mTLS configuration is
set on the fasthttp client's `TLSConfig`. `NextProtos` is set to
`["http/1.1"]` so the TLS handshake does not negotiate h2.

## Consequences

- More TCP connections per peer at high concurrency (mitigated by
  pooling, `MaxConnsPerHost: 64`).
- No per-stream flow control — a slow peer response blocks one
  connection but does not affect others (HTTP/1.1 pipelining is not
  used; each request gets its own connection from the pool).
- Rolling upgrade: a mixed cluster of old (HTTP/2) and new (HTTP/1.1)
  nodes can communicate because HTTP/2 endpoints fall back to HTTP/1.1
  when ALPN negotiation fails.
