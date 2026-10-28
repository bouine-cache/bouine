# Data-plane connection limit (`listen.max_connections`)

The connection cap, the refusal path, and how to read the rejection
signal. Introduced alongside `bouine_conn_rejections_total` (the exact
rejection counter).

## What the cap does

`listen.max_connections` caps simultaneously **open** data-plane
connections (HTTP + HTTPS share the budget; the admin and cluster
listeners are separate). Under HTTP/1.1 a request holds its
connection, so the cap also bounds the in-flight request pile a slow
origin can cause (issue #562). Over-limit connections get a raw

```
HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n
```

at accept, then close.

**Idle keep-alive connections hold a slot too** — a front-end's
keep-alive pool counts against the cap even when nothing is in flight.
Size the cap for the client population (CDN fronts hold tens of
thousands of idle keep-alives), not just peak request concurrency.

## Reading the signal

- `bouine_conn_rejections_total` — **exact** count of refused
  connections. Non-zero rate means the client population exceeds the
  cap: either raise the cap or fix the clients (a reconnect storm, an
  LB with a bigger keep-alive pool than you sized for).
- The warn log — `connection rejected: max_connections reached` — is
  **rate-limited to one line per 10 s** per listener, with the count of
  suppressed rejections folded into the next line
  (`suppressed_since_last`). Before the rate limit, a connection flood
  cost one structured-log line per refusal (24 040 lines in one
  measured run): the refusal path became an expense exactly when the
  process was busiest. Alert on the counter, grep the log.

## Sizing

- The shipped default is **65536**, matching the fd limit the daemon
  raises at boot (`applyRuntimeTuning` → `RaiseFileLimit(65536)`) — the
  same envelope NGINX's default worker set gives (16 384 × 4 workers).
  The historical 4096 refused legitimate client populations a single
  CDN front produces (measured: raw 503s for every connection past the
  4096th at 5 000 keep-alive VUs, while NGINX served 8 000 VUs without
  error).
- Each accepted fast-path connection pins roughly **32 KiB**
  (16 KiB read buffer + goroutine stack): 65 536 connections pin about
  2 GiB. On small pods, size the cap to `GOMEMLIMIT / 32 KiB` at most,
  and prefer closing idle connections sooner
  (`listen.idle_timeout`) to a small cap.
- `0` disables the cap entirely (unbounded; not recommended — the
  semaphore is the defense against slowloris-style goroutine piles).

## Failure modes

- **`bouine_conn_rejections_total` rising with 503s at clients past a
  fixed connection count**: population exceeds the cap — raise it or
  check for a reconnect storm (a `Connection: close` refusal makes
  clients reconnect, so a too-small cap under sustained population
  raises accept load).
- **`too many open files` in logs before the cap binds**: the fd limit
  is below the cap (the daemon could not raise it to 65 536); lower the
  cap below the fd limit or raise the container's fd ulimit.
- **5xx burst with in-flight far below the cap**: not this limit —
  check the fetch semaphore (`bouine_fetch_shed_total`, runbook 53) or
  origin ejection (runbook 54) first.
