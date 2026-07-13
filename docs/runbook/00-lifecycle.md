# 00 — Lifecycle operations

Start, stop, reload, and drain procedures for bouine.

---

## Start

```bash
bouine serve --config /etc/bouine/config.yaml --log-format json
```

On Kubernetes the StatefulSet container runs this command via the Helm
chart (see `deploy/helm/bouine/templates/statefulset.yaml`). The
`--log-format json` flag ensures structured logging for aggregation.

### Startup sequence

1. Config loaded and validated.
2. Storage tiers initialised (hot → warm).
3. Admin server starts on `listen.admin` (default `:9000`).
4. Data-plane listeners start on `listen.http`, `listen.https`.
5. `/readyz` returns `200` once store is loaded and all listeners are bound.
6. Cluster join (if `cluster.enabled`): memberlist contacts seed nodes
   with retry (every 2s for up to 120s). Join succeeds once at least
   one peer besides self is discovered. Cluster join runs in the
   background and does not block readiness — this avoids a
   StatefulSet bootstrapping deadlock where pod-0 waits for
   pod-1's DNS, but pod-1 hasn't started because pod-0 isn't ready.
   The headless Service should have `publishNotReadyAddresses: true`
   so peer DNS resolves during startup.
7. Active health checks begin for all upstream pools.

### Readiness vs liveness

| Probe      | Endpoint    | Meaning                                           |
|------------|-------------|---------------------------------------------------|
| Readiness  | `/readyz`   | Store loaded, all listeners bound, ready for traffic. |
| Liveness   | `/healthz`  | Process alive, can serve admin requests.          |

Kubernetes will not route traffic until `/readyz` returns `200`. A
failed liveness probe triggers a pod restart.

---

## Stop (graceful shutdown)

bouine uses a `shutdown.Sequencer` (`internal/runtime/shutdown`) that
runs registered steps in order, each with a time budget:

1. **Mark not ready** — `/readyz` returns `503`. Kubernetes stops
   sending new connections. The `preStop` hook sleeps one readiness
   period to let in-flight probes propagate.
2. **Drain data-plane listeners** — stop accepting new connections,
   finish in-flight requests within budget.
3. **Leave cluster** — `memberlist.Leave` with a timeout so peers
   remove this node from the hash ring.
4. **Flush storage** — WAL sync, warm-tier segment close.
5. **Close admin** — final metrics scrape window, then shutdown.

The total budget is `terminationGracePeriodSeconds` (Helm default: 30s).

### Manual stop

```bash
kill -TERM <pid>
# or on Kubernetes:
kubectl delete pod bouine-0
```

SIGTERM triggers the sequencer. SIGKILL (after grace period) is a hard
kill — avoid if possible.

---

## Reload (hot)

bouine supports hot reload of configuration and TLS certificates without
restarting the process. The `internal/config.Watcher` uses `fsnotify`
for file changes and also listens for `SIGHUP`.

### Via signal

```bash
kill -HUP <pid>
```

### Via admin API

```bash
curl -X POST http://127.0.0.1:9000/v1/config/reload
```

### Via CLI

```bash
bouine refresh <url>   # not the same — this is a soft-purge
```

> **Note**: config reload via API currently returns `200` immediately.
> The actual reload happens asynchronously via the Watcher.

### What is reloaded

| Component          | Hot-reloadable | Notes                              |
|--------------------|----------------|------------------------------------|
| Routes             | Yes            | New routes take effect immediately.|
| Upstream pools     | Yes            | Targets, health check config.      |
| Cache TTLs         | Yes            | Per-route cache settings.          |
| TLS certificates   | Yes            | Cert + key files watched.          |
| Listen addresses   | **No**         | Requires restart.                  |
| Storage settings   | **No**         | Requires restart.                  |
| Cluster settings   | **No**         | Requires restart.                  |

### Reload failure

If the new config file is invalid YAML or fails validation, the old
config stays in effect. The error is logged at `error` level:

```json
{"level":"ERROR","msg":"config reload failed","error":"..."}
```

Monitor `bouine_config_reload_total{result="error"}` in Prometheus.

---

## Drain (Kubernetes rolling update)

During a rolling update, Kubernetes sends SIGTERM to the old pod. The
graceful shutdown sequence (above) handles draining.

### Best practices

- Set `terminationGracePeriodSeconds` ≥ 30s (Helm default).
- PodDisruptionBudget (`minAvailable: 1`) prevents draining all replicas
  simultaneously.
- The `preStop` sleep ensures the endpoints controller removes the pod
  from the Service before connections stop.
- Monitor `bouine_inflight_requests` to confirm drain completes.

### Rolling update order

For a 3-replica StatefulSet:

1. `bouine-2` is terminated and drained.
2. New `bouine-2` starts, passes readiness, joins cluster.
3. `bouine-1` is terminated and drained.
4. … and so on.

The consistent-hash ring rebalances automatically as nodes join/leave.
