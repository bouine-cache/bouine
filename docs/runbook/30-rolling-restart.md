# 30 — Rolling restart (zero 5xx)

Procedure and verification for rolling bouine across a Kubernetes StatefulSet
without dropping traffic or returning 5xx errors to clients.

---

## Why this matters

bouine uses a graceful-shutdown sequence: on `SIGTERM` it stops accepting new
connections, drains in-flight requests (up to 10 s), leaves the cluster gossip
ring, and exits. Kubernetes sends `SIGTERM` when it evicts a pod during a
`rollout`. If the drain window is shorter than the longest in-flight request or
if the pod is removed from the Service `Endpoints` before `SIGTERM` is sent,
clients see connection-refused errors that manifest as 5xx in upstream metrics.

---

## Prerequisites

| Requirement | Why |
|-------------|-----|
| `terminationGracePeriodSeconds` ≥ 40 s | Must exceed drain duration (10 s) + in-flight request + cluster leave timeout (10 s). |
| `preStop` hook (httpGet `/drain`) | Kubernetes removes the pod from Endpoints asynchronously; the `/drain` endpoint marks the pod not-ready and blocks for `admin.drain_duration` (default 10 s) so kube-proxy can deregister before `SIGTERM` arrives. The distroless container image has no shell, so an `exec` sleep cannot be used. |
| `PodDisruptionBudget minAvailable: 2` | Prevents Kubernetes from evicting more than one pod at a time in a 3-node cluster. |
| `readinessProbe` on `/readyz` | Traffic is removed before the pod enters `Terminating` only if the probe fails; new pod receives traffic only once it passes. |
| `publishNotReadyAddresses: true` on the headless Service | Ensures DNS resolves all StatefulSet pods (including unready ones) for gossip seed discovery. |

---

## Step-by-step

```bash
# 1. Check that PDB is in place
kubectl get pdb -n bouine-prod

# 2. Start background traffic monitor (check for 5xx in real time)
kubectl -n bouine-prod logs -f -l app=bouine --prefix=true | \
  grep '"status":5' &
MONITOR_PID=$!

# 3. Trigger rolling restart
kubectl -n bouine-prod rollout restart statefulset/bouine

# 4. Wait for rollout (≈ 3 × terminationGracePeriodSeconds)
kubectl -n bouine-prod rollout status statefulset/bouine --timeout=180s

# 5. Verify no 5xx in the monitoring window
kill $MONITOR_PID 2>/dev/null
kubectl -n bouine-prod exec -it deploy/traffic-gen -- \
  bash -c 'grep -c "HTTP/1.1 5" /tmp/access.log || echo "0 errors"'
```

---

## Automated verification (SLO DP-5)

The `bench/loadtest/scenarios/4.5_rolling_update/run.sh` scenario runs k6 at
1 k RPS while executing a rolling restart and asserts that the 5xx error rate
stays at 0 %.

```bash
# Against K8s cluster
bash bench/loadtest/scenarios/4.5_rolling_update/run.sh
```

Expected output: `✓ http_req_failed rate<0.001%`.

---

## Helm chart settings (reference)

```yaml
# deploy/helm/bouine/values.yaml

# Ensures Kubernetes drains endpoints before SIGTERM.
lifecycle:
  preStop:
    httpGet:
      path: /drain
      port: admin

terminationGracePeriodSeconds: 40

# Prevents simultaneous eviction of two pods.
podDisruptionBudget:
  enabled: true
  minAvailable: 2

# Traffic only routed to ready pods.
readinessProbe:
  httpGet:
    path: /readyz
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 3
  failureThreshold: 3

# Drain duration (how long /drain blocks before returning).
# config:
#   admin:
#     drain_duration: 10s
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| 503 during rollout | `preStop` hook too short; kube-proxy updated Endpoints before pod drained | Increase `admin.drain_duration` in config (default 10 s) |
| `FailedPreStopHook` events | Container image is distroless (no `sh`/`sleep`); exec-based preStop fails | Use `httpGet` preStop to `/drain` (default in chart since 0.3.2) |
| 502 after new pod starts | New pod not yet joined gossip ring; peer-fetch fails | Check logs for cluster join status; `/readyz` passes before ring join by design (avoids StatefulSet deadlock). Wait for background join to complete or increase `initialDelaySeconds` |
| Rollout stuck | PDB `minAvailable` prevents eviction | Check `kubectl get pdb`; verify at least `minAvailable` pods are Ready |
| Long rollout | `terminationGracePeriodSeconds` too high relative to actual drain time | Reduce to `max(in_flight_p99_ms / 1000, 15)` seconds |

---

## Related runbooks

- [00-lifecycle.md](./00-lifecycle.md) — start, stop, hot-reload
- [20-purge-ban.md](./20-purge-ban.md) — cache invalidation
