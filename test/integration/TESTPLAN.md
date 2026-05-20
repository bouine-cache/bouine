# Manual cache behavior test plan

## Setup

```bash
# 1. Build and deploy
docker build -t bouine:dev .
kubectl -n bouine-test delete sts bouine 2>/dev/null
helm upgrade --install bouine deploy/helm/bouine \
  --namespace bouine-test --create-namespace \
  --set image.repository=bouine --set image.tag=dev \
  --set image.pullPolicy=Never --set replicaCount=3 \
  --set config.listen.https="" --set config.listen.http3="" \
  --set config.storage.hot_max_bytes=256MiB \
  --set config.storage.warm_dir="" --set warmVolume.enabled=false \
  --set topologySpreadConstraints="" \
  --set config.cluster.enabled=true \
  --set "config.upstream_pools[0].name=origin" \
  --set "config.upstream_pools[0].targets[0]=origin.bouine-test.svc:8080" \
  --set "config.routes[0].pool=origin"

# 2. Deploy the test origin (from test/integration/origin)
kubectl -n bouine-test delete pod origin 2>/dev/null
kubectl -n bouine-test delete svc origin 2>/dev/null
kubectl -n bouine-test run origin --image=golang:1.26-alpine \
  --overrides='{"spec":{"containers":[{"name":"origin","image":"golang:1.26-alpine","command":["go","run","./..."],"workingDir":"/src","ports":[{"containerPort":8080}],"volumeMounts":[{"name":"src","mountPath":"/src"}]}],"volumes":[{"name":"src","hostPath":{"path":"'$(pwd)'/test/integration/origin"}}]}}'
kubectl -n bouine-test expose pod origin --port=8080

# Or simpler — copy files into a running pod:
kubectl -n bouine-test run origin --image=golang:1.26-alpine -- sleep 3600
kubectl -n bouine-test cp test/integration/origin origin:/src
kubectl -n bouine-test exec origin -- sh -c 'cd /src && go run . &'
kubectl -n bouine-test expose pod origin --port=8080

# 3. Port-forward
kubectl -n bouine-test port-forward svc/bouine 8080:80 &
```

## Tests

Each test uses `curl -sI` to show headers. Look for `X-Cache` and
`Age` headers.

### 1. HIT — cacheable response served from cache

```bash
# First request: MISS (origin contacted)
curl -sI http://localhost:8080/hit
# Expect: X-Cache: MISS

# Second request: HIT (served from cache, same body)
curl -sI http://localhost:8080/hit
# Expect: X-Cache: HIT, Age: > 0
```

### 2. MISS — no-store prevents caching

```bash
curl -sI http://localhost:8080/miss
# Expect: X-Cache: MISS

curl -sI http://localhost:8080/miss
# Expect: X-Cache: MISS (always, never cached)
```

### 3. BYPASS — private response not stored

```bash
curl -sI http://localhost:8080/bypass
# Expect: no X-Cache header (bypassed entirely)

curl -sI http://localhost:8080/bypass
# Expect: no X-Cache header (bypassed entirely)
```

### 4. STALE — stale-while-revalidate

```bash
# First request: MISS
curl -sI http://localhost:8080/stale
# Expect: X-Cache: MISS

# Wait 2 seconds (TTL is 1s, SWR window is 3600s)
sleep 2

# Second request: HIT (stale but within SWR window)
curl -sI http://localhost:8080/stale
# Expect: X-Cache: HIT, Age: > 1
```

### 5. REVALIDATE — must-revalidate with ETag

```bash
# First request: MISS
curl -sI http://localhost:8080/revalidate
# Expect: X-Cache: MISS, ETag: "reval-v1"

# Second request: origin returns 304, cache refreshed
curl -sI http://localhost:8080/revalidate
# Expect: X-Cache: HIT (revalidated), Age: 0
```

### 6. Vary — different encodings cached separately

```bash
curl -sI -H "Accept-Encoding: gzip" http://localhost:8080/vary
# Expect: X-Cache: MISS

curl -sI -H "Accept-Encoding: gzip" http://localhost:8080/vary
# Expect: X-Cache: HIT

curl -sI -H "Accept-Encoding: br" http://localhost:8080/vary
# Expect: X-Cache: MISS (different variant)
```

### 7. Heuristic freshness — Last-Modified without Cache-Control

```bash
curl -sI http://localhost:8080/heuristic
# Expect: X-Cache: MISS

curl -sI http://localhost:8080/heuristic
# Expect: X-Cache: HIT (heuristic TTL = 10% of 24h ≈ 2.4h)
```

### 8. Purge — invalidate via admin API

```bash
# Populate cache
curl -s http://localhost:8080/hit > /dev/null

# Verify it's cached
curl -sI http://localhost:8080/hit | grep X-Cache
# Expect: X-Cache: HIT

# Purge (via a second port-forward to admin)
kubectl -n bouine-test port-forward svc/bouine 9000:9000 &
curl -X POST http://localhost:9000/v1/purge \
  -H 'Content-Type: application/json' -d '{"url":"/hit"}'

# Verify cache miss
curl -sI http://localhost:8080/hit | grep X-Cache
# Expect: X-Cache: MISS
```

### 9. Cluster peers — verify 3-node cluster

```bash
kubectl -n bouine-test exec bouine-0 -- /bouine cluster peers --server 127.0.0.1:9000
# Expect: 3 entries with different names (bouine-0, bouine-1, bouine-2)
```

### 10. Node failure — kill a pod, traffic keeps flowing

```bash
# Kill one pod
kubectl -n bouine-test delete pod bouine-1

# Requests still work on remaining pods
curl -sI http://localhost:8080/hit
# Expect: 200 (may be MISS if the killed pod owned the key)

# Pod comes back
kubectl -n bouine-test get pods -w
```

## Cleanup

```bash
helm uninstall bouine --namespace bouine-test
kubectl delete namespace bouine-test
kill %1 %2 2>/dev/null  # kill port-forwards
```
