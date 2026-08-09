# Example configurations

This directory ships reference configurations for common bouine
deployment shapes. Each example includes a `config.yaml`, a
`docker-compose.yaml` to boot a self-contained stack, and a `README.md`
explaining the trade-offs.

## Available examples

- **[api-gateway/](api-gateway/)** — short-TTL API caching with SWR and
  stale-if-error. Suitable for REST/GraphQL APIs.
- **[ecommerce/](ecommerce/)** — mixed cacheable + per-user-private routes
  with a 2-node cluster and warm tier. Includes a detailed README.
- **[static-site/](static-site/)** — CDN-like long-TTL caching for static
  assets with heuristic freshness for HTML.
- **[static-site-self-served/](static-site-self-served/)** — bouine serves
  files directly from disk, no origin server needed.

## Running

```bash
cd examples/api-gateway
docker compose up -d
curl -sI http://localhost:8080/ | grep x-cache
docker compose down -v
```
