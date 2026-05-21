# Example configurations

This directory ships reference configurations for common bouine
deployment shapes. Examples land in phase 4.5 (`PLAN.md §15`).

Planned:

- `static-site/` — CDN-like long-TTL caching for static assets.
- `api-gateway/` — short-TTL, surrogate-key driven invalidation in
  front of microservices.
- `ecommerce/` — mixed cacheable + per-user-private routes.
- `varnish-migration/` — output of `bouine config translate --vcl`
  alongside the original Varnish VCL for reference.

Each example includes: a `config.yaml`, a `docker-compose.yaml` to
boot a self-contained stack, and a `README.md` explaining the
trade-offs.
