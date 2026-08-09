# Self-Served Static Site Example

bouine serves files directly from disk — no separate origin server
needed. The OS page cache handles hot caching. Cache headers are set
explicitly on responses.

## Quick start

```bash
# Create sample files
mkdir -p html assets
echo '<!DOCTYPE html><html><body><h1>Hello from bouine</h1></body></html>' > html/index.html
echo 'body { font-family: sans-serif; }' > assets/style.css

docker compose up -d

# Serve HTML
curl -s http://localhost:8080/

# Serve CSS (with 24h Cache-Control)
curl -sI http://localhost:8080/assets/style.css | grep cache-control

# Admin health
curl -s http://localhost:9000/healthz
```

## Cleanup

```bash
docker compose down -v
```

## Config overview

- **`/assets/`**: served from `/var/www/assets`, `Cache-Control: public, max-age=86400`
- **`/` (fallback)**: served from `/var/www/html`, index.html fallback
- **No upstream pool** — bouine reads from disk directly
- **No cache** — the OS page cache handles caching; bouine just serves files
- **Security headers**: `X-Content-Type-Options: nosniff` on all responses

## Trade-offs

- No origin server needed — simplest possible setup
- OS page cache provides hot caching for free
- No bouine-level cache (enable per route if you need TTL-based eviction or cluster replication)
- `max_file_size: 50MiB` prevents serving oversized files
