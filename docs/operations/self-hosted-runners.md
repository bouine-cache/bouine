# Self-Hosted CI Runners

bouine's CI pipeline runs on GitHub-hosted runners (`ubuntu-latest`,
`ubuntu-24.04-arm`, `macos-14`). As we add heavier test jobs —
integration tests, benchmark gates, conformance gates — we'll move
them to self-hosted runners for reproducible CPU and memory.

## Current runner usage

| Job | Runner | Why |
|-----|--------|-----|
| `prek` | `ubuntu-latest` | Fast (30s), hosted is fine |
| `govulncheck` | `ubuntu-latest` | Fast, hosted is fine |
| `test-linux` | `ubuntu-latest`, `ubuntu-24.04-arm` | Hosted, 2-3 min |
| `test-macos` | `macos-14` | Hosted, push-only |
| `conformance` | `ubuntu-latest` | Hosted, needs Node.js |
| `bench` (commented out) | — | Needs pinned CPU, self-hosted |
| `loadtest-nightly` | `self-hosted` | Already self-hosted |

## Setting up self-hosted runners

### Prerequisites

- Linux amd64 machine (bare metal or VM with ≥ 4 vCPU, 8 GB RAM)
- Linux arm64 machine (optional, for arm64 testing)
- Docker installed (for integration tests using `kind`)
- Go 1.26.x installed

### Registration

1. Go to **Settings → Actions → Runners → New self-hosted runner** in the GitHub repo
2. Follow the platform-specific instructions to download and configure the runner
3. Add labels: `self-hosted`, `linux`, `amd64` (or `arm64`)
4. Install as a service: `./svc.sh install && ./svc.sh start`

### Runner groups

For security, create a runner group and restrict it to the bouine repo.
Self-hosted runners can execute arbitrary code from PRs — only register
them for repos you trust.

### CI workflow changes

Once self-hosted runners are registered, update `ci.yml`:

```yaml
# Before (hosted):
test-linux:
  runs-on: ubuntu-latest

# After (self-hosted):
test-linux:
  runs-on: [self-hosted, linux, amd64]
```

For jobs that need both hosted and self-hosted fallback:
```yaml
runs-on: ${{ vars.USE_SELF_HOSTED == 'true' && fromJSON('["self-hosted", "linux"]') || 'ubuntu-latest' }}
```

## Jobs to move to self-hosted (after setup)

1. **Benchmark gate** — uncomment the `bench` job, use `self-hosted` runner
   for pinned CPU. Compare with `benchstat` against `bench/results/baseline.txt`.
2. **Integration tests** — run `make integration` on self-hosted runner
   (in-process 3-node cluster, no K8s cluster needed).
3. **Chaos tests** — run `make chaos` on self-hosted runner.
4. **Conformance gate** — move conformance to self-hosted and add a
   score regression check (fail if score < 342).