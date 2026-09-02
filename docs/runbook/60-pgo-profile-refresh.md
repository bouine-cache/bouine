# PGO — profile-guided optimization and profile refresh

`bouine` ships with a committed `cmd/bouine/default.pgo` profile. The
Go toolchain picks it up automatically on every build of the main
package, so release binaries, Docker images, and local builds are all
PGO-compiled. Nothing to enable at runtime.

## What operators should know

- **No runtime impact.** PGO is a compile-time transformation
  (inlining + hot-call devirtualization). The binary is slightly larger
  (~2–5%); memory and startup characteristics are unchanged.
- **No config surface.** There is no `pgo:` section; if you build bouine
  yourself from a tarball, the committed profile applies unless you
  remove it or pass `-pgo=off` to `go build`. (The profile lives at
  `cmd/bouine/default.pgo` — moving it to the repo root silently
  disables PGO.)

## When the profile matters

If you build your own bouine binaries *and* you run a workload that
differs drastically from the shipped composite (hit-only 30k RPS,
miss-storm, mixed-realistic), you can re-capture from *your* traffic:

1. Enable the admin pprof endpoints: `admin.pprof_enabled: true`
   (admin port only, keep it network-isolated).
2. Capture a CPU profile under representative load:
   `GET /debug/pprof/profile?seconds=60` on the admin port.
3. Replace `cmd/bouine/default.pgo` with the captured profile, rebuild:
   `go build -pgo=auto -o bouine ./cmd/bouine`.

This is the same mechanism the project itself uses: `make pgo-refresh`
runs three load legs (hit / miss / mixed) against the load-test stack
and merges the profiles (ADR-0041). The `release-pgo.yml` workflow
refreshes the committed profile on every release-prepare PR.

## Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| Build says `pgo: no profile` | `cmd/bouine/default.pgo` deleted or corrupted | `git checkout cmd/bouine/default.pgo`, rebuild |
| Build fails `pgo: parse error` | non-gzip file named `default.pgo` in `cmd/bouine/` | restore from git |
| `make pgo-refresh` fails "only N samples" | capture legs too short / load too low | raise `PGO_PROFILE_SECS` or leg RPS |
| `make pgo-capture` fails "k6" not found | load-gen image tag changed | check `bench/loadtest/docker-compose.yaml` digest |

## Verification

- `make pgo-verify` builds with and without the profile and reports the
  binary-size delta (sanity, not a gate).
- `make bench-gate` alloc budgets are PGO-independent and must stay
  green with the profile installed.
