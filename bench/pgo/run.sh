#!/usr/bin/env bash
# bench/pgo/run.sh — Capture, merge, and install the default.pgo profile.
#
# Overview (see docs/decisions/0041-profile-guided-optimization.md):
#   1. Boot the single-node load-test topology (origin + bouine only).
#   2. Run three traffic legs against bouine — hit-only, miss-storm,
#      mixed — capturing a CPU profile for each via the admin
#      /debug/pprof/profile endpoint (admin.pprof_enabled: true).
#   3. Merge the three profiles (go tool pprof -proto) so the committed
#      profile represents both hit-heavy and miss-heavy deployments
#      (ADR-0041: users span a wide hit-ratio range; one leg would bias
#      inlining toward a single workload).
#   4. Install the merged profile as cmd/bouine/default.pgo — the
#      location `go build ./cmd/bouine` (and every build of the main
#      package) picks up automatically. NOTE: a repo-root default.pgo
#      is silently IGNORED for main-package builds; Go only consults
#      the main package's own directory.
#
# Usage:
#   bench/pgo/run.sh capture     # run legs + capture profiles (work dir)
#   bench/pgo/run.sh merge       # merge captured legs into cmd/bouine/default.pgo
#   bench/pgo/run.sh refresh     # capture + merge + sanity-check (CI)
#   bench/pgo/run.sh verify      # build with/without PGO, diff binary sizes
#
# Environment:
#   PGO_LEG_SECONDS   per-leg load duration      (default: 90)
#   PGO_PROFILE_SECS  CPU profile window        (default: 60, must be <= leg)
#   PGO_KEEP_STACK    retain work dir on success (default: 0)
#
# The work dir is bench/pgo/.stack/ (gitignored); the merged profile is
# installed at cmd/bouine/default.pgo, next to the main package.
#
# Requirements: docker compose, go (>= 1.27). k6 is not needed on the host —
# legs run inside the pinned grafana/k6 load-gen container.

set -euo pipefail

cd "$(dirname "$0")/../.."   # repo root

COMPOSE_FILE="bench/loadtest/docker-compose.yaml"
STACK_DIR="bench/pgo/.stack"
LEG_SECONDS="${PGO_LEG_SECONDS:-90}"
PROFILE_SECS="${PGO_PROFILE_SECS:-60}"
BOUINE_ADMIN="${BOUINE_ADMIN:-http://localhost:9000}"
ADMIN_TOKEN="${BOUINE_ADMIN_TOKEN:-loadtest-token}"

# Load legs, mirroring the §3.x scenarios but bouine-only. Format:
#   name:script:target:rate   (colon-separated; target URLs contain no colon
#   beyond the scheme — fields are read from the right: name, script, then
#   the remainder is target[:path] and rate is last)
legs=(
  "hit:const_rate:http://bouine:8080/hit:30000"
  "miss:const_rate:http://bouine:8080/miss:8000"
  "mixed:mixed_js:http://bouine:8080:8000"
)

log() { echo ">>> [pgo] $*" >&2; }

require_go() {
  command -v go >/dev/null 2>&1 || { echo "go toolchain required" >&2; exit 1; }
}

require_compose() {
  docker compose version >/dev/null 2>&1 || {
    echo "docker compose required (docker compose version failed)" >&2
    exit 1
  }
}

start_stack() {
  require_compose
  # Only bouine + origin: profile capture is single-node and must not
  # burn the full 4-proxy topology. Results dir is mounted for k6 output;
  # create it so compose does not invent a root-owned path.
  mkdir -p bench/loadtest/results
  log "starting bouine + origin (compose: $COMPOSE_FILE)"
  docker compose -f "$COMPOSE_FILE" up -d --wait origin
  docker compose -f "$COMPOSE_FILE" up -d bouine
  log "waiting for data plane to answer (start buffer)"
  local deadline=$((SECONDS + 60))
  until curl -fsS -o /dev/null "${BOUINE_ADMIN}/healthz" 2>/dev/null; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "bouine admin did not become ready within 60s" >&2
      exit 1
    fi
    sleep 1
  done
  # Warm the origin path once so the very first leg isn't paying
  # connection establishment inside the profile window.
  curl -fsS -o /dev/null "http://localhost:8080/hit" || true
}

stop_stack() {
  log "stacking down (volumes preserved)"
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}

cleanup_stack() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup_stack EXIT

run_leg() {
  # run_leg <name> <k6-script> <target> <rate>
  # The load-gen image entrypoint is "sh" (compose default) and the image
  # ships no bash; --entrypoint k6 runs the binary directly. --no-deps:
  # bouine + origin are already up (start_stack); nginx/varnish/envoy are
  # never started for PGO capture.
  local name="$1" script="$2" target="$3" rate="$4"

  case "$script" in
    const_rate)
      # Same shape as scenarios/lib/const_rate.js — constant arrival rate.
      docker compose -f "$COMPOSE_FILE" run --rm --no-deps \
        --entrypoint k6 \
        -e TARGET="$target" -e RATE="$rate" -e DURATION="${LEG_SECONDS}s" \
        load-gen \
        run --quiet "/scenarios/lib/const_rate.js" &
      ;;
    mixed_js)
      # Mixed realistic leg reuses the §3.6 script verbatim; its internal
      # 10k RPS executor is fine for the profile window.
      docker compose -f "$COMPOSE_FILE" run --rm --no-deps \
        --entrypoint k6 \
        -e BASE_URL="$target" \
        load-gen \
        run --quiet "/scenarios/3.6_mixed_realistic/k6.js" &
      ;;
    *)
      echo "unknown leg script: $script" >&2
      return 1
      ;;
  esac
  K6_PID=$!
}

wait_legs() {
  # wait for the k6 leg started by run_leg to finish
  wait "$K6_PID" || {
    echo "k6 leg failed (see compose logs above)" >&2
    exit 1
  }
}

capture_leg() {
  # capture_leg <name> <script> <target> <rate> -> bench/pgo/.stack/<name>.pb.gz
  local name="$1" script="$2" target="$3" rate="$4"
  local out="$STACK_DIR/${name}.pb.gz"
  log "leg '$name': ${PROFILE_SECS}s CPU profile @ $rate rps target=$target"

  run_leg "$name" "$script" "$target" "$rate"

  # Steady-state delay: skip the connection-ramp noise before profiling.
  local ramp
  ramp=$((LEG_SECONDS - PROFILE_SECS))
  [ "$ramp" -gt 10 ] && ramp=10
  [ "$ramp" -lt 0 ] && ramp=0
  sleep "$ramp"

  if ! curl -fsS -o "$out" \
       -H "Authorization: Bearer ${ADMIN_TOKEN}" \
       "${BOUINE_ADMIN}/debug/pprof/profile?seconds=${PROFILE_SECS}"; then
    echo "profile capture failed for leg '$name'" >&2
    kill "$K6_PID" 2>/dev/null || true
    wait "$K6_PID" 2>/dev/null || true
    exit 1
  fi

  wait_legs
  log "leg '$name' profile: $out ($(du -h "$out" | cut -f1))"
}

do_capture() {
  mkdir -p "$STACK_DIR"
  start_stack
  local leg name script target rate
  for leg in "${legs[@]}"; do
    # Read from the right: rate is the last field, script the second,
    # name the first; everything between is the target URL (which may
    # itself contain colons, e.g. http://bouine:8080/hit).
    rate="${leg##*:}"
    local rest="${leg%:*}"
    name="${rest%%:*}"
    local mid="${rest#*:}"
    script="${mid%%:*}"
    target="${mid#*:}"
    capture_leg "$name" "$script" "$target" "$rate"
  done
  stop_stack
  log "capture complete: $(find "$STACK_DIR" -name '*.pb.gz' -maxdepth 1 | tr '\n' ' ')"
}

do_merge() {
  require_go
  local merged="$STACK_DIR/merged.pb.gz"
  local -a profiles=()
  local leg name
  for leg in "${legs[@]}"; do
    name="${leg%%:*}"
    local f="$STACK_DIR/${name}.pb.gz"
    if [ ! -f "$f" ]; then
      echo "missing profile for leg '$name': $f (run 'capture' first)" >&2
      exit 1
    fi
    profiles+=("$f")
  done

  # pprof -proto with multiple sources writes a merged profile: samples
  # are summed per (function, line, mapping). CPU samples are additive,
  # so the merged profile is a frequency-weighted blend of the legs.
  go tool pprof -proto -output "$merged" "${profiles[@]}" >/dev/null
  log "merged ${#profiles[@]} legs -> $merged ($(du -h "$merged" | cut -f1))"

  # default.pgo must be a gzip-compressed protobuf profile in the main
  # package directory (cmd/bouine/); `go build ./cmd/bouine` discovers it
  # by name there. A repo-root default.pgo is silently ignored.
  cp "$merged" cmd/bouine/default.pgo
  log "installed cmd/bouine/default.pgo"
}

pgo_sanity() {
  # A profile with < 100 samples is too thin to steer inlining.
  # Raw dump sample lines look like:
  #   "          3   30000000: 1 2 3 4 5 6"
  # Count value lines between the "Samples:" and "Locations:" headers.
  local raw samples
  raw=$(go tool pprof -raw cmd/bouine/default.pgo 2>/dev/null || true)
  samples=$(printf '%s\n' "$raw" | sed -n '/^Samples:/,/^Locations:/p' \
              | grep -cE '^[[:space:]]+[0-9]+[[:space:]]+[0-9]+:' || true)
  if [ "${samples:-0}" -lt 100 ]; then
    echo "FAIL: merged profile has only ${samples:-0} samples (< 100)" >&2
    exit 1
  fi
  log "sanity: ${samples} samples in merged profile"
}

do_verify() {
  # Build with and without the profile to confirm PGO engages and to
  # surface the binary-size delta (inlining grows text, typically +2-5%).
  require_go
  local tmp
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' RETURN

  go build -o "$tmp/bouine-nopgo" -ldflags "-s -w" ./cmd/bouine
  mv cmd/bouine/default.pgo "$tmp/default.pgo.stash"
  # GOFLAGS unset: -pgo=off would also work, but moving the file also
  # guards against a stray non-gzipped file passing by name.
  go build -o "$tmp/bouine-pgo" -ldflags "-s -w" ./cmd/bouine
  mv "$tmp/default.pgo.stash" cmd/bouine/default.pgo

  local nopgo pgo
  nopgo=$(stat -f%z "$tmp/bouine-nopgo" 2>/dev/null || stat -c%s "$tmp/bouine-nopgo")
  pgo=$(stat -f%z "$tmp/bouine-pgo" 2>/dev/null || stat -c%s "$tmp/bouine-pgo")
  log "verify: no-pgo=${nopgo} bytes, pgo=${pgo} bytes (delta: $((pgo - nopgo)))"
  if [ "$pgo" -le "$nopgo" ]; then
    log "verify: pgo binary not larger — profile may not have engaged"
  fi
}

case "${1:-refresh}" in
  capture)  do_capture ;;
  merge)    do_merge ;;
  refresh)  do_capture; do_merge; pgo_sanity ;;
  verify)   do_verify ;;
  *)
    echo "usage: $0 {capture|merge|refresh|verify}" >&2
    exit 2
    ;;
esac

if [ "${PGO_KEEP_STACK:-0}" != "1" ]; then
  log "cleaning $STACK_DIR (set PGO_KEEP_STACK=1 to retain)"
  rm -rf "$STACK_DIR"
fi
