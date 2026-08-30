#!/usr/bin/env bash
# check-go-version-stamp -- pre-commit hook.
#
# Verifies that the GO_VERSION_STAMP comment in .pre-commit-config.yaml
# matches the Go toolchain declared in go.mod. The stamp is load-bearing:
# the prek-action CI cache key is derived from the pre-commit config file
# hash, so the stamp makes the cached hook environments (golangci-lint
# binary & friends, built with a specific Go toolchain) invalidate when
# the Go version changes. Without it, a Go bump leaves a stale binary in
# the cache and golangci-lint fails with "Go language version used to
# build golangci-lint is lower than the targeted Go version" (see commit
# 024819b, which had to disable the cache entirely for exactly this).
#
# Run manually after bumping go.mod:
#   make hooks   # or: make bump-go-stamp
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

go_version="$(sed -n 's/^go //p' go.mod | tr -d '[:space:]')"
if [ -z "$go_version" ]; then
  echo "check-go-version-stamp: could not parse 'go <version>' from go.mod" >&2
  exit 1
fi

stamp="$(sed -n 's/^# GO_VERSION_STAMP: \(.*\)$/\1/p' .pre-commit-config.yaml | head -n1 | tr -d '[:space:]')"

if [ "$stamp" != "$go_version" ]; then
  echo "check-go-version-stamp: .pre-commit-config.yaml GO_VERSION_STAMP ($stamp)" >&2
  echo "  does not match go.mod's Go version ($go_version)." >&2
  echo "" >&2
  echo "The stamp invalidates the CI prek cache when the Go toolchain changes." >&2
  echo "Fix with:  make bump-go-stamp" >&2
  exit 1
fi
