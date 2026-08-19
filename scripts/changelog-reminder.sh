#!/usr/bin/env bash
# changelog-reminder -- pre-push hook (warning, not a block).
#
# Prints a warning when the push includes feat/fix/perf/refactor commits
# but CHANGELOG.md was not modified in the same range. Exits 0 so it
# never blocks the push; the signal is advisory.
#
# Used by the prek "changelog-reminder" hook (pre-push stage).
set -euo pipefail

changelog="CHANGELOG.md"

# Determine the base of the push range.
range_base=""
if upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null); then
  range_base="$upstream"
elif git rev-parse --verify origin/main >/dev/null 2>&1; then
  range_base="origin/main"
else
  # No upstream and no origin/main -- nothing to compare, skip.
  exit 0
fi

# Collect user-facing commit types in the push range.
commits=$(git log --no-merges --format='%s' "$range_base..HEAD" 2>/dev/null \
  | grep -Ei '^(feat|fix|perf|refactor)(\(.+\))?:' || true)
if [ -z "$commits" ]; then
  exit 0
fi

# Check if CHANGELOG.md was modified in the push range.
if git diff --name-only "$range_base..HEAD" 2>/dev/null | grep -Fxq "$changelog"; then
  exit 0
fi

# Warn but don't fail.
{
  echo ""
  echo "  WARNING: CHANGELOG.md was not modified, but the push includes"
  echo "  feat/fix/perf/refactor commits:"
  echo ""
  echo "$commits" | sed 's/^/    /'
  echo ""
  echo "  Please add an entry under \"## [Unreleased]\" in ${changelog}."
  echo "  (This is a warning, not a block.)"
  echo ""
} >&2

exit 0
