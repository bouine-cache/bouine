#!/usr/bin/env bash
# Checks that a commit message carries a Signed-off-by trailer
# as required by the Developer Certificate of Origin (DCO).
#
# Called by prek as a commit-msg hook: $1 is the path to the commit message file.
set -eu

msg_file="$1"

if ! grep -qiE '^Signed-off-by: .+ <.+@.+>' "$msg_file"; then
  echo "Error: commit is missing a 'Signed-off-by:' trailer (DCO requirement)."
  echo ""
  echo "  Add it with:  git commit -s"
  echo "  Or manually add:  Signed-off-by: Your Name <you@example.com>"
  echo ""
  echo "  See CONTRIBUTING.md § 'Developer Certificate of Origin (DCO)' for details."
  exit 1
fi
