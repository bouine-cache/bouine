#!/usr/bin/env bash
# Validates rendered Helm manifests against Kubernetes strict schemas
# (kubeconform, master-standalone-strict). See ADR-0041.
#
# Renders the chart with autoscaling enabled: the HPA template does not
# render with default values, which is how an invalid autoscaling/v2
# field shipped in every chart since 0.1.2, missed by every existing gate.
set -euo pipefail

if ! command -v kubeconform >/dev/null 2>&1; then
    echo "WARN: kubeconform not installed, skipping" >&2
    echo "      install: go install github.com/yannh/kubeconform/cmd/kubeconform@latest" >&2
    exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

helm template bouine deploy/helm/bouine \
    --set autoscaling.enabled=true \
    --output-dir "$tmp" >/dev/null

find "$tmp" -name '*.yaml' -print0 | xargs -0 kubeconform -strict -ignore-missing-schemas
