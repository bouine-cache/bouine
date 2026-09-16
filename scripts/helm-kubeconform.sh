#!/usr/bin/env bash
# Validates rendered Helm manifests against Kubernetes strict schemas
# (kubeconform, master-standalone-strict). See ADR-0041.
#
# Renders two variants:
#   1. autoscaling enabled: the HPA template does not render with
#      default values, which is how an invalid autoscaling/v2 field
#      shipped in every chart since 0.1.2, missed by every existing gate.
#   2. https disabled (config.listen.https=""): an empty listen address
#      is the app's documented "disabled plane" form and the shape used
#      by `make test-k8s-setup` and by every deployment that terminates
#      TLS upstream. Chart 0.5.19 broke exactly this variant (schema
#      pattern + listenPort helper), so it renders in this gate from
#      now on.
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

helm template bouine deploy/helm/bouine \
    --set config.listen.https="" \
    --set networkPolicy.enabled=true \
    --output-dir "$tmp/https-disabled" >/dev/null

find "$tmp" -name '*.yaml' -print0 | xargs -0 kubeconform -strict -ignore-missing-schemas
