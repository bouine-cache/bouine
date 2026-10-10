#!/usr/bin/env bash
# Validates rendered Helm manifests against Kubernetes strict schemas
# (kubeconform, master-standalone-strict). See ADR-0048.
#
# Renders three variants:
#   1. autoscaling enabled: the HPA template does not render with
#      default values, which is how an invalid autoscaling/v2 field
#      shipped in every chart since 0.1.2, missed by every existing gate.
#   2. https disabled (config.listen.https=""): an empty listen address
#      is the app's documented "disabled plane" form and the shape used
#      by `make test-k8s-setup` and by every deployment that terminates
#      TLS upstream. Chart 0.5.19 broke exactly this variant (schema
#      pattern + listenPort helper), so it renders in this gate from
#      now on.
#   3. serviceMonitor enabled: the ServiceMonitor template does not
#      render with default values, and its CRD
#      (monitoring.coreos.com/v1) is not in kubeconform's default
#      schema location — both gaps let PR #738 (b0b1eac) ship
#      scrapeNativeHistograms/scrapeProtocols/scrapeClassicHistograms
#      inside endpoints[] (a spec-level field) past every gate. The
#      datree CRDs-catalog is added as a second -schema-location so the
#      rendered ServiceMonitor is validated against the real operator
#      CRD, and a third render variant forces it to render at all.
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

helm template bouine deploy/helm/bouine \
    --set serviceMonitor.enabled=true \
    --output-dir "$tmp/servicemonitor" >/dev/null

# The datree CRDs-catalog is archived (2024) but still served; the
# ServiceMonitor v1 schema is stable. -cache avoids re-downloading on
# subsequent runs (first run needs network; cached runs are offline).
cache_dir="$(git rev-parse --show-toplevel 2>/dev/null || echo .)/.kubeconform-cache"
mkdir -p "$cache_dir"

find "$tmp" -name '*.yaml' -print0 | xargs -0 kubeconform -strict -ignore-missing-schemas \
    -schema-location default \
    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
    -cache "$cache_dir"
