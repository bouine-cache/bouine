# ADR-0077: CLI access transport via API-server services/proxy (and the client-go dependency)

- **Status**: Accepted
- **Date**: 2026-11-27
- **Deciders**: @bouine-core
- **Phase**: post-v1.0 operator surface (issue #953, option C)
- **References**: issue #953 decision addendum, ADR-0076, `docs/deps.md`, `internal/admin/server.go` (`stripProxyPrefix`), `deploy/helm/bouine/templates/rbac.yaml`

## Context and Problem Statement

Operators currently need a manual `kubectl port-forward` before the CLI
can reach a live cluster's admin API. The CLI also hard-coded `http://`,
so remote deployments would receive the bearer token in cleartext.
Options were surveyed against CNCF practice (argocd's mixed model:
`--port-forward` via client-go SPDY plus an OIDC-exposed server;
crossplane/flux/kyverno: API-server only).

## Decision

**Option C: reach the admin API through the Kubernetes API-server
`services/proxy` subresource** using the operator's own kubeconfig
identity. A remote's server may be `kube://<namespace>/<svc>:<port>`.

- Daemon side: one outermost wrapper strips a single leading `/proxy`
  path segment in both the full and minimal admin handlers (no-op for
  direct traffic, no config field). The bearer token is still required
  end-to-end; no `X-Remote-*` identity header is trusted.
- Chart side: default-off `adminProxy.rbac.create` renders a Role/
  RoleBinding granting `services/proxy` on `resourceNames:
  ["<fullname>-admin"]` only, verbs `get, create`.
  `networkPolicy.adminApiServerCIDRs` allows the admin port from the
  API-server egress CIDRs when the NetworkPolicy is enabled.
- Client side: `k8s.io/client-go` (`clientcmd` + `rest`) is admitted into
  the main module to load kubeconfig (including exec credential plugins)
  and build API-server proxy URLs. The SDK stays dependency-free behind
  the `bouineapi.RoundTripper` interface; client-go is used only by the
  CLI transport.

## Consequences

- Positive: no manual pre-step; single audited gateway (API server) with
  per-user identity; no new reachable listener on bouine pods; works
  across clusters via kubeconfig contexts; admin-listener TLS is no
  longer a prerequisite (operator→API-server hop is TLS).
- Negative: **dependency weight, measured** on darwin/arm64, go1.27.4,
  client-go v0.37.3: 10 new modules, 78 `k8s.io/*` packages linked,
  CLI binary 46.4 MB → ~57-58 MB (+~25%), also landed in the distroless
  image; `govulncheck` in CI scans the new modules (pre-push gate cost
  grows). Accepted: the binary is an operator tool, not the data plane.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Built-in port-forward via client-go SPDY (argocd `--port-forward`) | Same client-go weight plus ~4 MB more (89 packages, ~+33% binary); still needs `pods/portforward` RBAC; fragile per-cluster forwards for fan-out. |
| Built-in port-forward via kubectl subprocess | Zero deps, but zombie processes, port conflicts, missing binary; rejected. |
| Hand-rolled kubeconfig parser | ~0 MB but cannot do exec credential plugins and is a parser to own forever; rejected. |
| In-namespace operator pod only | Works today with zero repo change; kept as the CI/automation pattern, not sufficient for interactive use. |
| Exposed admin Ingress with OIDC (argocd-server model) | Requires daemon admin TLS + auth upgrade; contradicts the current bearer-only threat model. Tracked as a separate hardening item. |
