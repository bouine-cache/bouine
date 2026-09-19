// Package admin is the L7 control plane. It serves the admin API,
// health/readiness probes, metrics, and the dashboard SPA. The admin
// surface MUST stay on its own listener; it is never bound on the
// data-plane port (see docs/architecture.md §8).
//
// The admin server uses fasthttp.Server. pprof handlers are wrapped via
// the pprofwrapper package, which isolates the net/http dependency.
// ADR-0034 documents the decision.
package admin
