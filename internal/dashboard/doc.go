// Package dashboard is the L7 operator dashboard. It serves
// server-rendered HTML via html/template + htmx at /dashboard/*
// on the admin port. All views show cluster-wide aggregated metrics
// collected via fan-out to /v1/peer/metrics on all live peers.
package dashboard
