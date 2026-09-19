// Package observability wires metrics (Prometheus), structured logging
// (slog), and OpenTelemetry tracing for every layer, and feeds the
// dashboard's in-memory ring buffers. All data-plane request metrics
// live here; metric names are namespaced bouine_* with a bounded label
// cardinality budget (see AGENTS.md §9).
package observability
