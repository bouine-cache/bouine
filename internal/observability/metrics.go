package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Metrics is the Prometheus registry shared across the daemon. Every
// component that exposes metrics MUST receive a *Metrics by injection
// — never reach for a package-level global.
//
// The registry is private to the daemon; the bundled
// go/process/runtime collectors are registered at construction. Add
// component-specific collectors via Registry.MustRegister(...).
//
// Stable.
type Metrics struct {
	Registry *prometheus.Registry
}

// NewMetrics builds a fresh registry seeded with the standard Go,
// process, and build-info collectors. The "bouine_" namespace is
// reserved for custom collectors registered by callers.
//
// Stable.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
	return &Metrics{Registry: r}
}

// Handler returns the http.Handler exposing the registry on /metrics.
// It is mounted on the admin port, never the data plane.
//
// Stable.
// Handler returns the fasthttp.RequestHandler exposing the registry on
// /metrics. It is mounted on the admin port, never the data plane.
//
// Stable.
func (m *Metrics) Handler() fasthttp.RequestHandler {
	h := promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Registry:          m.Registry,
	})
	return fasthttpadaptor.NewFastHTTPHandler(h)
}
