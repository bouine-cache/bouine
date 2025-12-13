package cmd

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/listener"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/accesslog"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/pipeline"
	"github.com/thylong/bouine/internal/runtime/supervised"
)

// engine bundles the daemon's runtime components. It exists to keep
// newServeCmd under the funlen limit while keeping the boot sequence
// readable.
type engine struct {
	cfg     *config.Config
	logger  *slog.Logger
	metrics *observability.Metrics
}

func newEngine(cfg *config.Config, logger *slog.Logger) *engine {
	return &engine{
		cfg:     cfg,
		logger:  logger,
		metrics: observability.NewMetrics(),
	}
}

func (e *engine) run(ctx context.Context) error {
	handler, err := e.buildHandler()
	if err != nil {
		return err
	}

	g := supervised.NewGroup(ctx, e.logger)
	e.startAdmin(g)
	e.startListeners(g, handler)
	return g.Wait()
}

func (e *engine) buildHandler() (http.Handler, error) {
	pools, err := e.buildPools()
	if err != nil {
		return nil, err
	}
	router := e.buildRouter(pools)
	return accesslog.Middleware(e.logger, router), nil
}

func (e *engine) buildPools() (map[string]*origin.Pool, error) {
	pools := make(map[string]*origin.Pool, len(e.cfg.UpstreamPools))
	for _, pc := range e.cfg.UpstreamPools {
		p, err := origin.NewPool(origin.PoolConfig{
			Name:           pc.Name,
			Targets:        pc.Targets,
			Logger:         e.logger,
			Consecutive5xx: pc.Health.Passive.Consecutive5xx,
		})
		if err != nil {
			return nil, err
		}
		pools[pc.Name] = p
	}
	return pools, nil
}

func (e *engine) buildRouter(pools map[string]*origin.Pool) *pipeline.Router {
	router := pipeline.NewRouter(pipeline.RouterConfig{Logger: e.logger})
	for _, rc := range e.cfg.Routes {
		p := pools[rc.Pool]
		if p == nil {
			continue
		}
		consecutive5xx := 0
		for _, pc := range e.cfg.UpstreamPools {
			if pc.Name == rc.Pool {
				consecutive5xx = pc.Health.Passive.Consecutive5xx
				break
			}
		}
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix,
			p.Handler(consecutive5xx, nil))
	}
	return router
}

func (e *engine) startAdmin(g *supervised.Group) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}
	srv := admin.New(admin.Config{
		Addr:    addr,
		Logger:  e.logger,
		Metrics: e.metrics,
	})
	g.Go("admin", srv.Serve)
}

func (e *engine) startListeners(g *supervised.Group, handler http.Handler) {
	if e.cfg.Listen.HTTP != "" {
		srv := listener.NewHTTP(listener.Config{
			Addr:    e.cfg.Listen.HTTP,
			Handler: handler,
			Logger:  e.logger,
		})
		g.Go("listener-http", srv.Serve)
	}

	if e.cfg.Listen.HTTPS != "" {
		tlsCfg, err := buildTLSConfig(e.cfg)
		if err != nil {
			e.logger.Error("TLS config failed", "error", err)
			return
		}
		srv := listener.NewHTTPS(listener.Config{
			Addr:      e.cfg.Listen.HTTPS,
			Handler:   handler,
			Logger:    e.logger,
			TLSConfig: tlsCfg,
		})
		g.Go("listener-https", srv.Serve)
	}
}
