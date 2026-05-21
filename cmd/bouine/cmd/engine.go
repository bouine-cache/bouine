package cmd

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/cache"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/listener"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/accesslog"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/pipeline"
	"github.com/thylong/bouine/internal/runtime/shutdown"
	"github.com/thylong/bouine/internal/runtime/supervised"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
)

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
	pools, err := e.buildPools()
	if err != nil {
		return err
	}

	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes: e.cfg.Storage.HotMaxBytes.Bytes(),
	})

	dpMetrics := observability.NewDataPlaneMetrics(e.metrics.Registry)
	handler := e.buildHandler(pools, store, dpMetrics)

	// Build optional cluster — must happen before admin so the
	// /v1/cluster/peers endpoint can reference the Members function.
	var peersFn func() []api.PeerInfo
	var clusterNode *cluster.Cluster
	if e.cfg.Cluster.Enabled && e.cfg.Listen.Cluster != "" {
		clusterNode, err = e.buildCluster()
		if err != nil {
			return err
		}
		peersFn = clusterNode.Members
	}

	g := supervised.NewGroup(ctx, e.logger)
	e.startAdmin(g, ctx, peersFn, store)
	e.startListeners(g, handler)
	e.startHealthChecks(g, pools)

	if clusterNode != nil {
		if len(e.cfg.Cluster.Join) > 0 {
			g.Go("cluster-join", func(joinCtx context.Context) error {
				return e.joinWithRetry(joinCtx, clusterNode)
			})
		}
		g.Go("cluster-leave", func(leaveCtx context.Context) error {
			<-leaveCtx.Done()
			return clusterNode.Leave(context.WithoutCancel(leaveCtx))
		})
	}

	return g.Wait()
}

func (e *engine) buildCluster() (*cluster.Cluster, error) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "bouine"
	}

	// In Kubernetes, POD_IP is injected via the Downward API.
	// memberlist needs it as the advertise address so peers can
	// reach this node by its real IP, not 0.0.0.0.
	advertiseAddr := ""
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		// Extract port from bind address.
		_, port, _ := net.SplitHostPort(e.cfg.Listen.Cluster)
		if port == "" {
			port = "8443"
		}
		advertiseAddr = podIP + ":" + port
	}

	return cluster.New(cluster.Config{
		NodeName:      hostname,
		BindAddr:      e.cfg.Listen.Cluster,
		AdvertiseAddr: advertiseAddr,
		Join:          e.cfg.Cluster.Join,
		PeerInfo: api.PeerInfo{
			Name:      hostname,
			Addr:      e.cfg.Listen.Cluster,
			AdminAddr: e.cfg.Listen.Admin,
			DataAddr:  e.cfg.Listen.HTTP,
			Weight:    1.0,
		},
		Logger: e.logger,
	})
}

func (e *engine) buildHandler(
	pools map[string]*origin.Pool,
	store storage.Store,
	dpMetrics *observability.DataPlaneMetrics,
) http.Handler {
	router := e.buildRouter(pools, store)
	metricsWrapped := dpMetrics.Middleware(router)
	return accesslog.Middleware(e.logger, metricsWrapped)
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

func (e *engine) buildRouter(pools map[string]*origin.Pool, store storage.Store) *pipeline.Router {
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
		upstream := p.Handler(consecutive5xx, nil)
		cached := cache.NewHandler(cache.HandlerConfig{
			Upstream: upstream,
			Store:    store,
			Logger:   e.logger,
		})
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, cached)
	}
	return router
}

func (e *engine) startAdmin(g *supervised.Group, ctx context.Context, peersFn func() []api.PeerInfo, store storage.Store) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	// Build shutdown sequencer — IsReady wired to /readyz.
	seq := shutdown.NewSequencer(e.logger)

	srv := admin.New(admin.Config{
		Addr:    addr,
		Logger:  e.logger,
		Metrics: e.metrics,
		PeersFn: peersFn,
		ReadyFn: seq.IsReady,
		PurgeFn: func(key api.Key) error {
			return store.Delete(ctx, key)
		},
		BanFn: func(expr api.BanExpr) (int, error) {
			return store.Ban(ctx, expr)
		},
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

		// HTTP/3 shares TLS certs with HTTPS. Bind on the same address
		// but over UDP.
		if e.cfg.Listen.HTTP3 != "" {
			h3TLS := tlsCfg.Clone()
			h3Srv := listener.NewHTTP3(listener.Config{
				Addr:      e.cfg.Listen.HTTP3,
				Handler:   handler,
				Logger:    e.logger,
				TLSConfig: h3TLS,
			})
			g.Go("listener-http3", h3Srv.Serve)
		}
	}
}

func (e *engine) startHealthChecks(
	g *supervised.Group,
	pools map[string]*origin.Pool,
) {
	for _, pc := range e.cfg.UpstreamPools {
		if pc.Health.Active.Path == "" {
			continue
		}
		p := pools[pc.Name]
		if p == nil {
			continue
		}
		hc := origin.NewActiveHealthChecker(p, origin.ActiveHealthConfig{
			Path:               pc.Health.Active.Path,
			Method:             pc.Health.Active.Method,
			Interval:           pc.Health.Active.Interval,
			Timeout:            pc.Health.Active.Timeout,
			HealthyThreshold:   pc.Health.Active.HealthyThreshold,
			UnhealthyThreshold: pc.Health.Active.UnhealthyThreshold,
			ExpectedCodes:      pc.Health.Active.ExpectedStatusCodes,
		}, e.logger)
		g.Go("health-"+pc.Name, hc.Run)
	}
}

// joinWithRetry attempts to join the cluster, retrying every 2 seconds
// for up to 60 seconds. Success is defined as having more than 1
// member (i.e. at least one peer besides self). This avoids false
// positives from self-join and partial memberlist.Join results.
func (e *engine) joinWithRetry(ctx context.Context, c *cluster.Cluster) error {
	seeds := e.cfg.Cluster.Join
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	deadline := time.After(60 * time.Second)
	for {
		_, err := c.Join(seeds)
		if err != nil {
			e.logger.Debug("cluster join attempt failed, retrying", "error", err)
		}
		if len(c.Members()) > 1 {
			e.logger.Info("cluster join succeeded", "members", len(c.Members()))
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-deadline:
			e.logger.Warn("cluster join: gave up after 60s, running with local member only", "seeds", seeds)
			return nil
		case <-ticker.C:
		}
	}
}
