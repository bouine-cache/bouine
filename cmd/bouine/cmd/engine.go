package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/cache"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/dashboard"
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
	cfg        *config.Config
	configPath string
	logger     *slog.Logger
	metrics    *observability.Metrics
}

func newEngine(cfg *config.Config, configPath string, logger *slog.Logger) *engine {
	return &engine{
		cfg:        cfg,
		configPath: configPath,
		logger:     logger,
		metrics:    observability.NewMetrics(),
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

	// Resolve admin token before rings and dashboard so all share the same value.
	token := e.cfg.Admin.Token
	if token == "" {
		tok := make([]byte, 16)
		_, _ = rand.Read(tok)
		token = hex.EncodeToString(tok)
		e.logger.Warn("admin token not configured — using auto-generated token",
			"token", token,
			"hint", "set admin.token in config to silence this warning")
	}

	// Initialise ring buffers for dashboard time-series.
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "bouine"
	}
	rings := observability.NewRings(hostname)
	snapshotPath := ""
	if e.cfg.Storage.WarmDir != "" {
		snapshotPath = e.cfg.Storage.WarmDir + "/metrics.snap"
	}
	if snapshotPath != "" {
		if err := rings.Load(snapshotPath); err != nil {
			e.logger.Warn("rings snapshot load failed", "error", err)
		}
	}
	dpMetrics.Rings = rings
	handler := e.buildHandler(pools, store, dpMetrics)

	// Build optional cluster — must happen before admin so the
	// /v1/cluster/peers endpoint can reference the Members function.
	var peersFn func() []api.PeerInfo
	var clusterNode *cluster.Cluster
	var broadcaster *cluster.Broadcaster
	if e.cfg.Cluster.Enabled && e.cfg.Listen.Cluster != "" {
		clusterNode, err = e.buildCluster()
		if err != nil {
			return err
		}
		peersFn = clusterNode.Members
		broadcaster = cluster.NewBroadcaster(clusterNode, nil, token)
	}

	g := supervised.NewGroup(ctx, e.logger)
	g.Go("rings", func(rCtx context.Context) error {
		rings.Start(rCtx, snapshotPath)
		return nil
	})
	e.startAdmin(g, ctx, peersFn, store, broadcaster, token, rings, clusterNode)
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
	// Use it to make all advertised addresses routable by peers.
	// Without this, PeerInfo fields contain bind addresses like ":9000"
	// which are not reachable from other pods.
	advertiseAddr := ""
	peerInfo := api.PeerInfo{
		Name:      hostname,
		Addr:      e.cfg.Listen.Cluster,
		AdminAddr: e.cfg.Listen.Admin,
		DataAddr:  e.cfg.Listen.HTTP,
		Weight:    1.0,
	}
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		advertiseAddr = podIP + ":" + listenPort(e.cfg.Listen.Cluster, "8443")
		peerInfo.Addr = advertiseAddr
		peerInfo.AdminAddr = podIP + ":" + listenPort(e.cfg.Listen.Admin, "9000")
		peerInfo.DataAddr = podIP + ":" + listenPort(e.cfg.Listen.HTTP, "80")
	}

	return cluster.New(cluster.Config{
		NodeName:      hostname,
		BindAddr:      e.cfg.Listen.Cluster,
		AdvertiseAddr: advertiseAddr,
		Join:          e.cfg.Cluster.Join,
		PeerInfo:      peerInfo,
		Logger:        e.logger,
	})
}

// listenPort extracts the port number from a ":port" bind address,
// falling back to defaultPort when the address is empty or unparseable.
func listenPort(addr, defaultPort string) string {
	if addr == "" {
		return defaultPort
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return defaultPort
	}
	return port
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
			Upstream:      upstream,
			Store:         store,
			Logger:        e.logger,
			NegativeTTL:   rc.Cache.NegativeTTL,
			JitterPercent: rc.Cache.JitterPercent,
			StayinAlive:   rc.Cache.StayinAlive,
		})
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, cached)
	}
	return router
}

func (e *engine) startAdmin(g *supervised.Group, ctx context.Context, peersFn func() []api.PeerInfo, store storage.Store, broadcaster *cluster.Broadcaster, token string, rings *observability.Rings, clusterNode *cluster.Cluster) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	seq := shutdown.NewSequencer(e.logger)
	dashMux := e.buildDashboard(ctx, peersFn, store, broadcaster, token, rings, addr, clusterNode)

	srv := admin.New(admin.Config{
		Addr:    addr,
		Token:   token,
		Logger:  e.logger,
		Metrics: e.metrics,
		PeersFn: peersFn,
		ReadyFn: seq.IsReady,
		PurgeFn: func(key api.Key) error {
			if err := store.Delete(ctx, key); err != nil {
				return err
			}
			if broadcaster != nil {
				broadcaster.BroadcastPurge(ctx, key, "")
			}
			return nil
		},
		BanFn: func(expr api.BanExpr) (int, error) {
			n, err := store.Ban(ctx, expr)
			if err != nil {
				return n, err
			}
			if broadcaster != nil {
				broadcaster.BroadcastBan(ctx, expr)
			}
			return n, nil
		},
		PeerPurgeFn: func(evt api.PurgeEvent) error {
			return store.Delete(ctx, evt.Key)
		},
		PeerBanFn: func(evt api.BanEvent) error {
			_, err := store.Ban(ctx, evt.Predicate)
			return err
		},
		PeerMetricsHandler: dashboard.PeerMetricsHandler(rings),
		DashboardHandler:   dashMux,
	})
	g.Go("admin", srv.Serve)
}

// buildDashboard wires and returns the dashboard ServeMux.
// clusterNode may be nil in single-node mode.
func (e *engine) buildDashboard(ctx context.Context, peersFn func() []api.PeerInfo, store storage.Store, broadcaster *cluster.Broadcaster, token string, rings *observability.Rings, addr string, clusterNode *cluster.Cluster) *http.ServeMux {
	dashMux := http.NewServeMux()
	snapshotPath := ""
	if e.cfg.Storage.WarmDir != "" {
		snapshotPath = e.cfg.Storage.WarmDir + "/metrics.snap"
	}
	var ringFn func() []api.RingSegment
	if clusterNode != nil {
		ringFn = clusterNode.RingSegments
	}
	_ = dashboard.New(dashboard.Config{
		Rings:        rings,
		PeersFn:      peersFn,
		SelfAddr:     addr,
		Token:        token,
		Logger:       e.logger,
		SnapshotPath: snapshotPath,
		StoreFn:      store.Stats,
		HotMaxBytes:  e.cfg.Storage.HotMaxBytes.Bytes(),
		ConfigPath:   e.configPath,
		ReloadFn:     func(_ *config.Config) error { return nil }, // hot-reload in future phase
		RingFn:       ringFn,
		PurgeFn: func(dCtx context.Context, urlStr string) error {
			key := cache.BuildKeyFromURL(urlStr)
			if err := store.Delete(dCtx, key); err != nil {
				return err
			}
			if broadcaster != nil {
				broadcaster.BroadcastPurge(ctx, key, "")
			}
			return nil
		},
		BanFn: func(dCtx context.Context, hostRegex, pathRegex string) (int, error) {
			expr := api.BanExpr{HostRegex: hostRegex, PathRegex: pathRegex}
			n, err := store.Ban(dCtx, expr)
			if err != nil {
				return n, err
			}
			if broadcaster != nil {
				broadcaster.BroadcastBan(ctx, expr)
			}
			return n, nil
		},
		RefreshFn: func(dCtx context.Context, urlStr string) error {
			return store.Delete(dCtx, cache.BuildKeyFromURL(urlStr))
		},
	}, dashMux)
	return dashMux
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
