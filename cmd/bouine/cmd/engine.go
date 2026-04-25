package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/cache"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/dashboard"
	"github.com/thylong/bouine/internal/dashboard/templates"
	"github.com/thylong/bouine/internal/listener"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/prefetch"
	"github.com/thylong/bouine/internal/runtime/shutdown"
	"github.com/thylong/bouine/internal/runtime/supervised"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
	webdash "github.com/thylong/bouine/web/dashboard"
)

type engine struct {
	cfg        *config.Config
	configPath string
	startTime  time.Time
	logger     *slog.Logger
	metrics    *observability.Metrics
}

func newEngine(cfg *config.Config, configPath string, logger *slog.Logger) *engine {
	return &engine{
		cfg:        cfg,
		configPath: configPath,
		startTime:  time.Now(),
		logger:     logger,
		metrics:    observability.NewMetrics(),
	}
}

func (e *engine) run(ctx context.Context) error {
	pools, err := e.buildPools()
	if err != nil {
		return err
	}

	store, err := e.buildStore()
	if err != nil {
		return err
	}

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

	// Build optional cluster before the data-plane handler so peer-fetch
	// functions can be passed into each route's cache.Handler.
	var peersFn func() []api.PeerInfo
	var clusterNode *cluster.Cluster
	var peerFetcher *cluster.PeerFetcher
	var broadcaster *cluster.Broadcaster
	if e.cfg.Cluster.Enabled && e.cfg.Listen.Cluster != "" {
		clusterNode, err = e.buildCluster()
		if err != nil {
			return err
		}
		peersFn = clusterNode.Members
		peerFetcher = cluster.NewPeerFetcher(nil) // nil = plain HTTP (no mTLS) for now
		broadcaster = cluster.NewBroadcaster(clusterNode, nil, token)
	}

	// Build handler first with no prefetcher (nil), then create the
	// prefetcher using the full cache handler as its upstream so its
	// background GETs are stored. The two are decoupled at this layer:
	// the cache handler checks h.prefetcher == nil at call time, so
	// setting it after construction is safe (no data race — it is set
	// before Serve() starts accepting connections).
	handler := e.buildHandler(pools, store, dpMetrics, clusterNode, peerFetcher, nil)

	// Prefetcher: warms the cache by following Link: rel=preload headers
	// from stored origin responses and optionally crawling sitemaps.
	pf := prefetch.New(prefetch.Config{
		Handler:         handler,
		MaxConcurrency:  32,
		SitemapURLs:     e.cfg.Prefetch.SitemapURLs,
		SitemapInterval: e.cfg.Prefetch.SitemapInterval,
		Logger:          e.logger,
	})

	// Config watcher — fsnotify + SIGHUP hot reload.
	watcher := config.NewWatcher(config.WatcherConfig{
		ConfigPath: e.configPath,
		Logger:     e.logger,
		OnConfig:   func(cfg *config.Config) { e.cfg = cfg },
	})

	g := supervised.NewGroup(ctx, e.logger)
	g.Go("rings", func(rCtx context.Context) error {
		rings.Start(rCtx, snapshotPath)
		return nil
	})
	g.Go("prefetch", pf.Run)
	g.Go("config-watcher", watcher.Run)
	e.startAdmin(g, ctx, peersFn, store, broadcaster, token, rings, clusterNode, watcher, peerFetcher)
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

func (e *engine) startAdmin(g *supervised.Group, ctx context.Context, peersFn func() []api.PeerInfo, store storage.Store, broadcaster *cluster.Broadcaster, token string, rings *observability.Rings, clusterNode *cluster.Cluster, watcher *config.Watcher, peerFetcher *cluster.PeerFetcher) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	seq := shutdown.NewSequencer(e.logger)
	dashMux := e.buildDashboard(ctx, peersFn, store, broadcaster, token, rings, addr, clusterNode, watcher)

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
		PeerFetchHandler:   cluster.NewPeerFetchHandler(store),
		PeerMetricsHandler: dashboard.PeerMetricsHandler(rings),
		DashboardHandler:   dashMux,
		FaviconHandler:     webdash.FaviconHandler(),
	})
	// Guard: only mount peer-fetch handler when running in cluster mode.
	// (admin.Config.PeerFetchHandler is nil-safe; the server skips it)
	_ = peerFetcher // suppress unused warning when cluster is disabled
	g.Go("admin", srv.Serve)
}

// buildDashboard wires and returns the dashboard ServeMux.
// clusterNode may be nil in single-node mode.
func (e *engine) buildDashboard(ctx context.Context, peersFn func() []api.PeerInfo, store storage.Store, broadcaster *cluster.Broadcaster, token string, rings *observability.Rings, addr string, clusterNode *cluster.Cluster, watcher *config.Watcher) *http.ServeMux {
	dashMux := http.NewServeMux()
	snapshotPath := ""
	if e.cfg.Storage.WarmDir != "" {
		snapshotPath = e.cfg.Storage.WarmDir + "/metrics.snap"
	}
	var ringFn func() []api.RingSegment
	if clusterNode != nil {
		ringFn = clusterNode.RingSegments
	}
	// Build cluster meta for the cluster page ring stats box.
	clusterMeta := templates.ClusterMeta{
		ProtocolVersion:  cluster.ClusterProtocolVersion,
		GossipInterval:   "5s",
		JoinRetryBudget:  "60s · 2s step",
		PeerFetchTimeout: "500ms",
	}
	if clusterNode != nil {
		clusterMeta.VirtualNodes = clusterNode.Config().VirtualNodes
		clusterMeta.LoadFactor = clusterNode.Config().LoadFactor
	}
	if e.cfg.Cluster.HopLimit > 0 {
		clusterMeta.HopLimit = e.cfg.Cluster.HopLimit
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
		WarmMaxBytes: e.cfg.Storage.WarmMaxBytes.Bytes(),
		Config:       e.cfg,
		ConfigPath:   e.configPath,
		StartTime:    e.startTime,
		ReloadFn:     func(_ *config.Config) error { return watcher.Reload() },
		RingFn:       ringFn,
		ClusterMeta:  clusterMeta,
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
			Addr:          e.cfg.Listen.HTTP,
			Handler:       handler,
			Logger:        e.logger,
			ProxyProtocol: e.cfg.Listen.ProxyProtocol,
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
			Addr:          e.cfg.Listen.HTTPS,
			Handler:       handler,
			Logger:        e.logger,
			TLSConfig:     tlsCfg,
			ProxyProtocol: e.cfg.Listen.ProxyProtocol,
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
