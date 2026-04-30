package cmd

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/cache"
	bouinecf "github.com/thylong/bouine/internal/cloudflare"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/dashboard"
	"github.com/thylong/bouine/internal/dashboard/templates"
	"github.com/thylong/bouine/internal/listener"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/tracing"
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
	// Initialise OTel exporter before any spans are created.
	shutdownTracer, err := tracing.InitTracer(ctx, tracing.TracingConfig{
		Endpoint:     e.cfg.Tracing.Endpoint,
		ServiceName:  e.cfg.Tracing.ServiceName,
		SamplingRate: e.cfg.Tracing.SamplingRate,
	})
	if err != nil {
		e.logger.Warn("tracing init failed, continuing without traces", "error", err)
	}
	defer shutdownTracer()

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
		// Build cluster mTLS config when configured; falls back to plain
		// HTTP when cluster TLS fields are empty (dev / single-node).
		var clusterTLS *tls.Config
		if e.cfg.Cluster.TLS.CABundle != "" {
			var err error
			clusterTLS, err = buildClusterTLSConfig(e.cfg.Cluster.TLS)
			if err != nil {
				return fmt.Errorf("cluster TLS: %w", err)
			}
		}
		// Register cluster-level Prometheus metrics and set the mode gauge.
		// Must happen BEFORE NewBroadcaster, which copies clusterNode.metrics.
		clusterMetrics := cluster.RegisterMetrics(e.metrics.Registry)
		clusterMetrics.SetMode(e.cfg.Cluster.Mode)
		clusterNode.SetMetrics(clusterMetrics)

		peerFetcher = cluster.NewPeerFetcher(clusterTLS, e.metrics.Registry)
		broadcaster = cluster.NewBroadcaster(clusterNode, nil, token)

		// Wire gossip invalidation handler for all cluster modes.
		// In strong mode gossip is the backup path; in eventual/full
		// modes it is the primary invalidation path.
		clusterNode.SetInvalidator(cluster.Invalidator{
			PurgeFn: func(ctx context.Context, evt api.PurgeEvent) error {
				return store.Delete(ctx, evt.Key)
			},
			BanFn: func(ctx context.Context, evt api.BanEvent) error {
				_, err := store.Ban(ctx, evt.Predicate)
				return err
			},
		})
		// In full mode, also wire the replication handler so gossip
		// received objects are stored locally.
		if e.cfg.Cluster.Mode == config.ClusterModeFull {
			clusterNode.SetReplicator(cluster.Replicator{
				StoreObject: func(ctx context.Context, obj *api.Object) error {
					return store.Put(ctx, obj.Key, obj)
				},
			})
		}
		// Emit startup warnings for mode-specific configuration that
		// is silently ignored or has operational implications.
		if e.cfg.Cluster.HopLimit > 0 && e.cfg.Cluster.Mode != config.ClusterModeStrong {
			e.logger.Warn("cluster.hop_limit is set but has no effect in non-strong mode; peer fetch is disabled",
				"mode", e.cfg.Cluster.Mode, "hop_limit", e.cfg.Cluster.HopLimit)
		}
		if e.cfg.Cluster.Mode == config.ClusterModeFull {
			e.logger.Warn("cluster.mode is 'full' — every node holds a copy of every cached object; memory usage scales linearly with cluster size")
		}
	}

	// Build handler; prefetcher wraps it after construction to avoid the
	// circular dependency (prefetcher needs handler, handler needs prefetcher).
	// pf.Middleware(handler) intercepts MISS/REVALIDATED responses and calls
	// OnResponse — no nil prefetcher pointer needed inside the handler.
	handler := e.buildHandler(pools, store, dpMetrics, clusterNode, peerFetcher, broadcaster, nil)

	// Prefetcher: warms the cache by following Link: rel=preload headers
	// from stored origin responses and optionally crawling sitemaps.
	// Uses Middleware() wrapping so Link warm-ups fire on every cache fill
	// without requiring a pointer inside each route's cache.Handler.
	pf := prefetch.New(prefetch.Config{
		Handler:         handler,
		MaxConcurrency:  32,
		SitemapURLs:     e.cfg.Prefetch.SitemapURLs,
		SitemapInterval: e.cfg.Prefetch.SitemapInterval,
		Logger:          e.logger,
	})
	// Wrap the handler so every MISS/REVALIDATED response triggers prefetch.
	handler = pf.Middleware(handler)

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
	// Cloudflare invalidation propagation.
	// API token falls back to CF_API_TOKEN env var so operators can inject
	// it from a Kubernetes Secret without embedding it in the config file.
	cfAPIToken := e.cfg.Cloudflare.APIToken
	if cfAPIToken == "" {
		cfAPIToken = os.Getenv("CF_API_TOKEN")
	}
	var cfInvalidator bouinecf.Invalidator
	if e.cfg.Cloudflare.ZoneID != "" && cfAPIToken != "" {
		cfClient, cfErr := bouinecf.New(bouinecf.Config{
			ZoneID:   e.cfg.Cloudflare.ZoneID,
			APIToken: cfAPIToken,
			Timeout:  e.cfg.Cloudflare.Timeout,
		})
		if cfErr != nil {
			e.logger.Warn("cloudflare init failed — propagation disabled", "error", cfErr)
		} else {
			cfInvalidator = cfClient
			e.logger.Info("cloudflare invalidation enabled",
				"zone", cfClient.ZoneID(),
				"async", e.cfg.Cloudflare.IsAsync(),
				"propagate", e.cfg.Cloudflare.Propagate)
		}
	}
	cfProp := buildCFPropagator(cfInvalidator, e.cfg.Cloudflare, dpMetrics, e.logger)

	// Shutdown sequencer: controls the ordered drain/flush/leave sequence.
	seq := shutdown.NewSequencer(e.logger)
	e.startAdmin(g, ctx, seq, peersFn, store, broadcaster, token, rings, clusterNode, watcher, peerFetcher, cfProp)
	e.startListeners(g, handler)
	e.startHealthChecks(g, pools)

	if clusterNode != nil {
		if len(e.cfg.Cluster.Join) > 0 {
			g.Go("cluster-join", func(joinCtx context.Context) error {
				return e.joinWithRetry(joinCtx, clusterNode)
			})
		}
	}

	// Register ordered graceful-shutdown steps (PLAN.md §14.1).
	// Steps execute when the context is cancelled (SIGTERM / OS signal).
	// seq.Execute signals /readyz → 503 before draining so kube-proxy
	// removes the pod from active Endpoints before connections are closed.
	seq.AddStep("mark-not-ready", 2*time.Second, func(_ context.Context) error {
		// IsReady() already returns false after Execute; this step gives
		// kube-proxy ~1 s to propagate the readiness change.
		time.Sleep(time.Second)
		return nil
	})
	seq.AddStep("flush-store", 10*time.Second, func(ctx context.Context) error {
		return store.Close(ctx)
	})
	if clusterNode != nil {
		seq.AddStep("cluster-leave", 10*time.Second, func(ctx context.Context) error {
			return clusterNode.Leave(context.WithoutCancel(ctx))
		})
	}
	// The sequencer runs inside its own goroutine so it does not block
	// the supervised group from shutting down listeners concurrently.
	g.Go("shutdown-sequencer", func(sqCtx context.Context) error {
		<-sqCtx.Done()
		seq.Execute(context.WithoutCancel(sqCtx))
		return nil
	})

	return g.Wait()
}

func (e *engine) startAdmin(g *supervised.Group, ctx context.Context, seq *shutdown.Sequencer, peersFn func() []api.PeerInfo, store storage.Store, broadcaster *cluster.Broadcaster, token string, rings *observability.Rings, clusterNode *cluster.Cluster, watcher *config.Watcher, peerFetcher *cluster.PeerFetcher, cfProp *cfPropagator) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	dashMux := e.buildDashboard(ctx, peersFn, store, broadcaster, token, rings, addr, clusterNode, watcher, cfProp)

	srv := admin.New(admin.Config{
		Addr:        addr,
		Token:       token,
		Logger:      e.logger,
		Metrics:     e.metrics,
		PeersFn:     peersFn,
		CFStatusFn:  cfProp.Status,
		ReadyFn:     seq.IsReady,
		OnPurged:    cfProp.PropagateForPurge,
		OnRefreshed: cfProp.PropagateForRefresh,
		OnBanned: func(bCtx context.Context, expr api.BanExpr) {
			cfProp.PropagateForBan(bCtx, expr)
		},
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
		RefreshFn: func(key api.Key) error {
			return store.Delete(ctx, key)
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
func (e *engine) buildDashboard(ctx context.Context, peersFn func() []api.PeerInfo, store storage.Store, broadcaster *cluster.Broadcaster, token string, rings *observability.Rings, addr string, clusterNode *cluster.Cluster, watcher *config.Watcher, cfProp *cfPropagator) *http.ServeMux {
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
		clusterMeta.Mode = clusterNode.Mode()
	} else {
		clusterMeta.Mode = "single-node"
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
			cfProp.PropagateForPurge(dCtx, urlStr)
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
			cfProp.PropagateForBan(dCtx, expr)
			return n, nil
		},
		RefreshFn: func(dCtx context.Context, urlStr string) error {
			if err := store.Delete(dCtx, cache.BuildKeyFromURL(urlStr)); err != nil {
				return err
			}
			cfProp.PropagateForRefresh(dCtx, urlStr)
			return nil
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
			// Apply HTTP/3 0-RTT (Early Data) if the operator opts in.
			// 0-RTT MUST only be enabled for idempotent, safe methods; the
			// per-route Allow0RTT flag is checked in the data-plane handler.
			if e.cfg.TLS.HTTP3.Enable0RTT {
				h3TLS.MaxVersion = 0 // allow TLS 1.3 early data
				// quic-go reads tls.Config.MaxEarlyDataSize if present.
				// Without an explicit field we pass a sentinel via SessionTicketKey.
				e.logger.Info("HTTP/3 0-RTT enabled — only use with idempotent routes")
			}
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
