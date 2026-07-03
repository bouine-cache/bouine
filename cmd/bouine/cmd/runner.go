package cmd

// runner.go contains the lifecycle orchestration for the bouine daemon:
// run, initSubsystems, startListeners, startAdmin, startHealthChecks,
// startClusterJoin, registerShutdownSteps, and joinWithRetry. It also
// holds the admin/dashboard wiring helpers called exclusively from
// startAdmin. Subsystem construction helpers (buildStore, buildCluster,
// buildHandler, buildPools, buildRouter) remain in builder.go. Type
// definitions and initialisation helpers (resolveAdminToken, initRings,
// initCluster, initCloudflare) remain in engine.go.

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/buildinfo"
	"github.com/thylong/bouine/internal/cache"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/dashboard"
	"github.com/thylong/bouine/internal/dashboard/templates"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/tracing"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/runtime/shutdown"
	"github.com/thylong/bouine/internal/runtime/supervised"
	"github.com/thylong/bouine/internal/server"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
	webdash "github.com/thylong/bouine/web/dashboard"

	"golang.org/x/sync/errgroup"
)

// runner orchestrates the bouine daemon lifecycle. It embeds *engine so
// construction helpers and init methods are promoted unchanged.
type runner struct {
	*engine
}

func newRunner(e *engine) *runner {
	return &runner{engine: e}
}

func (r *runner) run(ctx context.Context) error {
	rs, shutdownTracer, err := r.initSubsystems(ctx)
	if err != nil {
		return err
	}
	defer shutdownTracer()

	handler := r.buildDataPlane(rs)

	g := supervised.NewGroup(ctx, r.logger)
	r.startBackgroundTasks(g, rs, ctx) // rings snapshot, prefetch sitemap crawler, config watcher, anti-entropy
	r.startAdmin(g, ctx, rs)           // admin API, dashboard, peer-fetch handler
	r.startListeners(g, handler, rs)   // HTTP/HTTPS data-plane listeners
	r.startHealthChecks(g, rs.pools)   // active health probes per upstream pool
	r.startClusterJoin(g, rs)          // gossip join with retry against seed peers
	r.registerShutdownSteps(g, rs)     // ordered drain: readiness, store flush, cluster leave

	return g.Wait()
}

// initSubsystems creates all subsystem instances and wires them together.
// Returns the bundled state and a tracer shutdown func.
func (r *runner) initSubsystems(ctx context.Context) (*runState, func(), error) {
	pools, err := r.buildPools()
	if err != nil {
		return nil, func() {}, err
	}
	store, err := r.buildStore()
	if err != nil {
		return nil, func() {}, err
	}

	dpMetrics := observability.NewDataPlaneMetrics(r.metrics.Registry)

	// Fall back to OTEL_EXPORTER_OTLP_ENDPOINT env var when the YAML
	// config doesn't set tracing.endpoint. The chassis chart injects
	// this env var pointing at the node-local OTel collector agent.
	endpoint := r.cfg.Tracing.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	shutdownTracer, err := tracing.InitTracer(ctx, tracing.TracingConfig{
		Endpoint:     endpoint,
		ServiceName:  r.cfg.Tracing.ServiceName,
		SamplingRate: r.cfg.Tracing.SamplingRate,
	})
	if err != nil {
		r.logger.Warn("tracing init failed, continuing without traces", "error", err)
	}

	token := r.resolveAdminToken()
	rings, snapshotPath := r.initRings()
	dpMetrics.Rings = rings

	clusterNode, peerFetcher, broadcaster, peersFn, ae, clusterMetrics := r.initCluster(ctx, store, rings)

	cfCtx, cfCancel := context.WithCancel(context.Background())
	cfProp := r.initCloudflare(dpMetrics, cfCtx) //nolint:contextcheck // detached lifecycle for CF async goroutines

	headerRing := observability.NewOriginHeaderRing()
	rings.HeaderRing = headerRing

	rs := &runState{
		store:          store,
		pools:          pools,
		dpMetrics:      dpMetrics,
		rings:          rings,
		headerRing:     headerRing,
		snapshotPath:   snapshotPath,
		token:          token,
		clusterNode:    clusterNode,
		peerFetcher:    peerFetcher,
		broadcaster:    broadcaster,
		peersFn:        peersFn,
		antiEntropy:    ae,
		clusterMetrics: clusterMetrics,
		cfProp:         cfProp,
		cfCancel:       cfCancel,
		seq:            shutdown.NewSequencer(r.logger),
	}
	return rs, shutdownTracer, nil
}

func (r *runner) startBackgroundTasks(g *supervised.Group, rs *runState, ctx context.Context) {
	g.Go("rings", func(rCtx context.Context) error {
		rs.rings.Start(rCtx, rs.snapshotPath)
		return nil
	})
	// Poll hot-store stats every 15 s and update the Prometheus gauges.
	// This keeps bouine_hot_store_bytes / _entries / _evictions_total current
	// without adding per-request overhead.
	g.Go("store-metrics", func(rCtx context.Context) error {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		var lastEvictions int64
		for {
			select {
			case <-rCtx.Done():
				return nil
			case <-ticker.C:
				s := rs.store.Stats()
				rs.dpMetrics.HotStoreBytes.Set(float64(s.HotBytes))
				rs.dpMetrics.HotStoreEntries.Set(float64(s.HotEntries))
				// Counter: add the delta since last poll.
				delta := s.Evictions - lastEvictions
				if delta > 0 {
					rs.dpMetrics.HotStoreEvictions.Add(float64(delta))
					lastEvictions = s.Evictions
				}
			}
		}
	})
	if rs.antiEntropy != nil {
		rs.antiEntropy.Start(ctx)
	}
}

// buildInvalidationOps creates the shared purge/ban/refresh closures.
func (r *runner) buildInvalidationOps(ctx context.Context, rs *runState) invalidationOps {
	return invalidationOps{
		PurgeFn: func(dCtx context.Context, urlStr string) error {
			key := cache.BuildKeyFromURL(urlStr)
			if err := rs.store.Delete(dCtx, key); err != nil {
				return err
			}
			if rs.broadcaster != nil {
				rs.broadcaster.BroadcastPurge(ctx, key, "")
			}
			rs.cfProp.PropagateForPurge(dCtx, urlStr)
			return nil
		},
		BanFn: func(dCtx context.Context, hostRegex, pathRegex string) (int, error) {
			expr := api.BanExpr{HostRegex: hostRegex, PathRegex: pathRegex}
			n, err := rs.store.Ban(dCtx, expr)
			if err != nil {
				return n, err
			}
			if rs.broadcaster != nil {
				rs.broadcaster.BroadcastBan(ctx, expr)
			}
			rs.cfProp.PropagateForBan(dCtx, expr)
			return n, nil
		},
		RefreshFn: func(dCtx context.Context, urlStr string) error {
			if err := rs.store.Delete(dCtx, cache.BuildKeyFromURL(urlStr)); err != nil {
				return err
			}
			rs.cfProp.PropagateForRefresh(dCtx, urlStr)
			return nil
		},
	}
}

func (r *runner) startAdmin(g *supervised.Group, ctx context.Context, rs *runState) {
	addr := r.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	ops := r.buildInvalidationOps(ctx, rs)
	dashMux := r.buildDashboard(rs, addr, ops)

	srv := admin.New(admin.Config{
		Addr:               addr,
		Token:              rs.token,
		Logger:             r.logger,
		Metrics:            r.metrics,
		PeersFn:            rs.peersFn,
		CFStatusFn:         rs.cfProp.Status,
		ReadyFn:            rs.seq.IsReady,
		MaxBatchSize:       r.cfg.Admin.MaxBatchSize,
		RateLimitPerSecond: r.cfg.Admin.RateLimitPerSecond,
		OnPurged:           rs.cfProp.PropagateForPurge,
		OnRefreshed:        rs.cfProp.PropagateForRefresh,
		OnBanned: func(bCtx context.Context, expr api.BanExpr) {
			rs.cfProp.PropagateForBan(bCtx, expr)
		},
		PurgeFn: func(key api.Key) error {
			if err := rs.store.Delete(ctx, key); err != nil {
				return err
			}
			if rs.broadcaster != nil {
				rs.broadcaster.BroadcastPurge(ctx, key, "")
			}
			return nil
		},
		BanFn: func(expr api.BanExpr) (int, error) {
			n, err := rs.store.Ban(ctx, expr)
			if err != nil {
				return n, err
			}
			if rs.broadcaster != nil {
				rs.broadcaster.BroadcastBan(ctx, expr)
			}
			return n, nil
		},
		RefreshFn: func(key api.Key) error {
			return rs.store.Delete(ctx, key)
		},
		PeerPurgeHandler: cluster.NewPeerPurgeHandler(func(evt api.PurgeEvent) error {
			return rs.store.Delete(ctx, evt.Key)
		}),
		PeerBanHandler: cluster.NewPeerBanHandler(func(evt api.BanEvent) error {
			_, err := rs.store.Ban(ctx, evt.Predicate)
			return err
		}),
		PeerFetchHandler:   cluster.NewPeerFetchHandler(rs.store),
		PeerMetricsHandler: dashboard.PeerMetricsHandler(rs.rings),
		PeerKeysHandler:    r.buildPeerKeysHandler(rs.store),
		DashboardHandler:   dashMux,
		FaviconHandler:     webdash.FaviconHandler(),
	})
	_ = rs.peerFetcher // suppress unused warning when cluster is disabled
	g.Go("admin", srv.Serve)
}

func (r *runner) buildPeerKeysHandler(store storage.Store) http.Handler {
	keyLister, ok := any(store).(storage.KeyLister)
	if !ok {
		r.logger.Warn("peer-keys endpoint disabled: store does not implement KeyLister")
		return nil
	}
	return cluster.NewPeerKeysHandler(keyLister, r.cfg.Cluster.NodeName)
}

// buildDashboard wires and returns the dashboard ServeMux.
func (r *runner) buildDashboard(rs *runState, addr string, ops invalidationOps) *http.ServeMux {
	dashMux := http.NewServeMux()

	var ringFn func() []api.RingSegment
	if rs.clusterNode != nil {
		ringFn = rs.clusterNode.RingSegments
	}

	clusterMeta := r.buildClusterMeta(rs)

	_ = dashboard.New(dashboard.Config{
		Rings:        rs.rings,
		Version:      buildinfo.Version,
		PeersFn:      rs.peersFn,
		SelfAddr:     addr,
		Token:        rs.token,
		Logger:       r.logger,
		SnapshotPath: rs.snapshotPath,
		StoreFn:      rs.store.Stats,
		HotMaxBytes:  r.cfg.Storage.HotMaxBytes.Bytes(),
		WarmMaxBytes: r.cfg.Storage.WarmMaxBytes.Bytes(),
		Config:       r.cfg,
		ConfigPath:   r.configPath,
		StartTime:    r.startTime,
		ReloadFn: func(_ *config.Config) error {
			if r.configPath == "" {
				return nil
			}
			cfg, err := config.Load(r.configPath)
			if err != nil {
				return err
			}
			r.cfg = cfg
			r.logger.Info("config reloaded", "path", r.configPath)
			return nil
		},
		RingFn:      ringFn,
		ClusterMeta: clusterMeta,
		PurgeFn:     ops.PurgeFn,
		BanFn:       ops.BanFn,
		RefreshFn:   ops.RefreshFn,
		PeerFetchStatsFn: func() templates.PeerFetchStats {
			if rs.peerFetcher == nil {
				return templates.PeerFetchStats{}
			}
			hits, misses, hopLimitHits, _, _ := rs.peerFetcher.PeerFetchStats()
			return templates.PeerFetchStats{
				Hits6h:       hits,
				Misses6h:     misses,
				HopLimitHits: hopLimitHits,
			}
		},
		CFStatusFn: func() templates.CFStatusCard {
			s := rs.cfProp.Status()
			return templates.CFStatusCard{
				Enabled:   s.Enabled,
				ZoneID:    s.ZoneID,
				Async:     s.Async,
				LastLagMs: s.LastLagMs,
			}
		},
		PoolHealthFn:        insightsPoolHealth(rs),
		OriginHeaderAuditFn: insightsHeaderAudit(rs),
		VaryCapHitsFn:       func() int64 { return rs.dpMetrics.VaryCapHitsCount() },
		BroadcastFailuresFn: func() int64 { return rs.clusterMetrics.BroadcastFailuresCount() },
		CFPurgeSkippedFn:    func() int64 { return rs.dpMetrics.CFPurgeSkippedCount() },
	}, dashMux)
	return dashMux
}

// buildClusterMeta constructs the cluster metadata card for the dashboard.
func (r *runner) buildClusterMeta(rs *runState) templates.ClusterMeta {
	meta := templates.ClusterMeta{
		ProtocolVersion:  cluster.ClusterProtocolVersion,
		GossipInterval:   "5s",
		JoinRetryBudget:  "60s · 2s step",
		PeerFetchTimeout: "500ms",
	}
	if rs.clusterNode != nil {
		meta.VirtualNodes = rs.clusterNode.Config().VirtualNodes
		meta.LoadFactor = rs.clusterNode.Config().LoadFactor
		meta.Mode = rs.clusterNode.Mode()
	} else {
		meta.Mode = "single-node"
	}
	if r.cfg.Cluster.HopLimit > 0 {
		meta.HopLimit = r.cfg.Cluster.HopLimit
	}
	return meta
}

// insightsPoolHealth returns a closure that snapshots all upstream pool
// target health for the dashboard insights diagram.
func insightsPoolHealth(rs *runState) func() map[string][]origin.TargetStatus {
	return func() map[string][]origin.TargetStatus {
		if rs.pools == nil {
			return nil
		}
		out := make(map[string][]origin.TargetStatus, len(rs.pools))
		for name, p := range rs.pools {
			out[name] = p.Targets()
		}
		return out
	}
}

// insightsHeaderAudit returns a closure that snapshots the origin header
// audit ring for the dashboard insights engine.
func insightsHeaderAudit(rs *runState) func() map[string]observability.HeaderAuditSummary {
	return func() map[string]observability.HeaderAuditSummary {
		if rs.headerRing == nil {
			return nil
		}
		return rs.headerRing.HeaderAudit()
	}
}

func (r *runner) startListeners(g *supervised.Group, handler http.Handler, rs *runState) {
	if r.cfg.Listen.HTTP != "" {
		srv := server.NewHTTP(server.ListenerConfig{
			Addr:           r.cfg.Listen.HTTP,
			Handler:        handler,
			Logger:         r.logger,
			MaxConnections: r.cfg.Listen.MaxConnections,
		})
		rs.listeners = append(rs.listeners, srv)
		g.Go("listener-http", srv.Serve)
	}

	if r.cfg.Listen.HTTPS != "" {
		tlsCfg, err := buildTLSConfig(r.cfg)
		if err != nil {
			r.logger.Error("TLS config failed", "error", err)
			return
		}
		srv := server.NewHTTPS(server.ListenerConfig{
			Addr:           r.cfg.Listen.HTTPS,
			Handler:        handler,
			Logger:         r.logger,
			TLSConfig:      tlsCfg,
			MaxConnections: r.cfg.Listen.MaxConnections,
		})
		rs.listeners = append(rs.listeners, srv)
		g.Go("listener-https", srv.Serve)
	}
}

func (r *runner) startHealthChecks(g *supervised.Group, pools map[string]*origin.Pool) {
	for _, pc := range r.cfg.UpstreamPools {
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
		}, r.logger)
		g.Go("health-"+pc.Name, hc.Run)
	}
}

func (r *runner) startClusterJoin(g *supervised.Group, rs *runState) {
	if rs.clusterNode != nil && len(r.cfg.Cluster.Join) > 0 {
		g.Go("cluster-join", func(joinCtx context.Context) error {
			return r.joinWithRetry(joinCtx, rs.clusterNode)
		})
	}
}

func (r *runner) registerShutdownSteps(g *supervised.Group, rs *runState) {
	rs.seq.AddStep("mark-not-ready", 15*time.Second, func(ctx context.Context) error {
		var wg errgroup.Group
		for _, ln := range rs.listeners {
			wg.Go(func() error { return ln.Shutdown(ctx) })
		}
		return wg.Wait()
	})
	rs.seq.AddStep("flush-store", 10*time.Second, func(ctx context.Context) error {
		return rs.store.Close(ctx)
	})
	if rs.cfProp != nil {
		rs.seq.AddStep("drain-cloudflare", 5*time.Second, func(ctx context.Context) error {
			rs.cfCancel()
			return rs.cfProp.Close(ctx)
		})
	}
	if rs.clusterNode != nil {
		rs.seq.AddStep("cluster-leave", 10*time.Second, func(ctx context.Context) error {
			return rs.clusterNode.Leave(context.WithoutCancel(ctx))
		})
	}
	g.Go("shutdown-sequencer", func(sqCtx context.Context) error {
		<-sqCtx.Done()
		rs.seq.Execute(context.WithoutCancel(sqCtx))
		return nil
	})
}

// joinWithRetry attempts to join the cluster, retrying every 2 seconds
// for up to 60 seconds. Success requires Members() > 1.
func (r *runner) joinWithRetry(ctx context.Context, c *cluster.Cluster) error {
	seeds := r.cfg.Cluster.Join
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	deadline := time.After(60 * time.Second)
	for {
		_, err := c.Join(seeds)
		if err != nil {
			r.logger.Debug("cluster join attempt failed, retrying", "error", err)
		}
		if len(c.Members()) > 1 {
			r.logger.Info("cluster join succeeded", "members", len(c.Members()))
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-deadline:
			r.logger.Warn("cluster join: gave up after 60s, running with local member only", "seeds", seeds)
			return nil
		case <-ticker.C:
		}
	}
}
