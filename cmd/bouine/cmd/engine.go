package cmd

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/admin"
	"github.com/thylong/bouine/internal/buildinfo"
	"github.com/thylong/bouine/internal/cache"
	bouinecf "github.com/thylong/bouine/internal/cloudflare"
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

// runState bundles subsystem references created during engine startup.
// Passed to startAdmin and buildDashboard instead of 10+ positional args.
type runState struct {
	store        storage.Store
	pools        map[string]*origin.Pool
	dpMetrics    *observability.DataPlaneMetrics
	rings        *observability.Rings
	snapshotPath string
	token        string

	clusterNode *cluster.Cluster
	peerFetcher *cluster.PeerFetcher
	broadcaster *cluster.Broadcaster
	peersFn     func() []api.PeerInfo
	antiEntropy *cluster.AntiEntropy

	cfProp    *cfPropagator
	seq       *shutdown.Sequencer
	listeners []*server.Listener
}

// invalidationOps provides shared purge/ban/refresh closures used by
// both the admin API and the dashboard, eliminating duplication.
type invalidationOps struct {
	PurgeFn   func(ctx context.Context, url string) error
	BanFn     func(ctx context.Context, hostRegex, pathRegex string) (int, error)
	RefreshFn func(ctx context.Context, url string) error
}

func (e *engine) run(ctx context.Context) error {
	rs, shutdownTracer, err := e.initSubsystems(ctx)
	if err != nil {
		return err
	}
	defer shutdownTracer()

	handler := e.buildDataPlane(rs)

	g := supervised.NewGroup(ctx, e.logger)
	e.startBackgroundTasks(g, rs, ctx) // rings snapshot, prefetch sitemap crawler, config watcher, anti-entropy
	e.startAdmin(g, ctx, rs)           // admin API, dashboard, peer-fetch handler
	e.startListeners(g, handler, rs)   // HTTP/HTTPS data-plane listeners
	e.startHealthChecks(g, rs.pools)   // active health probes per upstream pool
	e.startClusterJoin(g, rs)          // gossip join with retry against seed peers
	e.registerShutdownSteps(g, rs)     // ordered drain: readiness, store flush, cluster leave

	return g.Wait()
}

// initSubsystems creates all subsystem instances and wires them together.
// Returns the bundled state and a tracer shutdown func.
func (e *engine) initSubsystems(ctx context.Context) (*runState, func(), error) {
	pools, err := e.buildPools()
	if err != nil {
		return nil, func() {}, err
	}
	store, err := e.buildStore()
	if err != nil {
		return nil, func() {}, err
	}

	dpMetrics := observability.NewDataPlaneMetrics(e.metrics.Registry)

	// Fall back to OTEL_EXPORTER_OTLP_ENDPOINT env var when the YAML
	// config doesn't set tracing.endpoint. The chassis chart injects
	// this env var pointing at the node-local OTel collector agent.
	endpoint := e.cfg.Tracing.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	shutdownTracer, err := tracing.InitTracer(ctx, tracing.TracingConfig{
		Endpoint:     endpoint,
		ServiceName:  e.cfg.Tracing.ServiceName,
		SamplingRate: e.cfg.Tracing.SamplingRate,
	})
	if err != nil {
		e.logger.Warn("tracing init failed, continuing without traces", "error", err)
	}

	token := e.resolveAdminToken()
	rings, snapshotPath := e.initRings()
	dpMetrics.Rings = rings

	clusterNode, peerFetcher, broadcaster, peersFn, ae := e.initCluster(ctx, store, rings)

	cfProp := e.initCloudflare(dpMetrics)

	rs := &runState{
		store:        store,
		pools:        pools,
		dpMetrics:    dpMetrics,
		rings:        rings,
		snapshotPath: snapshotPath,
		token:        token,
		clusterNode:  clusterNode,
		peerFetcher:  peerFetcher,
		broadcaster:  broadcaster,
		peersFn:      peersFn,
		antiEntropy:  ae,
		cfProp:       cfProp,
		seq:          shutdown.NewSequencer(e.logger),
	}
	return rs, shutdownTracer, nil
}

func (e *engine) resolveAdminToken() string {
	// Env var takes precedence so operators can inject a token via Vault
	// without baking it into the config ConfigMap.
	if token := os.Getenv("BOUINE_ADMIN_TOKEN"); token != "" {
		return token
	}

	token := e.cfg.Admin.Token
	if token == "" {
		tok := make([]byte, 16)
		_, _ = rand.Read(tok)
		token = hex.EncodeToString(tok)
		e.logger.Warn("admin token not configured — using auto-generated token",
			"token", token,
			"hint", "set admin.token in config or BOUINE_ADMIN_TOKEN env var to silence this warning")
	}
	return token
}

func (e *engine) initRings() (*observability.Rings, string) {
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
	return rings, snapshotPath
}

func (e *engine) initCluster(
	ctx context.Context,
	store storage.Store,
	rings *observability.Rings,
) (*cluster.Cluster, *cluster.PeerFetcher, *cluster.Broadcaster, func() []api.PeerInfo, *cluster.AntiEntropy) {
	if !e.cfg.Cluster.Enabled || e.cfg.Listen.Cluster == "" {
		return nil, nil, nil, nil, nil
	}

	clusterNode, err := e.buildCluster(ctx)
	if err != nil {
		e.logger.Error("cluster init failed", "error", err)
		return nil, nil, nil, nil, nil
	}

	var clusterTLS *tls.Config
	if e.cfg.Cluster.TLS.CABundle != "" {
		clusterTLS, err = buildClusterTLSConfig(e.cfg.Cluster.TLS)
		if err != nil {
			e.logger.Error("cluster TLS failed", "error", err)
			return nil, nil, nil, nil, nil
		}
	}

	clusterMetrics := cluster.RegisterMetrics(e.metrics.Registry)
	clusterMetrics.SetMode(e.cfg.Cluster.Mode)
	if rings != nil && rings.Replication != nil {
		clusterMetrics.OnReplicationBytes = func(direction string, bytes float64) {
			rings.Replication.RecordReplication(direction, int(bytes))
		}
	}
	clusterNode.SetMetrics(clusterMetrics)

	peerFetcher := cluster.NewPeerFetcher(clusterTLS, e.metrics.Registry)
	broadcaster := cluster.NewBroadcaster(clusterNode, nil, "")

	clusterNode.SetInvalidator(cluster.Invalidator{
		PurgeFn: func(ctx context.Context, evt api.PurgeEvent) error {
			return store.Delete(ctx, evt.Key)
		},
		BanFn: func(ctx context.Context, evt api.BanEvent) error {
			_, err := store.Ban(ctx, evt.Predicate)
			return err
		},
	})

	var ae *cluster.AntiEntropy
	if e.cfg.Cluster.Mode == config.ClusterModeFull {
		clusterNode.SetReplicator(cluster.Replicator{
			StoreObject: func(ctx context.Context, obj *api.Object) error {
				return store.Put(ctx, obj.Key, obj)
			},
		})
		keyLister, ok := any(store).(storage.KeyLister)
		if ok {
			ae = cluster.NewAntiEntropy(cluster.AntiEntropyConfig{
				Interval: e.cfg.Cluster.AntiEntropyInterval,
				Logger:   observability.NewSampledLogger(e.logger, observability.DefaultKeySampleRate),
			}, e.cfg.Cluster.NodeName, keyLister, peerFetcher, store, clusterNode.Members, clusterMetrics)
		}
	}

	if e.cfg.Cluster.HopLimit > 0 && e.cfg.Cluster.Mode != config.ClusterModeStrong {
		e.logger.Warn("cluster.hop_limit has no effect in non-strong mode",
			"mode", e.cfg.Cluster.Mode, "hop_limit", e.cfg.Cluster.HopLimit)
	}
	if e.cfg.Cluster.Mode == config.ClusterModeFull {
		e.logger.Warn("cluster.mode 'full': memory scales linearly with cluster size")
	}

	return clusterNode, peerFetcher, broadcaster, clusterNode.Members, ae
}

func (e *engine) initCloudflare(dpMetrics *observability.DataPlaneMetrics) *cfPropagator {
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
	return buildCFPropagator(cfInvalidator, e.cfg.Cloudflare, dpMetrics, e.logger)
}

// buildDataPlane assembles the data-plane handler chain.
func (e *engine) buildDataPlane(rs *runState) http.Handler {
	return e.buildHandler(rs)
}

func (e *engine) startBackgroundTasks(g *supervised.Group, rs *runState, ctx context.Context) {
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
func (e *engine) buildInvalidationOps(ctx context.Context, rs *runState) invalidationOps {
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

func (e *engine) startAdmin(g *supervised.Group, ctx context.Context, rs *runState) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	ops := e.buildInvalidationOps(ctx, rs)
	dashMux := e.buildDashboard(rs, addr, ops)

	srv := admin.New(admin.Config{
		Addr:        addr,
		Token:       rs.token,
		Logger:      e.logger,
		Metrics:     e.metrics,
		PeersFn:     rs.peersFn,
		CFStatusFn:  rs.cfProp.Status,
		ReadyFn:     rs.seq.IsReady,
		OnPurged:    rs.cfProp.PropagateForPurge,
		OnRefreshed: rs.cfProp.PropagateForRefresh,
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
		PeerPurgeFn: func(evt api.PurgeEvent) error {
			return rs.store.Delete(ctx, evt.Key)
		},
		PeerBanFn: func(evt api.BanEvent) error {
			_, err := rs.store.Ban(ctx, evt.Predicate)
			return err
		},
		PeerFetchHandler:   cluster.NewPeerFetchHandler(rs.store),
		PeerMetricsHandler: dashboard.PeerMetricsHandler(rs.rings),
		PeerKeysHandler:    e.buildPeerKeysHandler(rs.store),
		DashboardHandler:   dashMux,
		FaviconHandler:     webdash.FaviconHandler(),
	})
	_ = rs.peerFetcher // suppress unused warning when cluster is disabled
	g.Go("admin", srv.Serve)
}

func (e *engine) buildPeerKeysHandler(store storage.Store) http.Handler {
	keyLister, ok := any(store).(storage.KeyLister)
	if !ok {
		return nil
	}
	return cluster.NewPeerKeysHandler(keyLister, e.cfg.Cluster.NodeName)
}

// buildDashboard wires and returns the dashboard ServeMux.
func (e *engine) buildDashboard(rs *runState, addr string, ops invalidationOps) *http.ServeMux {
	dashMux := http.NewServeMux()

	var ringFn func() []api.RingSegment
	if rs.clusterNode != nil {
		ringFn = rs.clusterNode.RingSegments
	}

	clusterMeta := templates.ClusterMeta{
		ProtocolVersion:  cluster.ClusterProtocolVersion,
		GossipInterval:   "5s",
		JoinRetryBudget:  "60s · 2s step",
		PeerFetchTimeout: "500ms",
	}
	if rs.clusterNode != nil {
		clusterMeta.VirtualNodes = rs.clusterNode.Config().VirtualNodes
		clusterMeta.LoadFactor = rs.clusterNode.Config().LoadFactor
		clusterMeta.Mode = rs.clusterNode.Mode()
	} else {
		clusterMeta.Mode = "single-node"
	}
	if e.cfg.Cluster.HopLimit > 0 {
		clusterMeta.HopLimit = e.cfg.Cluster.HopLimit
	}

	_ = dashboard.New(dashboard.Config{
		Rings:        rs.rings,
		Version:      buildinfo.Version,
		PeersFn:      rs.peersFn,
		SelfAddr:     addr,
		Token:        rs.token,
		Logger:       e.logger,
		SnapshotPath: rs.snapshotPath,
		StoreFn:      rs.store.Stats,
		HotMaxBytes:  e.cfg.Storage.HotMaxBytes.Bytes(),
		WarmMaxBytes: e.cfg.Storage.WarmMaxBytes.Bytes(),
		Config:       e.cfg,
		ConfigPath:   e.configPath,
		StartTime:    e.startTime,
		ReloadFn: func(_ *config.Config) error {
			if e.configPath == "" {
				return nil
			}
			cfg, err := config.Load(e.configPath)
			if err != nil {
				return err
			}
			e.cfg = cfg
			e.logger.Info("config reloaded", "path", e.configPath)
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
				Enabled: s.Enabled,
				ZoneID:  s.ZoneID,
				Async:   s.Async,
			}
		},
	}, dashMux)
	return dashMux
}

func (e *engine) startListeners(g *supervised.Group, handler http.Handler, rs *runState) {
	if e.cfg.Listen.HTTP != "" {
		srv := server.NewHTTP(server.ListenerConfig{
			Addr:    e.cfg.Listen.HTTP,
			Handler: handler,
			Logger:  e.logger,
		})
		rs.listeners = append(rs.listeners, srv)
		g.Go("listener-http", srv.Serve)
	}

	if e.cfg.Listen.HTTPS != "" {
		tlsCfg, err := buildTLSConfig(e.cfg)
		if err != nil {
			e.logger.Error("TLS config failed", "error", err)
			return
		}
		srv := server.NewHTTPS(server.ListenerConfig{
			Addr:      e.cfg.Listen.HTTPS,
			Handler:   handler,
			Logger:    e.logger,
			TLSConfig: tlsCfg,
		})
		rs.listeners = append(rs.listeners, srv)
		g.Go("listener-https", srv.Serve)
	}
}

func (e *engine) startHealthChecks(g *supervised.Group, pools map[string]*origin.Pool) {
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

func (e *engine) startClusterJoin(g *supervised.Group, rs *runState) {
	if rs.clusterNode != nil && len(e.cfg.Cluster.Join) > 0 {
		g.Go("cluster-join", func(joinCtx context.Context) error {
			return e.joinWithRetry(joinCtx, rs.clusterNode)
		})
	}
}

func (e *engine) registerShutdownSteps(g *supervised.Group, rs *runState) {
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
