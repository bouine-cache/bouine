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
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/buildinfo"
	"github.com/bouine-cache/bouine/internal/cache"
	bouinecf "github.com/bouine-cache/bouine/internal/cloudflare"
	"github.com/bouine-cache/bouine/internal/cluster"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/dashboard"
	"github.com/bouine-cache/bouine/internal/dashboard/templates"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/observability/tracing"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/internal/runtime/shutdown"
	"github.com/bouine-cache/bouine/internal/runtime/supervised"
	"github.com/bouine-cache/bouine/internal/server"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
	webdash "github.com/bouine-cache/bouine/web/dashboard"

	"golang.org/x/sync/errgroup"
)

type engine struct {
	cfg        *config.Config
	configPath string
	startTime  time.Time
	logger     observability.Logger
	metrics    *observability.Metrics
}

func newEngine(cfg *config.Config, configPath string, logger *slog.Logger) *engine {
	return &engine{
		cfg:        cfg,
		configPath: configPath,
		startTime:  time.Now(),
		logger:     observability.NewSampledLogger(logger, observability.DefaultKeySampleRate),
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
	headerRing   *observability.OriginHeaderRing
	snapshotPath string
	token        string
	handlers     []*cache.Handler // all cache.Handler instances; refresh-enabled ones filtered via RefreshEnabled()

	clusterNode    *cluster.Cluster
	peerFetcher    *cluster.PeerFetcher
	broadcaster    *cluster.Broadcaster
	peersFn        func() []api.PeerInfo
	clusterMetrics *cluster.Metrics

	warmMetrics    *warm.Metrics
	startupMetrics *observability.StartupMetrics

	cfProp    *cfPropagator
	cfCancel  context.CancelFunc
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

// applyRuntimeTuning sets GOMAXPROCS from the cgroup CPU quota, raises
// the file descriptor limit, and applies the GOGC override from config.
// Called once at startup before any subsystems are initialized.
func (e *engine) applyRuntimeTuning() {
	if n, err := platform.RaiseFileLimit(65536); err != nil {
		e.logger.Warn("failed to raise file descriptor limit", "error", err)
	} else if n > 0 {
		e.logger.Info("file descriptor limit", "soft", n)
	}

	if os.Getenv("GOMAXPROCS") == "" {
		if n := platform.EffectiveGOMAXPROCS(); n > 0 && n != runtime.GOMAXPROCS(0) {
			runtime.GOMAXPROCS(n)
			e.logger.Info("set GOMAXPROCS from cgroup quota",
				"value", n, "num_cpu", runtime.NumCPU())
		}
	}

	if e.cfg.GOGC != nil {
		old := debug.SetGCPercent(*e.cfg.GOGC)
		e.logger.Info("set GOGC from config",
			"value", *e.cfg.GOGC, "previous", old)
	} else if gogcEnv := os.Getenv("GOGC"); gogcEnv != "" {
		e.logger.Info("GOGC from env var", "value", gogcEnv)
	} else {
		e.logger.Info("GOGC using Go default", "value", 100)
	}
}

func (e *engine) run(ctx context.Context) error {
	e.applyRuntimeTuning()

	// Create the sequencer early so the readiness gate is available
	// before initSubsystems. The admin server (started below) wires
	// ReadyFn to seq.IsReady, which checks both the shutdown flag and
	// all registered startup conditions.
	seq := shutdown.NewSequencer(e.logger)
	seq.Gate().Register("store-loaded")
	seq.Gate().Register("listeners-bound")

	// Create a ConditionsFn closure for the /readyz?detail=1 endpoint.
	// This exposes per-condition state so operators can see which startup
	// step is blocking readiness during slow startup.
	conditionsFn := func() []admin.Condition {
		conds := seq.Gate().Conditions()
		result := make([]admin.Condition, len(conds))
		for i, c := range conds {
			result[i] = admin.Condition{Name: c.Name, Ready: c.Ready}
		}
		return result
	}

	// Create a DrainFn closure for the /drain preStop endpoint. It marks
	// the pod as not-ready (so /readyz starts failing and kube-proxy
	// deregisters the pod) and then blocks for the configured drain
	// duration to give endpoint propagation time before SIGTERM arrives.
	drainDuration := e.cfg.Admin.DrainDuration
	if drainDuration <= 0 {
		drainDuration = 10 * time.Second
	}
	drainFn := func() {
		seq.Drain()
		e.logger.Info("drain: marking not-ready, sleeping", "duration", drainDuration.String())
		time.Sleep(drainDuration)
	}

	// Start a minimal admin server (healthz/readyz/version only) before
	// initSubsystems so K8s probes can reach the admin port during the
	// potentially long store-loading phase. The full admin handler is
	// swapped in after subsystems are ready.
	g := supervised.NewGroup(ctx, e.logger)
	adminAddr := e.cfg.Listen.Admin
	if adminAddr == "" {
		adminAddr = ":9000"
	}
	minimalAdmin := admin.NewMinimal(adminAddr, seq.IsReady, conditionsFn, drainFn, e.logger)
	g.Go("admin", minimalAdmin.Serve)

	startupBegin := time.Now()
	e.logger.Info("startup: init subsystems")

	rs, shutdownTracer, err := e.initSubsystems(ctx, seq)
	if err != nil {
		return err
	}
	defer shutdownTracer()

	e.logger.Info("startup: store loaded", "duration", time.Since(startupBegin).String())

	// Store loaded successfully — mark the condition ready.
	seq.Gate().MarkReady("store-loaded")
	updateStartupMetrics(seq, rs.startupMetrics)

	handler := e.buildDataPlane(rs)

	e.startBackgroundTasks(g, rs)                                    // rings snapshot, store metrics
	e.swapAdminHandler(ctx, rs, minimalAdmin, conditionsFn, drainFn) // swap full admin routes into the minimal server
	e.startListeners(g, handler, rs)                                 // HTTP/HTTPS data-plane listeners
	e.startHealthChecks(g, rs.pools)                                 // active health probes per upstream pool
	e.startClusterJoin(g, rs)                                        // gossip join with retry against seed peers
	e.registerShutdownSteps(g, rs)                                   // ordered drain: readiness, store flush, cluster leave

	// Listeners are started by startListeners (synchronous call above
	// starts the supervised goroutines). Mark ready after the call
	// returns — by then the listeners are bound.
	seq.Gate().MarkReady("listeners-bound")
	updateStartupMetrics(seq, rs.startupMetrics)

	// Cluster join runs in a supervised goroutine (startClusterJoin) and
	// does not gate readiness. This avoids a StatefulSet bootstrapping
	// deadlock where pod-0 can't join because pod-1 hasn't started, but
	// pod-1 won't start until pod-0 is ready. When all conditions are met,
	// log the total startup duration and record the histogram.
	if seq.IsReady() {
		startupDur := time.Since(startupBegin)
		e.logger.Info("startup: complete", "duration", startupDur.String())
		rs.startupMetrics.ObserveStartupDuration(startupDur.Seconds())
	} else {
		// Start a goroutine that waits for all conditions to be met,
		// then logs and records. This covers the async cluster-join path.
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if seq.IsReady() {
						startupDur := time.Since(startupBegin)
						e.logger.Info("startup: complete", "duration", startupDur.String())
						rs.startupMetrics.ObserveStartupDuration(startupDur.Seconds())
						rs.startupMetrics.SetPhase("ready")
						return
					}
				}
			}
		}()
	}

	return g.Wait()
}

// initSubsystems creates all subsystem instances and wires them together.
// Returns the bundled state and a tracer shutdown func.
func (e *engine) initSubsystems(ctx context.Context, seq *shutdown.Sequencer) (*runState, func(), error) {
	originMetrics := origin.RegisterMetrics(e.metrics.Registry)
	pools, err := e.buildPools(originMetrics)
	if err != nil {
		return nil, func() {}, err
	}
	// Register warm-tier metrics only when a warm tier is configured.
	// Without this gate, single-node / ephemeral deployments (WarmDir == "")
	// would still expose bouine_warm_* collectors reporting a 0-byte
	// "unlimited" budget, which misleads operators and alert rules into
	// thinking a warm tier exists and is healthy.
	var warmMetrics *warm.Metrics
	if e.cfg.Storage.WarmDir != "" {
		warmMetrics = warm.RegisterMetrics(e.metrics.Registry)
	}
	store, err := e.buildStore(warmMetrics)
	if err != nil {
		return nil, func() {}, err
	}

	dpMetrics := observability.NewDataPlaneMetrics(e.metrics.Registry)
	dpMetrics.SetAccessLog(e.logger, observability.DefaultKeySampleRate)

	startupMetrics := observability.NewStartupMetrics(e.metrics.Registry)

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

	clusterNode, peerFetcher, broadcaster, peersFn, clusterMetrics := e.initCluster(ctx, token)

	cfCtx, cfCancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cfCancel is stored in runState and called during shutdown
	cfProp := e.initCloudflare(dpMetrics, cfCtx)                //nolint:contextcheck // detached lifecycle for CF async goroutines

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
		clusterMetrics: clusterMetrics,
		warmMetrics:    warmMetrics,
		startupMetrics: startupMetrics,
		cfProp:         cfProp,
		cfCancel:       cfCancel,
		seq:            seq,
	}
	// Wire the gossip invalidator after rs is created so the purge
	// closure can call rs.purgeKey for variant-aware deletion.
	if clusterNode != nil {
		clusterNode.SetInvalidator(cluster.Invalidator{
			PurgeFn: func(ctx context.Context, evt api.PurgeEvent) error {
				return rs.purgeKey(ctx, evt.Key)
			},
			BanFn: func(ctx context.Context, evt api.BanEvent) error {
				_, err := store.Ban(ctx, evt.Predicate)
				return err
			},
		})
	}
	return rs, shutdownTracer, nil
}

// updateStartupMetrics syncs the readiness gate condition states into
// the Prometheus startup metrics gauges.
func updateStartupMetrics(seq *shutdown.Sequencer, m *observability.StartupMetrics) {
	if m == nil {
		return
	}
	for _, c := range seq.Gate().Conditions() {
		m.SetCondition(c.Name, c.Ready)
	}
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
	rings.URL.SetSampleRate(e.cfg.URLRingSampleRate)
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
	token string,
) (*cluster.Cluster, *cluster.PeerFetcher, *cluster.Broadcaster, func() []api.PeerInfo, *cluster.Metrics) {
	if e.cfg.Listen.Cluster == "" {
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
	clusterNode.SetMetrics(clusterMetrics)

	peerFetcher := cluster.NewPeerFetcher(clusterTLS, e.metrics.Registry, e.cfg.Cluster.HopLimit)
	broadcaster := cluster.NewBroadcaster(clusterNode, peerFetcher, token)

	if e.cfg.Cluster.HopLimit > 0 && e.cfg.Cluster.Mode != config.ClusterModeStrong {
		e.logger.Warn("cluster.hop_limit has no effect in non-strong mode",
			"mode", e.cfg.Cluster.Mode, "hop_limit", e.cfg.Cluster.HopLimit)
	}

	return clusterNode, peerFetcher, broadcaster, clusterNode.Members, clusterMetrics
}

func (e *engine) initCloudflare(dpMetrics *observability.DataPlaneMetrics, closeCtx context.Context) *cfPropagator {
	cfAPIToken := e.cfg.Cloudflare.APIToken
	if cfAPIToken == "" {
		cfAPIToken = os.Getenv("CF_API_TOKEN")
	}
	cfAPITokens := e.cfg.Cloudflare.APITokens
	if len(cfAPITokens) == 0 {
		if envTokens := os.Getenv("CF_API_TOKENS"); envTokens != "" {
			cfAPITokens = strings.Split(envTokens, ",")
			for i := range cfAPITokens {
				cfAPITokens[i] = strings.TrimSpace(cfAPITokens[i])
			}
		}
	}
	var cfInvalidator bouinecf.Invalidator
	if e.cfg.Cloudflare.ZoneID != "" && (cfAPIToken != "" || len(cfAPITokens) > 0) {
		cfClient, cfErr := bouinecf.New(bouinecf.Config{
			ZoneID:    e.cfg.Cloudflare.ZoneID,
			APIToken:  cfAPIToken,
			APITokens: cfAPITokens,
			Timeout:   e.cfg.Cloudflare.Timeout,
		})
		if cfErr != nil {
			e.logger.Warn("cloudflare init failed — propagation disabled", "error", cfErr)
		} else {
			cfInvalidator = cfClient
			// Wire token rotation metrics when multi-token mode is active.
			if pool := cfClient.TokenPool(); pool != nil {
				pool.OnRotate(func(idx int) {
					dpMetrics.CFTokenRotated.Inc()
					dpMetrics.CFTokenAvailable.Set(float64(pool.AvailableCount()))
				})
			}
			e.logger.Info("cloudflare invalidation enabled",
				"zone", cfClient.ZoneID(),
				"async", e.cfg.Cloudflare.IsAsync(),
				"propagate", e.cfg.Cloudflare.Propagate,
				"token_count", 1+len(cfAPITokens))
		}
	}
	return buildCFPropagator(cfInvalidator, e.cfg.Cloudflare, dpMetrics, e.logger, closeCtx)
}

// buildDataPlane assembles the data-plane handler chain.
func (e *engine) buildDataPlane(rs *runState) http.Handler {
	return e.buildHandler(rs)
}

func (e *engine) startBackgroundTasks(g *supervised.Group, rs *runState) {
	g.Go("rings", func(rCtx context.Context) error {
		rs.rings.Start(rCtx, rs.snapshotPath)
		return nil
	})
	// Poll hot-store and warm-store stats every 15 s and update the
	// Prometheus gauges. This keeps bouine_hot_store_* and bouine_warm_store_*
	// current without adding per-request overhead.
	g.Go("store-metrics", func(rCtx context.Context) error {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		var lastEvictions int64
		var lastWarmSelfHeals int64
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
				rs.dpMetrics.WarmStoreBytes.Set(float64(s.WarmBytes))
				rs.dpMetrics.WarmStoreEntries.Set(float64(s.WarmEntries))
				// Warm-tier disk-pressure gauge: disk_bytes reflects total
				// segment file sizes (live + tombstones + superseded).
				// max_bytes is set once at construction (never changes);
				// only disk_bytes needs polling.
				if rs.warmMetrics != nil {
					rs.warmMetrics.SetDiskBytes(s.WarmDiskBytes)
				}
				warmHealDelta := s.WarmSelfHeals - lastWarmSelfHeals
				if warmHealDelta > 0 {
					rs.dpMetrics.WarmStoreSelfHeals.Add(float64(warmHealDelta))
					lastWarmSelfHeals = s.WarmSelfHeals
				}
				// WAL async metrics: poll dropped entries and last sync time.
				if walStore, ok := rs.store.(interface {
					WALStats() (int64, time.Time)
				}); ok {
					dropped, lastSync := walStore.WALStats()
					if dropped > 0 {
						rs.dpMetrics.WALDroppedEntries.Add(float64(dropped))
					}
					if !lastSync.IsZero() {
						rs.dpMetrics.WALLastSyncTimestamp.Set(float64(lastSync.UnixNano()) / 1e9)
					}
				}
				// Refresh gauges: poll scheduler heap and registry sizes.
				for _, h := range rs.handlers {
					if !h.RefreshEnabled() {
						continue
					}
					scheduled, registry := h.RefreshStats()
					route := h.RouteName()
					if route != "" && rs.dpMetrics.RefreshScheduled != nil {
						rs.dpMetrics.RefreshScheduled.WithLabelValues(route).Set(float64(scheduled))
						rs.dpMetrics.RefreshRegistrySize.WithLabelValues(route).Set(float64(registry))
					}
				}
			}
		}
	})
}

// sanitizedConfig returns a deep copy of the config with secret fields
// zeroed out so GET /v1/config never exposes credentials.
func sanitizedConfig(cfg config.Config) config.Config {
	out := cfg
	out.Admin.Token = ""
	out.Cloudflare.APIToken = ""
	out.TLS = config.TLS{} // zero out cert/key file paths
	out.Cluster.TLS = config.ClusterTLS{}
	return out
}

// cacheCheck inspects the cache decision for a URL. It builds the
// cache key using the default key policy, looks up the store, and
// returns what the cache engine would do with this request.
func cacheCheck(ctx context.Context, rawURL string, rs *runState) admin.CacheCheckResult {
	policy := cache.NewKeyPolicy(nil, nil, nil, nil, false, false)
	key := cache.BuildKeyFromURL(rawURL, policy)
	result := admin.CacheCheckResult{
		URL:    rawURL,
		KeyHex: key.Hex(),
	}
	if key.IsZero() {
		result.CacheResult = "BYPASS"
		result.Source = "origin"
		return result
	}
	obj, source, err := rs.store.Get(ctx, key)
	if err != nil || obj == nil {
		result.CacheResult = "MISS"
		result.Source = "origin"
		return result
	}
	result.Source = string(source)
	now := time.Now()
	switch {
	case obj.Fresh(now):
		result.CacheResult = "HIT"
	case obj.StaleButServable(now):
		result.CacheResult = "STALE"
	default:
		result.CacheResult = "MISS"
	}
	return result
}

// purgeKey purges a key and its Vary variants from the local store,
// stopping after the first handler that owns the key. Cluster broadcast
// is the caller's responsibility.
func (rs *runState) purgeKey(ctx context.Context, key api.Key) error {
	for _, h := range rs.handlers {
		owned, err := h.Purge(ctx, key)
		if err != nil {
			return err
		}
		if owned {
			return nil
		}
	}
	return nil
}

// buildInvalidationOps creates the shared purge/ban/refresh closures.
// The broadcaster internally detaches from the engine's root context so
// peer fan-out survives shutdown; local store operations use dCtx.
func (e *engine) buildInvalidationOps(ctx context.Context, rs *runState) invalidationOps {
	return invalidationOps{
		PurgeFn: func(dCtx context.Context, urlStr string) error {
			key := cache.BuildKeyFromURL(urlStr, nil)
			if err := rs.purgeKey(dCtx, key); err != nil {
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
			if err := rs.purgeKey(dCtx, cache.BuildKeyFromURL(urlStr, nil)); err != nil {
				return err
			}
			rs.cfProp.PropagateForRefresh(dCtx, urlStr)
			return nil
		},
	}
}

// swapAdminHandler builds the full admin handler (all routes, auth,
// dashboard) and atomically swaps it into the already-listening minimal
// admin server. The admin listener was started before initSubsystems so
// K8s probes could reach healthz/readyz during store loading.
func (e *engine) swapAdminHandler(ctx context.Context, rs *runState, minimalAdmin *admin.Server, conditionsFn func() []admin.Condition, drainFn func()) {
	addr := e.cfg.Listen.Admin
	if addr == "" {
		addr = ":9000"
	}

	ops := e.buildInvalidationOps(ctx, rs)
	dashMux := e.buildDashboard(rs, addr, ops)

	srv := admin.New(admin.Config{
		Addr:       addr,
		Token:      rs.token,
		Logger:     e.logger,
		Metrics:    e.metrics,
		PeersFn:    rs.peersFn,
		CFStatusFn: rs.cfProp.Status,
		CFPropagateFn: func(ctx context.Context, req admin.CFPropagateRequest) error {
			return rs.cfProp.PropagateExternal(ctx, req)
		},
		StatsFn:  func() api.Stats { return rs.store.Stats() },
		ConfigFn: func() any { return sanitizedConfig(*e.cfg) },
		CacheCheckFn: func(ctx context.Context, rawURL string) admin.CacheCheckResult {
			return cacheCheck(ctx, rawURL, rs)
		},
		ReadyFn:            rs.seq.IsReady,
		ConditionsFn:       conditionsFn,
		DrainFn:            drainFn,
		MaxBatchSize:       e.cfg.Admin.MaxBatchSize,
		MaxBodyBytes:       e.cfg.Admin.MaxBodyBytes,
		RateLimitPerSecond: e.cfg.Admin.RateLimitPerSecond,
		PprofEnabled:       e.cfg.Admin.PprofEnabled,
		OnPurged:           rs.cfProp.PropagateForPurge,
		OnRefreshed:        rs.cfProp.PropagateForRefresh,
		OnBanned: func(bCtx context.Context, expr api.BanExpr) {
			rs.cfProp.PropagateForBan(bCtx, expr)
		},
		PurgeFn: func(key api.Key) error {
			if err := rs.purgeKey(ctx, key); err != nil {
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
			return rs.purgeKey(ctx, key)
		},
		PeerPurgeHandler: cluster.NewPeerPurgeHandler(func(evt api.PurgeEvent) error {
			return rs.purgeKey(ctx, evt.Key)
		}),
		PeerBanHandler: cluster.NewPeerBanHandler(func(evt api.BanEvent) error {
			_, err := rs.store.Ban(ctx, evt.Predicate)
			return err
		}),
		PeerFetchHandler:   cluster.NewPeerFetchHandler(rs.store, e.cfg.Cluster.HopLimit),
		PeerMetricsHandler: dashboard.PeerMetricsHandler(rs.rings),
		DashboardHandler:   dashMux,
		FaviconHandler:     webdash.FaviconHandler(),
	})
	_ = rs.peerFetcher // suppress unused warning when cluster is disabled
	minimalAdmin.SwapHandler(srv.Handler())
}

// buildDashboard wires and returns the dashboard ServeMux.
func (e *engine) buildDashboard(rs *runState, addr string, ops invalidationOps) *http.ServeMux {
	dashMux := http.NewServeMux()

	var ringFn func() []api.RingSegment
	if rs.clusterNode != nil {
		ringFn = rs.clusterNode.RingSegments
	}

	clusterMeta := e.buildClusterMeta(rs)

	_ = dashboard.New(dashboard.Config{
		Rings:        rs.rings,
		Version:      buildinfo.Version,
		PeersFn:      rs.peersFn,
		SelfAddr:     addr,
		Token:        rs.token,
		Logger:       e.logger,
		StoreFn:      rs.store.Stats,
		HotMaxBytes:  e.cfg.Storage.HotMaxBytes.Bytes(),
		WarmMaxBytes: e.cfg.Storage.WarmMaxBytes.Bytes(),
		Config:       e.cfg,
		ConfigPath:   e.configPath,
		StartTime:    e.startTime,
		RingFn:       ringFn,
		ClusterMeta:  clusterMeta,
		PurgeFn:      ops.PurgeFn,
		BanFn:        ops.BanFn,
		RefreshFn:    ops.RefreshFn,
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
func (e *engine) buildClusterMeta(rs *runState) templates.ClusterMeta {
	meta := templates.ClusterMeta{
		ProtocolVersion:  cluster.ClusterProtocolVersion,
		GossipInterval:   "5s",
		JoinRetryBudget:  "60s · 2s step",
		PeerFetchTimeout: "500ms",
	}
	if rs.clusterNode != nil {
		meta.VirtualNodes = rs.clusterNode.Config().VirtualNodes
		meta.Mode = rs.clusterNode.Mode()
	} else {
		meta.Mode = "single-node"
	}
	if e.cfg.Cluster.HopLimit > 0 {
		meta.HopLimit = e.cfg.Cluster.HopLimit
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

func (e *engine) startListeners(g *supervised.Group, handler http.Handler, rs *runState) {
	tcpFastOpen := boolDefault(e.cfg.Listen.TCPFastOpen, true)
	tcpDeferAccept := boolDefault(e.cfg.Listen.TCPDeferAccept, true)
	reusePort := boolDefault(e.cfg.Listen.ReusePort, platform.ReusePortSupported)

	var fastPathHandler api.FastPathHandler
	if e.cfg.Experimental.H1FastPath && rs.store != nil {
		fastPathHandler = cache.NewFastPathHandlerFromStore(rs.store)
		e.logger.Info("H1 fast path enabled", "experimental", true)
	}

	if e.cfg.Listen.HTTP != "" {
		srv := server.NewHTTP(server.ListenerConfig{
			Addr:           e.cfg.Listen.HTTP,
			Handler:        handler,
			Logger:         e.logger,
			MaxConnections: e.cfg.Listen.MaxConnections,
			TCPFastOpen:    tcpFastOpen,
			TCPDeferAccept: tcpDeferAccept,
			ReusePort:      reusePort,
			FastPath:       fastPathHandler,
			FastMetrics:    rs.dpMetrics,
			Scheme:         "http",
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
			Addr:           e.cfg.Listen.HTTPS,
			Handler:        handler,
			Logger:         e.logger,
			TLSConfig:      tlsCfg,
			MaxConnections: e.cfg.Listen.MaxConnections,
			TCPFastOpen:    tcpFastOpen,
			TCPDeferAccept: tcpDeferAccept,
			ReusePort:      reusePort,
			FastPath:       fastPathHandler,
			FastMetrics:    rs.dpMetrics,
			Scheme:         "https",
		})
		rs.listeners = append(rs.listeners, srv)
		g.Go("listener-https", srv.Serve)
	}
}

func boolDefault(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
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
	if rs.clusterNode == nil || len(e.cfg.Cluster.Join) == 0 {
		return
	}

	joinTimeout := e.cfg.Cluster.JoinTimeout
	if joinTimeout == 0 {
		joinTimeout = 120 * time.Second
	}

	g.Go("cluster-join", func(joinCtx context.Context) error {
		err := e.joinWithRetry(joinCtx, rs.clusterNode, joinTimeout)

		if err == nil {
			e.logger.Info("cluster join succeeded", "members", len(rs.clusterNode.Members()))
			return nil
		}

		e.logger.Warn("cluster join: initial attempt failed, continuing background retry",
			"error", err)

		// Continue retrying in the background. Peers may come up later
		// (e.g., during a rolling update with sequential pod starts).
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-joinCtx.Done():
				return nil
			case <-ticker.C:
				if _, jErr := rs.clusterNode.Join(e.cfg.Cluster.Join); jErr == nil {
					if len(rs.clusterNode.Members()) > 1 {
						e.logger.Info("cluster join succeeded (background retry)",
							"members", len(rs.clusterNode.Members()))
						return nil
					}
				}
			}
		}
	})
}

func (e *engine) registerShutdownSteps(g *supervised.Group, rs *runState) {
	rs.seq.AddStep("mark-not-ready", 15*time.Second, func(ctx context.Context) error {
		var wg errgroup.Group
		for _, ln := range rs.listeners {
			wg.Go(func() error { return ln.Shutdown(ctx) })
		}
		return wg.Wait()
	})
	// Close refresh-enabled handlers before the store to prevent
	// in-flight background refresh goroutines from calling store.Put
	// on a closed store.
	if len(rs.handlers) > 0 {
		rs.seq.AddStep("drain-refresh-handlers", 10*time.Second, func(ctx context.Context) error {
			var wg errgroup.Group
			for _, h := range rs.handlers {
				if !h.RefreshEnabled() {
					continue
				}
				wg.Go(func() error { return h.Close(ctx) })
			}
			return wg.Wait()
		})
	}
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
		if rs.peerFetcher != nil {
			rs.seq.AddStep("drain-peer-fetcher", 5*time.Second, func(ctx context.Context) error {
				return rs.peerFetcher.Close(ctx)
			})
		}
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
// for up to joinTimeout. Returns nil if join succeeded (Members() > 1),
// or an error identifying the failure if the deadline was reached.
func (e *engine) joinWithRetry(ctx context.Context, c *cluster.Cluster, joinTimeout time.Duration) error {
	seeds := e.cfg.Cluster.Join
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	deadline := time.After(joinTimeout)
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
			return fmt.Errorf("cluster join: gave up after %s, running with local member only", joinTimeout)
		case <-ticker.C:
		}
	}
}
