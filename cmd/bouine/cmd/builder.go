package cmd

// builder.go contains the wiring helpers that construct bouine subsystems
// from configuration: buildStore, buildCluster, buildHandler, buildPools,
// buildRouter, and the listenPort utility. Lifecycle orchestration (run,
// startAdmin, startListeners, etc.) remains in engine.go.

import (
	"context"
	"net"
	"os"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/cluster"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/observability/tracing"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/internal/server"
	"github.com/bouine-cache/bouine/internal/staticfile"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/storage/wal"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// buildStore creates a TieredStore (hot + warm + WAL) when WarmDir is
// configured, or a plain HotStore for ephemeral/dev deployments.
// warmMetrics, when non-nil, is injected into the warm store so it can
// increment over-budget, eviction, and compaction counters inline.
// walMetrics, when non-nil, is injected into the WAL log so it can
// record write duration, queue depth, and write count metrics.
func (e *engine) buildStore(warmMetrics *warm.Metrics, walMetrics *wal.Metrics) (storage.Store, error) {
	hotAlgo := e.cfg.Storage.HotEvictionAlgorithm
	if hotAlgo == "" {
		hotAlgo = e.cfg.Storage.EvictionAlgorithm
	}
	warmAlgo := e.cfg.Storage.WarmEvictionAlgorithm
	if warmAlgo == "" {
		warmAlgo = e.cfg.Storage.EvictionAlgorithm
	}
	hotCfg := storage.HotConfig{
		MaxBytes:             e.cfg.Storage.HotMaxBytes.Bytes(),
		Slab:                 e.cfg.Storage.HotMmapSlab,
		HotEvictionAlgorithm: hotAlgo,
	}
	if e.cfg.Storage.WarmDir == "" {
		return storage.NewHotStore(hotCfg), nil
	}
	return storage.NewTieredStore(storage.TieredConfig{
		Hot:                    hotCfg,
		Warm:                   &warm.Config{Dir: e.cfg.Storage.WarmDir, MaxBytes: e.cfg.Storage.WarmMaxBytes.Bytes(), MaxEntries: e.cfg.Storage.WarmMaxEntries, SegmentCacheSize: e.cfg.Storage.SegmentCacheSize, MaxDiskBytes: e.cfg.Storage.WarmMaxDiskBytes.Bytes(), MinFreeDisk: e.cfg.Storage.MinFreeDisk.Bytes(), Preallocate: e.cfg.Storage.WarmPreallocate.Bytes(), WarmEvictionAlgorithm: warmAlgo},
		WALDir:                 e.cfg.Storage.WarmDir + "/bouine.wal",
		BodyThreshold:          e.cfg.Storage.BodyThreshold.Bytes(),
		WarmSyncInterval:       e.cfg.Storage.WarmSyncInterval,
		WarmSyncBatchSize:      e.cfg.Storage.WarmSyncBatchSize,
		WALSyncInterval:        e.cfg.Storage.WALSyncInterval,
		CompactStartupDelay:    e.cfg.Storage.CompactStartupDelay,
		CompactInterval:        e.cfg.Storage.CompactInterval,
		CheckpointInterval:     e.cfg.Storage.CheckpointInterval,
		CheckpointWALThreshold: e.cfg.Storage.CheckpointWALThreshold,
		TombstoneQueueSize:     e.cfg.Storage.TombstoneQueueSize,
		TombstoneDrainInterval: e.cfg.Storage.TombstoneDrainInterval,
		Logger:                 e.logger,
		WarmMetrics:            warmMetrics,
		WALMetrics:             walMetrics,
	})
}

// buildCluster initialises the gossip cluster node from the Listen and Cluster
// config sections. When POD_IP is set (Kubernetes Downward API), it resolves the
// IP and builds fully-qualified advertise addresses for the cluster, admin, and
// data-plane ports so that peer-to-peer RPCs are routable across pods. If POD_IP
// is a hostname rather than a dotted IP, DNS lookup is retried up to five times
// with back-off to tolerate slow container start order in Docker Compose.
func (e *engine) buildCluster(ctx context.Context) (*cluster.Cluster, error) {
	hostname := e.cfg.Cluster.NodeName
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
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
		// Resolve hostname to IP if needed. memberlist requires a routable IP
		// for AdvertiseAddr (hostnames are rejected). Docker Compose service
		// names are valid DNS names but memberlist won't accept them directly.
		resolvedIP := podIP
		if net.ParseIP(podIP) == nil {
			// DNS may not be ready yet in Docker Compose (other containers
			// may still be starting). Retry a few times.
			for i := range 5 {
				addrs, err := net.DefaultResolver.LookupHost(ctx, podIP)
				if err == nil && len(addrs) > 0 {
					resolvedIP = addrs[0]
					break
				}
				if i < 4 {
					time.Sleep(time.Duration(i+1) * 500 * time.Millisecond)
				}
			}
			if resolvedIP == podIP {
				e.logger.Warn("cluster: could not resolve POD_IP hostname after retries, using as-is",
					"pod_ip", podIP)
			}
		}
		advertiseAddr = resolvedIP + ":" + listenPort(e.cfg.Listen.Cluster, "8443")
		peerInfo.Addr = advertiseAddr
		peerInfo.AdminAddr = resolvedIP + ":" + listenPort(e.cfg.Listen.Admin, "9000")
		peerInfo.DataAddr = resolvedIP + ":" + listenPort(e.cfg.Listen.HTTP, "80")
	}

	return cluster.New(cluster.Config{
		NodeName:          hostname,
		BindAddr:          e.cfg.Listen.Cluster,
		AdvertiseAddr:     advertiseAddr,
		Join:              e.cfg.Cluster.Join,
		PeerInfo:          peerInfo,
		Logger:            e.logger,
		Mode:              e.cfg.Cluster.Mode,
		HandoffQueueDepth: e.cfg.Cluster.HandoffQueueDepth,
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

// buildHandler assembles the full data-plane stack for a single HTTP
// listener. The middleware chain is:
//
//  1. tracing.FastHTTPMiddleware — single OTel span for the pipeline layer.
//  2. DataPlaneMetrics.FastHTTPMiddleware — Prometheus counters, histograms,
//     ring buffers, and merged structured access log.

// stripPrefixFastHTTP strips the given prefix from the request path before
// forwarding to next. Used by static routes; proxied routes are stripped
// inside cache.Handler. Both share cache.StripRequestURI so boundary
// semantics (exact match → "/", "?query" keeps its "/" root, mid-segment
// pass-through) are defined in exactly one place.
func stripPrefixFastHTTP(prefix string, next fasthttp.RequestHandler) fasthttp.RequestHandler {
	prefixBytes := []byte(prefix)
	return func(ctx *fasthttp.RequestCtx) {
		// When no strip applies, StripRequestURI returns the URI
		// unchanged, so re-setting it is a no-op.
		ctx.Request.SetRequestURIBytes(cache.StripRequestURI(prefixBytes, ctx.RequestURI()))
		next(ctx)
	}
}

func (e *engine) buildHandler(rs *runState) fasthttp.RequestHandler {
	router := e.buildRouter(rs)
	routeNames := make([]string, 0, len(e.cfg.Routes))
	for _, rc := range e.cfg.Routes {
		if rc.Name != "" {
			routeNames = append(routeNames, rc.Name)
		}
	}
	rs.dpMetrics.PreResolveRoutes(routeNames)
	rs.dpMetrics.SetNowFunc(platform.CoarseNow)

	// Native fasthttp middleware chain: tracing → metrics → router.
	// The cache handler's ServeRequest is synchronous (no adaptor
	// goroutine), so reading ctx.Response in the metrics middleware
	// after the handler returns is race-free.
	metricsWrapped := rs.dpMetrics.FastHTTPMiddleware(router.ServeRequest)
	return tracing.FastHTTPMiddleware("bouine.pipeline", metricsWrapped)
}

// buildPools constructs one origin.Pool per upstream_pools entry in the config.
// Each pool holds the target addresses and passive health state for a named
// upstream. Pools are keyed by name and passed to buildRouter so each route can
// reference its upstream by the name declared in config.
func (e *engine) buildPools(metrics *origin.Metrics) (map[string]*origin.Pool, error) {
	pools := make(map[string]*origin.Pool, len(e.cfg.UpstreamPools))
	for _, pc := range e.cfg.UpstreamPools {
		p, err := origin.NewPool(buildPoolConfig(pc, e.logger, metrics))
		if err != nil {
			return nil, err
		}
		pools[pc.Name] = p
	}
	return pools, nil
}

// buildPoolConfig maps an upstream pool's config into the origin pool
// constructor, including the connect policy (dial timeout, TCP
// keep-alive, per-host connection cap, idle duration, response header
// timeout). Zero values are resolved to built-in defaults inside
// origin.NewPool.
func buildPoolConfig(pc config.UpstreamPool, logger observability.Logger, metrics *origin.Metrics) origin.PoolConfig {
	return origin.PoolConfig{
		Name:                  pc.Name,
		Targets:               pc.Targets,
		Logger:                logger,
		Consecutive5xx:        pc.Health.Passive.Consecutive5xx,
		Metrics:               metrics,
		DialTimeout:           pc.Connect.Timeout,
		KeepAlive:             pc.Connect.KeepAlive,
		MaxConnsPerHost:       pc.Connect.MaxConnections,
		MaxIdleConnDuration:   pc.Connect.MaxIdleConnDuration,
		ResponseHeaderTimeout: pc.Connect.ResponseHeaderTimeout,
	}
}

// buildRouter constructs the pipeline.Router by iterating over the route table
// and wiring each route to its upstream pool and cache handler. For every route
// it resolves connection settings (dial timeout, keep-alive, optional hedge
// transport) from the matching upstream pool config, then builds a
// cache.Handler that owns the RFC 9111 state machine for that route.
//
// Cluster extensions are wired here rather than in cache.Handler so that the
// handler itself stays cluster-agnostic:
//
//   - In strong mode, OwnerFn and PeerFetch are set so on a MISS the handler
//     can route the request to the consistent-hash owner node.
//   - In eventual mode neither is set; every node caches independently.
//
// All cache handlers are collected into rs.handlers; the engine
// filters via Handler.RefreshEnabled() for shutdown drain and metric polling.
func (e *engine) buildRouter(rs *runState) *server.Router {
	router := server.NewRouter(server.RouterConfig{Logger: e.logger})
	for _, rc := range e.cfg.Routes {
		if rc.Static.Root != "" {
			e.buildStaticRoute(router, rs, rc)
			continue
		}
		p := rs.pools[rc.Pool]
		if p == nil {
			continue
		}
		cfg := cache.HandlerConfig{
			Upstream:                p.FastHandler(0),
			FastClient:              p.FastClient(),
			StripPrefix:             rc.Request.StripPrefix,
			Store:                   rs.store,
			Logger:                  e.logger,
			NegativeTTL:             rc.Cache.NegativeTTL,
			JitterPercent:           rc.Cache.JitterPercent,
			StayinAlive:             rc.Cache.StayinAlive,
			LogCacheKeys:            true,
			DefaultTTL:              rc.Cache.TTLDefault,
			OverrideTTL:             rc.Cache.TTLOverride,
			DefaultSWR:              rc.Cache.StaleWhileRevalidate,
			DefaultSIE:              rc.Cache.StaleIfError,
			AllowSetCookie:          rc.Cache.AllowSetCookie != nil && *rc.Cache.AllowSetCookie,
			MaxObjectSize:           rc.Cache.MaxObjectSize.Bytes(),
			MaxResponseBytes:        rc.Cache.MaxResponseBytes.Bytes(),
			MaxFetchConcurrency:     rc.Cache.MaxFetchConcurrency,
			FetchTimeout:            rc.Cache.FetchTimeout,
			FetchWaitTimeout:        rc.Cache.FetchWaitTimeout,
			MaxStreamingBufferBytes: rc.Cache.MaxStreamingBufferBytes.Bytes(),
			Policy:                  buildKeyPolicy(rc.Cache.Key),
			VaryCapHits:             rs.dpMetrics.VaryCapHits,
			StreamingBufferBytes:    rs.dpMetrics.StreamingBufferBytes,
			StreamingFallback:       rs.dpMetrics.StreamingFallbackTotal,
			FetchShed:               rs.dpMetrics.FetchShedTotal,
			RefreshBeforeExpiry:     rc.Cache.RefreshBeforeExpiry,
			RouteName:               rc.Name,
			RefreshMetrics:          rs.dpMetrics.RefreshMetricsVec(),
		}
		applyRefreshConfig(&cfg, rc.Cache)
		if rs.clusterNode != nil && rs.peerFetcher != nil && e.cfg.Cluster.Mode == config.ClusterModeStrong {
			cfg.OwnerFn = func(key api.Key) (api.PeerInfo, bool) {
				owner := rs.clusterNode.Owner(key)
				if owner.Name == "" {
					return api.PeerInfo{}, true
				}
				return owner, rs.clusterNode.IsLocal(key)
			}
			cfg.PeerFetch = func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error) {
				return rs.peerFetcher.Fetch(ctx, peer, api.PeerFetchRequest{Key: key})
			}
			// Write-to-owner RPC: a non-owner that fetches from origin
			// forwards the object to the owner so subsequent peer-fetches
			// hit (issue #509). Fire-and-forget in a bounded goroutine so
			// the response path is never blocked on the RPC.
			cfg.PeerPut = func(ctx context.Context, owner api.PeerInfo, obj *api.Object) {
				go func() {
					putCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cluster.PeerFetchTimeout)
					defer cancel()
					if err := rs.peerFetcher.Put(putCtx, owner, obj); err != nil {
						e.logger.Debug("peer put error (non-fatal)",
							"owner", owner.Name, "key", obj.Key, "error", err)
					}
				}()
			}
		}
		cached := cache.NewHandler(cfg)
		rs.handlers = append(rs.handlers, cached)
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Match.Methods, cached.ServeRequest)
	}
	return router
}

// buildStaticRoute wires a route that serves files from a local directory
// instead of proxying to an upstream pool. When cache is explicitly enabled
// for the route, the static handler is wrapped in a cache.Handler so cached
// objects benefit from the same TTL, SWR, SIE, eviction, and cluster
// replication as proxied responses. When cache is not explicitly enabled
// (default for static routes), the static handler serves directly from disk
// and the OS page cache provides the hot caching layer.
func (e *engine) buildStaticRoute(router *server.Router, rs *runState, rc config.Route) {
	sh, err := staticfile.New(staticfile.Config{
		Root:       rc.Static.Root,
		IndexFiles: rc.Static.Index,
		MaxBytes:   rc.Static.MaxFileSize.Bytes(),
		Logger:     e.logger,
		RouteLabel: rc.Name,
	})
	if err != nil {
		e.logger.Error("static route init failed, skipping", "route", rc.Name, "error", err)
		return
	}

	var handler fasthttp.RequestHandler = sh.ServeRequest

	// Apply strip_prefix if configured (reuses the same mechanism as
	// proxied routes — one place, one behavior).
	if rc.Request.StripPrefix != "" {
		handler = stripPrefixFastHTTP(rc.Request.StripPrefix, handler)
	}

	// Wrap in cache handler only when cache is explicitly enabled.
	cacheEnabled := rc.Cache.Enabled != nil && *rc.Cache.Enabled
	if cacheEnabled {
		cfg := cache.HandlerConfig{
			Upstream:                handler,
			Store:                   rs.store,
			Logger:                  e.logger,
			NegativeTTL:             rc.Cache.NegativeTTL,
			JitterPercent:           rc.Cache.JitterPercent,
			StayinAlive:             rc.Cache.StayinAlive,
			LogCacheKeys:            true,
			DefaultTTL:              rc.Cache.TTLDefault,
			OverrideTTL:             rc.Cache.TTLOverride,
			DefaultSWR:              rc.Cache.StaleWhileRevalidate,
			DefaultSIE:              rc.Cache.StaleIfError,
			MaxObjectSize:           rc.Cache.MaxObjectSize.Bytes(),
			MaxResponseBytes:        rc.Cache.MaxResponseBytes.Bytes(),
			MaxFetchConcurrency:     rc.Cache.MaxFetchConcurrency,
			FetchTimeout:            rc.Cache.FetchTimeout,
			FetchWaitTimeout:        rc.Cache.FetchWaitTimeout,
			MaxStreamingBufferBytes: rc.Cache.MaxStreamingBufferBytes.Bytes(),
			Policy:                  buildKeyPolicy(rc.Cache.Key),
			VaryCapHits:             rs.dpMetrics.VaryCapHits,
			StreamingBufferBytes:    rs.dpMetrics.StreamingBufferBytes,
			StreamingFallback:       rs.dpMetrics.StreamingFallbackTotal,
			FetchShed:               rs.dpMetrics.FetchShedTotal,
		}
		applyRefreshConfig(&cfg, rc.Cache)
		if rs.clusterNode != nil && rs.peerFetcher != nil && e.cfg.Cluster.Mode == config.ClusterModeStrong {
			cfg.OwnerFn = func(key api.Key) (api.PeerInfo, bool) {
				owner := rs.clusterNode.Owner(key)
				if owner.Name == "" {
					return api.PeerInfo{}, true
				}
				return owner, rs.clusterNode.IsLocal(key)
			}
			cfg.PeerFetch = func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error) {
				return rs.peerFetcher.Fetch(ctx, peer, api.PeerFetchRequest{Key: key})
			}
			// Write-to-owner RPC: a non-owner that fetches from origin
			// forwards the object to the owner so subsequent peer-fetches
			// hit (issue #509). Fire-and-forget in a bounded goroutine so
			// the response path is never blocked on the RPC.
			cfg.PeerPut = func(ctx context.Context, owner api.PeerInfo, obj *api.Object) {
				go func() {
					putCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cluster.PeerFetchTimeout)
					defer cancel()
					if err := rs.peerFetcher.Put(putCtx, owner, obj); err != nil {
						e.logger.Debug("peer put error (non-fatal)",
							"owner", owner.Name, "key", obj.Key, "error", err)
					}
				}()
			}
		}
		cached := cache.NewHandler(cfg)
		rs.handlers = append(rs.handlers, cached)
		handler = cached.ServeRequest
	}

	// When cache is not enabled, wire the staticfile handler's native
	// fasthttp ServeRequest method directly — no adaptor needed.
	if !cacheEnabled {
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Match.Methods, sh.ServeRequest)
		return
	}

	router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Match.Methods, handler)
}

// buildKeyPolicy compiles the route's cache key config into a
// pre-compiled KeyPolicy. Returns nil when no query/header policy
// is active (no allocation).
func buildKeyPolicy(rk config.RouteKey) *cache.KeyPolicy {
	if !hasKeyPolicy(rk) {
		return nil
	}
	return cache.NewKeyPolicy(
		buildStripSet(rk.StripQueryParams),
		buildKeepSet(rk.KeepQueryParams),
		buildExcludeHeaderSet(rk.ExcludeHeaders),
		rk.StripQueryPrefix,
		rk.StripEmptyParams,
		rk.DedupQueryParams,
	)
}

func buildKeepSet(params []string) map[string]bool {
	if len(params) == 0 {
		return nil
	}
	m := make(map[string]bool, len(params))
	for _, p := range params {
		m[p] = true
	}
	return m
}

// hasKeyPolicy checks query/header fields only. canonicalize_path
// is handled at the parser level, not in KeyPolicy.
func hasKeyPolicy(rk config.RouteKey) bool {
	return len(rk.StripQueryParams) > 0 || len(rk.ExcludeHeaders) > 0 ||
		len(rk.KeepQueryParams) > 0 || len(rk.StripQueryPrefix) > 0 ||
		rk.StripEmptyParams || rk.DedupQueryParams
}

// buildStripSet converts a config []string into a map for O(1) lookup.
// Returns nil when the list is empty (no allocation).
func buildStripSet(params []string) map[string]bool {
	if len(params) == 0 {
		return nil
	}
	m := make(map[string]bool, len(params))
	for _, p := range params {
		m[p] = true
	}
	return m
}

// buildExcludeHeaderSet converts a config []string of header names into
// a lowercase map for case-insensitive O(1) lookup. Returns nil when
// the list is empty.
func buildExcludeHeaderSet(headers []string) map[string]bool {
	if len(headers) == 0 {
		return nil
	}
	m := make(map[string]bool, len(headers))
	for _, h := range headers {
		m[strings.ToLower(h)] = true
	}
	return m
}

// applyRefreshConfig sets the refresh-before-expiry timing fields on
// the handler config from the route's cache policy. Called only when
// RefreshBeforeExpiry is true.
func applyRefreshConfig(cfg *cache.HandlerConfig, rc config.RouteCache) {
	if !rc.RefreshBeforeExpiry {
		return
	}
	ttlBasis := rc.TTLOverride
	if ttlBasis <= 0 {
		ttlBasis = rc.TTLDefault
	}
	marginPct := rc.RefreshMarginPercent
	if marginPct <= 0 {
		marginPct = 10
	}
	cfg.RefreshMargin = ttlBasis * time.Duration(marginPct) / 100
	cfg.RefreshTimeout = rc.RefreshTimeout
	cfg.RefreshConcurrency = rc.RefreshConcurrency
	cfg.RefreshMinHits = rc.RefreshMinHits
	cfg.RefreshPersistCycles = rc.RefreshPersistCycles
	cfg.RefreshMinScore = rc.RefreshMinScore
	cfg.RefreshMaxRPS = rc.RefreshMaxRPS
	cfg.RefreshReactiveFirst = rc.RefreshReactiveFirst
}

// buildHedgeTimeout returns the hedge timeout from the pool config,
// or 0 if hedging is not configured.
func buildHedgeTimeout(pc config.UpstreamPool) time.Duration {
	return pc.Connect.HedgeTimeout
}
