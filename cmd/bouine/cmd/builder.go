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
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// buildStore creates a TieredStore (hot + warm + WAL) when WarmDir is
// configured, or a plain HotStore for ephemeral/dev deployments.
// warmMetrics, when non-nil, is injected into the warm store so it can
// increment over-budget, eviction, and compaction counters inline.
// walMetrics, when non-nil, is injected into the WAL log so it can
// record write duration, queue depth, and write count metrics.
func (e *engine) buildStore(warmMetrics *warm.Metrics, walMetrics *wal.Metrics) (storage.Store, error) {
	rs := e.resolvedConfig().Storage
	hotCfg := storage.HotConfig{
		MaxBytes:             rs.HotMaxBytes.Bytes(),
		Slab:                 rs.HotMmapSlab,
		HotEvictionAlgorithm: rs.HotEvictionAlgorithm,
		BanTTL:               e.resolvedConfig().Cluster.BanTTL,
	}
	if rs.WarmDir == "" {
		return storage.NewHotStore(hotCfg), nil
	}
	return storage.NewTieredStore(storage.TieredConfig{
		Hot:                    hotCfg,
		Warm:                   &warm.Config{Dir: rs.WarmDir, MaxBytes: rs.WarmMaxBytes.Bytes(), MaxEntries: rs.WarmMaxEntries, SegmentCacheSize: rs.SegmentCacheSize, MaxDiskBytes: rs.WarmMaxDiskBytes.Bytes(), MinFreeDisk: rs.MinFreeDisk.Bytes(), Preallocate: rs.WarmPreallocate.Bytes(), WarmEvictionAlgorithm: rs.WarmEvictionAlgorithm},
		WALDir:                 rs.WarmDir + "/bouine.wal",
		BodyThreshold:          rs.BodyThreshold.Bytes(),
		WarmSyncInterval:       rs.WarmSyncInterval,
		WarmSyncBatchSize:      rs.WarmSyncBatchSize,
		WALSyncInterval:        rs.WALSyncInterval,
		CompactStartupDelay:    rs.CompactStartupDelay,
		CompactInterval:        rs.CompactInterval,
		CheckpointInterval:     rs.CheckpointInterval,
		CheckpointWALThreshold: rs.CheckpointWALThreshold,
		TombstoneQueueSize:     rs.TombstoneQueueSize,
		TombstoneDrainInterval: rs.TombstoneDrainInterval,
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

// pathRewriteFastHTTP rewrites the request path with the compiled
// path_rewrite before forwarding to next. The regex sibling of
// stripPrefixFastHTTP: same surface (static routes without cache),
// same boundary semantics via cache.PathRewrite.RewriteURI.
func pathRewriteFastHTTP(rw *cache.PathRewrite, next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		rw.Apply(&ctx.Request)
		next(ctx)
	}
}

// buildPathRewrite compiles the route's request.path_rewrite into a
// cache.PathRewrite. The pattern was validated and compiled once by
// config.validatePathRewrite; the second compile here is the one the
// handler keeps. Returns nil when path_rewrite is not configured.
func buildPathRewrite(rc config.Route) *cache.PathRewrite {
	pw := rc.Request.PathRewrite
	if pw.Match == "" || pw.Replace == "" {
		return nil
	}
	return cache.NewPathRewrite(pw.Match, pw.Replace)
}

func (e *engine) buildHandler(rs *runState) fasthttp.RequestHandler {
	router := e.buildRouter(rs)
	// Retain the router: startListeners wraps it into the routed H1
	// fast path (issue #696). Must be set before listeners start.
	rs.router = router
	// The metrics middleware attributes by upstream pool: the label set
	// stays bounded by the pool configuration, unlike route names.
	poolNames := make([]string, 0, len(e.cfg.UpstreamPools))
	for _, pc := range e.cfg.UpstreamPools {
		poolNames = append(poolNames, pc.Name)
	}
	rs.dpMetrics.PreResolveRoutes(poolNames)
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
	pools := make(map[string]*origin.Pool, len(e.resolvedConfig().Pools))
	for i := range e.cfg.UpstreamPools {
		pc := &e.cfg.UpstreamPools[i]
		p, err := origin.NewPool(e.buildPoolConfig(*pc, e.logger, metrics))
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
func (e *engine) buildPoolConfig(pc config.UpstreamPool, logger observability.Logger, metrics *origin.Metrics) origin.PoolConfig {
	rp := e.resolvedPool(pc.Name)
	if rp == nil {
		// Unreachable for pools built from a resolved config; fall back
		// to zero values (origin.NewPool applies its own defaults).
		rp = &config.ResolvedPool{Name: pc.Name}
	}
	return origin.PoolConfig{
		Name:                  pc.Name,
		Targets:               pc.Targets,
		Logger:                logger,
		Consecutive5xx:        rp.Consecutive5xx,
		EjectFor:              rp.EjectFor,
		HedgeTimeout:          rp.HedgeTimeout,
		Metrics:               metrics,
		DialTimeout:           rp.DialTimeout,
		KeepAlive:             rp.KeepAlive,
		MaxConnsPerHost:       rp.MaxConnsPerHost,
		MaxIdleConnDuration:   rp.MaxIdleConnDuration,
		ResponseHeaderTimeout: rp.ResponseHeaderTimeout,
	}
}

// resolvedPool returns the resolved pool matching the configured pool
// name, or nil when the config contains no pool with that name (the
// caller falls back to an unresolved placeholder).
func (e *engine) resolvedPool(name string) *config.ResolvedPool {
	for i := range e.resolvedConfig().Pools {
		if e.resolvedConfig().Pools[i].Name == name {
			return &e.resolvedConfig().Pools[i]
		}
	}
	return nil
}

// The per-route origin-fetch timeout resolution (explicit
// cache.fetch_timeout wins, else the pool's
// connect.response_header_timeout with its built-in default, never
// cache.defaultFetchTimeout) and the hedge-timeout pass-through moved
// into the resolved layer (config.ResolvedRoute.FetchTimeout /
// config.ResolvedPool.HedgeTimeout) so /v1/config shows the effective
// values.

// buildRouter constructs the server.Router by iterating over the route table
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
	// The H1 fast path is per route: each cache-enabled route registers
	// the FastPathHandler built from its own Handler, so hits carry the
	// route's pool attribution and run under the route's KeyPolicy
	// (issue #696). A single store-level handler cannot — the store is
	// shared across routes and knows nothing about them.
	buildRouteFP := func(cached *cache.Handler) *cache.FastPathHandler {
		fp := cache.NewFastPathHandler(cached)
		rs.fastPathHandlers = append(rs.fastPathHandlers, fp)
		return fp
	}
	for i, rc := range e.cfg.Routes {
		if rc.Static.Root != "" {
			e.buildStaticRoute(router, rs, rc, &e.resolvedConfig().Routes[i], buildRouteFP)
			continue
		}
		p := rs.pools[rc.Pool]
		if p == nil {
			continue
		}
		rrc := &e.resolvedConfig().Routes[i].Cache
		cfg := cache.HandlerConfig{
			Upstream:                p.FastHandler(0),
			FastClient:              p.FastClient(),
			StripPrefix:             rc.Request.StripPrefix,
			PathRewrite:             buildPathRewrite(rc),
			RequestHeaderSet:        rc.Request.HeaderSet,
			RequestHeaderRemove:     rc.Request.HeaderRemove,
			ResponseHeaderSet:       rc.Response.HeaderSet,
			ResponseHeaderRemove:    rc.Response.HeaderRemove,
			Store:                   rs.store,
			Logger:                  e.logger,
			Negative:                rrc.NegativeTTL.Policy(),
			JitterPercent:           rrc.JitterPercent,
			StayinAlive:             rrc.StayinAlive,
			LogCacheKeys:            true,
			DefaultTTL:              rrc.TTLDefault,
			OverrideTTL:             rrc.TTLOverride,
			DefaultSWR:              rrc.StaleWhileRevalidate,
			DefaultSIE:              rrc.StaleIfError,
			AllowSetCookie:          rrc.AllowSetCookie,
			MaxObjectSize:           rrc.MaxObjectSize.Bytes(),
			MaxResponseBytes:        rrc.MaxResponseBytes.Bytes(),
			MaxFetchConcurrency:     rrc.MaxFetchConcurrency,
			FetchTimeout:            rrc.FetchTimeout,
			FetchWaitTimeout:        rrc.FetchWaitTimeout,
			MaxStreamingBufferBytes: rrc.MaxStreamingBufferBytes.Bytes(),
			Policy:                  buildKeyPolicy(rc.Cache.Key),
			VaryCapHits:             rs.dpMetrics.VaryCapHits,
			StreamingBufferBytes:    rs.dpMetrics.StreamingBufferBytes,
			StreamingFallback:       rs.dpMetrics.StreamingFallbackTotal,
			FetchShed:               rs.dpMetrics.FetchShedTotal,
			RewarmFill:              rs.dpMetrics.RewarmFillTotal,
			RefreshBeforeExpiry:     rrc.RefreshBeforeExpiry,
			RouteName:               rc.Name,
			PoolName:                rc.Pool,
			RefreshMetrics:          rs.dpMetrics.RefreshMetricsVec(),
		}
		applyRefreshConfig(&cfg, rrc)
		if ownerFn, peerFetchFn := clusterFastPathClosures(e, rs); ownerFn != nil && peerFetchFn != nil {
			cfg.OwnerFn = ownerFn
			cfg.PeerFetch = peerFetchFn
			cfg.OnPeerVariantMismatch = func() {
				rs.clusterMetrics.IncPeerFetchVariantMismatch("consumer")
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
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Pool, rc.Match.Methods, cached.ServeRequest, buildRouteFP(cached))
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
//
//nolint:funlen // 84: the cached-static-route wiring mirrors the proxied-route block by design; splitting it would hide the symmetry
func (e *engine) buildStaticRoute(router *server.Router, rs *runState, rc config.Route, rr *config.ResolvedRoute, buildRouteFP func(*cache.Handler) *cache.FastPathHandler) {
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

	// strip_prefix / path_rewrite wrappers are wired ONLY on the
	// non-cached path: the router dispatches straight into this chain,
	// so the wrappers are the single application point there. When cache
	// is enabled the cache.Handler owns the origin-bound URI — its
	// originURI applies strip/rewrite on every miss, revalidate, and
	// bypass — so the upstream handed to it must be the bare static
	// handler. Wrapping both double-applies on every miss: a
	// non-idempotent pattern (/x/ -> /y/ on /x/x/f) silently resolved to
	// /y/y/f. The same restructure fixes strip_prefix, which carried the
	// identical double-strip on cached static routes (a /api prefix
	// stripped /api/api/f down to /f).
	var handler fasthttp.RequestHandler = sh.ServeRequest
	// cacheFP holds the route's fast-path handler when the static route
	// is cache-enabled; nil (no fast path) otherwise.
	var cacheFP *cache.FastPathHandler

	cacheEnabled := rc.Cache.Enabled != nil && *rc.Cache.Enabled
	if !cacheEnabled {
		if rc.Request.StripPrefix != "" {
			handler = stripPrefixFastHTTP(rc.Request.StripPrefix, handler)
		}
		if rw := buildPathRewrite(rc); rw != nil {
			handler = pathRewriteFastHTTP(rw, handler)
		}
	}

	if cacheEnabled {
		rrc := &rr.Cache
		cfg := cache.HandlerConfig{
			Upstream:                handler,
			StripPrefix:             rc.Request.StripPrefix,
			PathRewrite:             buildPathRewrite(rc),
			RequestHeaderSet:        rc.Request.HeaderSet,
			RequestHeaderRemove:     rc.Request.HeaderRemove,
			ResponseHeaderSet:       rc.Response.HeaderSet,
			ResponseHeaderRemove:    rc.Response.HeaderRemove,
			Store:                   rs.store,
			Logger:                  e.logger,
			Negative:                rrc.NegativeTTL.Policy(),
			JitterPercent:           rrc.JitterPercent,
			StayinAlive:             rrc.StayinAlive,
			LogCacheKeys:            true,
			DefaultTTL:              rrc.TTLDefault,
			OverrideTTL:             rrc.TTLOverride,
			DefaultSWR:              rrc.StaleWhileRevalidate,
			DefaultSIE:              rrc.StaleIfError,
			MaxObjectSize:           rrc.MaxObjectSize.Bytes(),
			MaxResponseBytes:        rrc.MaxResponseBytes.Bytes(),
			MaxFetchConcurrency:     rrc.MaxFetchConcurrency,
			FetchTimeout:            rrc.FetchTimeout,
			FetchWaitTimeout:        rrc.FetchWaitTimeout,
			MaxStreamingBufferBytes: rrc.MaxStreamingBufferBytes.Bytes(),
			Policy:                  buildKeyPolicy(rc.Cache.Key),
			VaryCapHits:             rs.dpMetrics.VaryCapHits,
			StreamingBufferBytes:    rs.dpMetrics.StreamingBufferBytes,
			StreamingFallback:       rs.dpMetrics.StreamingFallbackTotal,
			FetchShed:               rs.dpMetrics.FetchShedTotal,
			RewarmFill:              rs.dpMetrics.RewarmFillTotal,
		}
		applyRefreshConfig(&cfg, rrc)
		if ownerFn, peerFetchFn := clusterFastPathClosures(e, rs); ownerFn != nil && peerFetchFn != nil {
			cfg.OwnerFn = ownerFn
			cfg.PeerFetch = peerFetchFn
			cfg.OnPeerVariantMismatch = func() {
				rs.clusterMetrics.IncPeerFetchVariantMismatch("consumer")
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
		// Cache-enabled static routes register the per-route fast path
		// like proxied ones. Pool-less routes leave resp.Pool empty and
		// the metrics hook attributes them to "_default" — matching the
		// slow path's middleware behavior for pool-less routes.
		cacheFP = buildRouteFP(cached)
	}

	// When cache is not enabled, wire the staticfile handler's native
	// fasthttp ServeRequest method directly — no adaptor needed. Header
	// rewrites still apply: they wrap the handler the same way. Path
	// rewrites already landed in the handler chain above.
	if !cacheEnabled {
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Pool, rc.Match.Methods,
			wrapStaticRewrites(rc, handler), nil)
		return
	}

	router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Pool, rc.Match.Methods, handler, cacheFP)
}

// clusterFastPathClosures builds the ownerFn/peerFetch closures shared
// by the slow-path HandlerConfig and the fast-path handler (strong mode
// only). Reused so both paths hit the same ring and fetcher; nil when
// clustering is not in strong mode.
func clusterFastPathClosures(e *engine, rs *runState) (func(key api.Key) (owner api.PeerInfo, isLocal bool), func(ctx context.Context, peer api.PeerInfo, key api.Key, varyKey string) (*api.Object, error)) {
	if rs.clusterNode == nil || rs.peerFetcher == nil || e.cfg.Cluster.Mode != config.ClusterModeStrong {
		return nil, nil
	}
	ownerFn := func(key api.Key) (api.PeerInfo, bool) {
		owner := rs.clusterNode.Owner(key)
		if owner.Name == "" {
			return api.PeerInfo{}, true
		}
		return owner, rs.clusterNode.IsLocal(key)
	}
	peerFetch := func(ctx context.Context, peer api.PeerInfo, key api.Key, varyKey string) (*api.Object, error) {
		return rs.peerFetcher.Fetch(ctx, peer, api.PeerFetchRequest{Key: key, VaryKey: varyKey})
	}
	return ownerFn, peerFetch
}

// staticHeaderRewriter applies a route's header rewrite directives around
// a non-cached static handler. The cache.Handler implements the same
// semantics for cached routes; this keeps the two surfaces honest.
type staticHeaderRewriter struct {
	reqSet     map[string]string
	reqRemove  map[string]bool // lower-cased names
	respSet    map[string]string
	respRemove map[string]bool // canonical names
}

// wrapStaticRewrites wraps a non-cached static handler with the route's
// header rewrite directives when any are configured; otherwise it
// returns the handler unwrapped. The cache.Handler implements the same
// semantics for cached routes.
func wrapStaticRewrites(rc config.Route, next fasthttp.RequestHandler) fasthttp.RequestHandler {
	if len(rc.Request.HeaderSet) == 0 && len(rc.Request.HeaderRemove) == 0 &&
		len(rc.Response.HeaderSet) == 0 && len(rc.Response.HeaderRemove) == 0 {
		return next
	}
	rew := &staticHeaderRewriter{
		reqSet:     rc.Request.HeaderSet,
		reqRemove:  lowerRemoveList(rc.Request.HeaderRemove),
		respSet:    rc.Response.HeaderSet,
		respRemove: canonicalizeRemoveList(rc.Response.HeaderRemove),
	}
	return rew.wrap(next)
}

// canonicalizeRemoveList interns the response-side remove names so
// fasthttp's ResponseHeader.Del does canonical comparisons with
// pre-canonicalized keys.
func canonicalizeRemoveList(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[header.InternKey(n)] = true
	}
	return m
}

// lowerRemoveList lower-cases the request-side remove names for
// case-insensitive RequestHeader.Del matching.
func lowerRemoveList(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[strings.ToLower(n)] = true
	}
	return m
}

func (r *staticHeaderRewriter) wrap(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		for name := range r.reqRemove {
			ctx.Request.Header.Del(name)
		}
		for k, v := range r.reqSet {
			ctx.Request.Header.Set(k, v)
		}
		next(ctx)
		for name := range r.respRemove {
			ctx.Response.Header.Del(name)
		}
		for k, v := range r.respSet {
			ctx.Response.Header.Set(k, v)
		}
	}
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
		rk.IncludeHeaders,
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

// hasKeyPolicy checks the query/header fields only.
func hasKeyPolicy(rk config.RouteKey) bool {
	return len(rk.StripQueryParams) > 0 || len(rk.ExcludeHeaders) > 0 ||
		len(rk.IncludeHeaders) > 0 ||
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
// the handler config from the resolved route cache (the refresh margin
// is precomputed there). Called only when RefreshBeforeExpiry is true.
func applyRefreshConfig(cfg *cache.HandlerConfig, rrc *config.ResolvedRouteCache) {
	if !rrc.RefreshBeforeExpiry {
		return
	}
	cfg.RefreshMargin = rrc.RefreshMargin
	cfg.RefreshTimeout = rrc.RefreshTimeout
	cfg.RefreshConcurrency = rrc.RefreshConcurrency
	cfg.RefreshMinHits = rrc.RefreshMinHits
	cfg.RefreshPersistCycles = rrc.RefreshPersistCycles
	cfg.RefreshMinScore = rrc.RefreshMinScore
	cfg.RefreshMaxRPS = rrc.RefreshMaxRPS
	cfg.RefreshReactiveFirst = rrc.RefreshReactiveFirst
}

// resolvedConfig returns the materialized effective configuration,
// computing it lazily for engines constructed without one (tests).
func (e *engine) resolvedConfig() *config.Resolved {
	if e.resolved == nil {
		e.resolved = e.cfg.Resolve()
	}
	return e.resolved
}
