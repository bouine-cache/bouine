package cmd

// builder.go contains the wiring helpers that construct bouine subsystems
// from configuration: buildStore, buildCluster, buildHandler, buildPools,
// buildRouter, and the listenPort utility. Lifecycle orchestration (run,
// startAdmin, startListeners, etc.) remains in engine.go.

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/thylong/bouine/internal/cache"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability/accesslog"
	"github.com/thylong/bouine/internal/observability/tracing"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/server"
	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/internal/storage/warm"
	"github.com/thylong/bouine/pkg/api"
)

// buildStore creates a TieredStore (hot + warm + WAL) when WarmDir is
// configured, or a plain HotStore for ephemeral/dev deployments.
func (e *engine) buildStore() (storage.Store, error) {
	hotCfg := storage.HotConfig{MaxBytes: e.cfg.Storage.HotMaxBytes.Bytes()}
	if e.cfg.Storage.WarmDir == "" {
		return storage.NewHotStore(hotCfg), nil
	}
	return storage.NewTieredStore(storage.TieredConfig{
		Hot: hotCfg,
		Warm: &warm.Config{
			Dir:      e.cfg.Storage.WarmDir,
			MaxBytes: e.cfg.Storage.WarmMaxBytes.Bytes(),
		},
		WALDir: e.cfg.Storage.WarmDir + "/bouine.wal",
		Logger: e.logger,
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
		NodeName:      hostname,
		BindAddr:      e.cfg.Listen.Cluster,
		AdvertiseAddr: advertiseAddr,
		Join:          e.cfg.Cluster.Join,
		PeerInfo:      peerInfo,
		Logger:        e.logger,
		Mode:          e.cfg.Cluster.Mode,
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

// buildHandler assembles the full L1–L8 data-plane stack for a single HTTP
// listener. It delegates per-route cache and origin wiring to buildRouter, then
// wraps the result with three middleware layers in order:
//
//  1. DataPlaneMetrics.Middleware — Prometheus counters and histograms (L2).
//  2. tracing.HTTPMiddleware       — OpenTelemetry span for the pipeline layer.
//  3. accesslog.Middleware         — structured JSON access log entry per request.
func (e *engine) buildHandler(rs *runState) http.Handler {
	router := e.buildRouter(rs)
	metricsWrapped := rs.dpMetrics.Middleware(router)
	tracedL2 := tracing.HTTPMiddleware("bouine.pipeline", metricsWrapped)
	return accesslog.Middleware(e.logger, tracedL2)
}

// buildPools constructs one origin.Pool per upstream_pools entry in the config.
// Each pool holds the target addresses and passive health state for a named
// upstream. Pools are keyed by name and passed to buildRouter so each route can
// reference its upstream by the name declared in config.
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
//   - In full mode, ReplicateFn is set so after every cacheable fill the object
//     is broadcast to all peers via gossip.
//   - In eventual mode neither is set; every node caches independently.
func (e *engine) buildRouter(rs *runState) *server.Router {
	router := server.NewRouter(server.RouterConfig{Logger: e.logger})
	for _, rc := range e.cfg.Routes {
		p := rs.pools[rc.Pool]
		if p == nil {
			continue
		}
		consecutive5xx := 0
		var transport http.RoundTripper
		for _, pc := range e.cfg.UpstreamPools {
			if pc.Name != rc.Pool {
				continue
			}
			consecutive5xx = pc.Health.Passive.Consecutive5xx
			dialTimeout := pc.Connect.Timeout
			if dialTimeout <= 0 {
				dialTimeout = 10 * time.Second
			}
			keepAlive := pc.Connect.KeepAlive
			if keepAlive <= 0 {
				keepAlive = 30 * time.Second
			}
			base := &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   dialTimeout,
					KeepAlive: keepAlive,
				}).DialContext,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   true,
			}
			if pc.Connect.HedgeTimeout > 0 {
				transport = &origin.HedgedTransport{Inner: base, Timeout: pc.Connect.HedgeTimeout}
			} else {
				transport = base
			}
			break
		}
		upstream := p.Handler(consecutive5xx, transport)
		if rc.Request.StripPrefix != "" {
			upstream = stripPrefixHandler(rc.Request.StripPrefix, upstream)
		}
		cfg := cache.HandlerConfig{
			Upstream:            upstream,
			Store:               rs.store,
			Logger:              e.logger,
			NegativeTTL:         rc.Cache.NegativeTTL,
			JitterPercent:       rc.Cache.JitterPercent,
			StayinAlive:         rc.Cache.StayinAlive,
			DefaultTTL:          rc.Cache.TTLDefault,
			OverrideTTL:         rc.Cache.TTLOverride,
			DefaultSWR:          rc.Cache.StaleWhileRevalidate,
			DefaultSIE:          rc.Cache.StaleIfError,
			AllowSetCookie:      rc.Cache.AllowSetCookie != nil && *rc.Cache.AllowSetCookie,
			MaxObjectSize:       rc.Cache.MaxObjectSize.Bytes(),
			MaxResponseBytes:    rc.Cache.MaxResponseBytes.Bytes(),
			MaxFetchConcurrency: rc.Cache.MaxFetchConcurrency,
			StripQueryParams:    buildStripSet(rc.Cache.Key.StripQueryParams),
			ExcludeHeaders:      buildExcludeHeaderSet(rc.Cache.Key.ExcludeHeaders),
			VaryCapHits:         rs.dpMetrics.VaryCapHits,
		}
		if rs.clusterNode != nil && rs.peerFetcher != nil && e.cfg.Cluster.Mode == config.ClusterModeStrong {
			cfg.OwnerFn = func(key api.Key) (api.PeerInfo, bool) {
				owner := rs.clusterNode.Owner(key)
				return owner, rs.clusterNode.IsLocal(key)
			}
			cfg.PeerFetch = func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error) {
				return rs.peerFetcher.Fetch(ctx, peer, api.PeerFetchRequest{Key: key})
			}
		}
		if rs.broadcaster != nil && e.cfg.Cluster.Mode == config.ClusterModeFull {
			cfg.ReplicateFn = rs.broadcaster.BroadcastReplicate
		}
		cached := cache.NewHandler(cfg)
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, rc.Match.Methods, cached)
	}
	return router
}

// stripPrefixHandler strips the given prefix from r.URL.Path before
// forwarding to next. A shallow copy of *http.Request and *url.URL is
// made so the original path (used by the cache key builder) is preserved.
// Zero cost on cache hits: the upstream is only invoked on misses.
func stripPrefixHandler(prefix string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, prefix)
		if p == r.URL.Path {
			next.ServeHTTP(w, r)
			return
		}
		if p == "" || p[0] != '/' {
			p = "/" + p
		}
		r2 := new(http.Request)
		*r2 = *r
		u := new(url.URL)
		*u = *r.URL
		u.Path = p
		u.RawPath = ""
		r2.URL = u
		next.ServeHTTP(w, r2)
	})
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
