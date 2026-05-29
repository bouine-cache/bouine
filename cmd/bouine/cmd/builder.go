package cmd

// builder.go contains the wiring helpers that construct bouine subsystems
// from configuration: buildStore, buildCluster, buildHandler, buildPools,
// buildRouter, and the listenPort utility. Lifecycle orchestration (run,
// startAdmin, startListeners, etc.) remains in engine.go.

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/thylong/bouine/internal/cache"
	"github.com/thylong/bouine/internal/cluster"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/accesslog"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/internal/pipeline"
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
	clusterNode *cluster.Cluster,
	peerFetcher *cluster.PeerFetcher,
) http.Handler {
	router := e.buildRouter(pools, store, dpMetrics, clusterNode, peerFetcher)
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

func (e *engine) buildRouter(pools map[string]*origin.Pool, store storage.Store, dpMetrics *observability.DataPlaneMetrics, clusterNode *cluster.Cluster, peerFetcher *cluster.PeerFetcher) *pipeline.Router {
	router := pipeline.NewRouter(pipeline.RouterConfig{Logger: e.logger})
	for _, rc := range e.cfg.Routes {
		p := pools[rc.Pool]
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
		cfg := cache.HandlerConfig{
			Upstream:      upstream,
			Store:         store,
			Logger:        e.logger,
			NegativeTTL:   rc.Cache.NegativeTTL,
			JitterPercent: rc.Cache.JitterPercent,
			StayinAlive:   rc.Cache.StayinAlive,
			VaryCapHits:   dpMetrics.VaryCapHits,
		}
		// Wire cluster peer-fetch if enabled. Layer rule: builder.go is L8
		// and may import both L4 (cache) and L6 (cluster); the cache Handler
		// never imports cluster directly.
		if clusterNode != nil && peerFetcher != nil {
			cfg.OwnerFn = func(key api.Key) (api.PeerInfo, bool) {
				owner := clusterNode.Owner(key)
				return owner, clusterNode.IsLocal(key)
			}
			cfg.PeerFetch = func(ctx context.Context, peer api.PeerInfo, key api.Key) (*api.Object, error) {
				return peerFetcher.Fetch(ctx, peer, api.PeerFetchRequest{Key: key})
			}
		}
		cached := cache.NewHandler(cfg)
		router.AddRoute(rc.Match.Host, rc.Match.PathPrefix, rc.Name, cached)
	}
	return router
}
