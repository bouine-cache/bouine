package insights

import (
	"fmt"
	"strings"

	"github.com/thylong/bouine/internal/config"
)

func init() {
	rules = []ruleFunc{
		ruleCacheLowHitRate,
		ruleCacheDisabledHighTraffic,
		ruleCacheNoTTL,
		ruleCacheNoSWR,
		ruleCacheNoSIE,
		ruleCacheNoNegTTL,
		ruleCacheWarmNearFull,
		ruleCacheStayinAliveOff,
		ruleCacheVaryExplosion,
		ruleAnomalyBypassFlood,
		ruleUpstreamUnhealthyTarget,
		ruleUpstreamHigh5xx,
		ruleUpstreamNoHealthCheck,
		ruleUpstreamNoHedge,
		ruleUpstreamNoCacheControl,
		ruleUpstreamNoETag,
		ruleUpstreamNoSurrogateKey,
		ruleCDNNotConfigured,
		ruleCDNAsyncLatency,
		ruleClusterHopLimitNoEffect,
		ruleClusterPeerStale,
		ruleClusterFullModeMemory,
		ruleConfigKeyQueryParams,
		ruleConfigAllowSetCookie,
		ruleConfigJitterZero,
	}
}

func requestsPerMin(sparkline []int64) float64 {
	if len(sparkline) == 0 {
		return 0
	}
	var sum int64
	for _, v := range sparkline {
		sum += v
	}
	return float64(sum) / float64(len(sparkline))
}

func routeNameToConfig(data InsightData, routeName string) *config.Route {
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if r.Name == routeName {
			return r
		}
	}
	return nil
}

func isCacheEnabled(r *config.Route) bool {
	if r.Cache.Enabled == nil {
		return true
	}
	return *r.Cache.Enabled
}

// ── Cache insights ───────────────────────────────────────────────────

func ruleCacheLowHitRate(data InsightData) *Insight {
	var triggered []string
	var worstHitPct float64
	var worstRoute string
	var worstRequests int64
	for _, rs := range data.RouteStats {
		if rs.Requests < 100 {
			continue
		}
		if rs.HitPct < 70 {
			triggered = append(triggered, rs.Route)
			if worstRoute == "" || rs.HitPct < worstHitPct {
				worstHitPct = rs.HitPct
				worstRoute = rs.Route
				worstRequests = rs.Requests
			}
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	sev := SeverityLow
	if worstHitPct < 30 {
		sev = SeverityHigh
	} else if worstHitPct < 50 {
		sev = SeverityMed
	}
	return &Insight{
		ID:       "cache-low-hitrate",
		Severity: sev,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has %.0f%% hit rate", worstRoute, worstHitPct),
		Detail:   fmt.Sprintf("%d route(s) have hit rates below 70%%", len(triggered)),
		Evidence: fmt.Sprintf("hit%%: %.1f, requests: %d", worstHitPct, worstRequests),
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/routes",
	}
}

func ruleCacheDisabledHighTraffic(data InsightData) *Insight {
	for _, rs := range data.RouteStats {
		rpm := requestsPerMin(rs.Sparkline)
		if rpm < 100 {
			continue
		}
		r := routeNameToConfig(data, rs.Route)
		if r != nil && !isCacheEnabled(r) {
			sev := SeverityMed
			if rpm > 500 {
				sev = SeverityHigh
			}
			return &Insight{
				ID:       "cache-disabled-high-traffic",
				Severity: sev,
				Category: CategoryCache,
				Title:    fmt.Sprintf("Cache disabled on high-traffic route %s", rs.Route),
				Detail:   fmt.Sprintf("Route receives %.0f req/min but caching is disabled", rpm),
				Evidence: fmt.Sprintf("req/min: %.0f", rpm),
				Routes:   []string{rs.Route},
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleCacheNoTTL(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && r.Cache.TTLDefault == 0 {
			triggered = append(triggered, r.Name)
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-no-ttl",
		Severity: SeverityMed,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has no default TTL", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) have no TTL — relying on origin Cache-Control only", len(triggered)),
		Evidence: "TTLDefault == 0",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleCacheNoSWR(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && r.Cache.StaleWhileRevalidate == 0 {
			for _, rs := range data.RouteStats {
				if rs.Route == r.Name && requestsPerMin(rs.Sparkline) > 50 {
					triggered = append(triggered, r.Name)
					break
				}
			}
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-no-swr",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has no stale-while-revalidate", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) have no SWR — origin gets revalidation traffic", len(triggered)),
		Evidence: "SWR == 0",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleCacheNoSIE(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && r.Cache.StaleIfError == 0 {
			triggered = append(triggered, r.Name)
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-no-sie",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has no stale-if-error", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) have no SIE — origin errors cause client errors", len(triggered)),
		Evidence: "SIE == 0",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleCacheNoNegTTL(data InsightData) *Insight {
	var triggered []string
	for _, rs := range data.RouteStats {
		if rs.Errors == 0 {
			continue
		}
		r := routeNameToConfig(data, rs.Route)
		if r != nil && isCacheEnabled(r) && r.Cache.NegativeTTL == 0 {
			triggered = append(triggered, rs.Route)
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-no-negttl",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has no negative TTL", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) have no NegativeTTL — 404/503 responses not cached", len(triggered)),
		Evidence: "NegativeTTL == 0",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleCacheWarmNearFull(data InsightData) *Insight {
	maxBytes := int64(data.Config.Storage.WarmMaxBytes)
	if maxBytes <= 0 {
		return nil
	}
	pct := float64(data.StoreStats.WarmBytes) / float64(maxBytes) * 100
	if pct < 90 {
		return nil
	}
	sev := SeverityMed
	if pct > 95 {
		sev = SeverityHigh
	}
	return &Insight{
		ID:       "cache-warm-near-full",
		Severity: sev,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Warm tier at %.0f%% capacity", pct),
		Detail:   "Consider increasing warm_max_bytes or adding eviction pressure",
		Evidence: fmt.Sprintf("%d / %d bytes", data.StoreStats.WarmBytes, maxBytes),
		Action:   "/dashboard/config",
	}
}

func ruleCacheStayinAliveOff(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && !r.Cache.StayinAlive {
			for _, rs := range data.RouteStats {
				if rs.Route == r.Name && rs.Requests > 100 {
					triggered = append(triggered, r.Name)
					break
				}
			}
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-stayinalive-off",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has StayinAlive=false", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) with StayinAlive off — origin receives revalidation traffic", len(triggered)),
		Evidence: "StayinAlive == false",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleCacheVaryExplosion(data InsightData) *Insight {
	if data.VaryCapHits <= 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-vary-explosion",
		Severity: SeverityMed,
		Category: CategoryAnomaly,
		Title:    fmt.Sprintf("Vary cap hit %d times — too many variants", data.VaryCapHits),
		Detail:   "A route is generating too many Vary variants, exceeding MaxVariants. Check Vary header and key normalization.",
		Evidence: fmt.Sprintf("vary_cap_hits_total: %d", data.VaryCapHits),
		Action:   "/dashboard/routes",
	}
}

// ── Anomaly insights ─────────────────────────────────────────────────

func ruleAnomalyBypassFlood(data InsightData) *Insight {
	for _, b := range data.RequestBuckets {
		if b.Requests < 50 {
			continue
		}
		bypassPct := float64(b.Bypasses) / float64(b.Requests) * 100
		if bypassPct > 50 {
			return &Insight{
				ID:       "anomaly-bypass-flood",
				Severity: SeverityMed,
				Category: CategoryAnomaly,
				Title:    fmt.Sprintf("%.0f%% bypass rate — cache not engaging", bypassPct),
				Detail:   "More than half of requests are bypassing the cache. Check route cache config and Cache-Control headers.",
				Evidence: fmt.Sprintf("bypasses: %d / requests: %d", b.Bypasses, b.Requests),
				Action:   "/dashboard/routes",
			}
		}
	}
	return nil
}

// ── Upstream insights ────────────────────────────────────────────────

func ruleUpstreamUnhealthyTarget(data InsightData) *Insight {
	for pool, targets := range data.PoolHealth {
		unhealthy := 0
		for _, t := range targets {
			if !t.Healthy {
				unhealthy++
			}
		}
		if unhealthy > 0 {
			sev := SeverityMed
			if unhealthy == len(targets) {
				sev = SeverityHigh
			}
			return &Insight{
				ID:       "upstream-unhealthy-target",
				Severity: sev,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s has %d/%d targets unhealthy", pool, unhealthy, len(targets)),
				Detail:   "One or more upstream targets are not receiving traffic",
				Evidence: fmt.Sprintf("pool: %s, unhealthy: %d/%d", pool, unhealthy, len(targets)),
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleUpstreamHigh5xx(data InsightData) *Insight {
	for _, rs := range data.RouteStats {
		if rs.Requests < 100 {
			continue
		}
		errPct := float64(rs.Errors) / float64(rs.Requests) * 100
		if errPct > 5 {
			sev := SeverityMed
			if errPct > 10 {
				sev = SeverityHigh
			}
			r := routeNameToConfig(data, rs.Route)
			poolName := "unknown"
			if r != nil {
				poolName = r.Pool
			}
			return &Insight{
				ID:       "upstream-high-5xx",
				Severity: sev,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s returning %.1f%% 5xx errors", poolName, errPct),
				Detail:   fmt.Sprintf("Route %s has high error rate from upstream", rs.Route),
				Evidence: fmt.Sprintf("errors: %d / requests: %d", rs.Errors, rs.Requests),
				Routes:   []string{rs.Route},
				Action:   "/dashboard/routes",
			}
		}
	}
	return nil
}

func ruleUpstreamNoHealthCheck(data InsightData) *Insight {
	for _, pool := range data.Config.UpstreamPools {
		if pool.Health.Active.Path == "" {
			return &Insight{
				ID:       "upstream-no-health-check",
				Severity: SeverityMed,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s has no active health check", pool.Name),
				Detail:   "Without active health checks, unhealthy targets are only detected by passive ejection",
				Evidence: "ActiveHealthCheck.Path == \"\"",
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleUpstreamNoHedge(data InsightData) *Insight {
	poolP99 := make(map[string]int64)
	for _, rs := range data.RouteStats {
		r := routeNameToConfig(data, rs.Route)
		if r == nil {
			continue
		}
		if rs.P99MS > poolP99[r.Pool] {
			poolP99[r.Pool] = rs.P99MS
		}
	}
	for _, pool := range data.Config.UpstreamPools {
		p99 := poolP99[pool.Name]
		if p99 > 500 && pool.Connect.HedgeTimeout == 0 {
			return &Insight{
				ID:       "upstream-no-hedge",
				Severity: SeverityLow,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s p99=%dms but no hedge timeout", pool.Name, p99),
				Detail:   "Hedged requests can mask tail latency for idempotent methods",
				Evidence: fmt.Sprintf("p99: %dms, HedgeTimeout: 0", p99),
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleUpstreamNoCacheControl(data InsightData) *Insight {
	for pool, audit := range data.HeaderAudit {
		if audit.SampleCount < 50 {
			continue
		}
		missingPct := 100 - audit.HasCacheControlPct
		if missingPct > 20 {
			sev := SeverityLow
			if missingPct > 50 {
				sev = SeverityMed
			}
			return &Insight{
				ID:       "upstream-no-cache-control",
				Severity: sev,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Upstream %s missing Cache-Control on %.0f%% of responses", pool, missingPct),
				Detail:   "Without Cache-Control, bouine relies on route TTL defaults. Origin should emit explicit caching directives.",
				Evidence: fmt.Sprintf("samples: %d, missing CC: %.1f%%", audit.SampleCount, missingPct),
				Action:   "/dashboard/routes",
			}
		}
	}
	return nil
}

func ruleUpstreamNoETag(data InsightData) *Insight {
	for pool, audit := range data.HeaderAudit {
		if audit.SampleCount < 50 {
			continue
		}
		if audit.HasETagPct < 20 {
			return &Insight{
				ID:       "upstream-no-etag",
				Severity: SeverityLow,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Upstream %s not emitting ETag", pool),
				Detail:   "Without ETag, revalidation is impossible — bouine cannot serve stale-while-revalidate efficiently",
				Evidence: fmt.Sprintf("samples: %d, has ETag: %.1f%%", audit.SampleCount, audit.HasETagPct),
				Action:   "/dashboard/routes",
			}
		}
	}
	return nil
}

func ruleUpstreamNoSurrogateKey(data InsightData) *Insight {
	for pool, audit := range data.HeaderAudit {
		if audit.SampleCount < 50 {
			continue
		}
		if audit.HasSurrogateKeyPct < 20 {
			return &Insight{
				ID:       "upstream-no-surrogate-key",
				Severity: SeverityLow,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Upstream %s not emitting Surrogate-Key", pool),
				Detail:   "Without Surrogate-Key, granular invalidation by tag is impossible — only URL/prefix purges work",
				Evidence: fmt.Sprintf("samples: %d, has SK: %.1f%%", audit.SampleCount, audit.HasSurrogateKeyPct),
				Action:   "/dashboard/invalidation",
			}
		}
	}
	return nil
}

// ── CDN insights ─────────────────────────────────────────────────────

func ruleCDNNotConfigured(data InsightData) *Insight {
	if data.CFStatus.Enabled {
		return nil
	}
	return &Insight{
		ID:       "cdn-not-configured",
		Severity: SeverityLow,
		Category: CategoryCDN,
		Title:    "Cloudflare CDN propagation not configured",
		Detail:   "Configure cloudflare.zone_id and CF_API_TOKEN to propagate invalidations to the CDN edge",
		Evidence: "CFStatus.Enabled == false",
		Action:   "/dashboard/config",
	}
}

func ruleCDNAsyncLatency(data InsightData) *Insight {
	if !data.CFStatus.Enabled || !data.CFStatus.Async || data.CFStatus.LastLagMs <= 0 {
		return nil
	}
	return &Insight{
		ID:       "cdn-async-latency",
		Severity: SeverityLow,
		Category: CategoryCDN,
		Title:    fmt.Sprintf("CF propagation is async — last purge lagged %dms", data.CFStatus.LastLagMs),
		Detail:   "Async propagation fires after the admin response returns. Invalidations may lag behind local cache state.",
		Evidence: fmt.Sprintf("last_lag_ms: %d", data.CFStatus.LastLagMs),
		Action:   "/dashboard/invalidation",
	}
}

// ── Cluster insights ─────────────────────────────────────────────────

func ruleClusterHopLimitNoEffect(data InsightData) *Insight {
	if data.Config.Cluster.HopLimit > 0 && data.Config.Cluster.Mode != config.ClusterModeStrong {
		return &Insight{
			ID:       "cluster-hoplimit-no-effect",
			Severity: SeverityMed,
			Category: CategoryCluster,
			Title:    fmt.Sprintf("hop_limit=%d has no effect in '%s' mode", data.Config.Cluster.HopLimit, data.Config.Cluster.Mode),
			Detail:   "Hop limits only apply to strong consistency mode. In other modes, peer fetch is not hop-limited.",
			Evidence: fmt.Sprintf("hop_limit: %d, mode: %s", data.Config.Cluster.HopLimit, data.Config.Cluster.Mode),
			Action:   "/dashboard/config",
		}
	}
	return nil
}

func ruleClusterPeerStale(data InsightData) *Insight {
	for _, p := range data.PeerResults {
		if p.Stale {
			return &Insight{
				ID:       "cluster-peer-stale",
				Severity: SeverityMed,
				Category: CategoryCluster,
				Title:    fmt.Sprintf("Peer %s is stale (unreachable)", p.Name),
				Detail:   "The peer is not responding to dashboard metrics fan-out. Last known data is being shown.",
				Evidence: fmt.Sprintf("peer: %s, stale: true", p.Name),
				Action:   "/dashboard/cluster",
			}
		}
	}
	return nil
}

func ruleClusterFullModeMemory(data InsightData) *Insight {
	if data.Config.Cluster.Mode == config.ClusterModeFull {
		return &Insight{
			ID:       "cluster-full-mode-memory",
			Severity: SeverityLow,
			Category: CategoryCluster,
			Title:    "Cluster mode 'full': memory scales linearly with cluster size",
			Detail:   "Full replication mode copies all cache entries to every peer. Memory usage grows proportionally to cluster size × cache size.",
			Evidence: "mode: full",
			Action:   "/dashboard/config",
		}
	}
	return nil
}

// ── Config insights ──────────────────────────────────────────────────

func ruleConfigKeyQueryParams(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if len(r.Cache.Key.StripQueryParams) == 0 {
			path := r.Match.PathPrefix
			if strings.Contains(strings.ToLower(path), "search") || strings.Contains(strings.ToLower(path), "query") {
				triggered = append(triggered, r.Name)
			}
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "config-key-query-params",
		Severity: SeverityLow,
		Category: CategoryConfig,
		Title:    fmt.Sprintf("Route %s includes all query params in cache key", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) with search-like paths do not strip query params — each query variant creates a separate cache entry", len(triggered)),
		Evidence: "StripQueryParams is empty",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleConfigAllowSetCookie(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if r.Cache.AllowSetCookie != nil && *r.Cache.AllowSetCookie {
			triggered = append(triggered, r.Name)
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "config-allow-set-cookie",
		Severity: SeverityMed,
		Category: CategoryConfig,
		Title:    fmt.Sprintf("Route %s allows Set-Cookie in cache", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) allow Set-Cookie — may cache session-specific responses", len(triggered)),
		Evidence: "AllowSetCookie == true",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

func ruleConfigJitterZero(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && r.Cache.JitterPercent == 0 && r.Cache.TTLDefault > 0 {
			triggered = append(triggered, r.Name)
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "config-jitter-zero",
		Severity: SeverityLow,
		Category: CategoryConfig,
		Title:    fmt.Sprintf("Route %s has jitter=0", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) have no TTL jitter — synchronized expiries cause origin bursts", len(triggered)),
		Evidence: "JitterPercent == 0, TTL > 0",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

// ── Helpers ──────────────────────────────────────────────────────────

func truncateRoutes(routes []string) []string {
	if len(routes) <= 5 {
		return routes
	}
	return routes[:5]
}
