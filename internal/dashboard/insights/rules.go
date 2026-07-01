package insights

import (
	"fmt"
	"strings"
	"time"

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
		ruleUpstreamPoolNoTraffic,
		ruleCDNNotConfigured,
		ruleCDNAsyncLatency,
		ruleClusterHopLimitNoEffect,
		ruleClusterPeerStale,
		ruleClusterFullModeMemory,
		ruleConfigKeyQueryParams,
		ruleConfigAllowSetCookie,
		ruleConfigJitterZero,
		// Tier 1: config-derived standing insights.
		ruleConfigTLSBelow12,
		ruleConfigNoOCSPStapling,
		ruleConfigTracingSamplingZero,
		ruleConfigClusterDisabledWithPeers,
		ruleConfigAntiEntropyDisabled,
		ruleConfigPoolNoTimeout,
		ruleConfigPoolNoMaxConnections,
		ruleConfigMaxObjectSizeUnset,
		ruleConfigRouteStripsCacheHeaders,
		// Tier 2: existing ring data.
		ruleCacheHighEvictionRate,
		ruleCacheStaleHitRatioHigh,
		ruleCacheSWRConfiguredButUnused,
		ruleAnomalyLatencyP99Spike,
		ruleAnomalyRevalidationStorm,
		ruleUpstreamTargetErrorStreak,
		ruleCacheHotTierCritical,
		ruleCacheWarmEntriesZero,
		// Tier 3: existing data, new plumbing.
		ruleClusterReplicationStalled,
		ruleClusterNoReplicationTraffic,
		ruleClusterBroadcastFailures,
		ruleCDNLastError,
		ruleCDNPurgeSkipped,
		ruleConfigPoolPassiveEjectForever,
		ruleClusterPeerHealthDegraded,
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

func ruleUpstreamPoolNoTraffic(data InsightData) *Insight {
	poolReqs := make(map[string]int64)
	for _, rs := range data.RouteStats {
		r := routeNameToConfig(data, rs.Route)
		if r == nil {
			continue
		}
		poolReqs[r.Pool] += rs.Requests
	}
	for _, pool := range data.Config.UpstreamPools {
		if poolReqs[pool.Name] == 0 {
			return &Insight{
				ID:       "upstream-pool-no-traffic",
				Severity: SeverityLow,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s has received no traffic", pool.Name),
				Detail:   "The upstream pool is configured but no routes have sent requests to it. Check route matching or remove the unused pool.",
				Evidence: fmt.Sprintf("pool: %s, total_requests: 0", pool.Name),
				Action:   "/dashboard/config",
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

// ── Tier 1: Config-derived standing insights ─────────────────────────

func ruleConfigTLSBelow12(data InsightData) *Insight {
	mv := data.Config.TLS.MinVersion
	if mv == "" || data.Config.Listen.HTTPS == "" {
		return nil
	}
	if mv == "1.2" || mv == "1.3" {
		return nil
	}
	return &Insight{
		ID:       "config-tls-below-12",
		Severity: SeverityMed,
		Category: CategoryConfig,
		Title:    fmt.Sprintf("TLS min version is %s (below 1.2)", mv),
		Detail:   "TLS 1.0/1.1 are deprecated and vulnerable to known attacks",
		Evidence: fmt.Sprintf("min_version: %s", mv),
		Action:   "/dashboard/config",
	}
}

func ruleConfigNoOCSPStapling(data InsightData) *Insight {
	if data.Config.Listen.HTTPS == "" {
		return nil
	}
	ocsp := data.Config.TLS.OCSPStapling
	if ocsp != "" && ocsp != "off" {
		return nil
	}
	return &Insight{
		ID:       "config-no-ocsp-stapling",
		Severity: SeverityLow,
		Category: CategoryConfig,
		Title:    "OCSP stapling not configured",
		Detail:   "Without OCSP stapling, clients must contact the CA to verify certificate revocation status",
		Evidence: fmt.Sprintf("ocsp_stapling: %q", ocsp),
		Action:   "/dashboard/config",
	}
}

func ruleConfigTracingSamplingZero(data InsightData) *Insight {
	if data.Config.Tracing.Endpoint == "" {
		return nil
	}
	if data.Config.Tracing.SamplingRate > 0 {
		return nil
	}
	return &Insight{
		ID:       "config-tracing-sampling-zero",
		Severity: SeverityLow,
		Category: CategoryConfig,
		Title:    "Tracing endpoint configured but sampling rate is 0",
		Detail:   "Traces are exported to the collector but nothing is sampled — no traces will be sent",
		Evidence: fmt.Sprintf("endpoint: %s, sampling: 0", data.Config.Tracing.Endpoint),
		Action:   "/dashboard/config",
	}
}

func ruleConfigClusterDisabledWithPeers(data InsightData) *Insight {
	if data.Config.Cluster.Enabled {
		return nil
	}
	if len(data.Config.Cluster.Join) == 0 {
		return nil
	}
	return &Insight{
		ID:       "config-cluster-disabled-with-peers",
		Severity: SeverityMed,
		Category: CategoryConfig,
		Title:    "Cluster disabled but join addresses configured",
		Detail:   fmt.Sprintf("%d join address(es) configured but cluster.enabled is false", len(data.Config.Cluster.Join)),
		Evidence: fmt.Sprintf("join: %d entries, enabled: false", len(data.Config.Cluster.Join)),
		Action:   "/dashboard/config",
	}
}

func ruleConfigAntiEntropyDisabled(data InsightData) *Insight {
	if !data.Config.Cluster.Enabled || data.Config.Cluster.Mode != config.ClusterModeFull {
		return nil
	}
	if data.Config.Cluster.AntiEntropyInterval > 0 {
		return nil
	}
	return &Insight{
		ID:       "config-anti-entropy-disabled",
		Severity: SeverityLow,
		Category: CategoryConfig,
		Title:    "Anti-entropy disabled in full mode",
		Detail:   "Full replication without anti-entropy will drift over time as replication failures accumulate",
		Evidence: "mode: full, anti_entropy_interval: 0",
		Action:   "/dashboard/config",
	}
}

func ruleConfigPoolNoTimeout(data InsightData) *Insight {
	for _, pool := range data.Config.UpstreamPools {
		if pool.Connect.Timeout == 0 {
			return &Insight{
				ID:       "config-pool-no-timeout",
				Severity: SeverityMed,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s has no connect timeout", pool.Name),
				Detail:   "Without a connect timeout, origin can hang indefinitely and exhaust the connection pool",
				Evidence: fmt.Sprintf("pool: %s, connect_timeout: 0", pool.Name),
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleConfigPoolNoMaxConnections(data InsightData) *Insight {
	for _, pool := range data.Config.UpstreamPools {
		if pool.Connect.MaxConnections == 0 {
			return &Insight{
				ID:       "config-pool-no-max-connections",
				Severity: SeverityLow,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s has no max connections limit", pool.Name),
				Detail:   "Without a connection limit, a slow origin can exhaust file descriptors",
				Evidence: fmt.Sprintf("pool: %s, max_connections: 0", pool.Name),
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleConfigMaxObjectSizeUnset(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && r.Cache.MaxObjectSize == 0 {
			triggered = append(triggered, r.Name)
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "config-max-object-size-unset",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Route %s has no max object size", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) have no MaxObjectSize — large responses can exhaust RAM", len(triggered)),
		Evidence: "MaxObjectSize == 0",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

var cachingHeaders = map[string]bool{
	"cache-control": true,
	"etag":          true,
	"last-modified": true,
	"vary":          true,
	"surrogate-key": true,
	"age":           true,
	"expires":       true,
}

func ruleConfigRouteStripsCacheHeaders(data InsightData) *Insight {
	var triggered []string
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		for _, h := range r.Response.HeaderRemove {
			if cachingHeaders[strings.ToLower(h)] {
				triggered = append(triggered, r.Name)
				break
			}
		}
	}
	if len(triggered) == 0 {
		return nil
	}
	return &Insight{
		ID:       "config-route-strips-cache-headers",
		Severity: SeverityMed,
		Category: CategoryConfig,
		Title:    fmt.Sprintf("Route %s strips caching headers", triggered[0]),
		Detail:   fmt.Sprintf("%d route(s) remove caching-relevant response headers (Cache-Control, ETag, etc.)", len(triggered)),
		Evidence: "HeaderRemove contains caching headers",
		Routes:   truncateRoutes(triggered),
		Action:   "/dashboard/config",
	}
}

// ── Tier 2: Existing ring data ────────────────────────────────────────

func ruleCacheHighEvictionRate(data InsightData) *Insight {
	if data.StoreStats.HotEntries < 100 {
		return nil
	}
	evictDelta := data.StoreStats.Evictions - data.PrevStoreStats.Evictions
	if evictDelta <= 0 {
		return nil
	}
	ratio := float64(evictDelta) / float64(data.StoreStats.HotEntries) * 100
	if ratio < 5 {
		return nil
	}
	sev := SeverityMed
	if ratio > 10 {
		sev = SeverityHigh
	}
	return &Insight{
		ID:       "cache-high-eviction-rate",
		Severity: sev,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Hot tier evicting %.1f%% of entries per cycle", ratio),
		Detail:   "High eviction rate indicates the hot tier is too small for the working set",
		Evidence: fmt.Sprintf("evictions_delta: %d, hot_entries: %d, ratio: %.1f%%", evictDelta, data.StoreStats.HotEntries, ratio),
		Action:   "/dashboard/config",
	}
}

func ruleCacheStaleHitRatioHigh(data InsightData) *Insight {
	var totalReqs, totalStale int64
	for _, b := range data.RequestBuckets {
		totalReqs += b.Requests
		totalStale += b.StaleHits
	}
	if totalReqs < 100 {
		return nil
	}
	ratio := float64(totalStale) / float64(totalReqs) * 100
	if ratio < 20 {
		return nil
	}
	return &Insight{
		ID:       "cache-stale-hit-ratio-high",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    fmt.Sprintf("%.0f%% of hits are stale (SWR)", ratio),
		Detail:   "SWR is carrying the cache — the origin may be under-provisioned or TTLs are too short",
		Evidence: fmt.Sprintf("stale_hits: %d, requests: %d, ratio: %.1f%%", totalStale, totalReqs, ratio),
		Action:   "/dashboard/routes",
	}
}

func ruleCacheSWRConfiguredButUnused(data InsightData) *Insight {
	var totalMisses, totalStale int64
	for _, b := range data.RequestBuckets {
		totalMisses += b.Misses
		totalStale += b.StaleHits
	}
	if totalStale > 0 || totalMisses < 100 {
		return nil
	}
	swrRoutes := 0
	for i := range data.Config.Routes {
		r := &data.Config.Routes[i]
		if isCacheEnabled(r) && r.Cache.StaleWhileRevalidate > 0 {
			swrRoutes++
		}
	}
	if swrRoutes == 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-swr-configured-but-unused",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    "SWR configured but never serving stale",
		Detail:   fmt.Sprintf("%d route(s) have SWR but 0 stale hits with %d misses — SWR could have helped but didn't", swrRoutes, totalMisses),
		Evidence: fmt.Sprintf("stale_hits: 0, misses: %d, swr_routes: %d", totalMisses, swrRoutes),
		Action:   "/dashboard/config",
	}
}

func ruleAnomalyLatencyP99Spike(data InsightData) *Insight {
	n := len(data.RequestBuckets)
	if n < 7 {
		return nil
	}
	current := data.RequestBuckets[n-1]
	prev := data.RequestBuckets[n-6 : n-1]
	currentP99 := current.LatHist.Percentile(0.99)
	if currentP99 == 0 {
		return nil
	}
	var prevSum int64
	count := 0
	for _, b := range prev {
		p := b.LatHist.Percentile(0.99)
		if p > 0 {
			prevSum += p
			count++
		}
	}
	if count == 0 {
		return nil
	}
	avgPrev := float64(prevSum) / float64(count)
	if avgPrev == 0 {
		return nil
	}
	ratio := float64(currentP99) / avgPrev
	if ratio < 2 {
		return nil
	}
	sev := SeverityMed
	if ratio > 3 {
		sev = SeverityHigh
	}
	return &Insight{
		ID:       "anomaly-latency-p99-spike",
		Severity: sev,
		Category: CategoryAnomaly,
		Title:    fmt.Sprintf("p99 latency spike: %dms (%.1fx above average)", currentP99, ratio),
		Detail:   "Recent p99 latency is significantly higher than the preceding window",
		Evidence: fmt.Sprintf("current_p99: %dms, avg_prev_p99: %.0fms, ratio: %.1f", currentP99, avgPrev, ratio),
		Action:   "/dashboard/performance",
	}
}

func ruleAnomalyRevalidationStorm(data InsightData) *Insight {
	n := len(data.RequestBuckets)
	if n < 7 {
		return nil
	}
	current := data.RequestBuckets[n-1].Revalidated
	if current == 0 {
		return nil
	}
	prev := data.RequestBuckets[n-6 : n-1]
	var prevSum int64
	count := 0
	for _, b := range prev {
		if b.Revalidated > 0 {
			prevSum += b.Revalidated
			count++
		}
	}
	if count == 0 {
		return nil
	}
	avgPrev := float64(prevSum) / float64(count)
	if avgPrev == 0 {
		return nil
	}
	ratio := float64(current) / avgPrev
	if ratio < 3 {
		return nil
	}
	return &Insight{
		ID:       "anomaly-revalidation-storm",
		Severity: SeverityMed,
		Category: CategoryAnomaly,
		Title:    fmt.Sprintf("Revalidation storm: %d revalidations (%.1fx above average)", current, ratio),
		Detail:   "Sudden spike in conditional requests — TTLs may have expired simultaneously",
		Evidence: fmt.Sprintf("current: %d, avg_prev: %.0f, ratio: %.1f", current, avgPrev, ratio),
		Action:   "/dashboard/routes",
	}
}

func ruleUpstreamTargetErrorStreak(data InsightData) *Insight {
	for pool, targets := range data.PoolHealth {
		for _, t := range targets {
			if t.Healthy && t.ConsecutiveErrors >= 5 {
				return &Insight{
					ID:       "upstream-target-error-streak",
					Severity: SeverityMed,
					Category: CategoryUpstream,
					Title:    fmt.Sprintf("Target %s has %d consecutive errors (still healthy)", t.Addr, t.ConsecutiveErrors),
					Detail:   fmt.Sprintf("Pool %s target is about to be passively ejected", pool),
					Evidence: fmt.Sprintf("target: %s, errors: %d, pool: %s", t.Addr, t.ConsecutiveErrors, pool),
					Action:   "/dashboard/config",
				}
			}
		}
	}
	return nil
}

func ruleCacheHotTierCritical(data InsightData) *Insight {
	hotMax := int64(data.Config.Storage.HotMaxBytes)
	if hotMax <= 0 {
		return nil
	}
	pct := float64(data.StoreStats.HotBytes) / float64(hotMax) * 100
	if pct < 95 {
		return nil
	}
	if pct > 100 {
		pct = 100
	}
	return &Insight{
		ID:       "cache-hot-tier-critical",
		Severity: SeverityHigh,
		Category: CategoryCache,
		Title:    fmt.Sprintf("Hot tier at %.0f%% capacity (critical)", pct),
		Detail:   "Hot tier is nearly full — new entries will evict existing ones immediately",
		Evidence: fmt.Sprintf("hot_bytes: %d, hot_max: %d, pct: %.1f%%", data.StoreStats.HotBytes, hotMax, pct),
		Action:   "/dashboard/config",
	}
}

func ruleCacheWarmEntriesZero(data InsightData) *Insight {
	if data.Config.Storage.WarmMaxBytes <= 0 {
		return nil
	}
	if data.StoreStats.WarmEntries > 0 {
		return nil
	}
	return &Insight{
		ID:       "cache-warm-entries-zero",
		Severity: SeverityLow,
		Category: CategoryCache,
		Title:    "Warm tier configured but empty",
		Detail:   "WarmMaxBytes is set but WarmEntries is 0 — the warm tier is not receiving objects",
		Evidence: fmt.Sprintf("warm_entries: 0, warm_max_bytes: %d", data.Config.Storage.WarmMaxBytes),
		Action:   "/dashboard/config",
	}
}

// ── Tier 3: Existing data, new plumbing ───────────────────────────────

func ruleClusterReplicationStalled(data InsightData) *Insight {
	if data.Config.Cluster.Mode != config.ClusterModeFull {
		return nil
	}
	if data.ReplicationLastRecv == 0 {
		return nil
	}
	now := time.Now().Unix()
	stalledSecs := now - data.ReplicationLastRecv
	if stalledSecs < 300 {
		return nil
	}
	return &Insight{
		ID:       "cluster-replication-stalled",
		Severity: SeverityMed,
		Category: CategoryCluster,
		Title:    fmt.Sprintf("No replication received in %dm", stalledSecs/60),
		Detail:   "Full mode expects continuous replication. Stalled receive may indicate peer disconnection or broadcast failures.",
		Evidence: fmt.Sprintf("last_recv_unix: %d, stalled_secs: %d", data.ReplicationLastRecv, stalledSecs),
		Action:   "/dashboard/cluster",
	}
}

func ruleClusterNoReplicationTraffic(data InsightData) *Insight {
	if data.Config.Cluster.Mode != config.ClusterModeFull {
		return nil
	}
	if data.ReplicationBytes > 0 {
		return nil
	}
	return &Insight{
		ID:       "cluster-no-replication-traffic",
		Severity: SeverityLow,
		Category: CategoryCluster,
		Title:    "No replication traffic since startup",
		Detail:   "Full mode is active but no replication bytes have been sent or received",
		Evidence: "replication_bytes: 0, mode: full",
		Action:   "/dashboard/cluster",
	}
}

func ruleClusterBroadcastFailures(data InsightData) *Insight {
	if data.BroadcastFailures <= 0 {
		return nil
	}
	return &Insight{
		ID:       "cluster-broadcast-failures",
		Severity: SeverityMed,
		Category: CategoryCluster,
		Title:    fmt.Sprintf("%d cluster broadcast failures", data.BroadcastFailures),
		Detail:   "Invalidation fan-out to peers is failing — some peers may not receive purges or bans",
		Evidence: fmt.Sprintf("broadcast_failures: %d", data.BroadcastFailures),
		Action:   "/dashboard/cluster",
	}
}

func ruleCDNLastError(data InsightData) *Insight {
	if !data.CFStatus.Enabled || data.CFStatus.LastError == "" {
		return nil
	}
	return &Insight{
		ID:       "cdn-last-error",
		Severity: SeverityMed,
		Category: CategoryCDN,
		Title:    fmt.Sprintf("Cloudflare last error: %s", data.CFStatus.LastError),
		Detail:   "The last CF propagation attempt failed. CDN edge may have stale content.",
		Evidence: fmt.Sprintf("last_error: %s", data.CFStatus.LastError),
		Action:   "/dashboard/invalidation",
	}
}

func ruleCDNPurgeSkipped(data InsightData) *Insight {
	if data.CFPurgeSkipped <= 0 {
		return nil
	}
	return &Insight{
		ID:       "cdn-purge-skipped",
		Severity: SeverityLow,
		Category: CategoryCDN,
		Title:    fmt.Sprintf("%d CF purges skipped", data.CFPurgeSkipped),
		Detail:   "Purges are being skipped (e.g. rate limited or batch-full). CDN edge may lag behind local cache.",
		Evidence: fmt.Sprintf("purge_skipped: %d", data.CFPurgeSkipped),
		Action:   "/dashboard/invalidation",
	}
}

func ruleConfigPoolPassiveEjectForever(data InsightData) *Insight {
	for _, pool := range data.Config.UpstreamPools {
		if pool.Health.Passive.EjectFor == 0 && pool.Health.Active.Path == "" {
			return &Insight{
				ID:       "config-pool-passive-eject-forever",
				Severity: SeverityMed,
				Category: CategoryUpstream,
				Title:    fmt.Sprintf("Pool %s: ejected targets never rejoin", pool.Name),
				Detail:   "PassiveEjectFor is 0 and no active health check is configured — passively ejected targets are gone forever",
				Evidence: fmt.Sprintf("pool: %s, eject_for: 0, active_check: none", pool.Name),
				Action:   "/dashboard/config",
			}
		}
	}
	return nil
}

func ruleClusterPeerHealthDegraded(data InsightData) *Insight {
	for peer, uptime := range data.PeerHealth {
		if uptime < 90 {
			return &Insight{
				ID:       "cluster-peer-health-degraded",
				Severity: SeverityLow,
				Category: CategoryCluster,
				Title:    fmt.Sprintf("Peer %s uptime %.0f%%", peer, uptime),
				Detail:   "Peer uptime is below 90% over the 30-minute window — check network or node health",
				Evidence: fmt.Sprintf("peer: %s, uptime: %.1f%%", peer, uptime),
				Action:   "/dashboard/cluster",
			}
		}
	}
	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────

func truncateRoutes(routes []string) []string {
	if len(routes) <= 5 {
		return routes
	}
	return routes[:5]
}
