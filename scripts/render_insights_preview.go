// render_insights_preview generates a standalone HTML file of the
// insights page with mock data for visual inspection.
//
//go:build ignore

package main

import (
	"context"
	"os"

	"github.com/bouine-cache/bouine/internal/dashboard/templates"
)

func main() {
	data := templates.InsightsData{
		LayoutProps: templates.LayoutProps{
			Page:          "insights",
			PageTitle:     "Insights",
			NodeName:      "bouine-node-1",
			Version:       "dev",
			TimeRange:     "6h",
			PeerCount:     3,
			LivePeers:     3,
			SidebarReqS:   1247.3,
			SidebarHitPct: 87.2,
		},
		Nodes: []templates.ArchNode{
			{
				ID:     "client",
				Type:   "client",
				Label:  "Clients",
				Status: "healthy",
				Detail: "HTTP/1.1 + h2c + h3",
			},
			{
				ID:     "cdn",
				Type:   "cdn",
				Label:  "Cloudflare CDN",
				Status: "healthy",
				Detail: "zone abc123 · async",
			},
			{
				ID:     "bouine",
				Type:   "bouine",
				Label:  "bouine cluster",
				Status: "healthy",
				Detail: "mode: strong",
				Peers: []templates.PeerNode{
					{Name: "bouine-01", Status: "healthy"},
					{Name: "bouine-02", Status: "healthy"},
					{Name: "bouine-03", Status: "stale"},
				},
				StorageTiers: []templates.StorageTier{
					{Name: "Hot", Status: "healthy", Detail: "124 Mo"},
					{Name: "Warm", Status: "healthy", Detail: "4.2 Go"},
				},
			},
			{
				ID:     "pool:api-pool",
				Type:   "pool",
				Label:  "api",
				Status: "degraded",
				Detail: "api-pool · 2/3 targets",
			},
			{
				ID:     "pool:static-pool",
				Type:   "pool",
				Label:  "static",
				Status: "healthy",
				Detail: "static-pool · 4/4 targets",
			},
			{
				ID:     "pool:image-pool",
				Type:   "pool",
				Label:  "images",
				Status: "unhealthy",
				Detail: "image-pool · 0/2 targets",
			},
		},
		Insights: []templates.InsightCard{
			{
				ID:       "cache-low-hit-rate",
				Severity: "HIGH",
				Category: "cache",
				Title:    "Low cache hit rate on /api/v2/products",
				Detail:   "Hit rate is 23.5% over the last 6h, well below the 60% target for this route.",
				Evidence: "hit%: 23.5, requests: 18420",
				Routes:   []string{"/api/v2/products"},
				Action:   "Check upstream Cache-Control headers and TTL policy for this route",
				NodeIDs:  []string{"pool:api-pool"},
			},
			{
				ID:       "upstream-unhealthy",
				Severity: "HIGH",
				Category: "upstream",
				Title:    "All targets unhealthy in image-pool",
				Detail:   "0 of 2 targets are healthy in pool 'image-pool'. All traffic is failing.",
				Evidence: "pool: image-pool, healthy: 0/2",
				Action:   "Investigate origin server health or increase health check interval",
				NodeIDs:  []string{"pool:image-pool"},
			},
			{
				ID:       "cache-no-swr",
				Severity: "MED",
				Category: "cache",
				Title:    "stale-while-revalidate not configured",
				Detail:   "3 routes lack SWR, reducing resilience during origin slowdowns.",
				Evidence: "routes: /api/v1, /api/v2, /static",
				Routes:   []string{"/api/v1", "/api/v2", "/static"},
				Action:   "Add stale_while_revalidate to route cache policies",
				NodeIDs:  []string{"pool:api-pool", "pool:static-pool"},
			},
			{
				ID:       "upstream-no-cache-control",
				Severity: "MED",
				Category: "upstream",
				Title:    "Missing Cache-Control on api-pool responses",
				Detail:   "42% of sampled responses from api-pool lack Cache-Control headers.",
				Evidence: "pool: api-pool, 42.0% missing Cache-Control (58 samples)",
				Action:   "Ensure origin sends explicit Cache-Control for cacheable responses",
				NodeIDs:  []string{"pool:api-pool"},
			},
			{
				ID:       "cdn-async-latency",
				Severity: "MED",
				Category: "cdn",
				Title:    "Cloudflare async propagation lag elevated",
				Detail:   "Last purge propagation took 3400ms, above the 2000ms threshold.",
				Evidence: "lag: 3400ms",
				Action:   "Monitor CF API latency; consider synchronous mode for critical purges",
				NodeIDs:  []string{"cdn"},
			},
			{
				ID:       "cluster-peer-stale",
				Severity: "MED",
				Category: "cluster",
				Title:    "Peer bouine-03 is stale",
				Detail:   "Peer bouine-03 has not responded to gossip in over 30s.",
				Evidence: "peer: bouine-03, stale: true",
				Action:   "Check network connectivity or node health for bouine-03",
				NodeIDs:  []string{"bouine"},
			},
			{
				ID:       "cache-no-etag",
				Severity: "LOW",
				Category: "upstream",
				Title:    "ETag not set on static-pool responses",
				Detail:   "67% of sampled responses from static-pool lack ETag headers.",
				Evidence: "pool: static-pool, 67.0% missing ETag (45 samples)",
				Action:   "Enable ETag generation on the origin for conditional revalidation",
				NodeIDs:  []string{"pool:static-pool"},
			},
			{
				ID:       "config-jitter-zero",
				Severity: "LOW",
				Category: "config",
				Title:    "Jitter disabled on all routes",
				Detail:   "No routes have jitter_percent configured, which can cause thundering herds on TTL expiry.",
				Action:   "Set jitter_percent to 10-20% on high-traffic routes",
				NodeIDs:  []string{"bouine"},
			},
		},
		HighCount: 2,
		MedCount:  3,
		LowCount:  2,
	}

	ctx := context.Background()
	w, err := os.Create("/tmp/bouine-insights-preview.html")
	if err != nil {
		panic(err)
	}
	defer w.Close()

	if err := templates.Insights(data).Render(ctx, w); err != nil {
		panic(err)
	}

	println("Preview written to /tmp/bouine-insights-preview.html")
}
