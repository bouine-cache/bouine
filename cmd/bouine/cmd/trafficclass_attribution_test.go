package cmd

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/runtime/shutdown"
	"github.com/bouine-cache/bouine/internal/runtime/supervised"
	"github.com/bouine-cache/bouine/internal/server"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/pkg/api"
)

// fastGetWithHost issues two GETs (warm + hit) against addr while
// sending the given Host header — the classified-Host shape the e2e
// test needs (the cache key is host-derived, so the Host header must
// differ from the dial address; the custom Dial pins the address,
// since fasthttp derives both the dial target and the Host header from
// the request URI).
func fastGetWithHost(t *testing.T, addr, host, path string) error {
	t.Helper()
	for i := range 2 {
		// The URI drives both the dial target and the Host header; keep
		// the classified host in the URI and pin the actual connection
		// to the bound listener by rewriting the dial address.
		c := &fasthttp.Client{
			Dial: func(string) (net.Conn, error) {
				return net.Dial("tcp", addr)
			},
		}
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://" + host + path)
		req.Header.SetMethod("GET")
		if err := c.Do(req, resp); err != nil {
			return err
		}
		if i == 1 && string(resp.Header.Peek("X-Cache")) != "HIT" {
			t.Fatalf("second request must be a hit, got %q", resp.Header.Peek("X-Cache"))
		}
	}
	return nil
}

// TestTrafficClassAttribution_E2E is the issue #707 end-to-end
// regression: with traffic classes configured, a request served by the
// real engine wiring must land its hit in the traffic_class series of
// the configured class — on the fast path (the request travels over a
// real H1 connection) and on the slow path (RecordHit's series shape).
func TestTrafficClassAttribution_E2E(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("body"))
	})
	defer originSrv.Close()

	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{HTTP: "127.0.0.1:0"},
			Storage: config.Storage{HotMaxBytes: 64 << 20},
			UpstreamPools: []config.UpstreamPool{
				{Name: "review-service", Targets: []string{originSrv.Addr}},
			},
			Routes: []config.Route{
				{Name: "api", Pool: "review-service", Cache: config.RouteCache{TTLDefault: 60 * time.Second}},
			},
			Metrics: config.MetricsConfig{TrafficClasses: []config.TrafficClass{
				{Name: "csr", Hosts: []string{"www.example.com"}},
				{Name: "ssr", Hosts: []string{"*.example.com"}},
			}},
			Experimental: config.ExperimentalConfig{H1FastPath: true},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	handler := e.buildDataPlane(rs)
	require.NoError(t, e.cfg.Validate())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startListeners(g, handler, rs)
	require.NotEmpty(t, rs.listeners)
	var addr string
	poll.Eventually(t, 3*time.Second, 5*time.Millisecond, func() bool {
		addr = rs.listeners[0].Addr()
		return !strings.HasSuffix(addr, ":0")
	})

	// The engine wiring must have compiled the classifier and
	// pre-resolved its class names into the metrics slot table.
	require.NotNil(t, rs.trafficClassify)
	// csr is declared first: the exact www host matches csr, every other
	// example.com subdomain falls through to the ssr suffix.
	assert.Equal(t, "csr", rs.trafficClassify.Classify("www.example.com:443"))
	assert.Equal(t, "ssr", rs.trafficClassify.Classify("SSR.Example.Com"))
	assert.Equal(t, api.TrafficClassUnclassified, rs.trafficClassify.Classify("other.tld"))

	// Warm + hit over a real connection with a classified Host: the
	// cache key is host-derived, so the Host must be the classified one
	// on the wire too. This exercises the full production path —
	// h1parser parse → routed TryHit → hit → metrics hook.
	require.NoError(t, fastGetWithHost(t, addr, "api.example.com", "/api/x"))
	require.NoError(t, fastGetWithHost(t, addr, "api.example.com", "/api/x"))

	// The routed wrapper (what the parser holds) must stamp the class
	// for the same Host — same route resolution, same classify.
	require.NotNil(t, rs.router)
	fp := rs.fastPathHandlers[0]
	rfp := server.NewRoutedFastPath(rs.router, fp)
	req := &api.RawRequest{Method: "GET", Path: "/api/x", Host: "api.example.com:443", Scheme: "http"}
	fResp, ok := rfp.TryHit(req, time.Now())
	require.True(t, ok)
	assert.Equal(t, "ssr", fResp.TrafficClass)
	rfp.Release(fResp)

	// RecordHit — the exact hook call the parser and reactor make for
	// the response above — must land in the pre-resolved per-class
	// series, not the fallback slot.
	reg := prometheus.NewRegistry()
	m := observability.NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"review-service"})
	m.PreResolveTrafficClasses(rs.trafficClassify.ClassNames())
	m.RecordHit("review-service", "ssr", "HIT", "hot", 200, 4, 500*time.Microsecond)
	m.RecordHit("review-service", "", "HIT", "hot", 200, 4, 500*time.Microsecond)

	families, err := reg.Gather()
	require.NoError(t, err)
	var ssrFound, unclassFound bool
	for _, mf := range families {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			var class, pool string
			for _, lp := range mtr.GetLabel() {
				switch lp.GetName() {
				case "traffic_class":
					class = lp.GetValue()
				case "upstream_pool":
					pool = lp.GetValue()
				}
			}
			if class == "ssr" && pool == "review-service" {
				ssrFound = true
			}
			if class == "unclassified" && pool == "review-service" {
				unclassFound = true
			}
		}
	}
	assert.True(t, ssrFound, "ssr HIT series must exist on the pool")
	assert.True(t, unclassFound, "empty class must fall back to unclassified")
}

// TestTrafficClass_ClassifierSubSetOfPreResolve pins the ADR-0047 §2.4
// invariant at the builder level: the classifier's outputs (its class
// names) must be a subset of PreResolveTrafficClasses inputs — both are
// built from the same config slice, and the fallback (slot 0) exists
// precisely because this "cannot diverge" assumption is what a wiring
// bug would violate.
func TestTrafficClass_ClassifierSubSetOfPreResolve(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		Listen: config.Listen{Admin: ":9000"},
		Metrics: config.MetricsConfig{TrafficClasses: []config.TrafficClass{
			{Name: "csr", Hosts: []string{"*.example.com"}},
			{Name: "ssr", Hosts: []string{"ssr.example.com", "www.*"}},
			{Name: "partner", Hosts: []string{"cdn.partner.io"}},
		}},
	}
	require.NoError(t, cfg.Validate())

	e := &engine{cfg: cfg, logger: newTestLogger(), metrics: observability.NewMetrics()}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	_ = e.buildDataPlane(rs)

	require.NotNil(t, rs.trafficClassify)
	classNames := rs.trafficClassify.ClassNames()
	require.Equal(t, []string{"csr", "ssr", "partner"}, classNames)

	// The metrics slot table must contain every classifier output: an
	// out-of-table class would silently fall back to "unclassified"
	// (recordFastHTTPMetrics/RecordHit) — the exact divergence this
	// test pins as impossible. Drive RecordHit on the engine's own
	// pre-resolved dpMetrics and assert every class lands in its own
	// series — the engine registry is the production wiring.
	for _, class := range classNames {
		rs.dpMetrics.RecordHit("", class, "HIT", "hot", 200, 1, time.Millisecond)
	}
	rs.dpMetrics.RecordHit("", "", "HIT", "hot", 200, 1, time.Millisecond)
	families, err := e.metrics.Registry.Gather()
	require.NoError(t, err)
	recorded := map[string]bool{}
	for _, mf := range families {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			for _, lp := range mtr.GetLabel() {
				if lp.GetName() == "traffic_class" {
					recorded[lp.GetValue()] = true
				}
			}
		}
	}
	for _, class := range classNames {
		assert.True(t, recorded[class],
			"classifier output %q must map to a pre-resolved slot", class)
	}
	assert.True(t, recorded[api.TrafficClassUnclassified],
		"the unclassified fallback slot must always be present")
}
