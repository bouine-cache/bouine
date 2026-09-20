package config

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults_AdminListenerEnabled(t *testing.T) {
	t.Parallel()
	d := Defaults()
	require.NotEqual(t, "", d.Listen.Admin)
}

func TestParse_EmptyYieldsDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Parse(nil)
	require.NoError(t, err, "parse")
	require.NotEqual(t, "", cfg.Listen.Admin)
}

func TestParse_RejectsUnknownKeys(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte("nonsensical_field: 1\n"))
	require.Error(t, err)
}

func TestParse_RejectsDuplicatePool(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
  - name: app
    targets: [b:1]
`
	_, err := Parse([]byte(yamlSrc))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate-pool error, got %v", err)
	}
}

func TestParse_RejectsUnknownPoolInRoute(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: missing
`
	_, err := Parse([]byte(yamlSrc))
	if err == nil || !strings.Contains(err.Error(), "unknown pool") {
		t.Fatalf("expected unknown-pool error, got %v", err)
	}
}

func TestParse_HappyPath(t *testing.T) {
	t.Parallel()
	yamlSrc := `
listen:
  http:  ":80"
  https: ":443"
  admin: ":9000"
storage:
  hot_max_bytes: 2Go
  warm_max_bytes: 20Go
upstream_pools:
  - name: app
    targets: [app.local:8080]
    health:
      active:
        path: /healthz
        interval: 5s
        timeout: 1s
        unhealthy_threshold: 3
routes:
  - match: { host: api.example.com }
    pool: app
    cache:
      ttl_default: 60s
      stale_while_revalidate: 30s
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "parse")
	got := cfg.Storage.HotMaxBytes.Bytes()
	require.Equal(t, int64(2_000_000_000), got)
	if len(cfg.Routes) != 1 || cfg.Routes[0].Pool != "app" {
		t.Fatalf("unexpected routes: %+v", cfg.Routes)
	}
	require.Equal(t, float64(5), cfg.UpstreamPools[0].Health.Active.Interval.Seconds())
}

func TestLoad_FromDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	err := os.WriteFile(path, []byte("listen:\n  admin: ':9001'\n"), 0o600)
	require.NoError(t, err, "write")
	cfg, err := Load(path)
	require.NoError(t, err, "load")
	require.Equal(t, ":9001", cfg.Listen.Admin)
}

func TestByteSize_Forms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1KB", 1000},
		{"1KiB", 1024},
		{"2GiB", 2 << 30},
		{"1.5MiB", int64(1.5 * (1 << 20))},
		{"128Mi", 128 << 20},
		{"1Gi", 1 << 30},
		{"4Ki", 4 << 10},
		{"1Ti", 1 << 40},
		{"1Ko", 1000},
		{"512Mo", 512_000_000},
		{"2Go", 2_000_000_000},
		{"1To", 1_000_000_000_000},
	}
	for _, tc := range cases {
		var b ByteSize
		yamlSrc := []byte("hot_max_bytes: " + tc.in + "\n")
		var s Storage
		if err := yamlUnmarshal(t, yamlSrc, &s); err != nil {
			t.Errorf("parse %q: %v", tc.in, err)
			continue
		}
		b = s.HotMaxBytes
		assert.Equal(t, tc.want, b.Bytes())
	}
}

func TestClusterMode_DefaultIsStrong(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	require.Equal(t, ClusterModeStrong, cfg.Cluster.Mode)
}

func TestClusterMode_EmptyDefaultsToStrong(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000", Cluster: ":8443"}, Cluster: Cluster{}}
	err := cfg.Validate()
	require.NoError(t, err, "validate")
	require.Equal(t, ClusterModeStrong, cfg.Cluster.Mode)
}

func TestClusterMode_ValidModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{ClusterModeStrong, ClusterModeEventual} {
		cfg := Config{Listen: Listen{Admin: ":9000", Cluster: ":8443"}, Cluster: Cluster{Mode: mode}}
		err := cfg.Validate()
		assert.Nil(t, err)
		assert.Equal(t, mode, cfg.Cluster.Mode)
	}
}

func TestClusterMode_InvalidValue(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000", Cluster: ":8443"}, Cluster: Cluster{Mode: "invalid"}}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster.mode: must be")
}

func TestClusterHandoffQueueDepth_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{HandoffQueueDepth: -1},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "handoff_queue_depth")
}

func TestClusterHandoffQueueDepth_ZeroAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{HandoffQueueDepth: 0},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestClusterHandoffQueueDepth_ExceedsUpperBoundRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{HandoffQueueDepth: maxHandoffQueueDepth + 1},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "handoff_queue_depth")
	require.Contains(t, err.Error(), "must be <=")
}

func TestClusterHandoffQueueDepth_AtUpperBoundAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{HandoffQueueDepth: maxHandoffQueueDepth},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestClusterPeerFetchConcurrency_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{PeerFetchConcurrency: -1},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "peer_fetch_concurrency")
	require.Contains(t, err.Error(), "must be >=")
}

func TestClusterPeerFetchConcurrency_ZeroAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{PeerFetchConcurrency: 0},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestClusterPeerFetchConcurrency_ExceedsUpperBoundRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{PeerFetchConcurrency: MaxPeerFetchConcurrency + 1},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "peer_fetch_concurrency")
	require.Contains(t, err.Error(), "must be <=")
}

func TestClusterBanTTL_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{BanTTL: -time.Minute},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "ban_ttl")
	require.Contains(t, err.Error(), "must be >=")
}

func TestClusterBanTTL_TooShortRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{BanTTL: 500 * time.Millisecond},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "ban_ttl")
	require.Contains(t, err.Error(), "must be >= 1s")
}

func TestClusterBanTTL_ZeroAcceptedUsesDefault(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{BanTTL: 0},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestClusterBanTTL_MinutesAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{BanTTL: 15 * time.Minute},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestClusterPeerFetchConcurrency_AtUpperBoundAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000", Cluster: ":8443"},
		Cluster: Cluster{PeerFetchConcurrency: MaxPeerFetchConcurrency},
	}
	err := cfg.Validate()
	require.NoError(t, err)
}

func TestClusterMode_NonStrongRequiresListener(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Mode: ClusterModeEventual}}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires listen.cluster")
}

func TestClusterMode_StrongWithoutListener(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Mode: ClusterModeStrong}}
	err := cfg.Validate()
	require.NoError(t, err, "strong mode without listener should be valid")
}

// --- Route.Name auto-derivation ---

func TestValidate_RouteNameAutoDerived(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	cases := []struct {
		name     string
		route    Route
		wantName string
	}{
		{"host+prefix", Route{Match: RouteMatch{Host: "api.example.com", PathPrefix: "/v1"}, Pool: "app"}, "api.example.com:/v1"},
		{"prefix only", Route{Match: RouteMatch{PathPrefix: "/products"}, Pool: "app"}, "/products"},
		{"catch-all", Route{Match: RouteMatch{}, Pool: "app"}, "_catch-all"},
		{"explicit name kept", Route{Name: "custom", Match: RouteMatch{PathPrefix: "/"}, Pool: "app"}, "custom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Listen:        Listen{Admin: ":9000"},
				UpstreamPools: []UpstreamPool{pool},
				Routes:        []Route{tc.route},
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			if cfg.Routes[0].Name != tc.wantName {
				t.Fatalf("Name = %q, want %q", cfg.Routes[0].Name, tc.wantName)
			}
		})
	}
}

// --- TTLOverride validation ---

func TestValidate_RouteCache_NegativeDurationsRejected(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	base := Route{Pool: "app", Cache: RouteCache{}}
	cases := []struct {
		name string
		set  func(rc *RouteCache)
	}{
		{"ttl_default", func(rc *RouteCache) { rc.TTLDefault = -1 }},
		{"stale_while_revalidate", func(rc *RouteCache) { rc.StaleWhileRevalidate = -1 }},
		{"stale_if_error", func(rc *RouteCache) { rc.StaleIfError = -1 }},
		{"fetch_timeout", func(rc *RouteCache) { rc.FetchTimeout = -1 }},
		{"fetch_timeout", func(rc *RouteCache) { rc.FetchTimeout = 6 * time.Minute }},
		{"fetch_timeout", func(rc *RouteCache) { rc.FetchTimeout = 5 * time.Minute }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := base
			tc.set(&r.Cache)
			cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{r}}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("error %q does not mention field %q", err, tc.name)
			}
		})
	}
}

func TestValidate_PoolDurations_NegativeRejected(t *testing.T) {
	t.Parallel()
	base := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	cases := []struct {
		name string
		set  func(p *UpstreamPool)
	}{
		{"health.active.interval", func(p *UpstreamPool) { p.Health.Active.Interval = -1 }},
		{"health.active.timeout", func(p *UpstreamPool) { p.Health.Active.Timeout = -1 }},
		{"health.passive.eject_for", func(p *UpstreamPool) { p.Health.Passive.EjectFor = -1 }},
		{"connect.timeout", func(p *UpstreamPool) { p.Connect.Timeout = -1 }},
		{"connect.keep_alive", func(p *UpstreamPool) { p.Connect.KeepAlive = -1 }},
		{"connect.max_idle_conn_duration", func(p *UpstreamPool) { p.Connect.MaxIdleConnDuration = -1 }},
		{"connect.response_header_timeout", func(p *UpstreamPool) { p.Connect.ResponseHeaderTimeout = -1 }},
		{"connect.max_connections", func(p *UpstreamPool) { p.Connect.MaxConnections = -1 }},
		{"connect.hedge_timeout", func(p *UpstreamPool) { p.Connect.HedgeTimeout = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := base
			tc.set(&p)
			cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{p}}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("error %q does not mention field %q", err, tc.name)
			}
		})
	}
}

func TestValidate_ListenIdleTimeout_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000", IdleTimeout: -1}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for negative listen.idle_timeout")
	}
	if !strings.Contains(err.Error(), "listen.idle_timeout") {
		t.Fatalf("error %q does not mention listen.idle_timeout", err)
	}
}

func TestValidate_ListenReadTimeout_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000", ReadTimeout: -1}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for negative listen.read_timeout")
	}
	if !strings.Contains(err.Error(), "listen.read_timeout") {
		t.Fatalf("error %q does not mention listen.read_timeout", err)
	}
}

func TestValidate_ListenReadTimeout_AtOrAboveSafetyNetRejected(t *testing.T) {
	t.Parallel()
	for _, v := range []time.Duration{maxReadTimeout, maxReadTimeout + time.Second} {
		cfg := Config{Listen: Listen{Admin: ":9000", ReadTimeout: v}}
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("expected error for listen.read_timeout %v", v)
		}
		if !strings.Contains(err.Error(), "listen.read_timeout") {
			t.Fatalf("error %q does not mention listen.read_timeout", err)
		}
	}
}

func TestValidate_ListenReadTimeout_BelowSafetyNetAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000", ReadTimeout: maxReadTimeout - time.Second}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid listen.read_timeout rejected: %v", err)
	}
}

// TestValidate_PoolResponseHeaderTimeout_AtOrAboveSafetyNetRejected
// pins the ordering constraint on the pool knob that routes inherit as
// their default origin wait: connect.response_header_timeout must stay
// strictly below the data plane's 5-minute safety-net WriteTimeout, the
// same rule fetch_timeout already follows.
func TestValidate_PoolResponseHeaderTimeout_AtOrAboveSafetyNetRejected(t *testing.T) {
	t.Parallel()
	for _, v := range []time.Duration{maxFetchTimeout, maxFetchTimeout + time.Second} {
		cfg := Config{
			Listen:        Listen{Admin: ":9000"},
			UpstreamPools: []UpstreamPool{{Name: "app", Targets: []string{"a:1"}, Connect: ConnectPolicy{ResponseHeaderTimeout: v}}},
		}
		err := cfg.Validate()
		if err == nil {
			t.Fatalf("expected error for connect.response_header_timeout %v", v)
		}
		if !strings.Contains(err.Error(), "connect.response_header_timeout") {
			t.Fatalf("error %q does not mention connect.response_header_timeout", err)
		}
	}
}

func TestValidate_PoolResponseHeaderTimeout_BelowSafetyNetAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{{Name: "app", Targets: []string{"a:1"}, Connect: ConnectPolicy{ResponseHeaderTimeout: maxFetchTimeout - time.Second}}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid connect.response_header_timeout rejected: %v", err)
	}
}

func TestParse_TTLOverride_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
    cache:
      ttl_override: 1h
      ttl_default:  30s
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	require.Len(t, cfg.Routes, 1)
	assert.Equal(t, time.Duration(int64(1)*60*60*1e9), cfg.Routes[0].Cache.TTLOverride)
}

func TestValidate_TTLOverride_NegativeRejected(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Cache: RouteCache{TTLOverride: -1}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "ttl_override") {
		t.Fatalf("expected ttl_override validation error, got %v", err)
	}
}

func TestValidate_StripPrefix_MustStartWithSlash(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Request: RouteRequest{StripPrefix: "no-slash"}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "strip_prefix") {
		t.Fatalf("expected strip_prefix validation error, got %v", err)
	}
}

func TestParse_StripPrefix_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { path_prefix: /api/v1 }
    pool: app
    request:
      strip_prefix: /api/v1
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, "/api/v1", cfg.Routes[0].Request.StripPrefix)
}

func TestParse_PathRewrite_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { path_prefix: /payment/orchestrator/callback }
    pool: app
    request:
      path_rewrite:
        match: ^/payment/orchestrator/callback/(.*)$
        replace: /scrooge/callback/$1
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, `^/payment/orchestrator/callback/(.*)$`, cfg.Routes[0].Request.PathRewrite.Match)
	assert.Equal(t, "/scrooge/callback/$1", cfg.Routes[0].Request.PathRewrite.Replace)
}

func TestValidate_PathRewrite_RequiresBothFields(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	cases := []struct {
		name string
		pw   PathRewriteConfig
	}{
		{"match only", PathRewriteConfig{Match: `^/a/`}},
		{"replace only", PathRewriteConfig{Replace: "/b/"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			route := Route{Pool: "app", Request: RouteRequest{PathRewrite: tc.pw}}
			cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "requires both match and replace") {
				t.Fatalf("expected path_rewrite both-fields error, got %v", err)
			}
		})
	}
}

func TestValidate_PathRewrite_MutuallyExclusiveWithStripPrefix(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Request: RouteRequest{
		StripPrefix: "/api/v1",
		PathRewrite: PathRewriteConfig{Match: `^/api/`, Replace: "/x/"},
	}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	var fe *FieldError
	if err == nil || !errors.As(err, &fe) || fe.Path != "routes[0].request.path_rewrite" || !strings.Contains(fe.Message, "mutually exclusive with strip_prefix") {
		t.Fatalf("expected strip_prefix/path_rewrite exclusivity error on request.path_rewrite, got %v", err)
	}
}

func TestValidate_PathRewrite_RejectsInvalidPattern(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Request: RouteRequest{
		PathRewrite: PathRewriteConfig{Match: "(", Replace: "/x/"},
	}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	var fe *FieldError
	if err == nil || !errors.As(err, &fe) || fe.Path != "routes[0].request.path_rewrite.match" || !strings.Contains(fe.Message, "not a valid regular expression") {
		t.Fatalf("expected invalid-pattern error on path_rewrite.match, got %v", err)
	}
}

func TestValidate_PathRewrite_RejectsOversizedPattern(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Request: RouteRequest{
		PathRewrite: PathRewriteConfig{
			Match:   "^/" + strings.Repeat("a", MaxPathRewritePatternBytes) + "/$",
			Replace: "/x/",
		},
	}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	var fe *FieldError
	if err == nil || !errors.As(err, &fe) || fe.Path != "routes[0].request.path_rewrite.match" || !strings.Contains(fe.Message, "exceeds") {
		t.Fatalf("expected pattern size-cap error on path_rewrite.match, got %v", err)
	}
}

func TestValidate_PathRewrite_RejectsOversizedReplace(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Request: RouteRequest{
		PathRewrite: PathRewriteConfig{
			Match:   `^/a/`,
			Replace: "/x/" + strings.Repeat("a", MaxPathRewritePatternBytes),
		},
	}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	var fe *FieldError
	if err == nil || !errors.As(err, &fe) || fe.Path != "routes[0].request.path_rewrite.replace" || !strings.Contains(fe.Message, "exceeds") {
		t.Fatalf("expected replace size-cap error on path_rewrite.replace, got %v", err)
	}
}

func TestValidate_PathRewrite_AcceptedNearBoundary(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	// A pattern within the cap (repeating 5-byte [a-z] groups) must pass
	// validation — the cap rejects the oversized, not the large-but-
	// legal.
	pattern := "^/" + strings.Repeat("[a-z]", (MaxPathRewritePatternBytes-len("^/"))/5-1) + "$"
	if len(pattern) > MaxPathRewritePatternBytes {
		t.Fatalf("test construction: pattern %d > cap %d", len(pattern), MaxPathRewritePatternBytes)
	}
	route := Route{Pool: "app", Request: RouteRequest{
		PathRewrite: PathRewriteConfig{Match: pattern, Replace: "/x/"},
	}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("in-cap pattern must validate, got %v", err)
	}
}

func TestValidate_PathRewrite_EmptyBlockAccepted(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app"}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("route without path_rewrite must validate, got %v", err)
	}
}

// TestValidate_PathRewrite_TemplateRefs pins the template-reference
// resolution: every $-reference must resolve against the pattern.
// Go's Expand silently expands unknown references to "" — the `$1x`
// typo below would corrupt every rewritten path with no error
// anywhere if validation did not reject it.
func TestValidate_PathRewrite_TemplateRefs(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	tests := []struct {
		name    string
		match   string
		replace string
		wantErr string
	}{
		{"plain index ok", `^/a/(.*)$`, "/b/$1", ""},
		{"whole match ok", `^/a/`, "/b$0", ""},
		{"named group ok", `^/u/(?P<name>[a-z]+)$`, "/user/$name", ""},
		{"braced disambiguation ok", `^/a/(.*)$`, "/b/${1}x", ""},
		{"literal dollar ok", `^/a/(.*)$`, "/b/$$$1", ""},
		{"dollar at end is lone", `^/a/`, "/b/$", "lone '$'"},
		{"index run past groups is a name", `^/a/(.*)$`, "/b/$1x", "unknown group \"1x\""},
		{"out-of-range index", `^/a/(.*)$`, "/b/$2", "only 1 capture group"},
		{"leading zero index", `^/a/(.*)$`, "/b/$01", "leading-zero group indexes"},
		{"braced leading zero index", `^/a/(.*)$`, "/b/${01}x", "leading-zero group indexes"},
		{"double zero index", `^/a/(.*)$`, "/b/$00", "leading-zero group indexes"},
		{"unknown name", `^/a/(.*)$`, "/b/$user", "unknown group \"user\""},
		{"unterminated brace", `^/a/(.*)$`, "/b/${1", "unterminated"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			route := Route{Pool: "app", Request: RouteRequest{
				PathRewrite: PathRewriteConfig{Match: tc.match, Replace: tc.replace},
			}}
			cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("template must validate, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// TestValidate_PathRewrite_ControlBytes pins the control-byte gate: raw
// CR/LF/NUL in match or replace are rejected (paths cannot carry them,
// so they could only produce a corrupted origin request), while the
// equivalent escaped forms in the pattern stay legal.
func TestValidate_PathRewrite_ControlBytes(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	tests := []struct {
		name    string
		match   string
		replace string
		wantErr string
	}{
		{"CR in match", "^/a\r/x/", "/b/", "raw control byte"},
		{"LF in match", "^/a\n/x/", "/b/", "raw control byte"},
		{"NUL in match", "^/a\x00/x/", "/b/", "raw control byte"},
		{"CR in replace", `^/a/(.*)$`, "/b/\r$1", "raw control byte"},
		{"LF in replace", `^/a/(.*)$`, "/b/\n$1", "raw control byte"},
		{"DEL in replace", `^/a/(.*)$`, "/b/\x7f$1", "raw control byte"},
		{"escaped form in match legal", `^/a/\x0d/(.*)$`, "/b/$1", ""},
		{"escape syntax legal", `^/a/[\r\n]/(.*)$`, "/b/$1", ""},
		{"space in replace", `^/a/(.*)$`, "/b/a b$1", "cannot carry it raw"},
		{"question mark in replace", `^/a/(.*)$`, "/b/$1?x=1", "cannot carry it raw"},
		{"hash in replace", `^/a/(.*)$`, "/b/$1#frag", "cannot carry it raw"},
		{"regex quantifier in match stays legal", `^/a/(x?)$`, "/b/$1", ""},
		{"percent-encoded space in replace legal", `^/a/(.*)$`, "/b/a%20b/$1", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			route := Route{Pool: "app", Request: RouteRequest{
				PathRewrite: PathRewriteConfig{Match: tc.match, Replace: tc.replace},
			}}
			cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("must validate, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestParse_MethodsNormalisedToUpper(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com, methods: [get, Post] }
    pool: app
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	if cfg.Routes[0].Match.Methods[0] != "GET" || cfg.Routes[0].Match.Methods[1] != "POST" {
		t.Errorf("methods not normalised: %v", cfg.Routes[0].Match.Methods)
	}
}

func TestValidate_MethodsRejectsUnknown(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	route := Route{Pool: "app", Match: RouteMatch{Methods: []string{"FROBNICATE"}}}
	cfg := Config{Listen: Listen{Admin: ":9000"}, UpstreamPools: []UpstreamPool{pool}, Routes: []Route{route}}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "unknown HTTP method") {
		t.Fatalf("expected unknown-method error, got %v", err)
	}
}

func TestParse_EmptyMethodsMatchAll(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	assert.Len(t, cfg.Routes[0].Match.Methods, 0)
}

// --- HotMaxBytes GOMEMLIMIT derivation (issue #161) ---

func TestResolveHotMaxBytes_DerivesFromGomemLimitDefaultRatio(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		limit string
		want  int64
	}{
		{"24GiB default 75%", "24GiB", int64(24<<30) * 75 / 100},
		{"3GiB default 75%", "3GiB", int64(3<<30) * 75 / 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := Storage{}
			s.ResolveHotMaxBytes(tc.limit)
			if got := s.HotMaxBytes.Bytes(); got != tc.want {
				t.Fatalf("HotMaxBytes = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestResolveHotMaxBytes_ExplicitOverrideKept(t *testing.T) {
	t.Parallel()
	s := Storage{HotMaxBytes: ByteSize(1 << 30)} // 1 GiB explicit
	s.ResolveHotMaxBytes("24GiB")
	got := s.HotMaxBytes.Bytes()
	require.Equal(t, int64(1)<<30, got)
}

func TestResolveHotMaxBytes_NoGomemLimitStaysZero(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveHotMaxBytes("")
	require.Equal(t, int64(0), s.HotMaxBytes.Bytes())
}

func TestResolveHotMaxBytes_InvalidGomemLimitIgnored(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveHotMaxBytes("garbage")
	require.Equal(t, int64(0), s.HotMaxBytes.Bytes())
}

func TestParse_DerivesHotMaxBytesFromGomemLimitEnv(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "24GiB")
	yamlSrc := `
listen:
  admin: ":9000"
storage:
  warm_dir: /tmp`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "parse")
	want := int64(24<<30) * 75 / 100
	got := cfg.Storage.HotMaxBytes.Bytes()
	require.Equal(t, want, got)
}

func TestParse_EmptyConfigDerivesHotMaxBytesFromGomemLimit(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "3GiB")
	cfg, err := Parse(nil)
	require.NoError(t, err, "parse")
	want := int64(3<<30) * 75 / 100
	got := cfg.Storage.HotMaxBytes.Bytes()
	require.Equal(t, want, got)
}

func TestResolveHotMaxBytes_PlainIntegerBytes(t *testing.T) {
	t.Parallel()
	// The Go runtime's GOMEMLIMIT is a plain byte count when no unit
	// suffix is supplied.
	s := Storage{}
	s.ResolveHotMaxBytes("3221225472") // 3 GiB
	want := int64(3221225472) * 75 / 100
	got := s.HotMaxBytes.Bytes()
	require.Equal(t, want, got)
}

func TestParse_ExplicitHotMaxBytesNotOverriddenByGomemLimit(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "24GiB")
	yamlSrc := `
listen:
  admin: ":9000"
storage:
  hot_max_bytes: 2GiB
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "parse")
	got := cfg.Storage.HotMaxBytes.Bytes()
	require.Equal(t, int64(2<<30), got)
}

func TestCluster_FullMode_Rejected(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000", Cluster: ":8443"}, Cluster: Cluster{Mode: "full"}}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster.mode: must be")
}

func TestWALSyncInterval_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000"},
		Storage: Storage{WALSyncInterval: -2 * time.Second},
	}
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "wal_sync_interval")
}

func TestWALSyncInterval_NegativeOneAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000"},
		Storage: Storage{WALSyncInterval: -1},
	}
	err := cfg.Validate()
	require.NoError(t, err, "wal_sync_interval = -1 should be accepted, got")
}

func TestResolveWarmMaxEntries_DerivesFromGomemLimitDefaultRatio(t *testing.T) {
	t.Parallel()
	// 14 GiB * 15% / (100 * 160) = 14*1024^3 * 15 / 16000 = ~14,092,861 entries
	limit := int64(14 << 30)
	want := limit * 15 / (100 * 160)
	s := Storage{}
	s.ResolveWarmMaxEntries("14GiB")
	require.Equal(t, want, s.WarmMaxEntries)
}

func TestResolveWarmMaxEntries_DefaultRatio(t *testing.T) {
	t.Parallel()
	limit := int64(14 << 30)
	want := limit * 15 / (100 * 160)
	s := Storage{}
	s.ResolveWarmMaxEntries("14GiB")
	require.Equal(t, want, s.WarmMaxEntries)
}

func TestResolveWarmMaxEntries_ExplicitOverrideKept(t *testing.T) {
	t.Parallel()
	s := Storage{WarmMaxEntries: 5_000_000}
	s.ResolveWarmMaxEntries("14GiB")
	require.Equal(t, int64(5_000_000), s.WarmMaxEntries)
}

func TestResolveWarmMaxEntries_NoGomemLimitStaysZero(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveWarmMaxEntries("")
	require.Equal(t, int64(0), s.WarmMaxEntries)
}

func TestResolveWarmMaxEntries_InvalidGomemLimitIgnored(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveWarmMaxEntries("garbage")
	require.Equal(t, int64(0), s.WarmMaxEntries)
}

func TestResolveMaxStreamingBufferBytes_DerivesFromGomemLimitDefaultRatio(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		limit string
		want  int64
	}{
		{"768MiB default 7%", "768MiB", int64(768<<20) * 7 / 100},
		{"14GiB default 7%", "14GiB", int64(14<<30) * 7 / 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := RouteCache{}
			c.ResolveMaxStreamingBufferBytes(tc.limit)
			require.Equal(t, tc.want, c.MaxStreamingBufferBytes.Bytes())
		})
	}
}

func TestResolveMaxStreamingBufferBytes_ExplicitOverrideKept(t *testing.T) {
	t.Parallel()
	c := RouteCache{MaxStreamingBufferBytes: ByteSize(32 << 20)}
	c.ResolveMaxStreamingBufferBytes("24GiB")
	require.Equal(t, int64(32<<20), c.MaxStreamingBufferBytes.Bytes())
}

func TestResolveMaxStreamingBufferBytes_NoGomemLimitStaysZero(t *testing.T) {
	t.Parallel()
	c := RouteCache{}
	c.ResolveMaxStreamingBufferBytes("")
	require.Equal(t, int64(0), c.MaxStreamingBufferBytes.Bytes())
}

func TestResolveMaxStreamingBufferBytes_InvalidGomemLimitIgnored(t *testing.T) {
	t.Parallel()
	c := RouteCache{}
	c.ResolveMaxStreamingBufferBytes("garbage")
	require.Equal(t, int64(0), c.MaxStreamingBufferBytes.Bytes())
}

func TestParse_DerivesMaxStreamingBufferBytesFromGomemLimitEnv(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "768MiB")
	yamlSrc := `
listen:
  admin: ":9000"
routes:
  - name: test
    match:
      host: example.com
    pool: upstream
upstream_pools:
  - name: upstream
    targets:
      - "http://localhost:8080"
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "parse")
	want := int64(768<<20) * 7 / 100
	got := cfg.Routes[0].Cache.MaxStreamingBufferBytes.Bytes()
	require.Equal(t, want, got)
}

func TestParse_ExplicitMaxStreamingBufferBytesNotOverriddenByGomemLimit(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "24GiB")
	yamlSrc := `
listen:
  admin: ":9000"
routes:
  - name: test
    match:
      host: example.com
    pool: upstream
    cache:
      max_streaming_buffer_bytes: 128MiB
upstream_pools:
  - name: upstream
    targets:
      - "http://localhost:8080"
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "parse")
	require.Equal(t, int64(128<<20), cfg.Routes[0].Cache.MaxStreamingBufferBytes.Bytes())
}

func TestParse_GOGC(t *testing.T) {
	t.Parallel()
	gogc := 200
	cases := []struct {
		name string
		yaml string
		want *int
	}{
		{"unset defaults to nil", "", nil},
		{"200", "gogc: 200\n", &gogc},
		{"-1 (off)", "gogc: -1\n", ptrInt(-1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := Parse([]byte(tc.yaml))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if tc.want == nil {
				if cfg.GOGC != nil {
					t.Fatalf("expected nil GOGC, got %d", *cfg.GOGC)
				}
				return
			}
			if cfg.GOGC == nil {
				t.Fatalf("expected GOGC=%d, got nil", *tc.want)
			}
			if *cfg.GOGC != *tc.want {
				t.Fatalf("expected GOGC=%d, got %d", *tc.want, *cfg.GOGC)
			}
		})
	}
}

func TestValidate_GOGC(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		gogc int
		ok   bool
	}{
		{"1 is valid", 1, true},
		{"100 is valid", 100, true},
		{"200 is valid", 200, true},
		{"-1 (off) is valid", -1, true},
		{"0 is invalid", 0, false},
		{"-2 is invalid", -2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := Defaults()
			c.GOGC = &tc.gogc
			err := c.Validate()
			if tc.ok && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func ptrInt(v int) *int { return &v }

func TestExpandEnvVars_Simple(t *testing.T) {
	t.Setenv("BOUINE_TEST_HOST", "api.example.com")
	got := expandEnvVars([]byte("listen:\n  http: \":80\"\nupstream_pools:\n  - name: app\n    targets: [\"${BOUINE_TEST_HOST}:8080\"]\n"))
	require.Contains(t, string(got), "api.example.com:8080")
}

func TestExpandEnvVars_DefaultValue(t *testing.T) {
	t.Setenv("BOUINE_MISSING", "")
	got := expandEnvVars([]byte("host: ${BOUINE_MISSING:-fallback.example.com}"))
	require.Contains(t, string(got), "fallback.example.com")
}

func TestExpandEnvVars_EscapeDollar(t *testing.T) {
	got := expandEnvVars([]byte("cost: $$5.00"))
	require.Equal(t, "cost: $5.00", string(got))
}

func TestExpandEnvVars_NoMatch(t *testing.T) {
	got := expandEnvVars([]byte("no vars here"))
	require.Equal(t, "no vars here", string(got))
}

func TestExpandEnvVars_UnclosedBrace(t *testing.T) {
	got := expandEnvVars([]byte("val: ${UNCLOSED"))
	require.Equal(t, "val: ${UNCLOSED", string(got))
}

// TestExpandEnvVars_NonNameBracesLeftLiteral pins the interpolation
// restriction: only env-var-shaped names expand. Digit-leading and
// otherwise non-name braced runs (path_rewrite capture-group references
// such as ${1}) survive the loader verbatim instead of being silently
// replaced with the value of an env var that cannot exist.
func TestExpandEnvVars_NonNameBracesLeftLiteral(t *testing.T) {
	t.Setenv("BOUINE_TEST_HOST", "api.example.com")
	for _, raw := range []string{
		"replace: /b/${1}x",
		"replace: /b/${01}",
		"replace: /b/${1x}",
		"replace: /b/${a.b}",
		"replace: /b/${}",
	} {
		got := string(expandEnvVars([]byte(raw)))
		require.Equal(t, raw, got, "non-name braces must survive verbatim: %s", raw)
	}
	// Env-var-shaped names still expand, with and without defaults.
	got := string(expandEnvVars([]byte("a: ${BOUINE_TEST_HOST} b: ${unset_var:-d}")))
	require.Contains(t, got, "api.example.com")
	require.Contains(t, got, "b: d")
}

// TestParse_PathRewrite_BracedRefSurvivesInterpolation pins the
// documented `${1}x` disambiguation syntax end-to-end through the YAML
// loader: the braced capture-group reference must reach validation
// intact (the ${VAR} interpolation only applies to env-var-shaped
// names) and resolve against the pattern's groups.
func TestParse_PathRewrite_BracedRefSurvivesInterpolation(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { path_prefix: /a }
    pool: app
    request:
      path_rewrite:
        match: ^/a/(.*)$
        replace: /b/${1}x
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "the documented ${1}x braced syntax must load and validate")
	assert.Equal(t, "/b/${1}x", cfg.Routes[0].Request.PathRewrite.Replace)
}

func TestParse_EnvVarInterpolation(t *testing.T) {
	t.Setenv("BOUINE_ORIGIN", "origin.example.com")
	yamlSrc := `
upstream_pools:
  - name: app
    targets: ["${BOUINE_ORIGIN}:8080"]
routes:
  - match: {}
    pool: app
    cache:
      ttl_default: 60s`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err)
	require.Equal(t, "origin.example.com:8080", cfg.UpstreamPools[0].Targets[0])
}

func TestParse_HotEvictionAlgorithm_Default(t *testing.T) {
	t.Parallel()
	cfg, err := Parse(nil)
	require.NoError(t, err)
	require.Equal(t, "", cfg.Storage.HotEvictionAlgorithm)
	require.Equal(t, "", cfg.Storage.WarmEvictionAlgorithm)
	require.Equal(t, "", cfg.Storage.EvictionAlgorithm)
}

func TestParse_EvictionAlgorithm_Invalid(t *testing.T) {
	t.Parallel()
	yamlSrc := `
storage:
  hot_eviction_algorithm: random
`
	_, err := Parse([]byte(yamlSrc))
	require.Error(t, err)
	require.Contains(t, err.Error(), `eviction_algorithm`)
}

func TestParse_HotEvictionAlgorithm_Cachaner(t *testing.T) {
	t.Parallel()
	yamlSrc := `
storage:
  hot_eviction_algorithm: cachaner
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err)
	require.Equal(t, "cachaner", cfg.Storage.HotEvictionAlgorithm)
}

func TestParse_SharedEvictionAlgorithm_Cachaner(t *testing.T) {
	t.Parallel()
	yamlSrc := `
storage:
  eviction_algorithm: cachaner
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err)
	require.Equal(t, "cachaner", cfg.Storage.EvictionAlgorithm)
}

func TestParse_WarmEvictionAlgorithm_Cachaner(t *testing.T) {
	t.Parallel()
	yamlSrc := `
storage:
  warm_eviction_algorithm: cachaner
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err)
	require.Equal(t, "cachaner", cfg.Storage.WarmEvictionAlgorithm)
}

// TestValidate_H1ReactorRequiresFastPath asserts that
// experimental.h1_reactor without experimental.h1_fast_path is
// rejected at load time instead of silently no-oping at startup.
func TestValidate_H1ReactorRequiresFastPath(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.Listen.HTTP = ":8080"
	cfg.Experimental.H1Reactor = true
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "experimental.h1_reactor: requires experimental.h1_fast_path")

	// With the fast path on, the same config validates.
	cfg.Experimental.H1FastPath = true
	assert.NoError(t, cfg.Validate())
}

// TestValidate_H1FastPeerPathRequiresFastPath asserts that
// experimental.h1_fast_peer_path without experimental.h1_fast_path is
// rejected at load time instead of silently no-oping at startup, and
// that the flag defaults to off.
func TestValidate_H1FastPeerPathRequiresFastPath(t *testing.T) {
	t.Parallel()

	// Default config leaves the flag off.
	require.False(t, Defaults().Experimental.H1FastPeerPath)

	// Flag on without the fast path must be rejected.
	cfg := Defaults()
	cfg.Listen.HTTP = ":8080"
	cfg.Experimental.H1FastPeerPath = true
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "experimental.h1_fast_peer_path: requires experimental.h1_fast_path")

	// With the fast path on, the same config validates.
	cfg.Experimental.H1FastPath = true
	assert.NoError(t, cfg.Validate())

	// YAML round-trip: the flag parses from its documented key.
	parsed, err := Parse([]byte("experimental:\n  h1_fast_path: true\n  h1_fast_peer_path: true\n"))
	require.NoError(t, err)
	assert.True(t, parsed.Experimental.H1FastPeerPath)
}

// TestValidate_PeerIdleBelowAdminIdle asserts the idle-timeout ordering
// between the peer-fetch client and the admin server: the client must
// close idle peer connections before the admin server reaps them, or
// the first peer RPC on a reaped connection fails with EOF/broken pipe.
func TestValidate_PeerIdleBelowAdminIdle(t *testing.T) {
	t.Parallel()

	// Explicit inversion must be rejected.
	cfg := Defaults()
	cfg.Listen.Cluster = ":8443"
	cfg.Cluster.PeerMaxIdleConnDuration = 360 * time.Second
	cfg.Admin.IdleTimeout = 300 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "peer_max_idle_conn_duration")

	// Equal values must also be rejected (strict ordering).
	cfg.Cluster.PeerMaxIdleConnDuration = 300 * time.Second
	require.Error(t, cfg.Validate())

	// Client below server must validate.
	cfg.Cluster.PeerMaxIdleConnDuration = 120 * time.Second
	cfg.Admin.IdleTimeout = 300 * time.Second
	require.NoError(t, cfg.Validate())

	// Client set against the built-in 300s server default must enforce
	// the ordering too.
	cfg = Defaults()
	cfg.Listen.Cluster = ":8443"
	cfg.Cluster.PeerMaxIdleConnDuration = 360 * time.Second
	require.Error(t, cfg.Validate())

	// Unset client idle uses the built-in 120s default and validates.
	cfg = Defaults()
	cfg.Listen.Cluster = ":8443"
	cfg.Admin.IdleTimeout = 300 * time.Second
	require.NoError(t, cfg.Validate())
}

// TestValidate_AdminIdleTimeout_NegativeRejected asserts negative
// admin.idle_timeout values are rejected at load time.
func TestValidate_AdminIdleTimeout_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.Admin.IdleTimeout = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin.idle_timeout")
}

// TestValidate_PeerIdleNegativeRejected asserts negative
// cluster.peer_max_idle_conn_duration values are rejected.
func TestValidate_PeerIdleNegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	cfg.Listen.Cluster = ":8443"
	cfg.Cluster.PeerMaxIdleConnDuration = -1 * time.Second
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "peer_max_idle_conn_duration")
}

// --- cache.key.include_headers (issue #632) ---

func TestParse_IncludeHeaders_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: [app.local:8080]
routes:
  - match: { host: api.example.com }
    pool: app
    cache:
      ttl_default: 60s
      key:
        include_headers: [Accept-Language, X-Geo-Region]
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "the strict decoder must accept include_headers")
	require.Equal(t, []string{"Accept-Language", "X-Geo-Region"},
		cfg.Routes[0].Cache.Key.IncludeHeaders)
}

// TestValidate_IncludeHeaders_Rejections pins every validation rule:
// the include list participates in the cache key, so an unsound list
// (wildcard, duplicate, overlap with exclude_headers, unbounded) is a
// security issue (threat-model T06), not a cosmetic one.
func TestValidate_IncludeHeaders_Rejections(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	mk := func(key RouteKey) Config {
		return Config{
			Listen:        Listen{Admin: ":9000"},
			UpstreamPools: []UpstreamPool{pool},
			Routes:        []Route{{Pool: "app", Cache: RouteCache{Key: key}}},
		}
	}
	tests := []struct {
		name string
		key  RouteKey
		want string
	}{
		{"star is unkeyable", RouteKey{IncludeHeaders: []string{"Accept-Language", "*"}}, `include_headers[1]: must not be "*"`},
		{"padded star is still a star", RouteKey{IncludeHeaders: []string{" *"}}, `must not be "*"`},
		{"empty entry", RouteKey{IncludeHeaders: []string{"Accept-Language", ""}}, "include_headers[1]: must be a non-empty header name"},
		{"whitespace-only entry", RouteKey{IncludeHeaders: []string{" "}}, "must be a non-empty"},
		{
			"case-insensitive duplicate", RouteKey{IncludeHeaders: []string{"Accept-Language", "accept-language"}},
			"is a duplicate",
		},
		{
			"padded duplicate", RouteKey{IncludeHeaders: []string{"Accept-Language", " accept-language"}},
			"is a duplicate",
		},
		{
			"comma entry is two fields downstream", RouteKey{IncludeHeaders: []string{"X-Geo", "x,y"}},
			"must be a single RFC 9110 §5.1 header name",
		},
		{"space inside entry", RouteKey{IncludeHeaders: []string{"X Geo"}}, "must be a single RFC 9110 §5.1 header name"},
		{
			"overlap with exclude_headers", RouteKey{
				IncludeHeaders: []string{"Accept-Language"},
				ExcludeHeaders: []string{"X-Request-ID", "accept-language"},
			}, "is also listed in include_headers",
		},
		{
			"padded overlap with exclude_headers", RouteKey{
				IncludeHeaders: []string{"Accept-Language"},
				ExcludeHeaders: []string{" accept-language "},
			}, "is also listed in include_headers",
		},
		{
			"more than 16 entries", RouteKey{IncludeHeaders: []string{
				"H01", "H02", "H03", "H04", "H05", "H06", "H07", "H08",
				"H09", "H10", "H11", "H12", "H13", "H14", "H15", "H16", "H17",
			}}, "capped at 16 entries",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := mk(tc.key)
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestValidate_IncludeHeaders_Accepted(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	cfg := Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{pool},
		Routes: []Route{{Pool: "app", Cache: RouteCache{Key: RouteKey{
			// 16 entries, mixed case, distinct from exclude_headers.
			IncludeHeaders: []string{
				"H01", "H02", "H03", "H04", "H05", "H06", "H07", "H08",
				"H09", "H10", "H11", "H12", "H13", "H14", "H15", "Accept-Language",
			},
			ExcludeHeaders: []string{"X-Request-ID"},
		}}}},
	}
	require.NoError(t, cfg.Validate())
}

// TestParse_DocsArchitectureExample pins the regression that filed
// issue #632: the flagship config example in docs/architecture.md §9
// uses cache.key.include_headers and must parse under the strict
// decoder (KnownFields) and validate. The YAML block is extracted from
// the doc at runtime so the test fails when the example and the real
// schema drift apart again.
func TestParse_DocsArchitectureExample(t *testing.T) {
	t.Parallel()
	wd, err := os.Getwd()
	require.NoError(t, err)
	docPath := filepath.Join(wd, "..", "..", "docs", "architecture.md")
	raw, err := os.ReadFile(docPath) //nolint:gosec // repo fixture read at test time
	require.NoError(t, err)

	re := regexp.MustCompile("(?s)```yaml\n(listen:.*?)```")
	m := re.FindSubmatch(raw)
	require.NotNil(t, m, "docs/architecture.md must contain the flagship ```yaml config example")
	example := string(m[1])

	cfg, err := Parse([]byte(example))
	require.NoError(t, err,
		"the docs/architecture.md example must parse and validate; a drift here re-files issue #632")
	require.NotEmpty(t, cfg.Routes)
	require.Equal(t, []string{"Accept-Language"}, cfg.Routes[0].Cache.Key.IncludeHeaders,
		"the example exercises cache.key.include_headers")
}

// --- negative_ttl validation ---

func TestValidate_NegativeTTLMap(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	valid, err := NegTTLMap(map[string]time.Duration{
		"404": 30 * time.Second, "5xx": 10 * time.Second,
	})
	require.NoError(t, err)
	validCfg := Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{pool},
		Routes:        []Route{{Pool: "app", Cache: RouteCache{NegativeTTL: valid}}},
	}
	require.NoError(t, validCfg.Validate())

	cases := []struct {
		name string
		m    map[string]time.Duration
	}{
		{"negative_ttl", map[string]time.Duration{"40o4": time.Second}},
		{"negative_ttl", map[string]time.Duration{"99": time.Second}},
		{"negative_ttl", map[string]time.Duration{"200": time.Second}},
		{"negative_ttl", map[string]time.Duration{"400-999": time.Second}},
		{"negative_ttl", map[string]time.Duration{"3xx": time.Second}},
		{"negative_ttl", map[string]time.Duration{"404": -time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// NegTTLMap is the programmatic path; the same parser
			// (api.NewStatusTTLMap) gates the YAML path, so the
			// rejection contract is proven here once.
			_, err := NegTTLMap(tc.m)
			require.Error(t, err, tc.name)
			if !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("error %q does not mention field %q", err, tc.name)
			}
		})
	}
}

func TestParse_NegativeTTLOneKeyTwoForms(t *testing.T) {
	t.Parallel()
	// The scalar shorthand decodes to the default error set.
	m := mustParse(t, []byte(`
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: api
    pool: app
    cache:
      negative_ttl: 30s
`)).Routes[0].Cache.NegativeTTL.Policy()
	entries := m.Entries()
	require.Len(t, entries, 4)
	for _, e := range entries {
		require.Equal(t, 30*time.Second, e.TTL, e.Key)
	}
	for _, code := range []int{404, 405, 410, 501} {
		require.True(t, m.Cacheable(code), "status %d", code)
	}

	// A bare number means seconds in both forms (durationValue shared
	// by the scalar and the map entries), so `negative_ttl: 30` is not
	// silently 30ns.
	cfg := mustParse(t, []byte(`
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: api
    pool: app
    cache:
      negative_ttl: 30
`))
	require.True(t, cfg.Routes[0].Cache.NegativeTTL.Policy().Cacheable(404))
	require.Equal(t, 30*time.Second, cfg.Routes[0].Cache.NegativeTTL.Policy().TTL(404), "bare scalar means seconds")

	// The map form is a complete per-status policy.
	cfg = mustParse(t, []byte(`
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: api
    pool: app
    cache:
      negative_ttl:
        404: 1m
        5xx: 10s
        410: 0
`))
	p := cfg.Routes[0].Cache.NegativeTTL.Policy()
	require.Len(t, p.Entries(), 3)
	require.Equal(t, time.Minute, p.TTL(404))
	require.Equal(t, 10*time.Second, p.TTL(502), "class member")
	require.Equal(t, time.Duration(0), p.TTL(410), "explicit zero disables")
	require.False(t, p.Cacheable(410))

	// The separate status_ttl key no longer exists (strict decode).
	_, err := Parse([]byte(`
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: api
    pool: app
    cache:
      status_ttl:
        404: 1m
`))
	require.Error(t, err, "status_ttl must be rejected: negative_ttl is the single key")
	require.Contains(t, err.Error(), "status_ttl")
}

// Validate resolves the route's negative-caching policy exactly once;
// consumers read it via Policy() instead of re-validating the raw map.
func TestValidate_NegativeTTLPolicyResolved(t *testing.T) {
	t.Parallel()
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	neg, err := NegTTLMap(map[string]time.Duration{
		"404": 30 * time.Second, "5xx": 10 * time.Second, "503": 0,
	})
	require.NoError(t, err)
	cfg := Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{pool},
		Routes:        []Route{{Pool: "app", Cache: RouteCache{NegativeTTL: neg}}},
	}
	require.NoError(t, cfg.Validate())
	p := cfg.Routes[0].Cache.NegativeTTL.Policy()
	require.NotNil(t, p, "Validate must resolve the policy")
	require.True(t, p.Cacheable(404))
	require.Equal(t, 10*time.Second, p.TTL(502))
	require.False(t, p.Cacheable(503), "exact zero shadows class")

	// Empty policy: no negative caching, Policy returns nil.
	cfg.Routes[0].Cache.NegativeTTL = NegTTLScalar(0)
	require.NoError(t, cfg.Validate())
	require.Nil(t, cfg.Routes[0].Cache.NegativeTTL.Policy())

	// Scalar shorthand resolves to the same expanded policy.
	cfg.Routes[0].Cache.NegativeTTL = NegTTLScalar(30 * time.Second)
	require.NoError(t, cfg.Validate())
	p = cfg.Routes[0].Cache.NegativeTTL.Policy()
	require.True(t, p.Cacheable(410))
	require.False(t, p.Cacheable(503))
}

func mustParse(t *testing.T, b []byte) *Config {
	t.Helper()
	cfg, err := Parse(b)
	require.NoError(t, err)
	return cfg
}
