package config

import (
	"os"
	"path/filepath"
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
	require.Contains(t, err.Error(), "cluster.mode must be")
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
		{"negative_ttl", func(rc *RouteCache) { rc.NegativeTTL = -1 }},
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
		{"connect.response_header_timeout", func(p *UpstreamPool) { p.Connect.ResponseHeaderTimeout = -1 }},
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
	require.Contains(t, err.Error(), "cluster.mode must be")
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
