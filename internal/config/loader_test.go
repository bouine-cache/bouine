package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaults_AdminListenerEnabled(t *testing.T) {
	t.Parallel()
	d := Defaults()
	if d.Listen.Admin == "" {
		t.Fatal("admin listener should be enabled by default")
	}
}

func TestParse_EmptyYieldsDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Listen.Admin == "" {
		t.Fatal("expected admin default")
	}
}

func TestParse_RejectsUnknownKeys(t *testing.T) {
	t.Parallel()
	_, err := Parse([]byte("nonsensical_field: 1\n"))
	if err == nil {
		t.Fatal("expected error on unknown key")
	}
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
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := cfg.Storage.HotMaxBytes.Bytes(); got != 2_000_000_000 {
		t.Fatalf("HotMaxBytes = %d, want %d", got, 2_000_000_000)
	}
	if len(cfg.Routes) != 1 || cfg.Routes[0].Pool != "app" {
		t.Fatalf("unexpected routes: %+v", cfg.Routes)
	}
	if cfg.UpstreamPools[0].Health.Active.Interval.Seconds() != 5 {
		t.Fatalf("interval = %v", cfg.UpstreamPools[0].Health.Active.Interval)
	}
}

func TestLoad_FromDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	if err := os.WriteFile(path, []byte("listen:\n  admin: ':9001'\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen.Admin != ":9001" {
		t.Fatalf("admin = %q", cfg.Listen.Admin)
	}
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
		if b.Bytes() != tc.want {
			t.Errorf("%q -> %d, want %d", tc.in, b.Bytes(), tc.want)
		}
	}
}

func TestClusterMode_DefaultIsStrong(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	if cfg.Cluster.Mode != ClusterModeStrong {
		t.Fatalf("default cluster mode = %q, want %q", cfg.Cluster.Mode, ClusterModeStrong)
	}
}

func TestClusterMode_EmptyDefaultsToStrong(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Cluster.Mode != ClusterModeStrong {
		t.Fatalf("empty mode = %q, want %q", cfg.Cluster.Mode, ClusterModeStrong)
	}
}

func TestClusterMode_ValidModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{ClusterModeStrong, ClusterModeEventual} {
		cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true, Mode: mode}}
		if err := cfg.Validate(); err != nil {
			t.Errorf("mode %q: unexpected error: %v", mode, err)
		}
		if cfg.Cluster.Mode != mode {
			t.Errorf("mode %q: got %q", mode, cfg.Cluster.Mode)
		}
	}
}

func TestClusterMode_InvalidValue(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true, Mode: "invalid"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid cluster mode")
	}
	if !strings.Contains(err.Error(), "cluster.mode must be") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterMode_NonStrongRequiresEnabled(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: false, Mode: ClusterModeEventual}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for eventual mode without cluster enabled")
	}
	if !strings.Contains(err.Error(), "requires cluster.enabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClusterMode_StrongWithoutEnabled(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: false, Mode: ClusterModeStrong}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("strong mode without enabled should be valid: %v", err)
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Routes) != 1 {
		t.Fatalf("expected 1 route")
	}
	if cfg.Routes[0].Cache.TTLOverride != 1*60*60*1e9 {
		t.Errorf("TTLOverride = %v, want 1h", cfg.Routes[0].Cache.TTLOverride)
	}
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Routes[0].Request.StripPrefix != "/api/v1" {
		t.Errorf("StripPrefix = %q, want /api/v1", cfg.Routes[0].Request.StripPrefix)
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Routes[0].Match.Methods) != 0 {
		t.Errorf("empty methods should be nil/empty, got %v", cfg.Routes[0].Match.Methods)
	}
}

// --- HotMaxBytes GOMEMLIMIT derivation (issue #161) ---

func TestResolveHotMaxBytes_DerivesFromGomemLimitDefaultRatio(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		limit string
		want  int64
		ratio int
	}{
		{"24GiB default 75%", "24GiB", int64(24<<30) * 75 / 100, 0},
		{"3GiB default 75%", "3GiB", int64(3<<30) * 75 / 100, 0},
		{"24GiB ratio 70", "24GiB", int64(24<<30) * 70 / 100, 70},
		{"24GiB ratio 80", "24GiB", int64(24<<30) * 80 / 100, 80},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := Storage{HotMaxBytesRatio: tc.ratio}
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
	if got := s.HotMaxBytes.Bytes(); got != 1<<30 {
		t.Fatalf("explicit override clobbered: got %d, want %d", got, 1<<30)
	}
}

func TestResolveHotMaxBytes_NoGomemLimitStaysZero(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveHotMaxBytes("")
	if s.HotMaxBytes.Bytes() != 0 {
		t.Fatalf("expected 0 when GOMEMLIMIT unset, got %d", s.HotMaxBytes.Bytes())
	}
}

func TestResolveHotMaxBytes_InvalidGomemLimitIgnored(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveHotMaxBytes("garbage")
	if s.HotMaxBytes.Bytes() != 0 {
		t.Fatalf("expected 0 for invalid GOMEMLIMIT, got %d", s.HotMaxBytes.Bytes())
	}
}

func TestParse_DerivesHotMaxBytesFromGomemLimitEnv(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "24GiB")
	yamlSrc := `
listen:
  admin: ":9000"
storage:
  eviction: sieve
`
	cfg, err := Parse([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := int64(24<<30) * 75 / 100
	if got := cfg.Storage.HotMaxBytes.Bytes(); got != want {
		t.Fatalf("HotMaxBytes = %d, want %d (24GiB*0.75)", got, want)
	}
}

func TestParse_EmptyConfigDerivesHotMaxBytesFromGomemLimit(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "3GiB")
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := int64(3<<30) * 75 / 100
	if got := cfg.Storage.HotMaxBytes.Bytes(); got != want {
		t.Fatalf("HotMaxBytes = %d, want %d (3GiB*0.75)", got, want)
	}
}

func TestResolveHotMaxBytes_PlainIntegerBytes(t *testing.T) {
	t.Parallel()
	// The Go runtime's GOMEMLIMIT is a plain byte count when no unit
	// suffix is supplied.
	s := Storage{}
	s.ResolveHotMaxBytes("3221225472") // 3 GiB
	want := int64(3221225472) * 75 / 100
	if got := s.HotMaxBytes.Bytes(); got != want {
		t.Fatalf("HotMaxBytes = %d, want %d", got, want)
	}
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
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := cfg.Storage.HotMaxBytes.Bytes(); got != 2<<30 {
		t.Fatalf("explicit hot_max_bytes overridden by GOMEMLIMIT: got %d, want %d", got, 2<<30)
	}
}

func TestValidate_HotMaxBytesRatioOutOfRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		ratio int
	}{
		{"negative", -1},
		{"over 100", 101},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Listen:  Listen{Admin: ":9000"},
				Storage: Storage{HotMaxBytesRatio: tc.ratio},
			}
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected error for ratio %d", tc.ratio)
			}
			if !strings.Contains(err.Error(), "hot_max_bytes_ratio") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCluster_FullMode_Rejected(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: Listen{Admin: ":9000"}, Cluster: Cluster{Enabled: true, Mode: "full"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for mode 'full' which has been removed")
	}
	if !strings.Contains(err.Error(), "full mode has been removed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWALSyncInterval_NegativeRejected(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000"},
		Storage: Storage{WALSyncInterval: -2 * time.Second},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for wal_sync_interval < -1")
	}
	if !strings.Contains(err.Error(), "wal_sync_interval") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWALSyncInterval_NegativeOneAccepted(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:  Listen{Admin: ":9000"},
		Storage: Storage{WALSyncInterval: -1},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("wal_sync_interval = -1 should be accepted, got: %v", err)
	}
}

func TestResolveWarmMaxEntries_DerivesFromGomemLimitDefaultRatio(t *testing.T) {
	t.Parallel()
	// 14 GiB * 15% / (100 * 128) = 14*1024^3 * 15 / 12800 = ~16,777,216 entries
	limit := int64(14 << 30)
	want := limit * 15 / (100 * 128)
	s := Storage{}
	s.ResolveWarmMaxEntries("14GiB")
	if s.WarmMaxEntries != want {
		t.Fatalf("WarmMaxEntries = %d, want %d", s.WarmMaxEntries, want)
	}
}

func TestResolveWarmMaxEntries_CustomRatio(t *testing.T) {
	t.Parallel()
	limit := int64(14 << 30)
	want := limit * 10 / (100 * 128)
	s := Storage{WarmMaxEntriesRatio: 10}
	s.ResolveWarmMaxEntries("14GiB")
	if s.WarmMaxEntries != want {
		t.Fatalf("WarmMaxEntries = %d, want %d", s.WarmMaxEntries, want)
	}
}

func TestResolveWarmMaxEntries_ExplicitOverrideKept(t *testing.T) {
	t.Parallel()
	s := Storage{WarmMaxEntries: 5_000_000}
	s.ResolveWarmMaxEntries("14GiB")
	if s.WarmMaxEntries != 5_000_000 {
		t.Fatalf("explicit override clobbered: got %d, want 5000000", s.WarmMaxEntries)
	}
}

func TestResolveWarmMaxEntries_NoGomemLimitStaysZero(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveWarmMaxEntries("")
	if s.WarmMaxEntries != 0 {
		t.Fatalf("expected 0 when GOMEMLIMIT unset, got %d", s.WarmMaxEntries)
	}
}

func TestResolveWarmMaxEntries_InvalidGomemLimitIgnored(t *testing.T) {
	t.Parallel()
	s := Storage{}
	s.ResolveWarmMaxEntries("garbage")
	if s.WarmMaxEntries != 0 {
		t.Fatalf("expected 0 for invalid GOMEMLIMIT, got %d", s.WarmMaxEntries)
	}
}

func TestValidate_WarmMaxEntriesRatioRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		ratio int
		ok    bool
	}{
		{"zero is valid (use default)", 0, true},
		{"1 is valid", 1, true},
		{"100 is valid", 100, true},
		{"negative is invalid", -1, false},
		{"101 is invalid", 101, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := Defaults()
			c.Storage.WarmMaxEntriesRatio = tc.ratio
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
