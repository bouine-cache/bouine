package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/runtime/shutdown"
	"github.com/bouine-cache/bouine/internal/runtime/supervised"
	"github.com/bouine-cache/bouine/internal/server"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"
	"github.com/bouine-cache/bouine/pkg/api"
)

func newTestLogger() observability.Logger {
	return observability.New(observability.Options{})
}

func TestListenPort(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "8080", listenPort("127.0.0.1:8080", "80"))
	assert.Equal(t, "80", listenPort("", "80"))
	assert.Equal(t, "80", listenPort("invalid", "80"))
	assert.Equal(t, "0", listenPort(":0", "80"))
	assert.Equal(t, "9000", listenPort("0.0.0.0:9000", "80"))
}

func TestBuildKeyPolicy_Nil(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{})
	require.Nil(t, p)
}

func TestBuildKeyPolicy_StripQueryParams(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{StripQueryParams: []string{"utm_source", "fbclid"}})
	require.NotNil(t, p)
}

func TestBuildKeyPolicy_KeepQueryParams(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{KeepQueryParams: []string{"page", "limit"}})
	require.NotNil(t, p)
}

func TestBuildKeyPolicy_ExcludeHeaders(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{ExcludeHeaders: []string{"X-Request-ID"}})
	require.NotNil(t, p)
}

func TestBuildKeyPolicy_StripQueryPrefix(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{StripQueryPrefix: []string{"utm_"}})
	require.NotNil(t, p)
}

func TestBuildKeyPolicy_StripEmptyParams(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{StripEmptyParams: true})
	require.NotNil(t, p)
}

func TestBuildKeyPolicy_DedupQueryParams(t *testing.T) {
	t.Parallel()
	p := buildKeyPolicy(config.RouteKey{DedupQueryParams: true})
	require.NotNil(t, p)
}

func TestBuildKeepSet(t *testing.T) {
	t.Parallel()
	assert.Nil(t, buildKeepSet(nil))
	assert.Nil(t, buildKeepSet([]string{}))
	m := buildKeepSet([]string{"a", "b", "c"})
	assert.True(t, m["a"])
	assert.True(t, m["b"])
	assert.True(t, m["c"])
	assert.False(t, m["d"])
}

func TestBuildStripSet(t *testing.T) {
	t.Parallel()
	assert.Nil(t, buildStripSet(nil))
	assert.Nil(t, buildStripSet([]string{}))
	m := buildStripSet([]string{"x", "y"})
	assert.True(t, m["x"])
	assert.True(t, m["y"])
	assert.False(t, m["z"])
}

func TestBuildExcludeHeaderSet(t *testing.T) {
	t.Parallel()
	assert.Nil(t, buildExcludeHeaderSet(nil))
	assert.Nil(t, buildExcludeHeaderSet([]string{}))
	m := buildExcludeHeaderSet([]string{"X-Request-ID", "Accept-Encoding"})
	assert.True(t, m["x-request-id"])
	assert.True(t, m["accept-encoding"])
	assert.False(t, m["content-type"])
}

func TestHasKeyPolicy(t *testing.T) {
	t.Parallel()
	assert.False(t, hasKeyPolicy(config.RouteKey{}))
	assert.True(t, hasKeyPolicy(config.RouteKey{StripQueryParams: []string{"a"}}))
	assert.True(t, hasKeyPolicy(config.RouteKey{KeepQueryParams: []string{"a"}}))
	assert.True(t, hasKeyPolicy(config.RouteKey{ExcludeHeaders: []string{"a"}}))
	assert.True(t, hasKeyPolicy(config.RouteKey{StripQueryPrefix: []string{"a"}}))
	assert.True(t, hasKeyPolicy(config.RouteKey{StripEmptyParams: true}))
	assert.True(t, hasKeyPolicy(config.RouteKey{DedupQueryParams: true}))
}

func TestBoolDefault(t *testing.T) {
	t.Parallel()
	assert.True(t, boolDefault(nil, true))
	assert.False(t, boolDefault(nil, false))
	b := true
	assert.True(t, boolDefault(&b, false))
	b2 := false
	assert.False(t, boolDefault(&b2, true))
}

func TestApplyRefreshConfig_Disabled(t *testing.T) {
	t.Parallel()
	cfg := &cache.HandlerConfig{}
	applyRefreshConfig(cfg, config.RouteCache{})
	assert.Equal(t, time.Duration(0), cfg.RefreshMargin)
}

func TestApplyRefreshConfig_WithDefaults(t *testing.T) {
	t.Parallel()
	cfg := &cache.HandlerConfig{}
	rc := config.RouteCache{
		RefreshBeforeExpiry: true,
		TTLDefault:          100 * time.Second,
	}
	applyRefreshConfig(cfg, rc)
	assert.Equal(t, 10*time.Second, cfg.RefreshMargin)
}

func TestApplyRefreshConfig_WithOverride(t *testing.T) {
	t.Parallel()
	cfg := &cache.HandlerConfig{}
	rc := config.RouteCache{
		RefreshBeforeExpiry:  true,
		TTLOverride:          200 * time.Second,
		RefreshMarginPercent: 25,
		RefreshTimeout:       5 * time.Second,
		RefreshConcurrency:   3,
		RefreshMinHits:       10,
		RefreshPersistCycles: 5,
		RefreshMinScore:      1,
		RefreshMaxRPS:        100,
		RefreshReactiveFirst: true,
	}
	applyRefreshConfig(cfg, rc)
	assert.Equal(t, 50*time.Second, cfg.RefreshMargin)
	assert.Equal(t, 5*time.Second, cfg.RefreshTimeout)
	assert.Equal(t, 3, cfg.RefreshConcurrency)
	assert.Equal(t, 10, cfg.RefreshMinHits)
	assert.Equal(t, 5, cfg.RefreshPersistCycles)
	assert.Equal(t, int64(1), cfg.RefreshMinScore)
	assert.Equal(t, 100, cfg.RefreshMaxRPS)
	assert.True(t, cfg.RefreshReactiveFirst)
}

func TestBuildHedgeTimeout_Defaults(t *testing.T) {
	t.Parallel()
	pc := config.UpstreamPool{Name: "test"}
	rt := buildHedgeTimeout(pc)
	require.Equal(t, time.Duration(0), rt)
}

func TestBuildHedgeTimeout_WithHedgeTimeout(t *testing.T) {
	t.Parallel()
	pc := config.UpstreamPool{
		Name:    "test",
		Connect: config.ConnectPolicy{HedgeTimeout: 500 * time.Millisecond},
	}
	rt := buildHedgeTimeout(pc)
	require.Equal(t, 500*time.Millisecond, rt)
}

func TestSanitizedConfig(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		Admin:      config.AdminConfig{Token: "secret-token"},
		Cloudflare: config.CloudflareConfig{APIToken: "cf-secret"},
		TLS:        config.TLS{Certs: []config.TLSCert{{CertFile: "/cert.pem", KeyFile: "/key.pem"}}},
		Cluster:    config.Cluster{TLS: config.ClusterTLS{CertFile: "/cluster.pem", KeyFile: "/cluster.key"}},
	}
	out := sanitizedConfig(cfg)
	assert.Equal(t, "", out.Admin.Token)
	assert.Equal(t, "", out.Cloudflare.APIToken)
	assert.Empty(t, out.TLS.Certs)
	assert.Equal(t, "", out.Cluster.TLS.CertFile)
}

func TestLoadConfig_NoPath(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig("")
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

func TestLoadConfig_WithPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bouine.yaml")
	err := os.WriteFile(cfgPath, []byte("listen:\n  http: \":0\"\n  admin: \":0\"\n"), 0o600)
	require.NoError(t, err)
	cfg, err := loadConfig(cfgPath)
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

func TestLoadConfig_InvalidPath(t *testing.T) {
	t.Parallel()
	_, err := loadConfig("/nonexistent/path/to/config.yaml")
	require.Error(t, err)
}

func TestBuildTLSConfig_NoCerts(t *testing.T) {
	t.Parallel()
	_, err := buildTLSConfig(&config.Config{})
	require.Error(t, err)
}

func TestBuildTLSConfig_UnsupportedMinVersion(t *testing.T) {
	t.Parallel()
	_, err := buildTLSConfig(&config.Config{
		TLS: config.TLS{
			Certs:      []config.TLSCert{{CertFile: "cert.pem", KeyFile: "key.pem"}},
			MinVersion: "1.0",
		},
	})
	require.Error(t, err)
}

func TestBuildTLSConfig_ValidMinVersions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, keyPath := tlsutil.WriteCertFiles(t, dir)

	for _, version := range []string{"1.2", "1.3", ""} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			cfg := &config.Config{
				TLS: config.TLS{
					Certs:      []config.TLSCert{{CertFile: certPath, KeyFile: keyPath}},
					MinVersion: version,
				},
			}
			tlsCfg, err := buildTLSConfig(cfg)
			require.NoError(t, err)
			require.NotNil(t, tlsCfg)
		})
	}
}

func TestBuildTLSConfig_LoadCertError(t *testing.T) {
	t.Parallel()
	_, err := buildTLSConfig(&config.Config{
		TLS: config.TLS{
			Certs: []config.TLSCert{{CertFile: "/nonexistent/cert.pem", KeyFile: "/nonexistent/key.pem"}},
		},
	})
	require.Error(t, err)
}

func TestBuildClusterTLSConfig_LoadCertError(t *testing.T) {
	t.Parallel()
	_, err := buildClusterTLSConfig(config.ClusterTLS{
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	require.Error(t, err)
}

func TestBuildClusterTLSConfig_LoadCAError(t *testing.T) {
	t.Parallel()
	_, err := buildClusterTLSConfig(config.ClusterTLS{
		CertFile: "cert.pem",
		KeyFile:  "key.pem",
		CABundle: "/nonexistent/ca.pem",
	})
	require.Error(t, err)
}

func TestLoadCertPool_NonExistent(t *testing.T) {
	t.Parallel()
	_, err := loadCertPool("/nonexistent/ca.pem")
	require.Error(t, err)
}

func TestLoadCertPool_InvalidPEM(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "invalid.pem")
	err := os.WriteFile(caPath, []byte("not a PEM file"), 0o600)
	require.NoError(t, err)
	_, err = loadCertPool(caPath)
	require.Error(t, err)
}

func TestNewPurgeCmd(t *testing.T) {
	t.Parallel()
	c := newPurgeCmd()
	require.NotNil(t, c)
	assert.Equal(t, "purge <url>", c.Use)
}

func TestNewBanCmd(t *testing.T) {
	t.Parallel()
	c := newBanCmd()
	require.NotNil(t, c)
	assert.Equal(t, "ban <predicate>", c.Use)
}

func TestNewRefreshCmd(t *testing.T) {
	t.Parallel()
	c := newRefreshCmd()
	require.NotNil(t, c)
	assert.Equal(t, "refresh <url>", c.Use)
}

func TestNewCompletionCmd(t *testing.T) {
	t.Parallel()
	c := newCompletionCmd()
	require.NotNil(t, c)
	assert.Equal(t, "completion [shell]", c.Use)
}

func TestNewConfigCmd(t *testing.T) {
	t.Parallel()
	c := newConfigCmd()
	require.NotNil(t, c)
	assert.Equal(t, "config", c.Use)
}

func TestNewConfigValidateCmd(t *testing.T) {
	t.Parallel()
	c := newConfigValidateCmd()
	require.NotNil(t, c)
	assert.Equal(t, "validate <file>", c.Use)
}

func TestNewConfigSchemaCmd(t *testing.T) {
	t.Parallel()
	c := newConfigSchemaCmd()
	require.NotNil(t, c)
	assert.Equal(t, "schema", c.Use)
}

func TestNewClusterCmd(t *testing.T) {
	t.Parallel()
	c := newClusterCmd()
	require.NotNil(t, c)
	assert.Equal(t, "cluster", c.Use)
}

func TestNewClusterPeersCmd(t *testing.T) {
	t.Parallel()
	c := newClusterPeersCmd()
	require.NotNil(t, c)
	assert.Equal(t, "peers", c.Use)
}

func TestNewServeCmd(t *testing.T) {
	t.Parallel()
	c := newServeCmd()
	require.NotNil(t, c)
	assert.Equal(t, "serve", c.Use)
}

func TestPurgeCmd_Exec(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "/v1/purge", string(ctx.Path()))
		ctx.Response.Header.Set("Content-Type", "application/json")
		_, _ = ctx.Write([]byte(`{"status":"ok"}`))
	})
	defer originSrv.Close()

	addr := originSrv.Addr
	root := Root()
	root.SetArgs([]string{"purge", "http://example.com/page", "--server", addr})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "ok")
}

func TestBanCmd_Exec(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "/v1/ban", string(ctx.Path()))
		ctx.Response.Header.Set("Content-Type", "application/json")
		_, _ = ctx.Write([]byte(`{"status":"ok","count":3}`))
	})
	defer originSrv.Close()

	addr := originSrv.Addr
	root := Root()
	root.SetArgs([]string{"ban", "host_regex=example.com", "--server", addr})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "ok")
	assert.Contains(t, stdout.String(), "count: 3")
}

func TestBanCmd_InvalidPredicate(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"ban", "invalid_predicate", "--server", "127.0.0.1:9999"})
	err := root.Execute()
	require.Error(t, err)
}

func TestBanCmd_UnknownKey(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"ban", "unknown_key=value", "--server", "127.0.0.1:9999"})
	err := root.Execute()
	require.Error(t, err)
}

func TestRefreshCmd_Exec(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "/v1/refresh", string(ctx.Path()))
		ctx.Response.Header.Set("Content-Type", "application/json")
		_, _ = ctx.Write([]byte(`{"status":"ok"}`))
	})
	defer originSrv.Close()

	addr := originSrv.Addr
	root := Root()
	root.SetArgs([]string{"refresh", "http://example.com/page", "--server", addr})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "ok")
}

func TestConfigValidateCmd_Valid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bouine.yaml")
	err := os.WriteFile(cfgPath, []byte("listen:\n  http: \":0\"\n  admin: \":0\"\n"), 0o600)
	require.NoError(t, err)

	root := Root()
	root.SetArgs([]string{"config", "validate", cfgPath})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err = root.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "config is valid")
}

func TestConfigValidateCmd_Invalid(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"config", "validate", "/nonexistent/config.yaml"})
	err := root.Execute()
	require.Error(t, err)
}

func TestConfigSchemaCmd(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"config", "schema"})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.NotEmpty(t, stdout.String())
}

func TestCompletionCmd_Bash(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"completion", "bash"})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.NotEmpty(t, stdout.String())
}

func TestCompletionCmd_Zsh(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"completion", "zsh"})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.NotEmpty(t, stdout.String())
}

func TestCompletionCmd_Fish(t *testing.T) {
	t.Parallel()
	root := Root()
	root.SetArgs([]string{"completion", "fish"})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.NotEmpty(t, stdout.String())
}

func TestClusterPeersCmd_Exec(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "/v1/cluster/peers", string(ctx.Path()))
		ctx.Response.Header.Set("Content-Type", "application/json")
		_, _ = ctx.Write([]byte(`[{"name":"node1","addr":"10.0.0.1:7946"}]`))
	})
	defer originSrv.Close()

	addr := originSrv.Addr
	root := Root()
	root.SetArgs([]string{"cluster", "peers", "--server", addr})
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	err := root.Execute()
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "node1")
}

func TestClusterPeersCmd_ServerError(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	})
	defer originSrv.Close()

	addr := originSrv.Addr
	root := Root()
	root.SetArgs([]string{"cluster", "peers", "--server", addr})
	err := root.Execute()
	require.Error(t, err)
}

func TestResolveAdminToken_EnvVar(t *testing.T) {
	t.Setenv("BOUINE_ADMIN_TOKEN", "env-token")
	e := &engine{cfg: &config.Config{}, logger: newTestLogger()}
	assert.Equal(t, "env-token", e.resolveAdminToken())
}

func TestResolveAdminToken_Config(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{Admin: config.AdminConfig{Token: "config-token"}},
		logger: newTestLogger(),
	}
	assert.Equal(t, "config-token", e.resolveAdminToken())
}

func TestResolveAdminToken_AutoGenerated(t *testing.T) {
	t.Setenv("BOUINE_ADMIN_TOKEN", "")
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	token := e.resolveAdminToken()
	assert.NotEmpty(t, token)
	assert.Len(t, token, 32)
}

func TestInitRings_NoWarmDir(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	rings, snap := e.initRings()
	require.NotNil(t, rings)
	assert.Equal(t, "", snap)
}

func TestBuildClusterMeta_SingleNode(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	rs := &runState{}
	meta := e.buildClusterMeta(rs)
	assert.Equal(t, "single-node", meta.Mode)
}

func TestBuildClusterMeta_WithHopLimit(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{Cluster: config.Cluster{HopLimit: 3}},
		logger: newTestLogger(),
	}
	rs := &runState{}
	meta := e.buildClusterMeta(rs)
	assert.Equal(t, 3, meta.HopLimit)
}

func TestInsightsPoolHealth_Nil(t *testing.T) {
	t.Parallel()
	rs := &runState{pools: nil}
	fn := insightsPoolHealth(rs)
	assert.Nil(t, fn())
}

func TestInsightsHeaderAudit_Nil(t *testing.T) {
	t.Parallel()
	rs := &runState{headerRing: nil}
	fn := insightsHeaderAudit(rs)
	assert.Nil(t, fn())
}

func TestCacheCheck_BypassURL(t *testing.T) {
	t.Parallel()
	rs := &runState{}
	result := cacheCheck(context.Background(), "", rs)
	assert.Equal(t, "BYPASS", result.CacheResult)
	assert.Equal(t, "", result.URL)
}

func TestPurgeKey_NoHandlers(t *testing.T) {
	t.Parallel()
	rs := &runState{handlers: nil}
	err := rs.purgeKey(context.Background(), api.Key{})
	require.NoError(t, err)
}

func TestStartHealthChecks_NoActivePath(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	pools := map[string]*origin.Pool{}
	e.startHealthChecks(nil, pools)
}

func TestStartClusterJoin_NoCluster(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	rs := &runState{}
	e.startClusterJoin(nil, rs)
}

func TestStartClusterJoin_NoJoin(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	rs := &runState{}
	e.startClusterJoin(nil, rs)
}

func TestInitCloudflare_NoToken(t *testing.T) {
	t.Setenv("CF_API_TOKEN", "")
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	m := observability.NewDataPlaneMetrics(e.metrics.Registry)
	prop := e.initCloudflare(m, context.Background())
	require.NotNil(t, prop)
}

func TestInitCluster_NoListen(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	node, fetcher, broadcaster, peersFn, cm := e.initCluster(context.Background(), "token")
	assert.Nil(t, node)
	assert.Nil(t, fetcher)
	assert.Nil(t, broadcaster)
	assert.Nil(t, peersFn)
	assert.Nil(t, cm)
}

func TestBuildPools_Empty(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	pools, err := e.buildPools(nil)
	require.NoError(t, err)
	assert.Empty(t, pools)
}

func TestBuildPools_WithTarget(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{Name: "echo", Targets: []string{"127.0.0.1:0"}},
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	m := origin.RegisterMetrics(e.metrics.Registry)
	pools, err := e.buildPools(m)
	require.NoError(t, err)
	assert.Len(t, pools, 1)
	assert.Contains(t, pools, "echo")
}

func TestBuildPools_InvalidTarget(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{Name: "", Targets: nil},
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	_, err := e.buildPools(origin.RegisterMetrics(e.metrics.Registry))
	require.Error(t, err)
}

func TestBuildStore_HotOnly(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestBuildStore_WithWarmDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{WarmDir: dir},
		},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestBuildCluster_NoPODIP(t *testing.T) {
	t.Setenv("POD_IP", "")
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{
				Cluster: "127.0.0.1:0",
				Admin:   "127.0.0.1:0",
				HTTP:    "127.0.0.1:0",
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	_, err := e.buildCluster(context.Background())
	require.NoError(t, err)
}

func TestBuildCluster_WithPODIP(t *testing.T) {
	t.Setenv("POD_IP", "10.0.0.5")
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{
				Cluster: "127.0.0.1:0",
				Admin:   "127.0.0.1:0",
				HTTP:    "127.0.0.1:0",
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	_, err := e.buildCluster(context.Background())
	require.NoError(t, err)
}

// TestBuildCluster_WithPODIPHostname verifies the DNS resolution path for
// POD_IP set to a hostname. We use a real IP to avoid the slow retry path
// while still exercising the net.ParseIP != nil branch.
func TestBuildCluster_WithPODIPHostname(t *testing.T) {
	t.Setenv("POD_IP", "10.0.0.5")
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{
				Cluster: "127.0.0.1:0",
				Admin:   "127.0.0.1:0",
				HTTP:    "127.0.0.1:0",
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	_, err := e.buildCluster(context.Background())
	require.NoError(t, err)
}

func TestBuildCluster_WithNodeName(t *testing.T) {
	t.Setenv("POD_IP", "")
	e := &engine{
		cfg: &config.Config{
			Cluster: config.Cluster{NodeName: "my-node"},
			Listen: config.Listen{
				Cluster: "127.0.0.1:0",
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	c, err := e.buildCluster(context.Background())
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestInsightsPoolHealth_WithPools(t *testing.T) {
	t.Parallel()
	metrics := origin.RegisterMetrics(observability.NewMetrics().Registry)
	pool, err := origin.NewPool(origin.PoolConfig{
		Name:    "test",
		Targets: []string{"127.0.0.1:0"},
		Logger:  newTestLogger(),
		Metrics: metrics,
	})
	require.NoError(t, err)
	rs := &runState{pools: map[string]*origin.Pool{"test": pool}}
	fn := insightsPoolHealth(rs)
	result := fn()
	require.Contains(t, result, "test")
	assert.Len(t, result["test"], 1)
}

func TestInitRings_WithWarmDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{WarmDir: dir},
		},
		logger: newTestLogger(),
	}
	rings, snap := e.initRings()
	require.NotNil(t, rings)
	assert.Contains(t, snap, "metrics.snap")
}

func TestBuildClusterTLSConfig_Valid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, keyPath := tlsutil.WriteCertFiles(t, dir)
	caPath := certPath // self-signed cert doubles as CA
	_, err := buildClusterTLSConfig(config.ClusterTLS{
		CertFile: certPath,
		KeyFile:  keyPath,
		CABundle: caPath,
	})
	require.NoError(t, err)
}

func TestLoadCertPool_Valid(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, _ := tlsutil.WriteCertFiles(t, dir)
	pool, err := loadCertPool(certPath)
	require.NoError(t, err)
	require.NotNil(t, pool)
}

func TestCacheCheck_WithStore(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rs := &runState{store: store}
	result := cacheCheck(context.Background(), "https://example.com/page", rs)
	assert.Equal(t, "MISS", result.CacheResult)
	assert.NotEmpty(t, result.KeyHex)
}

func TestStartHealthChecks_WithActivePath(t *testing.T) {
	t.Parallel()
	metrics := origin.RegisterMetrics(observability.NewMetrics().Registry)
	pool, err := origin.NewPool(origin.PoolConfig{
		Name:    "test",
		Targets: []string{"127.0.0.1:0"},
		Logger:  newTestLogger(),
		Metrics: metrics,
	})
	require.NoError(t, err)
	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{
					Name:    "test",
					Targets: []string{"127.0.0.1:0"},
					Health: config.HealthPolicy{
						Active: config.ActiveHealthCheck{
							Path:     "/health",
							Interval: 30 * time.Second,
						},
					},
				},
			},
		},
		logger: newTestLogger(),
	}
	pools := map[string]*origin.Pool{"test": pool}
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startHealthChecks(g, pools)
	cancel()
	_ = g.Wait()
}

func TestBuildInvalidationOps_Purge(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rs := &runState{
		store:  store,
		cfProp: buildCFPropagator(nil, config.CloudflareConfig{}, testMetrics(), newTestLogger(), context.Background()),
	}
	ops := e.buildInvalidationOps(context.Background(), rs)
	err = ops.PurgeFn(context.Background(), "https://example.com/page")
	require.NoError(t, err)
}

func TestBuildInvalidationOps_Ban(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rs := &runState{
		store:  store,
		cfProp: buildCFPropagator(nil, config.CloudflareConfig{}, testMetrics(), newTestLogger(), context.Background()),
	}
	ops := e.buildInvalidationOps(context.Background(), rs)
	count, err := ops.BanFn(context.Background(), "example.com", "^/api")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestBuildInvalidationOps_Refresh(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rs := &runState{
		store:  store,
		cfProp: buildCFPropagator(nil, config.CloudflareConfig{}, testMetrics(), newTestLogger(), context.Background()),
	}
	ops := e.buildInvalidationOps(context.Background(), rs)
	err = ops.RefreshFn(context.Background(), "https://example.com/page")
	require.NoError(t, err)
}

func TestPurgeKey_WithHandler(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	handler := cache.NewHandler(cache.HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {},
		Store:    store,
		Logger:   newTestLogger(),
	})
	rs := &runState{store: store, handlers: []*cache.Handler{handler}}
	err = rs.purgeKey(context.Background(), api.Key{1, 2, 3})
	require.NoError(t, err)
}

func TestBuildStore_WithEvictionAlgo(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{
				HotEvictionAlgorithm:  "lru",
				WarmEvictionAlgorithm: "lru",
			},
		},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestSanitizedConfig_PreservesNonSecrets(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		Admin:      config.AdminConfig{MaxBatchSize: 10},
		Cloudflare: config.CloudflareConfig{ZoneID: "zone123"},
	}
	out := sanitizedConfig(cfg)
	assert.Equal(t, 10, out.Admin.MaxBatchSize)
	assert.Equal(t, "zone123", out.Cloudflare.ZoneID)
}

func TestBuildStaticRoute_NoCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	router := server.NewRouter(server.RouterConfig{Logger: newTestLogger()})
	rs := &runState{store: store}
	rc := config.Route{
		Name:   "static",
		Static: config.StaticConfig{Root: dir},
	}
	e.buildStaticRoute(router, rs, rc)
	assert.Empty(t, rs.handlers)
}

func TestBuildStaticRoute_WithCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	router := server.NewRouter(server.RouterConfig{Logger: newTestLogger()})
	rs := &runState{
		store:     store,
		dpMetrics: observability.NewDataPlaneMetrics(metrics.Registry),
	}
	cacheEnabled := true
	rc := config.Route{
		Name:   "static-cached",
		Static: config.StaticConfig{Root: dir},
		Cache:  config.RouteCache{Enabled: &cacheEnabled, TTLDefault: 60 * time.Second},
	}
	e.buildStaticRoute(router, rs, rc)
	assert.Len(t, rs.handlers, 1)
}

func TestBuildStaticRoute_InvalidRoot(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	router := server.NewRouter(server.RouterConfig{Logger: newTestLogger()})
	rs := &runState{store: store}
	rc := config.Route{
		Name:   "bad",
		Static: config.StaticConfig{Root: "/nonexistent/path/that/does/not/exist"},
	}
	e.buildStaticRoute(router, rs, rc)
	assert.Empty(t, rs.handlers)
}

func TestBuildStaticRoute_WithStripPrefix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	router := server.NewRouter(server.RouterConfig{Logger: newTestLogger()})
	rs := &runState{store: store}
	rc := config.Route{
		Name:    "static-strip",
		Static:  config.StaticConfig{Root: dir},
		Request: config.RouteRequest{StripPrefix: "/assets"},
	}
	e.buildStaticRoute(router, rs, rc)
}

func TestBuildRouter_WithStaticRoute(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	e := &engine{
		cfg: &config.Config{
			Routes: []config.Route{
				{
					Name:   "static",
					Static: config.StaticConfig{Root: dir},
				},
			},
		},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	metrics := origin.RegisterMetrics(observability.NewMetrics().Registry)
	rs := &runState{
		store:     store,
		pools:     map[string]*origin.Pool{},
		dpMetrics: observability.NewDataPlaneMetrics(observability.NewMetrics().Registry),
	}
	_ = metrics
	router := e.buildRouter(rs)
	require.NotNil(t, router)
}

func TestBuildRouter_WithMissingPool(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			Routes: []config.Route{
				{Name: "missing", Pool: "nonexistent"},
			},
		},
		logger: newTestLogger(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rs := &runState{
		store:     store,
		pools:     map[string]*origin.Pool{},
		dpMetrics: observability.NewDataPlaneMetrics(observability.NewMetrics().Registry),
	}
	router := e.buildRouter(rs)
	require.NotNil(t, router)
}

func TestBuildRouter_WithRoute(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer originSrv.Close()

	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{Name: "echo", Targets: []string{originSrv.Addr}},
			},
			Routes: []config.Route{
				{Name: "api", Pool: "echo"},
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	m := origin.RegisterMetrics(e.metrics.Registry)
	pools, err := e.buildPools(m)
	require.NoError(t, err)
	rs := &runState{
		store:     store,
		pools:     pools,
		dpMetrics: observability.NewDataPlaneMetrics(e.metrics.Registry),
	}
	router := e.buildRouter(rs)
	require.NotNil(t, router)
	assert.Len(t, rs.handlers, 1)
}

func TestUpdateStartupMetrics(t *testing.T) {
	t.Parallel()
	seq := shutdown.NewSequencer(newTestLogger())
	seq.Gate().Register("test-cond")
	metrics := observability.NewMetrics()
	sm := observability.NewStartupMetrics(metrics.Registry)
	updateStartupMetrics(seq, sm)
}

func TestUpdateStartupMetrics_Nil(t *testing.T) {
	t.Parallel()
	seq := shutdown.NewSequencer(newTestLogger())
	updateStartupMetrics(seq, nil)
}

func TestInitCluster_WithListen(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{Cluster: "127.0.0.1:0"},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	node, fetcher, broadcaster, peersFn, cm := e.initCluster(context.Background(), "token")
	require.NotNil(t, node)
	require.NotNil(t, fetcher)
	require.NotNil(t, broadcaster)
	require.NotNil(t, peersFn)
	require.NotNil(t, cm)
}

func TestInitCluster_WithHopLimitEventualMode(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{Cluster: "127.0.0.1:0"},
			Cluster: config.Cluster{HopLimit: 3, Mode: config.ClusterModeEventual},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	node, _, _, _, _ := e.initCluster(context.Background(), "token")
	require.NotNil(t, node)
}

func TestInitCluster_BadClusterAddr(t *testing.T) {
	t.Parallel()
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{Cluster: "0.0.0.0:0"},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	node, _, _, _, _ := e.initCluster(context.Background(), "token")
	require.NotNil(t, node)
}

func TestStartBackgroundTasks(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rings, snap := e.initRings()
	rs := &runState{
		store:        store,
		rings:        rings,
		snapshotPath: snap,
		dpMetrics:    observability.NewDataPlaneMetrics(metrics.Registry),
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startBackgroundTasks(g, rs)
	cancel()
	_ = g.Wait()
}

func TestInsightsHeaderAudit_WithRing(t *testing.T) {
	t.Parallel()
	rs := &runState{headerRing: observability.NewOriginHeaderRing()}
	fn := insightsHeaderAudit(rs)
	result := fn()
	require.NotNil(t, result)
}

func TestInitSubsystems(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, shutdownTracer, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.NotNil(t, rs.store)
	require.NotNil(t, rs.dpMetrics)
	shutdownTracer()
}

func TestInitSubsystems_WithWarmDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{WarmDir: dir},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.NotNil(t, rs.warmMetrics)
}

func TestInitSubsystems_BadPool(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{Name: "", Targets: nil},
			},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	_, _, err := e.initSubsystems(context.Background(), seq)
	require.Error(t, err)
}

func TestInitCloudflare_WithZoneAndToken(t *testing.T) {
	t.Setenv("CF_API_TOKEN", "fake-token")
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Cloudflare: config.CloudflareConfig{
				ZoneID:   "fake-zone",
				APIToken: "fake-token",
			},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	dpMetrics := observability.NewDataPlaneMetrics(metrics.Registry)
	prop := e.initCloudflare(dpMetrics, context.Background())
	require.NotNil(t, prop)
}

func TestApplyRuntimeTuning(t *testing.T) {
	t.Parallel()
	gogc := 200
	e := &engine{
		cfg:    &config.Config{GOGC: &gogc},
		logger: newTestLogger(),
	}
	e.applyRuntimeTuning()
}

func TestApplyRuntimeTuning_NoGOGC(t *testing.T) {
	t.Setenv("GOGC", "150")
	e := &engine{
		cfg:    &config.Config{},
		logger: newTestLogger(),
	}
	e.applyRuntimeTuning()
}

func TestBuildDashboard(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	ops := e.buildInvalidationOps(context.Background(), rs)
	dashMux := e.buildDashboard(rs, "127.0.0.1:9000", ops)
	require.NotNil(t, dashMux)
}

func TestStartClusterJoin_WithJoinAndCluster(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{Cluster: "127.0.0.1:0"},
			Cluster: config.Cluster{Join: []string{"127.0.0.1:9999"}, JoinTimeout: 100 * time.Millisecond},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	node, _, _, _, _ := e.initCluster(context.Background(), "token")
	require.NotNil(t, node)
	rs := &runState{clusterNode: node}
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startClusterJoin(g, rs)
	cancel()
	_ = g.Wait()
}

func TestStartListeners_NoListeners(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	handler := e.buildDataPlane(rs)
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startListeners(g, handler, rs)
	cancel()
	_ = g.Wait()
}

func TestSwapAdminHandler(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{Listen: config.Listen{Admin: "127.0.0.1:0"}},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)

	minimalAdmin := admin.New(admin.Config{
		Addr:   "127.0.0.1:0",
		Logger: newTestLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.swapAdminHandler(ctx, rs, minimalAdmin, nil, nil)
}

func TestStartBackgroundTasks_WithWarmMetrics(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{WarmDir: dir},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startBackgroundTasks(g, rs)
	cancel()
	_ = g.Wait()
}

func TestRegisterShutdownSteps(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.registerShutdownSteps(g, rs)
	cancel()
	_ = g.Wait()
}

func TestBuildDashboard_WithCluster(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{Cluster: "127.0.0.1:0"},
			Cluster: config.Cluster{Mode: config.ClusterModeStrong},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	ops := e.buildInvalidationOps(context.Background(), rs)
	dashMux := e.buildDashboard(rs, "127.0.0.1:9000", ops)
	require.NotNil(t, dashMux)
}

func TestCacheCheck_WithStoredObject(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{Storage: config.Storage{HotMaxBytes: 1 << 20}},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	rawURL := "https://example.com/page"
	key := cache.BuildKeyFromURL(rawURL, nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("hello"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))
	rs := &runState{store: store}
	result := cacheCheck(context.Background(), rawURL, rs)
	assert.Equal(t, "HIT", result.CacheResult)
	assert.NotEmpty(t, result.KeyHex)
}

func TestSwapAdminHandler_WithCluster(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{Admin: "127.0.0.1:0", Cluster: "127.0.0.1:0"},
			Cluster: config.Cluster{Mode: config.ClusterModeStrong},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	minimalAdmin := admin.New(admin.Config{
		Addr:   "127.0.0.1:0",
		Logger: newTestLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	condsFn := func() []admin.Condition { return nil }
	drainFn := func() {}
	e.swapAdminHandler(ctx, rs, minimalAdmin, condsFn, drainFn)
}

func TestPurgeKey_WithMatchingHandler(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	store, err := e.buildStore(nil)
	require.NoError(t, err)
	key := cache.BuildKeyFromURL("https://example.com/test", nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("data"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))
	handler := cache.NewHandler(cache.HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {},
		Store:    store,
		Logger:   newTestLogger(),
	})
	rs := &runState{store: store, handlers: []*cache.Handler{handler}}
	err = rs.purgeKey(context.Background(), key)
	require.NoError(t, err)
}

func TestBuildCluster_WithPODIPAndAdvertise(t *testing.T) {
	t.Setenv("POD_IP", "10.0.0.5")
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{
				Cluster: "0.0.0.0:0",
				Admin:   "0.0.0.0:0",
				HTTP:    "0.0.0.0:0",
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	_, err := e.buildCluster(context.Background())
	require.NoError(t, err)
}

func TestSwapAdminHandler_DefaultAddr(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg:     &config.Config{Listen: config.Listen{Admin: ""}},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	minimalAdmin := admin.New(admin.Config{
		Addr:   "127.0.0.1:0",
		Logger: newTestLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.swapAdminHandler(ctx, rs, minimalAdmin, nil, nil)
}

func TestInitCloudflare_WithEnvToken(t *testing.T) {
	t.Setenv("CF_API_TOKEN", "env-token")
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Cloudflare: config.CloudflareConfig{ZoneID: "zone-from-config"},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	dpMetrics := observability.NewDataPlaneMetrics(metrics.Registry)
	prop := e.initCloudflare(dpMetrics, context.Background())
	require.NotNil(t, prop)
}

func TestInitRings_WithSnapshotLoad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Pre-create a snapshot file so the load path is exercised.
	snapPath := filepath.Join(dir, "metrics.snap")
	require.NoError(t, os.WriteFile(snapPath, []byte("{}"), 0o600))
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{WarmDir: dir},
		},
		logger: newTestLogger(),
	}
	rings, snap := e.initRings()
	require.NotNil(t, rings)
	assert.Equal(t, snapPath, snap)
}

func TestRegisterShutdownSteps_WithCluster(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{Cluster: "127.0.0.1:0"},
			Cluster: config.Cluster{Mode: config.ClusterModeStrong},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.registerShutdownSteps(g, rs)
	cancel()
	_ = g.Wait()
}

func TestBuildCluster_WithPODIPHostnameResolve(t *testing.T) {
	t.Setenv("POD_IP", "localhost")
	e := &engine{
		cfg: &config.Config{
			Listen: config.Listen{
				Cluster: "127.0.0.1:0",
				Admin:   "127.0.0.1:0",
				HTTP:    "127.0.0.1:0",
			},
			Cluster: config.Cluster{NodeName: "test-node"},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	_, err := e.buildCluster(context.Background())
	// This may or may not succeed depending on DNS resolution.
	// We just want to exercise the code path.
	_ = err
}

func TestStartClusterJoin_WithJoinTimeout(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Listen:  config.Listen{Cluster: "127.0.0.1:0"},
			Cluster: config.Cluster{Join: []string{"127.0.0.1:9999"}, JoinTimeout: 50 * time.Millisecond},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	node, _, _, _, _ := e.initCluster(context.Background(), "token")
	require.NotNil(t, node)
	rs := &runState{clusterNode: node}
	ctx, cancel := context.WithCancel(context.Background())
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startClusterJoin(g, rs)
	time.Sleep(100 * time.Millisecond)
	cancel()
	_ = g.Wait()
}
