package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// maxFetchTimeout is the upper bound for fetch_timeout. It must stay
// strictly below internal/server.safetyNetWriteTimeout so the write
// deadline never fires during an origin fetch. The two constants are
// duplicated across packages because the layering rules (L1 and L2
// cannot depend on each other) prevent a shared import. If you change
// one, change the other.
const maxFetchTimeout = 5 * time.Minute

// Defaults returns a Config populated with safe defaults. The
// "admin: :9000" listener is enabled so the daemon is operable even
// with an empty config file.
func Defaults() Config {
	return Config{
		Listen: Listen{
			Admin: ":9000",
		},
		TLS: TLS{
			MinVersion: "1.2",
		},
		Cluster: Cluster{
			Mode:     ClusterModeStrong,
			HopLimit: 2,
		},
		Admin: AdminConfig{
			DrainDuration: 10 * time.Second,
		},
	}
}

// defaultHotMaxBytesRatio is the fraction of GOMEMLIMIT used to derive
// the hot store budget when hot_max_bytes is unset. 75% leaves headroom
// for RSS overhead and lets the Go GC run without aggressive cycles
// near GOMEMLIMIT (issue #161).
const defaultHotMaxBytesRatio = 75

// defaultWarmMaxEntriesRatio is the fraction of GOMEMLIMIT used to derive
// the warm index entry cap when warm_max_entries is unset. 15% leaves
// headroom for the hot store (75%), Go runtime overhead, and GC
// fragmentation. At 14 GiB GOMEMLIMIT, 15% = ~16 M entries (~2 GiB heap).
const defaultWarmMaxEntriesRatio = 15

// defaultStreamingBufferRatio is the fraction of GOMEMLIMIT used to derive
// the streaming tee buffer cap when max_streaming_buffer_bytes is unset.
// 7% leaves headroom for the hot store (75%), warm index (15%), Go runtime
// overhead, and fasthttp response buffers (bytebufferpool). At 768 MiB
// GOMEMLIMIT, 7% = 53 MiB, bounding peak tee buffer allocations to ~106 MiB
// (bytes.Buffer doubles) — below the OOMKill threshold with margin for the
// in-loop cap to act before all 32 fetchSem slots fill. At 14 GiB
// GOMEMLIMIT, 7% = ~1 GiB, generous enough that the cap rarely triggers
// in normal traffic.
const defaultStreamingBufferRatio = 7

// Load reads a YAML file from path, applies Defaults, and validates.
// Strict mode rejects unknown fields so typos surface immediately.
func Load(path string) (*Config, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", path, err)
	}

	raw, err := os.ReadFile(abs) //nolint:gosec // configured path, see threat-model T15
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", abs, err)
	}
	return Parse(raw)
}

// Parse decodes YAML bytes into a Config, applying Defaults underneath
// and rejecting unknown keys. An empty input is valid and yields a
// config equal to Defaults().
//
// Environment variable interpolation is applied before YAML decoding:
// ${VAR} is replaced with the value of VAR from the process environment.
// ${VAR:-default} provides a fallback when VAR is unset or empty.
// Literal dollar signs can be escaped as $$.
func Parse(b []byte) (*Config, error) {
	expanded := expandEnvVars(b)
	cfg := Defaults()
	if len(strings.TrimSpace(string(expanded))) > 0 {
		dec := yaml.NewDecoder(strings.NewReader(string(expanded)))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("yaml decode: %w", err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Derive hot_max_bytes from GOMEMLIMIT when unset so the budget
	// adapts to the runtime memory limit of the deployment (issue #161).
	// Runs for both empty and populated configs so an empty config file
	// in a container with GOMEMLIMIT still gets an eviction budget.
	cfg.Storage.ResolveHotMaxBytes(os.Getenv("GOMEMLIMIT"))
	cfg.Storage.ResolveWarmMaxEntries(os.Getenv("GOMEMLIMIT"))
	// Derive max_streaming_buffer_bytes from GOMEMLIMIT when unset so
	// the streaming tee buffer cap adapts to the pod's memory limit
	// (PR #524 OOMKill fix). Per-route like the other cache fields.
	for i := range cfg.Routes {
		cfg.Routes[i].Cache.ResolveMaxStreamingBufferBytes(os.Getenv("GOMEMLIMIT"))
	}
	return &cfg, nil
}

// Validate runs cross-field checks. It is called by Load/Parse but is
// also useful from tests.
//
//nolint:gocyclo // 22: validation is a flat checklist of independent fields
func (c *Config) Validate() error {
	// At least one listener must be enabled. Admin is OK as a sole
	// listener when no TLS is configured.
	if c.Listen.HTTP == "" && c.Listen.HTTPS == "" &&
		c.Listen.Admin == "" {
		return errors.New("config: at least one listener must be configured")
	}

	// Upstream pool names must be unique.
	seen := make(map[string]struct{}, len(c.UpstreamPools))
	for i := range c.UpstreamPools {
		p := &c.UpstreamPools[i]
		if p.Name == "" {
			return errors.New("config: upstream pool with empty name")
		}
		if _, dup := seen[p.Name]; dup {
			return fmt.Errorf("config: duplicate upstream pool %q", p.Name)
		}
		seen[p.Name] = struct{}{}
		if len(p.Targets) == 0 {
			return fmt.Errorf("config: upstream pool %q has no targets", p.Name)
		}
		if err := validatePoolDurations(p); err != nil {
			return err
		}
	}

	// Every route must reference a declared pool.
	for i := range c.Routes {
		if err := c.validateRoute(i, seen); err != nil {
			return err
		}
	}

	// Cluster mode validation.
	if err := c.validateCluster(); err != nil {
		return err
	}

	// SO_REUSEPORT is only supported on Linux. The config package is a
	// leaf and cannot import internal/platform, so we check GOOS directly.
	// platform.ReusePortSupported mirrors this check.
	if c.Listen.ReusePort != nil && *c.Listen.ReusePort && runtime.GOOS != "linux" {
		return errors.New("config: listen.reuse_port is only supported on Linux")
	}

	// GOGC must be -1 (off) or a positive percentage. Zero is invalid
	// (would trigger GC on every allocation) and negative values other
	// than -1 are meaningless.
	if c.GOGC != nil && *c.GOGC != -1 && *c.GOGC <= 0 {
		return errors.New("config: gogc must be -1 (off) or a positive percentage")
	}

	return nil
}

// expandEnvVars replaces ${VAR} and ${VAR:-default} patterns in the
// raw config bytes with values from the process environment. $$ is
// expanded to a literal $ to allow escaping.
func expandEnvVars(b []byte) []byte {
	s := string(b)
	var sb strings.Builder
	sb.Grow(len(s))
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '$' && s[i+1] == '$' {
			sb.WriteByte('$')
			i += 2
			continue
		}
		if i+1 < len(s) && s[i] == '$' && s[i+1] == '{' {
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				sb.WriteByte(s[i])
				i++
				continue
			}
			expr := s[i+2 : i+2+end]
			name := expr
			defVal := ""
			if idx := strings.Index(expr, ":-"); idx >= 0 {
				name = expr[:idx]
				defVal = expr[idx+2:]
			}
			val := os.Getenv(name)
			if val == "" {
				val = defVal
			}
			sb.WriteString(val)
			i += 2 + end + 1
			continue
		}
		sb.WriteByte(s[i])
		i++
	}
	return []byte(sb.String())
}

// ResolveHotMaxBytes derives the hot store memory budget from the
// GOMEMLIMIT value (as exported by the Go runtime) when hot_max_bytes
// is not explicitly configured. This keeps SIEVE eviction headroom
// below the Go runtime soft memory limit so the GC does not enter a
// death spiral as the cache fills (issue #161).
//
// When hot_max_bytes is set explicitly it is kept as-is (operator
// override). When GOMEMLIMIT is empty or unparseable, HotMaxBytes is
// left unchanged so the HotStore falls back to its internal default.
// In practice the Go runtime rejects an invalid GOMEMLIMIT before the
// process starts, so the unparseable branch is unreachable for the env
// var; it is tolerated only so unit tests can pass synthetic values.
func (s *Storage) ResolveHotMaxBytes(goMemLimit string) {
	if s.HotMaxBytes > 0 {
		return
	}
	raw := strings.TrimSpace(goMemLimit)
	if raw == "" {
		return
	}
	n, err := parseByteSize(raw)
	if err != nil || n <= 0 {
		return
	}
	s.HotMaxBytes = ByteSize(n * int64(defaultHotMaxBytesRatio) / 100)
}

// ResolveWarmMaxEntries derives the warm index entry cap from the
// GOMEMLIMIT value when warm_max_entries is not explicitly configured.
// This bounds the Go heap cost of the warm index (map[uint64]warmLoc +
// SIEVE entries) to a percentage of the runtime memory limit, preventing
// unbounded heap growth that leads to OOMKill (exit 137).
//
// When warm_max_entries is set explicitly it is kept as-is (operator
// override). When GOMEMLIMIT is empty or unparseable, WarmMaxEntries is
// left unchanged (zero = unlimited). The 160 constant is inlined from
// warm.EstimatedWarmLocHeapBytes to avoid a circular import.
func (s *Storage) ResolveWarmMaxEntries(goMemLimit string) {
	if s.WarmMaxEntries > 0 {
		return
	}
	raw := strings.TrimSpace(goMemLimit)
	if raw == "" {
		return
	}
	n, err := parseByteSize(raw)
	if err != nil || n <= 0 {
		return
	}
	// 160 = warm.EstimatedWarmLocHeapBytes. Inlined to avoid circular
	// import (config -> warm -> storage). Update both if warmLoc changes.
	s.WarmMaxEntries = n * int64(defaultWarmMaxEntriesRatio) / (100 * 160)
}

// ResolveMaxStreamingBufferBytes derives the streaming tee buffer cap
// from the GOMEMLIMIT value when max_streaming_buffer_bytes is not
// explicitly configured. This bounds the total memory held in live
// streaming buffers during concurrent miss-fetches, preventing the
// OOMKill that occurs under slow-origin events when all fetchSem slots
// fill with buffered responses (PR #524).
//
// When max_streaming_buffer_bytes is set explicitly it is kept as-is
// (operator override). When GOMEMLIMIT is empty or unparseable, the
// field is left unchanged so the handler falls back to its built-in
// default (64 MiB).
func (c *RouteCache) ResolveMaxStreamingBufferBytes(goMemLimit string) {
	if c.MaxStreamingBufferBytes > 0 {
		return
	}
	raw := strings.TrimSpace(goMemLimit)
	if raw == "" {
		return
	}
	n, err := parseByteSize(raw)
	if err != nil || n <= 0 {
		return
	}
	c.MaxStreamingBufferBytes = ByteSize(n * int64(defaultStreamingBufferRatio) / 100)
}

// validateRoute checks a single route entry and normalises its fields.
// A route must specify exactly one of Pool or Static.Root.
//
//nolint:gocyclo // 16: flat checklist + name derivation, no sub-functions to extract
func (c *Config) validateRoute(i int, pools map[string]struct{}) error {
	r := &c.Routes[i]
	hasPool := r.Pool != ""
	hasStatic := r.Static.Root != ""
	if hasPool && hasStatic {
		return fmt.Errorf("config: route %d has both pool and static.root — specify exactly one", i)
	}
	if !hasPool && !hasStatic {
		return fmt.Errorf("config: route %d has no pool or static.root", i)
	}
	if hasPool {
		if _, ok := pools[r.Pool]; !ok {
			return fmt.Errorf("config: route %d references unknown pool %q", i, r.Pool)
		}
	}
	if hasStatic {
		if err := validateStatic(i, r.Static); err != nil {
			return err
		}
	}
	// Auto-derive Route.Name when empty so Prometheus metrics and the
	// dashboard have consistent route labels without requiring operators
	// to name every route.
	if r.Name == "" {
		switch {
		case r.Match.Host != "":
			r.Name = r.Match.Host + ":" + r.Match.PathPrefix
		case r.Match.PathPrefix != "":
			r.Name = r.Match.PathPrefix
		default:
			r.Name = "_catch-all"
		}
	}
	for j, m := range r.Match.Methods {
		up := strings.ToUpper(strings.TrimSpace(m))
		if !isKnownHTTPMethod(up) {
			return fmt.Errorf("config: route %d methods[%d] unknown HTTP method %q", i, j, m)
		}
		r.Match.Methods[j] = up
	}
	if sp := r.Request.StripPrefix; sp != "" && !strings.HasPrefix(sp, "/") {
		return fmt.Errorf("config: route %d strip_prefix must start with '/', got %q", i, sp)
	}
	return validateRouteCache(i, r.Cache)
}

// validateStatic validates a StaticConfig block.
func validateStatic(i int, sc StaticConfig) error {
	if !filepath.IsAbs(sc.Root) {
		return fmt.Errorf("config: route %d static.root must be an absolute path, got %q", i, sc.Root)
	}
	if sc.MaxFileSize < 0 {
		return fmt.Errorf("config: route %d static.max_file_size must be >= 0, got %s", i, sc.MaxFileSize)
	}
	for j, idx := range sc.Index {
		if strings.Contains(idx, "/") {
			return fmt.Errorf("config: route %d static.index[%d] must not contain '/', got %q", i, j, idx)
		}
	}
	return nil
}

func validateRouteCache(i int, rc RouteCache) error {
	if rc.TTLOverride < 0 {
		return fmt.Errorf("config: route %d ttl_override must be >= 0, got %v", i, rc.TTLOverride)
	}
	if rc.TTLDefault < 0 {
		return fmt.Errorf("config: route %d ttl_default must be >= 0, got %v", i, rc.TTLDefault)
	}
	if rc.StaleWhileRevalidate < 0 {
		return fmt.Errorf("config: route %d stale_while_revalidate must be >= 0, got %v", i, rc.StaleWhileRevalidate)
	}
	if rc.StaleIfError < 0 {
		return fmt.Errorf("config: route %d stale_if_error must be >= 0, got %v", i, rc.StaleIfError)
	}
	if rc.NegativeTTL < 0 {
		return fmt.Errorf("config: route %d negative_ttl must be >= 0, got %v", i, rc.NegativeTTL)
	}
	if rc.JitterPercent < 0 || rc.JitterPercent > 50 {
		return fmt.Errorf("config: route %d jitter_percent must be 0–50, got %d", i, rc.JitterPercent)
	}
	if rc.MaxResponseBytes < 0 {
		return fmt.Errorf("config: route %d max_response_bytes must be >= 0, got %s", i, rc.MaxResponseBytes)
	}
	if rc.MaxStreamingBufferBytes < 0 {
		return fmt.Errorf("config: route %d max_streaming_buffer_bytes must be >= 0, got %s", i, rc.MaxStreamingBufferBytes)
	}
	if rc.MaxFetchConcurrency < 0 {
		return fmt.Errorf("config: route %d max_fetch_concurrency must be >= 0, got %d", i, rc.MaxFetchConcurrency)
	}
	if rc.FetchTimeout < 0 {
		return fmt.Errorf("config: route %d fetch_timeout must be >= 0, got %v", i, rc.FetchTimeout)
	}
	if rc.FetchTimeout >= maxFetchTimeout {
		return fmt.Errorf("config: route %d fetch_timeout must be < %v (data plane safety-net WriteTimeout), got %v", i, maxFetchTimeout, rc.FetchTimeout)
	}
	if err := validateRouteKey(i, rc.Key); err != nil {
		return err
	}
	return validateRefreshConfig(i, rc)
}

//nolint:gocyclo // 22: validation is a flat checklist of independent fields
func validateRefreshConfig(i int, rc RouteCache) error {
	if rc.RefreshBeforeExpiry {
		if rc.TTLDefault <= 0 && rc.TTLOverride <= 0 {
			return fmt.Errorf("config: route %d refresh_before_expiry requires ttl_default or ttl_override > 0", i)
		}
	}
	if rc.RefreshMarginPercent < 0 || rc.RefreshMarginPercent > 50 {
		return fmt.Errorf("config: route %d refresh_margin_percent must be 0-50, got %d", i, rc.RefreshMarginPercent)
	}
	if rc.RefreshConcurrency < 0 || rc.RefreshConcurrency > 64 {
		return fmt.Errorf("config: route %d refresh_concurrency must be 0-64, got %d", i, rc.RefreshConcurrency)
	}
	if rc.RefreshTimeout < 0 || rc.RefreshTimeout > 120*time.Second {
		return fmt.Errorf("config: route %d refresh_timeout must be 0-120s, got %v", i, rc.RefreshTimeout)
	}
	if rc.RefreshMinHits < 0 {
		return fmt.Errorf("config: route %d refresh_min_hits must be >= 0, got %d", i, rc.RefreshMinHits)
	}
	if rc.RefreshPersistCycles < 0 {
		return fmt.Errorf("config: route %d refresh_persist_cycles must be >= 0, got %d", i, rc.RefreshPersistCycles)
	}
	if rc.RefreshPersistCycles > 0 && rc.RefreshMinHits <= 0 {
		return fmt.Errorf("config: route %d refresh_persist_cycles requires refresh_min_hits > 0", i)
	}
	if rc.RefreshMinScore < 0 {
		return fmt.Errorf("config: route %d refresh_min_score must be >= 0, got %d", i, rc.RefreshMinScore)
	}
	if rc.RefreshMinScore > 0 && rc.RefreshMinHits <= 0 {
		return fmt.Errorf("config: route %d refresh_min_score requires refresh_min_hits > 0", i)
	}
	if rc.RefreshMaxRPS < 0 || rc.RefreshMaxRPS > 10000 {
		return fmt.Errorf("config: route %d refresh_max_rps must be 0 or 1-10000, got %d", i, rc.RefreshMaxRPS)
	}
	if rc.RefreshReactiveFirst {
		if rc.StaleWhileRevalidate <= 0 {
			return fmt.Errorf("config: route %d refresh_reactive_first requires stale_while_revalidate > 0", i)
		}
		if rc.RefreshMinHits <= 0 {
			return fmt.Errorf("config: route %d refresh_reactive_first requires refresh_min_hits > 0", i)
		}
	}
	return nil
}

// validateRouteKey validates cache key construction fields on a route.
func validateRouteKey(i int, rk RouteKey) error {
	if len(rk.KeepQueryParams) > 0 {
		if len(rk.StripQueryParams) > 0 {
			return fmt.Errorf("config: route %d keep_query_params is mutually exclusive with strip_query_params", i)
		}
		if len(rk.StripQueryPrefix) > 0 {
			return fmt.Errorf("config: route %d keep_query_params is mutually exclusive with strip_query_prefix", i)
		}
	}
	if len(rk.StripQueryPrefix) > 16 {
		return fmt.Errorf("config: route %d strip_query_prefix capped at 16 entries, got %d", i, len(rk.StripQueryPrefix))
	}
	for j, p := range rk.StripQueryPrefix {
		if p == "" {
			return fmt.Errorf("config: route %d strip_query_prefix[%d] must be non-empty", i, j)
		}
	}
	return nil
}

func validatePoolDurations(p *UpstreamPool) error {
	if p.Health.Active.Interval < 0 {
		return fmt.Errorf("config: upstream pool %q health.active.interval must be >= 0, got %v", p.Name, p.Health.Active.Interval)
	}
	if p.Health.Active.Timeout < 0 {
		return fmt.Errorf("config: upstream pool %q health.active.timeout must be >= 0, got %v", p.Name, p.Health.Active.Timeout)
	}
	if p.Health.Passive.EjectFor < 0 {
		return fmt.Errorf("config: upstream pool %q health.passive.eject_for must be >= 0, got %v", p.Name, p.Health.Passive.EjectFor)
	}
	if p.Connect.Timeout < 0 {
		return fmt.Errorf("config: upstream pool %q connect.timeout must be >= 0, got %v", p.Name, p.Connect.Timeout)
	}
	if p.Connect.KeepAlive < 0 {
		return fmt.Errorf("config: upstream pool %q connect.keep_alive must be >= 0, got %v", p.Name, p.Connect.KeepAlive)
	}
	if p.Connect.ResponseHeaderTimeout < 0 {
		return fmt.Errorf("config: upstream pool %q connect.response_header_timeout must be >= 0, got %v", p.Name, p.Connect.ResponseHeaderTimeout)
	}
	if p.Connect.HedgeTimeout < 0 {
		return fmt.Errorf("config: upstream pool %q connect.hedge_timeout must be >= 0, got %v", p.Name, p.Connect.HedgeTimeout)
	}
	return nil
}

// validateCluster checks and normalises cluster configuration. The
// cluster is considered enabled when Listen.Cluster is non-empty.
func (c *Config) validateCluster() error {
	if c.Listen.Cluster != "" {
		c.Cluster.Mode = strings.TrimSpace(c.Cluster.Mode)
		switch c.Cluster.Mode {
		case ClusterModeStrong, ClusterModeEventual:
			// valid
		case "":
			c.Cluster.Mode = ClusterModeStrong
		default:
			return fmt.Errorf("config: cluster.mode must be %q or %q, got %q",
				ClusterModeStrong, ClusterModeEventual, c.Cluster.Mode)
		}
	} else if c.Cluster.Mode != "" && c.Cluster.Mode != ClusterModeStrong {
		return fmt.Errorf("config: cluster.mode %q requires listen.cluster to be set", c.Cluster.Mode)
	}
	if c.Cluster.Mode == "" {
		c.Cluster.Mode = ClusterModeStrong
	}
	if c.Cluster.HandoffQueueDepth < 0 {
		return fmt.Errorf("config: cluster.handoff_queue_depth must be >= 0 (0 = default), got %d",
			c.Cluster.HandoffQueueDepth)
	}
	if c.Cluster.HandoffQueueDepth > maxHandoffQueueDepth {
		return fmt.Errorf("config: cluster.handoff_queue_depth must be <= %d, got %d (each slot costs a pointer + message header per peer)",
			maxHandoffQueueDepth, c.Cluster.HandoffQueueDepth)
	}
	if err := c.validatePeerFetchConfig(); err != nil {
		return err
	}
	if err := c.validateClusterStorage(); err != nil {
		return err
	}
	if err := validateEvictionAlgorithm(&c.Storage); err != nil {
		return err
	}
	return nil
}

// validateClusterStorage checks the cluster storage sync settings.
// Extracted from validateCluster to keep cyclomatic complexity under
// the gocyclo limit.
func (c *Config) validateClusterStorage() error {
	if c.Storage.WarmSyncInterval < -1 {
		return fmt.Errorf("config: storage.warm_sync_interval must be >= -1 (-1 = disabled), got %v", c.Storage.WarmSyncInterval)
	}
	if c.Storage.WarmSyncBatchSize < 0 {
		return fmt.Errorf("config: storage.warm_sync_batch_size must be >= 0, got %v", c.Storage.WarmSyncBatchSize)
	}
	if c.Storage.WALSyncInterval < -1 {
		return fmt.Errorf("config: storage.wal_sync_interval must be >= -1 (-1 = synchronous mode), got %v", c.Storage.WALSyncInterval)
	}
	if c.Storage.TombstoneQueueSize < 0 {
		return fmt.Errorf("config: storage.tombstone_queue_size must be >= 0 (0 = default 65536), got %v", c.Storage.TombstoneQueueSize)
	}
	if c.Storage.TombstoneDrainInterval < -1 {
		return fmt.Errorf("config: storage.tombstone_drain_interval must be >= -1 (-1 = disabled), got %v", c.Storage.TombstoneDrainInterval)
	}
	return nil
}

// validatePeerFetchConfig checks the peer fetch pipelining settings
// (ADR-0039). Extracted from validateCluster to keep cyclomatic
// complexity under the gocyclo limit.
func (c *Config) validatePeerFetchConfig() error {
	if c.Cluster.PeerMaxConnsPerHost < 0 {
		return fmt.Errorf("config: cluster.peer_max_conns_per_host must be >= 0 (0 = default 8), got %d",
			c.Cluster.PeerMaxConnsPerHost)
	}
	if c.Cluster.PeerMaxIdleConnDuration < 0 {
		return fmt.Errorf("config: cluster.peer_max_idle_conn_duration must be >= 0 (0 = default 120s), got %v",
			c.Cluster.PeerMaxIdleConnDuration)
	}
	return nil
}

// validateEvictionAlgorithm checks the eviction policy selection for
// both tiers. The shared EvictionAlgorithm sets the default for both
// tiers; HotEvictionAlgorithm and WarmEvictionAlgorithm override it
// per-tier. All three accept "", "sieve", or "cachaner".
//
// This function is a pure check — it does not mutate s.
func validateEvictionAlgorithm(s *Storage) error {
	for _, algo := range []string{s.EvictionAlgorithm, s.HotEvictionAlgorithm, s.WarmEvictionAlgorithm} {
		switch algo {
		case "", "sieve", "cachaner":
			// valid
		default:
			return fmt.Errorf("config: storage eviction_algorithm must be \"sieve\" or \"cachaner\", got %q", algo)
		}
	}
	return nil
}

// isKnownHTTPMethod returns true for standard HTTP methods accepted in
// route match.methods. Non-standard methods are rejected at parse time.
func isKnownHTTPMethod(m string) bool {
	switch m {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE":
		return true
	}
	return false
}

// ---- ByteSize YAML unmarshalling ----

// UnmarshalYAML implements yaml.Unmarshaler for ByteSize. Accepted
// forms: an integer (bytes) or a string suffixed with B/KB/KiB/MB/MiB/
// GB/GiB/TB/TiB/Ko/Mo/Go/To (case-insensitive).
func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		raw := strings.TrimSpace(value.Value)
		if raw == "" {
			*b = 0
			return nil
		}
		n, err := parseByteSize(raw)
		if err != nil {
			return fmt.Errorf("config: invalid byte size %q: %w", raw, err)
		}
		*b = ByteSize(n)
		return nil
	default:
		return fmt.Errorf("config: byte size must be scalar, got kind %d", value.Kind)
	}
}

// Bytes returns the value as a plain int64.
func (b ByteSize) Bytes() int64 { return int64(b) }

// IsZero reports whether the ByteSize is zero so yaml.v3 omitempty works.
func (b ByteSize) IsZero() bool { return int64(b) == 0 }

// MarshalYAML emits ByteSize as its human-readable string form (e.g. "2Go")
// so the YAML representation matches what operators write in config files.
func (b ByteSize) MarshalYAML() (interface{}, error) {
	if b == 0 {
		return 0, nil
	}
	return b.String(), nil
}

// MarshalJSON emits ByteSize as its human-readable string form so the
// JSON representation in the dashboard matches the YAML representation.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	if b == 0 {
		return []byte("0"), nil
	}
	return json.Marshal(b.String())
}

// String returns a human-readable representation of the byte size.
func (b ByteSize) String() string {
	v := int64(b)
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.0fGo", float64(v)/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.0fMo", float64(v)/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.0fKo", float64(v)/(1<<10))
	default:
		return fmt.Sprintf("%dB", v)
	}
}

// byteSizeUnits maps the uppercased unit suffix to its multiplier.
// Unknown units are rejected by parseByteSize.
var byteSizeUnits = map[string]float64{
	"":    1,
	"B":   1,
	"K":   1e3,
	"KB":  1e3,
	"KI":  1 << 10,
	"KIB": 1 << 10,
	"KO":  1e3,
	"M":   1e6,
	"MB":  1e6,
	"MI":  1 << 20,
	"MIB": 1 << 20,
	"MO":  1e6,
	"G":   1e9,
	"GB":  1e9,
	"GI":  1 << 30,
	"GIB": 1 << 30,
	"GO":  1e9,
	"T":   1e12,
	"TB":  1e12,
	"TI":  1 << 40,
	"TIB": 1 << 40,
	"TO":  1e12,
}

func parseByteSize(s string) (int64, error) {
	// Numeric-only — treat as bytes.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}

	// Find the split between number and unit.
	i := 0
	for i < len(s) && (s[i] == '-' || s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	num := strings.TrimSpace(s[:i])
	unit := strings.ToUpper(strings.TrimSpace(s[i:]))
	if num == "" {
		return 0, fmt.Errorf("missing number")
	}
	val, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", num, err)
	}
	mult, ok := byteSizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	return int64(val * mult), nil
}
