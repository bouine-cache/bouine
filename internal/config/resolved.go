// Package-level resolved configuration. Resolved is the fully
// materialized view of a validated Config: every zero-means-default
// field is replaced by its effective value, negative disable sentinels
// become explicit bools, per-tier overrides are collapsed, and
// cross-field derivations (per-route fetch timeout inheriting the
// pool's response-header timeout) are computed once.
//
// Resolved is the single source of truth for /v1/config and for the
// cmd-layer builders. It is secret-free by construction: tokens, cert
// paths, and key paths have no counterpart here.
package config

import (
	"os"
	"time"
)

// Effective defaults for consumer-read fields, mirrored from the
// consumer packages because config is a leaf and cannot import them.
// Each constant names its mirror; if you change one, change the other.
const (
	// DefaultDialTimeout mirrors origin.defaultDialTimeout (10s).
	DefaultDialTimeout = 10 * time.Second
	// DefaultKeepAlive mirrors origin.defaultKeepAlive (30s).
	DefaultKeepAlive = 30 * time.Second
	// DefaultMaxConnsPerHost mirrors origin.defaultOriginMaxConnsPerHost (64).
	DefaultMaxConnsPerHost = 64
	// DefaultMaxIdleConnDuration mirrors origin.DefaultMaxIdleConnDuration (90s).
	DefaultMaxIdleConnDuration = 90 * time.Second
	// DefaultResponseHeaderTimeout mirrors origin.DefaultResponseHeaderTimeout (30s).
	DefaultResponseHeaderTimeout = 30 * time.Second
	// DefaultFetchWaitTimeout mirrors cache.defaultFetchWaitTimeout (100ms).
	DefaultFetchWaitTimeout = 100 * time.Millisecond
	// DefaultMaxResponseBytes mirrors cache.defaultMaxResponseBytes (4 MiB).
	DefaultMaxResponseBytes = ByteSize(4 << 20)
	// DefaultFetchConcurrency mirrors cache.defaultFetchConcurrency (32).
	DefaultFetchConcurrency = 32
	// DefaultStreamingBufferBytes mirrors cache.defaultMaxStreamingBufferBytes (64 MiB),
	// used when GOMEMLIMIT is unset so the derived cap has a floor.
	DefaultStreamingBufferBytes = ByteSize(64 << 20)
	// DefaultAdminIdleTimeout mirrors admin.DefaultAdminIdleTimeout (300s).
	DefaultAdminIdleTimeout = 300 * time.Second
	// DefaultJoinTimeout mirrors cmd defaultJoinTimeout (120s).
	DefaultJoinTimeout = 120 * time.Second
	// DefaultHandoffQueueDepth mirrors cluster.defaultHandoffQueueDepth (4096).
	DefaultHandoffQueueDepth = 4096
	// DefaultRefreshMarginPercent is the refresh fire point as a
	// percentage of TTL (mirrors the builder's inline default of 10).
	DefaultRefreshMarginPercent = 10
)

// Resolved is the fully materialized effective configuration. Produce
// it with Config.Resolve after validation.
//
//nolint:govet // fieldalignment: built once at boot and read by /v1/config; grouped by section for readability, byte savings irrelevant
type Resolved struct {
	Listen Listen
	TLS    ResolvedTLS
	Admin  ResolvedAdmin
	// Cloudflare carries the CF propagation settings; the API tokens
	// have no counterpart in the resolved tree (secret hygiene).
	Cloudflare ResolvedCloudflare
	Storage    ResolvedStorage
	Cluster    ResolvedCluster
	Pools      []ResolvedPool
	Routes     []ResolvedRoute
	// GOGC is the validated GOGC setting; nil means leave the Go
	// runtime default untouched.
	GOGC *int
}

// ResolvedCloudflare carries the non-secret Cloudflare propagation
// settings.
type ResolvedCloudflare struct {
	Async  *bool
	ZoneID string
}

// ResolvedTLS carries the effective minimum TLS version. Cert and key
// paths are deliberately absent (secret hygiene).
type ResolvedTLS struct {
	MinVersion TLSVersion
}

// ResolvedAdmin carries the admin-plane settings with the idle timeout
// materialized.
type ResolvedAdmin struct {
	// IdleTimeout is always non-zero: zero config resolves to
	// DefaultAdminIdleTimeout.
	IdleTimeout        time.Duration
	DrainDuration      time.Duration
	MaxBatchSize       int
	MaxBodyBytes       int
	RateLimitPerSecond int
	PprofEnabled       bool
}

// ResolvedStorage carries the storage section with per-tier eviction
// overrides collapsed to concrete algorithms and disable sentinels
// turned into explicit bools. Durations keep the raw config value when
// zero (the storage layer owns the zero-default for those); -1 never
// appears.
type ResolvedStorage struct {
	HotEvictionAlgorithm   EvictionAlgorithm
	WarmEvictionAlgorithm  EvictionAlgorithm
	WarmDir                string
	HotMaxBytes            ByteSize
	WarmMaxBytes           ByteSize
	WarmMaxDiskBytes       ByteSize
	MinFreeDisk            ByteSize
	WarmPreallocate        ByteSize
	BodyThreshold          ByteSize
	WarmMaxEntries         int64
	SegmentCacheSize       int
	WarmSyncBatchSize      int
	WarmSyncInterval       time.Duration
	WALSyncInterval        time.Duration
	CompactInterval        time.Duration
	CompactStartupDelay    time.Duration
	CheckpointInterval     time.Duration
	CheckpointWALThreshold int64
	TombstoneQueueSize     int
	TombstoneDrainInterval time.Duration
	HotMmapSlab            bool
	// WarmSyncDisabled is true when warm_sync_interval was -1.
	WarmSyncDisabled bool
	// WALSyncPerEntry is true when wal_sync_interval was -1
	// (synchronous per-entry fsync mode).
	WALSyncPerEntry bool
	// CompactionDisabled is true when compact_interval was -1.
	CompactionDisabled bool
	// CompactStartImmediate is true when compact_startup_delay was -1.
	CompactStartImmediate bool
	// CheckpointingDisabled is true when checkpoint_interval was -1.
	CheckpointingDisabled bool
}

// ResolvedCluster carries the cluster section with the mode and
// queue-depth defaults materialized.
type ResolvedCluster struct {
	// Mode is always non-empty: zero config resolves to
	// ClusterModeStrong.
	Mode     ClusterMode
	NodeName string
	Join     []string
	HopLimit int
	// JoinTimeout is always non-zero: zero config resolves to
	// DefaultJoinTimeout.
	JoinTimeout time.Duration
	// HandoffQueueDepth is always non-zero: zero config resolves to
	// DefaultHandoffQueueDepth.
	HandoffQueueDepth       int
	PeerMaxConnsPerHost     int
	PeerFetchConcurrency    int
	PeerMaxIdleConnDuration time.Duration
	BanTTL                  time.Duration
}

// ResolvedPool carries one upstream pool with its connect policy
// materialized; every duration/int is the effective value the origin
// pool will run with.
type ResolvedPool struct {
	Name                  string
	Targets               []string
	Consecutive5xx        int
	EjectFor              time.Duration
	HedgeTimeout          time.Duration
	DialTimeout           time.Duration
	KeepAlive             time.Duration
	MaxConnsPerHost       int
	MaxIdleConnDuration   time.Duration
	ResponseHeaderTimeout time.Duration
}

// ResolvedRoute carries one route with match/pool wiring and the cache
// knobs concretized.
type ResolvedRoute struct {
	Name       string
	Host       string
	PathPrefix string
	Methods    []string
	Pool       string
	// Static section (raw): static-file routes bypass the cache unless
	// explicitly enabled.
	Static               StaticConfig
	StripPrefix          string
	PathRewrite          PathRewriteConfig
	RequestHeaderSet     map[string]string
	RequestHeaderRemove  []string
	ResponseHeaderSet    map[string]string
	ResponseHeaderRemove []string
	Cache                ResolvedRouteCache
}

// ResolvedRouteCache is the route's cache section with every
// zero-means-default knob materialized.
//
//nolint:govet // fieldalignment: built once at boot; grouping pointer aggregates first keeps the section readable, byte savings irrelevant
type ResolvedRouteCache struct {
	// Field order groups the pointer-bearing aggregates first
	// (fieldalignment-optimal); layout perf is irrelevant on this
	// boot-once path, but keep leading pointer bytes contiguous.
	Key                  RouteKey
	NegativeTTL          NegativeTTLConfig
	Enabled              *bool
	TTLDefault           time.Duration
	TTLOverride          time.Duration
	StaleWhileRevalidate time.Duration
	StaleIfError         time.Duration
	// FetchTimeout is the effective per-route origin-wait bound: an
	// explicit fetch_timeout wins, otherwise the pool's resolved
	// response_header_timeout (never the cache layer's 60s fallback,
	// which exists only for pool-less callers).
	FetchTimeout time.Duration
	// FetchWaitTimeout is always non-zero: zero resolves to
	// DefaultFetchWaitTimeout.
	FetchWaitTimeout time.Duration
	// MaxStreamingBufferBytes is the effective streaming tee cap:
	// GOMEMLIMIT derivation (7%) when set, else DefaultStreamingBufferBytes.
	MaxStreamingBufferBytes ByteSize
	// RefreshMargin is the absolute time before expiry at which the
	// background refresh fires (RefreshMarginPercent applied to the
	// effective TTL basis), 0 when refresh_before_expiry is off.
	RefreshMargin  time.Duration
	RefreshTimeout time.Duration
	// MaxObjectSize keeps the raw value: zero means no limit (documented
	// behavior, not a defaulting sentinel).
	MaxObjectSize ByteSize
	// MaxResponseBytes is always non-zero: zero resolves to
	// DefaultMaxResponseBytes.
	MaxResponseBytes ByteSize
	RefreshMinScore  int64
	JitterPercent    int
	// MaxFetchConcurrency is always non-zero: zero resolves to
	// DefaultFetchConcurrency.
	MaxFetchConcurrency  int
	RefreshMinHits       int
	RefreshPersistCycles int
	RefreshMaxRPS        int
	RefreshConcurrency   int
	AllowSetCookie       bool
	RefreshBeforeExpiry  bool
	RefreshReactiveFirst bool
	StayinAlive          bool
}

// Resolve materializes the effective configuration. It must be called
// on a validated Config (Validate returned nil); non-validated input
// can produce nonsense values without an error. Resolve is idempotent
// with respect to the GOMEMLIMIT derivations Parse applies.
func (c *Config) Resolve() *Resolved {
	r := &Resolved{
		Listen: c.Listen,
		TLS: ResolvedTLS{
			MinVersion: TLSVersion(c.TLS.MinVersion),
		},
		Admin: ResolvedAdmin{
			IdleTimeout:        resolveDuration(c.Admin.IdleTimeout, DefaultAdminIdleTimeout),
			DrainDuration:      c.Admin.DrainDuration,
			MaxBatchSize:       c.Admin.MaxBatchSize,
			MaxBodyBytes:       c.Admin.MaxBodyBytes,
			RateLimitPerSecond: c.Admin.RateLimitPerSecond,
			PprofEnabled:       c.Admin.PprofEnabled,
		},
		Cloudflare: ResolvedCloudflare{
			ZoneID: c.Cloudflare.ZoneID,
			Async:  c.Cloudflare.Async,
		},
		Storage: c.resolveStorage(),
		Cluster: c.resolveCluster(),
		GOGC:    c.GOGC,
		Pools:   make([]ResolvedPool, 0, len(c.UpstreamPools)),
		Routes:  make([]ResolvedRoute, 0, len(c.Routes)),
	}
	if r.TLS.MinVersion == "" {
		r.TLS.MinVersion = TLSVersion1_2
	}

	memLimit := os.Getenv("GOMEMLIMIT")
	storage := c.Storage
	storage.ResolveHotMaxBytes(memLimit)
	storage.ResolveWarmMaxEntries(memLimit)
	r.Storage.HotMaxBytes = storage.HotMaxBytes
	r.Storage.WarmMaxEntries = storage.WarmMaxEntries

	for i := range c.UpstreamPools {
		r.Pools = append(r.Pools, resolvePool(&c.UpstreamPools[i]))
	}
	for i := range c.Routes {
		r.Routes = append(r.Routes, c.resolveRoute(&c.Routes[i], memLimit))
	}
	return r
}

// resolveStorage materializes the storage section.
func (c *Config) resolveStorage() ResolvedStorage {
	s := c.Storage
	hot := s.EvictionAlgorithm
	if s.HotEvictionAlgorithm != "" {
		hot = s.HotEvictionAlgorithm
	}
	warm := s.EvictionAlgorithm
	if s.WarmEvictionAlgorithm != "" {
		warm = s.WarmEvictionAlgorithm
	}
	if hot == "" {
		hot = EvictionSieve
	}
	if warm == "" {
		warm = EvictionSieve
	}
	return ResolvedStorage{
		HotEvictionAlgorithm:   hot,
		WarmEvictionAlgorithm:  warm,
		WarmMaxBytes:           s.WarmMaxBytes,
		WarmMaxDiskBytes:       s.WarmMaxDiskBytes,
		MinFreeDisk:            s.MinFreeDisk,
		WarmPreallocate:        s.WarmPreallocate,
		BodyThreshold:          s.BodyThreshold,
		SegmentCacheSize:       s.SegmentCacheSize,
		WarmDir:                s.WarmDir,
		HotMmapSlab:            s.HotMmapSlab,
		WarmSyncInterval:       resolveNonNegative(s.WarmSyncInterval),
		WarmSyncDisabled:       s.WarmSyncInterval == -1,
		WarmSyncBatchSize:      s.WarmSyncBatchSize,
		WALSyncInterval:        resolveNonNegative(s.WALSyncInterval),
		WALSyncPerEntry:        s.WALSyncInterval == -1,
		CompactInterval:        resolveNonNegative(s.CompactInterval),
		CompactionDisabled:     s.CompactInterval == -1,
		CompactStartupDelay:    resolveNonNegative(s.CompactStartupDelay),
		CompactStartImmediate:  s.CompactStartupDelay == -1,
		CheckpointInterval:     resolveNonNegative(s.CheckpointInterval),
		CheckpointingDisabled:  s.CheckpointInterval == -1,
		CheckpointWALThreshold: s.CheckpointWALThreshold,
		TombstoneQueueSize:     s.TombstoneQueueSize,
		TombstoneDrainInterval: resolveNonNegative(s.TombstoneDrainInterval),
	}
}

// resolveCluster materializes the cluster section.
func (c *Config) resolveCluster() ResolvedCluster {
	cl := c.Cluster
	mode := cl.Mode
	if mode == "" {
		mode = ClusterModeStrong
	}
	joinTimeout := cl.JoinTimeout
	if joinTimeout == 0 {
		joinTimeout = DefaultJoinTimeout
	}
	handoff := cl.HandoffQueueDepth
	if handoff == 0 {
		handoff = DefaultHandoffQueueDepth
	}
	return ResolvedCluster{
		Mode:                    mode,
		NodeName:                cl.NodeName,
		Join:                    cl.Join,
		HopLimit:                cl.HopLimit,
		JoinTimeout:             joinTimeout,
		HandoffQueueDepth:       handoff,
		PeerMaxConnsPerHost:     cl.PeerMaxConnsPerHost,
		PeerFetchConcurrency:    cl.PeerFetchConcurrency,
		PeerMaxIdleConnDuration: cl.PeerMaxIdleConnDuration,
		BanTTL:                  cl.BanTTL,
	}
}

// resolvePool materializes one upstream pool's connect policy.
func resolvePool(p *UpstreamPool) ResolvedPool {
	return ResolvedPool{
		Name:                  p.Name,
		Targets:               p.Targets,
		Consecutive5xx:        p.Health.Passive.Consecutive5xx,
		EjectFor:              p.Health.Passive.EjectFor,
		HedgeTimeout:          p.Connect.HedgeTimeout,
		DialTimeout:           resolveDuration(p.Connect.Timeout, DefaultDialTimeout),
		KeepAlive:             resolveDuration(p.Connect.KeepAlive, DefaultKeepAlive),
		MaxConnsPerHost:       resolvePositive(p.Connect.MaxConnections, DefaultMaxConnsPerHost),
		MaxIdleConnDuration:   resolveDuration(p.Connect.MaxIdleConnDuration, DefaultMaxIdleConnDuration),
		ResponseHeaderTimeout: resolveDuration(p.Connect.ResponseHeaderTimeout, DefaultResponseHeaderTimeout),
	}
}

// resolveRoute materializes one route, including the cross-field
// fetch-timeout inheritance from the route's pool.
func (c *Config) resolveRoute(rc *Route, memLimit string) ResolvedRoute {
	poolConn := DefaultResponseHeaderTimeout
	for i := range c.UpstreamPools {
		if c.UpstreamPools[i].Name == rc.Pool {
			poolConn = resolveDuration(c.UpstreamPools[i].Connect.ResponseHeaderTimeout, DefaultResponseHeaderTimeout)
			break
		}
	}

	cch := rc.Cache
	streaming := cch.MaxStreamingBufferBytes
	if streaming <= 0 {
		// Mirror Parse's derivation, then fall back to the fixed floor
		// when GOMEMLIMIT is unset.
		tmp := RouteCache{MaxStreamingBufferBytes: streaming}
		tmp.ResolveMaxStreamingBufferBytes(memLimit)
		streaming = tmp.MaxStreamingBufferBytes
		if streaming <= 0 {
			streaming = DefaultStreamingBufferBytes
		}
	}
	fetchTimeout := cch.FetchTimeout
	if fetchTimeout <= 0 && rc.Static.Root == "" {
		fetchTimeout = poolConn
	}

	return ResolvedRoute{
		Name:                 rc.Name,
		Host:                 rc.Match.Host,
		PathPrefix:           rc.Match.PathPrefix,
		Methods:              rc.Match.Methods,
		Pool:                 rc.Pool,
		Static:               rc.Static,
		StripPrefix:          rc.Request.StripPrefix,
		PathRewrite:          rc.Request.PathRewrite,
		RequestHeaderSet:     rc.Request.HeaderSet,
		RequestHeaderRemove:  rc.Request.HeaderRemove,
		ResponseHeaderSet:    rc.Response.HeaderSet,
		ResponseHeaderRemove: rc.Response.HeaderRemove,
		Cache: ResolvedRouteCache{
			Enabled:                 cch.Enabled,
			AllowSetCookie:          cch.AllowSetCookie != nil && *cch.AllowSetCookie,
			Key:                     cch.Key,
			NegativeTTL:             cch.NegativeTTL,
			TTLDefault:              cch.TTLDefault,
			TTLOverride:             cch.TTLOverride,
			StaleWhileRevalidate:    cch.StaleWhileRevalidate,
			StaleIfError:            cch.StaleIfError,
			JitterPercent:           cch.JitterPercent,
			MaxObjectSize:           cch.MaxObjectSize,
			MaxResponseBytes:        resolveByteSize(cch.MaxResponseBytes, DefaultMaxResponseBytes),
			MaxFetchConcurrency:     resolvePositive(cch.MaxFetchConcurrency, DefaultFetchConcurrency),
			FetchTimeout:            fetchTimeout,
			FetchWaitTimeout:        resolveDuration(cch.FetchWaitTimeout, DefaultFetchWaitTimeout),
			MaxStreamingBufferBytes: streaming,
			RefreshMinHits:          cch.RefreshMinHits,
			RefreshPersistCycles:    cch.RefreshPersistCycles,
			RefreshMinScore:         cch.RefreshMinScore,
			RefreshMaxRPS:           cch.RefreshMaxRPS,
			RefreshMargin:           refreshMargin(&cch),
			RefreshTimeout:          cch.RefreshTimeout,
			RefreshConcurrency:      cch.RefreshConcurrency,
			RefreshBeforeExpiry:     cch.RefreshBeforeExpiry,
			RefreshReactiveFirst:    cch.RefreshReactiveFirst,
			StayinAlive:             cch.StayinAlive,
		},
	}
}

// refreshMargin computes the absolute refresh margin from the TTL
// basis (ttl_override, else ttl_default) and the margin percent,
// mirroring the builder's inline computation.
func refreshMargin(rc *RouteCache) time.Duration {
	if !rc.RefreshBeforeExpiry {
		return 0
	}
	basis := rc.TTLOverride
	if basis <= 0 {
		basis = rc.TTLDefault
	}
	pct := rc.RefreshMarginPercent
	if pct <= 0 {
		pct = DefaultRefreshMarginPercent
	}
	return basis * time.Duration(pct) / 100
}

// resolveDuration returns def when v is zero.
func resolveDuration(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// resolvePositive returns def when v is not positive.
func resolvePositive(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// resolveByteSize returns def when v is zero.
func resolveByteSize(v, def ByteSize) ByteSize {
	if v == 0 {
		return def
	}
	return v
}

// resolveNonNegative maps a -1 disable sentinel to 0 so no negative
// value ever reaches a Resolved consumer; the paired bool carries the
// disable decision.
func resolveNonNegative(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	return v
}
