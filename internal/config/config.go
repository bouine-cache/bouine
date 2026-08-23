// Package config loads, validates, and exposes the bouine YAML
// configuration tree. It is the single source of truth for runtime
// settings (listeners, TLS, upstream pools, storage, cluster, routes).
//
// Additive changes only — removing or renaming a field requires a
// major version bump.
package config

import "time"

// Config is the root of the bouine configuration tree.
//
// Stable.
type Config struct {
	// Listen controls every network listener. Empty addresses disable
	// the corresponding listener.
	Listen Listen `yaml:"listen,omitempty" json:"listen,omitempty"`

	// TLS configures the data-plane TLS handshake. The control-plane
	// admin listener has its own minimal TLS hook (see internal/admin).
	TLS TLS `yaml:"tls,omitempty" json:"tls,omitempty"`

	// Storage controls the hot + warm tiers.
	Storage Storage `yaml:"storage,omitempty" json:"storage,omitempty"`

	// Cluster controls peer discovery and fan-out.
	Cluster Cluster `yaml:"cluster,omitempty" json:"cluster,omitempty"`

	// UpstreamPools declares the origin / backend pools that routes
	// reference by name.
	UpstreamPools []UpstreamPool `yaml:"upstream_pools,omitempty" json:"upstream_pools,omitempty"`

	// Routes are matched in declaration order; the first match wins.
	Routes []Route `yaml:"routes,omitempty" json:"routes,omitempty"`

	// Admin controls the admin API security settings.
	Admin AdminConfig `yaml:"admin,omitempty" json:"admin,omitempty"`

	// Cloudflare configures optional invalidation propagation to the
	// downstream Cloudflare CDN. When zone_id and api_token are set,
	// purge/ban/refresh operations are forwarded to the CF edge.
	Cloudflare CloudflareConfig `yaml:"cloudflare,omitempty" json:"cloudflare,omitempty"`

	// Tracing configures OpenTelemetry span export. Empty endpoint = no-op.
	Tracing TracingConfig `yaml:"tracing,omitempty" json:"tracing,omitempty"`

	// GOGC controls the Go GC trigger percentage. nil defers to the
	// GOGC env var (Go default 100). A value of -1 disables the
	// percentage-based trigger entirely, relying solely on GOMEMLIMIT.
	// Raising GOGC (e.g. 200) reduces GC frequency at the cost of higher
	// transient heap usage. Safe to raise when GOMEMLIMIT is set, since
	// the soft memory limit acts as a hard cap.
	GOGC *int `yaml:"gogc,omitempty" json:"gogc,omitempty"`

	// URLRingSampleRate controls 1-in-N sampling for the dashboard URL
	// ring buffer. Default 0 = record every non-HIT request. A value of
	// 100 records 1 in 100, reducing sync.Map overhead under high miss
	// rates. Set to 1 to record every call (debug mode).
	URLRingSampleRate int `yaml:"url_ring_sample_rate,omitempty" json:"url_ring_sample_rate,omitempty"`

	// Experimental holds opt-in features that are not yet stable.
	// Fields default to off (zero value) and must be explicitly enabled.
	Experimental ExperimentalConfig `yaml:"experimental,omitempty" json:"experimental,omitempty"`
}

// ExperimentalConfig holds opt-in experimental features.
type ExperimentalConfig struct {
	// H1FastPath enables the custom HTTP/1.1 parser that bypasses
	// net/http on cache hits. When true, GET/HEAD requests with no
	// conditional headers are served directly from the parsed request
	// without allocating *http.Request or http.ResponseWriter. Misses
	// and non-GET/HEAD requests fall through to net/http unchanged.
	// Default false.
	H1FastPath bool `yaml:"h1_fast_path,omitempty" json:"h1_fast_path,omitempty"`

	// FasthttpMigration enables the fasthttp HTTP stack for the entire
	// daemon (data plane, origin fetch, peer fetch, admin, dashboard).
	// When false, the daemon uses net/http (legacy behavior). When true,
	// the daemon uses fasthttp with the rewritten h1parser for cache-hit
	// zero-alloc serving. See ADR-0034 and issue #521 for the full
	// migration plan. Default false.
	FasthttpMigration bool `yaml:"fasthttp_migration,omitempty" json:"fasthttp_migration,omitempty"`
}

// Listen enumerates the listener addresses. Empty strings disable.
type Listen struct {
	HTTP           string `yaml:"http,omitempty" json:"http,omitempty"`
	HTTPS          string `yaml:"https,omitempty" json:"https,omitempty"`
	Admin          string `yaml:"admin,omitempty" json:"admin,omitempty"`
	Cluster        string `yaml:"cluster,omitempty" json:"cluster,omitempty"`
	MaxConnections int    `yaml:"max_connections,omitempty" json:"max_connections,omitempty"`
	// TCPFastOpen enables Linux TCP_FASTOPEN on data-plane listeners.
	// nil defaults to true on Linux and no-op on other platforms.
	TCPFastOpen *bool `yaml:"tcp_fast_open,omitempty" json:"tcp_fast_open,omitempty"`
	// TCPDeferAccept enables Linux TCP_DEFER_ACCEPT on data-plane
	// listeners. nil defaults to true on Linux and no-op elsewhere.
	TCPDeferAccept *bool `yaml:"tcp_defer_accept,omitempty" json:"tcp_defer_accept,omitempty"`
	// ReusePort enables SO_REUSEPORT on data-plane listeners, creating
	// N parallel accept loops (one per GOMAXPROCS) for better connection
	// distribution under high load. nil defaults to true on Linux, false
	// on other platforms (macOS/BSD have different SO_REUSEPORT semantics).
	ReusePort *bool `yaml:"reuse_port,omitempty" json:"reuse_port,omitempty"`
}

// TLS configures the data-plane TLS handshake. Multiple certs are
// supported via SNI; the first matching cert wins.
type TLS struct {
	Certs      []TLSCert `yaml:"certs,omitempty" json:"certs,omitempty"`
	MinVersion string    `yaml:"min_version,omitempty" json:"min_version,omitempty"`
}

// TLSCert is a single cert/key pair plus its SNI matches.
type TLSCert struct {
	CertFile string   `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
	KeyFile  string   `yaml:"key_file,omitempty" json:"key_file,omitempty"`
	SNI      []string `yaml:"sni,omitempty" json:"sni,omitempty"`
}

// Storage controls embedded hot + warm tiers. Phase 2+.
type Storage struct {
	HotMaxBytes ByteSize `yaml:"hot_max_bytes,omitempty" json:"hot_max_bytes,omitempty"`
	// HotMmapSlab enables the mmap'd slab allocator for hot store body
	// bytes. When true, bodies are allocated from mmap'd regions instead
	// of Go heap, reducing GC pressure. Default false (Go heap).
	HotMmapSlab  bool     `yaml:"hot_mmap_slab,omitempty" json:"hot_mmap_slab,omitempty"`
	WarmDir      string   `yaml:"warm_dir,omitempty" json:"warm_dir,omitempty"`
	WarmMaxBytes ByteSize `yaml:"warm_max_bytes,omitempty" json:"warm_max_bytes,omitempty"`
	// WarmMaxEntries caps the warm-tier index size in entries. Zero means
	// derive from GOMEMLIMIT (see WarmMaxEntriesRatio). A positive value
	// overrides the derived limit. Negative means unlimited.
	WarmMaxEntries int64 `yaml:"warm_max_entries,omitempty" json:"warm_max_entries,omitempty"`
	// WarmMaxDiskBytes caps the total physical disk usage of warm-tier
	// segment files (live + dead bytes). Unlike WarmMaxBytes which limits
	// logical bytes in the index, this limits actual disk consumption.
	// When exceeded, compaction is triggered immediately. Zero means
	// unlimited (backward compatible).
	WarmMaxDiskBytes ByteSize `yaml:"warm_max_disk_bytes,omitempty" json:"warm_max_disk_bytes,omitempty"`
	// MinFreeDisk is the minimum free disk space (in bytes) to maintain
	// on the warm-tier filesystem. When free space drops below this,
	// compaction is triggered immediately and warm promotion is paused.
	// Zero means no filesystem-level monitoring (backward compatible).
	// This is defense-in-depth alongside WarmMaxDiskBytes: the latter
	// catches bouine's own disk growth, while MinFreeDisk catches
	// external growth (orphaned PVCs, other workloads).
	MinFreeDisk ByteSize `yaml:"min_free_disk,omitempty" json:"min_free_disk,omitempty"`
	// WarmPreallocate, when non-zero, pre-creates segment files at
	// startup totaling this size. The warm store operates as a circular
	// buffer: when all pre-allocated segments are full, the oldest sealed
	// segment is evicted and overwritten. This eliminates disk
	// amplification from append-only segments. Zero means create segments
	// on demand (backward compatible, subject to disk amplification).
	WarmPreallocate ByteSize `yaml:"warm_preallocate,omitempty" json:"warm_preallocate,omitempty"`
	// CompactInterval controls how often the warm-tier compaction check
	// runs. Default 30m (applied when warm_dir is set and the field is
	// zero). Set to -1 to disable periodic compaction (not recommended —
	// dead bytes accumulate without bound).
	CompactInterval time.Duration `yaml:"compact_interval,omitempty" json:"compact_interval,omitempty"`
	// BodyThreshold controls the hot/warm admission boundary during
	// normal operation. Objects with BodySize > this value are written
	// to warm on every Put (with fsync). Objects below this value are
	// written to warm only by the background sync loop. Default 64 KiB.
	// Set to 0 to write all objects to warm on every Put (high disk I/O).
	BodyThreshold ByteSize `yaml:"body_threshold,omitempty" json:"body_threshold,omitempty"`
	// WarmSyncInterval controls how often the hot→warm background sync
	// runs. Default 60s (applied when warm_dir is set and the field is
	// zero). Set to -1 to explicitly disable the sync loop. Only
	// effective when warm_dir is configured.
	WarmSyncInterval time.Duration `yaml:"warm_sync_interval,omitempty" json:"warm_sync_interval,omitempty"`
	// WarmSyncBatchSize caps the number of entries written to warm per
	// sync cycle. Default 5000. When the hot working set exceeds this,
	// entries are rotated across cycles.
	WarmSyncBatchSize int `yaml:"warm_sync_batch_size,omitempty" json:"warm_sync_batch_size,omitempty"`
	// WALSyncInterval controls the async WAL fsync batching interval.
	// Default 100ms. Entries are enqueued to a bounded channel and
	// fsynced in batches by a background goroutine. Set to -1 for
	// synchronous mode (per-entry fsync, same as pre-ADR-0024 behavior).
	// Only effective when warm_dir is configured. See ADR-0024.
	WALSyncInterval time.Duration `yaml:"wal_sync_interval,omitempty" json:"wal_sync_interval,omitempty"`
	// CompactStartupDelay delays the first compaction check after
	// startup. Compaction scans all segments and can take seconds with
	// millions of keys — running it during startup (when WAL replay,
	// cluster join, and initial traffic compete for I/O) causes probe
	// timeouts and CrashLoopBackOff. Default 5 minutes. 0 means use the
	// default. Set to -1 to start compaction immediately.
	CompactStartupDelay time.Duration `yaml:"compact_startup_delay,omitempty" json:"compact_startup_delay,omitempty"`
	// CheckpointInterval controls how often a snapshot + WAL truncate
	// checkpoint runs. Default 5m. 0 means use the default. Set to -1
	// to disable periodic checkpointing (WAL grows until compaction).
	// Shorter intervals = faster restart but more background I/O.
	CheckpointInterval time.Duration `yaml:"checkpoint_interval,omitempty" json:"checkpoint_interval,omitempty"`
	// CheckpointWALThreshold triggers a checkpoint when the WAL entry
	// count exceeds this value, regardless of the interval. Bounds WAL
	// replay time on unclean restart. Default 100000.
	CheckpointWALThreshold int64 `yaml:"checkpoint_wal_threshold,omitempty" json:"checkpoint_wal_threshold,omitempty"`
	// SegmentCacheSize caps the number of concurrently open segment
	// file descriptors in the warm tier. 0 means auto (min(segCount,
	// 256)). -1 means unlimited (no eviction, all segments stay open).
	// When the cache is full and a new segment is opened, the least-
	// recently-accessed segment with zero in-flight readers is closed.
	SegmentCacheSize int `yaml:"segment_cache_size,omitempty" json:"segment_cache_size,omitempty"`
	// TombstoneQueueSize controls the buffer size of the hot→warm
	// tombstone channel. When the buffer fills, tombstones are dropped
	// (the on-disk tombstone still guarantees durability via segment scan;
	// the WAL delete is a fast-replay optimization). Default 65536.
	// Increasing this reduces drops under bursty eviction pressure.
	TombstoneQueueSize int `yaml:"tombstone_queue_size,omitempty" json:"tombstone_queue_size,omitempty"`
	// TombstoneDrainInterval controls how often the dedicated drain
	// goroutine flushes the tombstone and warm-evict queues to the warm
	// tier + WAL. Default 1s. 0 means use the default. Set to -1 to
	// disable the dedicated drain goroutine (drains only on the warm
	// sync cycle, which may cause drops under sustained eviction).
	TombstoneDrainInterval time.Duration `yaml:"tombstone_drain_interval,omitempty" json:"tombstone_drain_interval,omitempty"`
	// EvictionAlgorithm selects the eviction policy for both tiers.
	// "" and "sieve" (the default) use the SIEVE visited-bit sweep.
	// "cachaner" uses SIEVE with a 3-bit frequency counter that gives
	// hot objects up to 7 second chances (vs SIEVE's 1) before
	// eviction. This is the shared default; per-tier fields below
	// override it.
	EvictionAlgorithm string `yaml:"eviction_algorithm,omitempty" json:"eviction_algorithm,omitempty"`
	// HotEvictionAlgorithm overrides the eviction policy for the hot
	// tier only. When non-empty, it takes precedence over
	// EvictionAlgorithm. Accepts the same values.
	HotEvictionAlgorithm string `yaml:"hot_eviction_algorithm,omitempty" json:"hot_eviction_algorithm,omitempty"`
	// WarmEvictionAlgorithm overrides the eviction policy for the warm
	// tier only. When non-empty, it takes precedence over
	// EvictionAlgorithm. Accepts the same values.
	WarmEvictionAlgorithm string `yaml:"warm_eviction_algorithm,omitempty" json:"warm_eviction_algorithm,omitempty"`
}

// Cluster consistency modes. The mode controls how cache keys are
// distributed across nodes and how invalidations propagate.
const (
	// ClusterModeStrong shards keys via consistent hash ring; peer fetch on
	// miss; 1 copy per key; invalidation via HTTP fan-out + gossip.
	ClusterModeStrong = "strong"
	// ClusterModeEventual caches locally with no peer fetch; N independent
	// copies; invalidation via gossip only (eventual consistency).
	ClusterModeEventual = "eventual"
)

// maxHandoffQueueDepth is the upper bound for cluster.handoff_queue_depth.
// Each slot costs a pointer + message header in memberlist's per-peer
// linked list; 1<<20 × 50 peers ≈ 50 M entries worst case.
// Keep in sync with cluster.MaxHandoffQueueDepth.
const maxHandoffQueueDepth = 1 << 20 // 1,048,576

// Cluster controls peer membership and fan-out. The cluster is enabled
// when Listen.Cluster is non-empty; there is no separate enabled flag.
type Cluster struct {
	// NodeName overrides the hostname used for gossip membership. When
	// empty, defaults to os.Hostname(). Required when running multiple
	// nodes on the same host (e.g. integration tests).
	NodeName string `yaml:"node_name,omitempty" json:"node_name,omitempty"`
	// Mode determines the cluster consistency model. Accepted values:
	//   "strong"    — consistent hash ring, peer fetch on miss (default)
	//   "eventual"  — local cache, gossip invalidation, no peer fetch
	// Empty defaults to "strong" for backward compatibility.
	Mode     string   `yaml:"mode,omitempty" json:"mode,omitempty"`
	Join     []string `yaml:"join,omitempty" json:"join,omitempty"`
	HopLimit int      `yaml:"hop_limit,omitempty" json:"hop_limit,omitempty"`
	// JoinTimeout is the maximum time to wait for cluster join before
	// giving up. In strong mode, the pod stays not-ready if join fails
	// within this timeout. In eventual mode, the pod becomes ready and
	// continues retrying in the background. Default 120s. 0 means use
	// the default.
	JoinTimeout time.Duration `yaml:"join_timeout,omitempty" json:"join_timeout,omitempty"`
	// HandoffQueueDepth sets memberlist's HandoffQueueDepth — the
	// number of messages buffered per peer when the handler is busy.
	// The default (0) uses bouine's default of 4096, which is 4× the
	// memberlist upstream default of 1024. A burst of cache
	// invalidations can exceed the upstream default and cause message
	// drops; 4096 absorbs typical production bursts. Negative values
	// are rejected.
	HandoffQueueDepth int `yaml:"handoff_queue_depth,omitempty" json:"handoff_queue_depth,omitempty"`
	// TLS configures mTLS for peer-to-peer cluster communication.
	// When non-empty, peer-fetch and broadcast RPCs use TLS with client
	// certificates. Leave empty for plain HTTP (dev / single-node use).
	TLS ClusterTLS `yaml:"tls,omitempty" json:"tls,omitempty"`
	// PeerMaxConnsPerHost limits the number of HTTP/1.1 pipelined
	// connections per peer. With pipelining (PipelineClient), each
	// connection handles MaxPendingRequests concurrent in-flight
	// requests. Default 8, which with 16 pending requests gives 128
	// concurrent peer fetches per peer — matching the old HTTP/2
	// capacity. Set to 1 to disable pipelining.
	PeerMaxConnsPerHost int `yaml:"peer_max_conns_per_host,omitempty" json:"peer_max_conns_per_host,omitempty"`
	// PeerMaxIdleConnDuration controls how long idle peer connections
	// are kept before closing. Default 120s (longer than fasthttp's
	// 10s default) to keep connections warm between peer fetch bursts.
	PeerMaxIdleConnDuration time.Duration `yaml:"peer_max_idle_conn_duration,omitempty" json:"peer_max_idle_conn_duration,omitempty"`
}

// ClusterTLS holds the mTLS configuration for cluster inter-node RPCs.
type ClusterTLS struct {
	// CABundle is the path to the CA certificate that signed all cluster
	// peer certificates. Required when TLS is enabled.
	CABundle string `yaml:"ca_bundle,omitempty" json:"ca_bundle,omitempty"`
	// CertFile is the path to this node's client certificate.
	CertFile string `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
	// KeyFile is the path to this node's private key.
	KeyFile string `yaml:"key_file,omitempty" json:"key_file,omitempty"`
}

// UpstreamPool is a named set of origin targets with a shared health
// policy and connect policy.
type UpstreamPool struct {
	Name    string        `yaml:"name,omitempty" json:"name,omitempty"`
	Targets []string      `yaml:"targets,omitempty" json:"targets,omitempty"`
	Health  HealthPolicy  `yaml:"health,omitempty" json:"health,omitempty"`
	Connect ConnectPolicy `yaml:"connect,omitempty" json:"connect,omitempty"`
}

// HealthPolicy aggregates active + passive health checks.
type HealthPolicy struct {
	Active  ActiveHealthCheck  `yaml:"active,omitempty" json:"active,omitempty"`
	Passive PassiveHealthCheck `yaml:"passive,omitempty" json:"passive,omitempty"`
}

// ActiveHealthCheck is the optional probe sent by the upstream pool.
type ActiveHealthCheck struct {
	Path                string        `yaml:"path,omitempty" json:"path,omitempty"`
	Method              string        `yaml:"method,omitempty" json:"method,omitempty"`
	Interval            time.Duration `yaml:"interval,omitempty" json:"interval,omitempty"`
	Timeout             time.Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	HealthyThreshold    int           `yaml:"healthy_threshold,omitempty" json:"healthy_threshold,omitempty"`
	UnhealthyThreshold  int           `yaml:"unhealthy_threshold,omitempty" json:"unhealthy_threshold,omitempty"`
	ExpectedStatusCodes []int         `yaml:"expected_status_codes,omitempty" json:"expected_status_codes,omitempty"`
}

// PassiveHealthCheck is the rolling-error-rate ejection policy.
type PassiveHealthCheck struct {
	Consecutive5xx int           `yaml:"consecutive_5xx,omitempty" json:"consecutive_5xx,omitempty"`
	EjectFor       time.Duration `yaml:"eject_for,omitempty" json:"eject_for,omitempty"`
}

// ConnectPolicy bounds dial behaviour.
type ConnectPolicy struct {
	Timeout   time.Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	KeepAlive time.Duration `yaml:"keep_alive,omitempty" json:"keep_alive,omitempty"`
	// MaxConnections caps the number of concurrent connections per upstream
	// host (http.Transport.MaxConnsPerHost). Zero means unlimited — the
	// default, which preserves existing behaviour. Set this to bound FD
	// consumption under high miss ratios with hedged requests. Note: this
	// is per-host, not per-pool; a pool with N targets gets N × MaxConnections.
	// This is distinct from Listen.MaxConnections, which caps total TCP
	// connections to the data plane.
	MaxConnections int `yaml:"max_connections,omitempty" json:"max_connections,omitempty"`
	// ResponseHeaderTimeout bounds the time waiting for the origin's
	// response headers after the request is fully sent. Zero applies a
	// safe built-in default (30s). This is the primary defence against
	// slow-origin resource exhaustion now that WriteTimeout is 0 on the
	// data plane.
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout,omitempty" json:"response_header_timeout,omitempty"`
	// HedgeTimeout fires a duplicate request to the same pool when the
	// primary does not respond within this duration. Zero disables hedging.
	// Only applies to idempotent methods (GET, HEAD, OPTIONS).
	HedgeTimeout time.Duration `yaml:"hedge_timeout,omitempty" json:"hedge_timeout,omitempty"`
}

// Route declares a host/path match and its per-request behaviour.
// A route must specify exactly one of Pool or Static.Root — the former
// proxies to an upstream pool, the latter serves files from a local
// directory.
type Route struct {
	// Name is the human-readable route label used in Prometheus metrics and
	// the operator dashboard. Defaults to host:path_prefix when empty.
	Name     string        `yaml:"name,omitempty" json:"name,omitempty"`
	Match    RouteMatch    `yaml:"match,omitempty" json:"match,omitempty"`
	Pool     string        `yaml:"pool,omitempty" json:"pool,omitempty"`
	Static   StaticConfig  `yaml:"static,omitempty" json:"static,omitempty"`
	Cache    RouteCache    `yaml:"cache,omitempty" json:"cache,omitempty"`
	Request  RouteRequest  `yaml:"request,omitempty" json:"request,omitempty"`
	Response RouteResponse `yaml:"response,omitempty" json:"response,omitempty"`
}

// StaticConfig configures a route to serve files from a local directory
// instead of proxying to an upstream pool. The directory is resolved and
// symlink-evaluated once at startup; per-request path traversal is
// prevented by path.Clean + filepath.Rel containment check.
type StaticConfig struct {
	// Root is the absolute path to the directory from which files are
	// served. Must be non-empty when the route has no Pool. Symlinks in
	// Root are resolved once at startup.
	Root string `yaml:"root,omitempty" json:"root,omitempty"`
	// Index files tried (in order) when the request path maps to a
	// directory. If none match, bouine returns 404. Entries must not
	// contain "/".
	Index []string `yaml:"index,omitempty" json:"index,omitempty"`
	// MaxFileSize is the per-file size cap. Files larger than this are
	// rejected with 413. Zero (default) applies a 10 MiB limit.
	MaxFileSize ByteSize `yaml:"max_file_size,omitempty" json:"max_file_size,omitempty"`
}

// RouteMatch is the predicate for selecting a route.
type RouteMatch struct {
	Host       string `yaml:"host,omitempty" json:"host,omitempty"`
	PathPrefix string `yaml:"path_prefix,omitempty" json:"path_prefix,omitempty"`
	// Methods restricts this route to the listed HTTP methods (e.g.
	// [GET, HEAD]). Empty means match all methods (default).
	// Methods are normalised to upper-case at parse time.
	Methods []string `yaml:"methods,omitempty" json:"methods,omitempty"`
}

// RouteCache is the per-route cache policy.
type RouteCache struct {
	Enabled *bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// TTLDefault is the fallback cache lifetime applied when the origin
	// sends no freshness information (no max-age/s-maxage, no valid
	// Expires, no Last-Modified). When > 0 it also makes such a response
	// eligible for storage — without it, a bare 200 is treated as
	// uncacheable and every request is a MISS. Blocking directives
	// (no-store, private, no-cache, Set-Cookie without freshness,
	// Vary: *, Authorization) are always honoured. Mirrors nginx's
	// proxy_cache_valid. Zero disables the fallback.
	TTLDefault           time.Duration `yaml:"ttl_default,omitempty" json:"ttl_default,omitempty"`
	StaleWhileRevalidate time.Duration `yaml:"stale_while_revalidate,omitempty" json:"stale_while_revalidate,omitempty"`
	StaleIfError         time.Duration `yaml:"stale_if_error,omitempty" json:"stale_if_error,omitempty"`
	// NegativeTTL caches error responses (404, 405, 410, 501) for
	// the configured duration. Zero disables negative caching.
	NegativeTTL time.Duration `yaml:"negative_ttl,omitempty" json:"negative_ttl,omitempty"`
	// JitterPercent adds a random ±N% to every TTL to prevent
	// synchronized expiry stampedes. Range 0–50; 0 disables.
	JitterPercent int `yaml:"jitter_percent,omitempty" json:"jitter_percent,omitempty"`
	// StayinAlive enables emergency stale mode: when the upstream is
	// unreachable or returns 5xx, serve the cached object regardless
	// of how long ago it expired. Keeps the route alive until the
	// upstream recovers.
	StayinAlive bool `yaml:"stayin_alive,omitempty" json:"stayin_alive,omitempty"`
	// AllowSetCookie controls whether responses containing a Set-Cookie
	// header are eligible for caching.
	//
	// Default (nil / false): Set-Cookie in the response blocks caching
	// unconditionally, matching nginx's proxy_cache behaviour and
	// preventing session-cookie replay across users.
	//
	// When explicitly set to true: caching is permitted per RFC 9111
	// (Set-Cookie + explicit freshness), but Set-Cookie is stripped from
	// the stored object so subsequent cache HITs do not replay cookies
	// intended for the first client.
	AllowSetCookie *bool `yaml:"allow_set_cookie,omitempty" json:"allow_set_cookie,omitempty"`
	// TTLOverride, when > 0, forces bouine's internal cache TTL to this
	// value regardless of the upstream's Cache-Control/Expires headers.
	// The upstream's response headers are forwarded to downstream clients
	// unaltered — TTLOverride only changes how long bouine keeps the
	// object before revalidating. This lets operators decouple bouine's
	// storage lifetime from the TTL that a downstream CDN (e.g.
	// Cloudflare) should observe.
	// RFC 9111 boolean directives (no-store, private, no-cache,
	// must-revalidate, proxy-revalidate) are always respected; TTLOverride
	// only replaces the numeric freshness lifetime.
	// Zero (default) = honour the upstream's freshness headers.
	TTLOverride time.Duration `yaml:"ttl_override,omitempty" json:"ttl_override,omitempty"`
	// MaxObjectSize, when > 0, skips caching for responses whose body
	// exceeds this size. The response is still proxied to the client —
	// only storage is skipped. Prevents large downloads from evicting
	// useful cache entries. Zero (default) = no limit.
	MaxObjectSize ByteSize `yaml:"max_object_size,omitempty" json:"max_object_size,omitempty"`
	// MaxResponseBytes is a hard limit on the amount of response body
	// data buffered in memory during an upstream fetch. When exceeded
	// the fetch is aborted and the client receives a 502. Unlike
	// MaxObjectSize (which only skips storage), this prevents the
	// allocation that causes OOM. Zero (default) applies a safe
	// built-in limit (64 MiB).
	MaxResponseBytes ByteSize `yaml:"max_response_bytes,omitempty" json:"max_response_bytes,omitempty"`
	// MaxFetchConcurrency bounds the number of concurrent foreground
	// origin fetches per route. When the limit is reached, additional
	// fetches block until a slot frees or the request context is
	// cancelled. Zero (default) applies a safe built-in limit (64).
	MaxFetchConcurrency int `yaml:"max_fetch_concurrency,omitempty" json:"max_fetch_concurrency,omitempty"`
	// FetchTimeout bounds the total time for an origin fetch (header +
	// body). When exceeded, the fetch is aborted and the client receives
	// a 502 (or stale content if stayin-alive is enabled). Zero applies
	// a safe built-in default (60s). This replaces the blanket
	// WriteTimeout on the data plane, which was the wrong tool for a
	// caching reverse proxy.
	//
	// Must be strictly less than the data plane's safety-net WriteTimeout
	// (internal/server.safetyNetWriteTimeout, currently 5 minutes, mirrored
	// by config.maxFetchTimeout). Otherwise the write deadline can fire
	// during the origin fetch, aborting the client connection before the
	// fetch completes.
	FetchTimeout time.Duration `yaml:"fetch_timeout,omitempty" json:"fetch_timeout,omitempty"`
	// RefreshBeforeExpiry enables proactive background conditional
	// revalidation. A background timer fires at TTL - margin, performing
	// a conditional fetch (If-None-Match / If-Modified-Since). On 304,
	// the TTL is refreshed in place — the object never expires and
	// clients always see cache hits. On 200, the object is replaced.
	//
	// Requires caching to be enabled. Objects with TTL < 5s are not
	// scheduled. Negative-cached objects (404/405/410/501) are not
	// refreshed.
	RefreshBeforeExpiry bool `yaml:"refresh_before_expiry,omitempty" json:"refresh_before_expiry,omitempty"`
	// RefreshMarginPercent controls when the background refresh fires,
	// as a percentage of TTL. Default 10 (fire at 90% of TTL). Range 1-50.
	RefreshMarginPercent int `yaml:"refresh_margin_percent,omitempty" json:"refresh_margin_percent,omitempty"`
	// RefreshConcurrency bounds concurrent background refresh fetches
	// per route. Default 8. Zero means use the default. Range 1-64.
	RefreshConcurrency int `yaml:"refresh_concurrency,omitempty" json:"refresh_concurrency,omitempty"`
	// RefreshTimeout is the maximum duration for a single background
	// refresh fetch. Default 10s. Range 5s-120s.
	RefreshTimeout time.Duration `yaml:"refresh_timeout,omitempty" json:"refresh_timeout,omitempty"`
	// RefreshMinHits is the minimum number of cache hits an object must
	// accumulate during its TTL window to qualify for re-scheduling
	// after a background refresh. Zero (default) disables the gate —
	// every cached object is refreshed regardless of access frequency.
	// When set to N > 0, only objects hit at least N times are
	// re-scheduled; unpopular long-tail objects expire naturally,
	// reducing origin traffic on routes with many distinct paths.
	// The first TTL window always gets one refresh cycle; the gate
	// only applies on re-scheduling after a refresh completes.
	RefreshMinHits int `yaml:"refresh_min_hits,omitempty" json:"refresh_min_hits,omitempty"`
	// RefreshPersistCycles is the number of additional TTL cycles to
	// keep refreshing an object after the popularity gate
	// (refresh_min_hits) would block re-scheduling. Each background
	// refresh that finds Hits < minHits decrements the counter; any
	// popular refresh (Hits >= minHits) resets it to the configured
	// value. Zero (default) disables persistence — the gate kills
	// re-scheduling immediately. Requires refresh_min_hits > 0.
	RefreshPersistCycles int `yaml:"refresh_persist_cycles,omitempty" json:"refresh_persist_cycles,omitempty"`
	// RefreshMinScore is the minimum refresh priority score required for
	// re-scheduling after a background refresh. The score is computed as
	// staleHits × obj.BodySize, where staleHits is the per-window hit count
	// from the previous TTL window. This weights the refresh decision by
	// object size: a 4 MB object with 1 hit outranks a 512 B object with
	// 100 hits. Zero (default) disables the score gate. When both
	// refresh_min_hits and refresh_min_score are set, both gates must pass.
	// Requires refresh_before_expiry and refresh_min_hits > 0.
	RefreshMinScore int64 `yaml:"refresh_min_score,omitempty" json:"refresh_min_score,omitempty"`
	// RefreshMaxRPS caps the number of background refresh fetches per
	// second per route. When the cap is reached, pending refreshes are
	// deferred with jittered backoff rather than dropped. Zero (default)
	// means no rate limit. Requires refresh_before_expiry. Range 0 or
	// 1–10000.
	RefreshMaxRPS int `yaml:"refresh_max_rps,omitempty" json:"refresh_max_rps,omitempty"`
	// RefreshReactiveFirst changes the refresh strategy from proactive to
	// reactive for the initial TTL window. New objects are not scheduled
	// for proactive refresh. Instead, they rely on stale-while-revalidate
	// (SWR): if accessed while stale, a background revalidation refreshes
	// the object, and the popularity gate decides whether to promote it
	// to proactive refresh for subsequent windows. Requires
	// refresh_before_expiry, stale_while_revalidate > 0, and
	// refresh_min_hits > 0.
	RefreshReactiveFirst bool `yaml:"refresh_reactive_first,omitempty" json:"refresh_reactive_first,omitempty"`
	// Key controls cache key construction for this route.
	Key RouteKey `yaml:"key,omitempty" json:"key,omitempty"`
}

// RouteKey configures cache key construction for a route.
type RouteKey struct {
	// StripQueryParams removes the listed query parameter names from
	// the cache key. The parameters are still forwarded to the upstream.
	// This prevents tracking/analytics params (utm_source, fbclid, etc.)
	// from fragmenting the cache.
	StripQueryParams []string `yaml:"strip_query_params,omitempty" json:"strip_query_params,omitempty"`
	// ExcludeHeaders removes the listed request header names from the
	// Vary-based variant key. The headers are still forwarded to the
	// upstream and the origin's Vary response header is left intact —
	// only the key computation skips them. Use this to prevent
	// per-request headers (X-Request-ID, X-Trace-ID) from fragmenting
	// the cache when the origin includes them in Vary.
	ExcludeHeaders []string `yaml:"exclude_headers,omitempty" json:"exclude_headers,omitempty"`
	// KeepQueryParams, when non-empty, restricts the cache key to only
	// these query parameters; all others are excluded. Mutually
	// exclusive with strip_query_params and strip_query_prefix.
	// Equivalent to Varnish qs.keep().
	KeepQueryParams []string `yaml:"keep_query_params,omitempty" json:"keep_query_params,omitempty"`
	// StripQueryPrefix strips query params whose names start with any of
	// these prefixes. Covers "strip all utm_* / fb_* / _ga" without
	// enumerating every variant. Capped at 16 entries in validation.
	StripQueryPrefix []string `yaml:"strip_query_prefix,omitempty" json:"strip_query_prefix,omitempty"`
	// StripEmptyParams removes query params with empty values
	// (?foo=&bar=1 -> ?bar=1). Does NOT apply to params in
	// keep_query_params (allowlisted params are always kept, even
	// with empty values).
	StripEmptyParams bool `yaml:"strip_empty_params,omitempty" json:"strip_empty_params,omitempty"`
	// DedupQueryParams keeps only the first value for duplicate query
	// params (?a=2&a=1 -> ?a=2). "First" is first in request order
	// (matches Varnish qs.unique()). Values are NOT sorted when dedup
	// is enabled.
	DedupQueryParams bool `yaml:"dedup_query_params,omitempty" json:"dedup_query_params,omitempty"`
	// CanonicalizePath normalizes the path component at parse time:
	// percent-decode unreserved chars, uppercase remaining hex,
	// resolve dot-segments. Applies at the listener level: if any
	// route on a listener enables this, all requests on that
	// listener get canonical paths. Default false.
	CanonicalizePath bool `yaml:"canonicalize_path,omitempty" json:"canonicalize_path,omitempty"`
}

// RouteRequest is the per-route request-side header rewrite block.
type RouteRequest struct {
	HeaderSet    map[string]string `yaml:"header_set,omitempty" json:"header_set,omitempty"`
	HeaderRemove []string          `yaml:"header_remove,omitempty" json:"header_remove,omitempty"`
	// StripPrefix removes this path prefix from the request URL before
	// forwarding to the upstream. The cache key uses the original path
	// so different routes with the same stripped path don't collide.
	// Must start with "/" when non-empty.
	StripPrefix string `yaml:"strip_prefix,omitempty" json:"strip_prefix,omitempty"`
}

// RouteResponse is the per-route response-side header rewrite block.
type RouteResponse struct {
	HeaderSet    map[string]string `yaml:"header_set,omitempty" json:"header_set,omitempty"`
	HeaderRemove []string          `yaml:"header_remove,omitempty" json:"header_remove,omitempty"`
}

// CloudflareConfig configures Cloudflare Cache API invalidation propagation.
type CloudflareConfig struct {
	// ZoneID is the Cloudflare zone identifier (non-secret; visible in the
	// Cloudflare dashboard URL). Required when propagation is enabled.
	ZoneID string `yaml:"zone_id,omitempty" json:"zone_id,omitempty"`
	// APIToken must have the "Cache Purge" permission for this zone.
	// Inject via the CF_API_TOKEN environment variable; never hardcode.
	APIToken string `yaml:"api_token,omitempty" json:"api_token,omitempty"`
	// APITokens is an optional list of additional API tokens for rate limit
	// spreading. When non-empty, the client rotates across all tokens
	// (including APIToken) to multiply the effective rate limit budget.
	// Inject via CF_API_TOKENS (comma-separated) environment variable.
	APITokens []string `yaml:"api_tokens,omitempty" json:"api_tokens,omitempty"`
	// Propagate controls which bouine operations forward to Cloudflare.
	Propagate CloudflarePropagation `yaml:"propagate,omitempty" json:"propagate,omitempty"`
	// Async controls whether CF propagation blocks the admin response.
	// Defaults to true when omitted: the /v1/purge|ban|refresh response
	// returns immediately and CF invalidation fires in a background goroutine.
	// Set async: false to wait for CF confirmation (~50–300 ms extra latency).
	Async *bool `yaml:"async,omitempty" json:"async,omitempty"`
	// Timeout for individual CF API calls (default 10s).
	Timeout time.Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	// Batch configures purge batching and deduplication to reduce CF API
	// call volume. When MaxBatchSize > 0, individual purge requests are
	// coalesced into batched, deduplicated API calls. When 0 (default),
	// every purge fires an immediate individual API call (passthrough).
	Batch CloudflareBatchConfig `yaml:"batch,omitempty" json:"batch,omitempty"`
	// Circuit configures the circuit breaker that protects against
	// cascading failures during CF API outages. When enabled, the circuit
	// opens after N consecutive failures, failing fast to avoid wasting
	// resources on a known-down API.
	Circuit CloudflareCircuitConfig `yaml:"circuit,omitempty" json:"circuit,omitempty"`
	// Retry configures the dead-letter queue for failed CF purges. When
	// enabled, failed purge items are enqueued and retried with exponential
	// backoff. This survives transient CF outages without losing
	// invalidations, without increasing CF API request volume (items are
	// deduplicated and go through the same batching pipeline on retry).
	Retry CloudflareRetryConfig `yaml:"retry,omitempty" json:"retry,omitempty"`
}

// CloudflareBatchConfig configures CF purge batching.
type CloudflareBatchConfig struct {
	// MaxBatchSize is the maximum number of items (URLs, tags, prefixes,
	// or hosts) coalesced into a single CF API call. When reached the
	// batch flushes immediately. 0 disables batching (passthrough mode).
	MaxBatchSize int `yaml:"max_batch_size,omitempty" json:"max_batch_size,omitempty"`
	// MaxWait is the maximum time an item waits before a flush is
	// triggered. Default 500ms when 0 and batching is enabled.
	MaxWait time.Duration `yaml:"max_wait,omitempty" json:"max_wait,omitempty"`
}

// CloudflareCircuitConfig configures the CF circuit breaker.
type CloudflareCircuitConfig struct {
	// Enabled controls whether the circuit breaker is active.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// FailureThreshold is the number of consecutive failures before the
	// circuit opens. Default 5 when 0.
	FailureThreshold int `yaml:"failure_threshold,omitempty" json:"failure_threshold,omitempty"`
	// OpenTimeout is how long the circuit stays open before probing.
	// Default 30s when 0.
	OpenTimeout time.Duration `yaml:"open_timeout,omitempty" json:"open_timeout,omitempty"`
	// HalfOpenMaxCalls is the number of probe calls in half-open state.
	// Default 1 when 0.
	HalfOpenMaxCalls int `yaml:"half_open_max_calls,omitempty" json:"half_open_max_calls,omitempty"`
}

// CloudflareRetryConfig configures the CF purge retry queue (DLQ).
type CloudflareRetryConfig struct {
	// Enabled controls whether the retry queue is active.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// MaxQueueSize is the maximum number of items the queue can hold.
	// When full, new failed items are dropped. Default 1000 when 0.
	MaxQueueSize int `yaml:"max_queue_size,omitempty" json:"max_queue_size,omitempty"`
	// MaxRetries is the maximum number of retry attempts per item.
	// Default 3 when 0.
	MaxRetries int `yaml:"max_retries,omitempty" json:"max_retries,omitempty"`
	// BaseDelay is the initial retry delay. Delays grow exponentially.
	// Default 1s when 0.
	BaseDelay time.Duration `yaml:"base_delay,omitempty" json:"base_delay,omitempty"`
	// MaxDelay caps the exponential backoff. Default 30s when 0.
	MaxDelay time.Duration `yaml:"max_delay,omitempty" json:"max_delay,omitempty"`
}

// IsAsync reports whether CF propagation should run asynchronously.
// Nil (field omitted in YAML) and explicit true both return true.
func (c CloudflareConfig) IsAsync() bool {
	if c.Async == nil {
		return true
	}
	return *c.Async
}

// CloudflarePropagation selects which bouine invalidation operations are
// forwarded to the Cloudflare edge.
type CloudflarePropagation struct {
	// Purge forwards POST /v1/purge to CF PurgeSingleFile.
	Purge bool `yaml:"purge,omitempty" json:"purge,omitempty"`
	// Ban forwards POST /v1/ban to CF PurgeByTags/PurgeByPrefixes/PurgeByHostnames.
	Ban bool `yaml:"ban,omitempty" json:"ban,omitempty"`
	// Refresh forwards POST /v1/refresh to CF PurgeSingleFile.
	Refresh bool `yaml:"refresh,omitempty" json:"refresh,omitempty"`
}

// TracingConfig configures OTel trace export.
type TracingConfig struct {
	// Endpoint is the OTLP/HTTP collector, e.g. "http://otel-collector:4318".
	// Empty disables tracing (no-op).
	Endpoint     string  `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	ServiceName  string  `yaml:"service_name,omitempty" json:"service_name,omitempty"`
	SamplingRate float64 `yaml:"sampling_rate,omitempty" json:"sampling_rate,omitempty"`
}

// AdminConfig controls admin API security.
type AdminConfig struct {
	// Token is the bearer token required on all admin write endpoints.
	// If empty, a random token is generated at startup and logged as
	// a WARN so operators are forced to notice it.
	Token string `yaml:"token,omitempty" json:"token,omitempty"`
	// MaxBatchSize caps the number of URLs accepted in a single
	// POST /v1/purge/batch request. Zero (default) applies a safe
	// built-in limit (1000).
	MaxBatchSize int `yaml:"max_batch_size,omitempty" json:"max_batch_size,omitempty"`
	// MaxBodyBytes caps the request body for admin write endpoints
	// (POST). Zero (default) applies a 1 MiB limit, matching the peer
	// RPC request body limit in internal/cluster/handlers.go.
	MaxBodyBytes int `yaml:"max_body_bytes,omitempty" json:"max_body_bytes,omitempty"`
	// RateLimitPerSecond caps the number of write requests per second
	// on the admin API. Zero (default) disables rate limiting.
	RateLimitPerSecond int `yaml:"rate_limit_per_second,omitempty" json:"rate_limit_per_second,omitempty"`
	// PprofEnabled enables net/http/pprof handlers under /debug/pprof/* on the
	// admin port. The routes are auth-exempt; the admin port is expected
	// to be network-isolated in production. Default is false.
	PprofEnabled bool `yaml:"pprof_enabled,omitempty" json:"pprof_enabled,omitempty"`
	// DrainDuration is how long the /drain endpoint blocks before
	// returning. Used by the K8s preStop httpGet hook to keep the pod
	// alive while kube-proxy deregisters it. Zero defaults to 10s.
	DrainDuration time.Duration `yaml:"drain_duration,omitempty" json:"drain_duration,omitempty"`
}

// ByteSize is a typed size in bytes, parsed from strings like "2Go"
// or "512Mo". It is implemented as int64 so it composes with stdlib
// arithmetic.
//
// Stable surface — the YAML representation is what matters.
type ByteSize int64
