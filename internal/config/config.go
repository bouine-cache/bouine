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
}

// Listen enumerates the listener addresses. Empty strings disable.
type Listen struct {
	HTTP           string `yaml:"http,omitempty" json:"http,omitempty"`
	HTTPS          string `yaml:"https,omitempty" json:"https,omitempty"`
	Admin          string `yaml:"admin,omitempty" json:"admin,omitempty"`
	Cluster        string `yaml:"cluster,omitempty" json:"cluster,omitempty"`
	MaxConnections int    `yaml:"max_connections,omitempty" json:"max_connections,omitempty"`
}

// TLS configures the data-plane TLS handshake. Multiple certs are
// supported via SNI; the first matching cert wins.
type TLS struct {
	Certs        []TLSCert `yaml:"certs,omitempty" json:"certs,omitempty"`
	ALPN         []string  `yaml:"alpn,omitempty" json:"alpn,omitempty"`
	MinVersion   string    `yaml:"min_version,omitempty" json:"min_version,omitempty"`
	OCSPStapling string    `yaml:"ocsp_stapling,omitempty" json:"ocsp_stapling,omitempty"`
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
	// HotMaxBytesRatio is the percentage of GOMEMLIMIT used to derive
	// HotMaxBytes when hot_max_bytes is not explicitly set. Zero means
	// use the default (75); otherwise the value must be 1–100. Deriving
	// from GOMEMLIMIT keeps SIEVE eviction headroom below the Go runtime
	// soft memory limit so the GC does not enter a death spiral as the
	// cache fills (issue #161). An explicit hot_max_bytes always takes
	// precedence.
	HotMaxBytesRatio int      `yaml:"hot_max_bytes_ratio,omitempty" json:"hot_max_bytes_ratio,omitempty"`
	WarmDir          string   `yaml:"warm_dir,omitempty" json:"warm_dir,omitempty"`
	WarmMaxBytes     ByteSize `yaml:"warm_max_bytes,omitempty" json:"warm_max_bytes,omitempty"`
	Eviction         string   `yaml:"eviction,omitempty" json:"eviction,omitempty"`
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
	// ClusterModeFull caches locally with active replication to all peers;
	// N complete copies; invalidation + replication via gossip.
	ClusterModeFull = "full"
)

// Cluster controls peer membership and fan-out. Phase 4+.
type Cluster struct {
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	// NodeName overrides the hostname used for gossip membership. When
	// empty, defaults to os.Hostname(). Required when running multiple
	// nodes on the same host (e.g. integration tests).
	NodeName string `yaml:"node_name,omitempty" json:"node_name,omitempty"`
	// Mode determines the cluster consistency model. Accepted values:
	//   "strong"    — consistent hash ring, peer fetch on miss (default)
	//   "eventual"  — local cache, gossip invalidation, no peer fetch
	//   "full"      — local cache + full replication, gossip everything
	// Empty defaults to "strong" for backward compatibility.
	Mode     string   `yaml:"mode,omitempty" json:"mode,omitempty"`
	Join     []string `yaml:"join,omitempty" json:"join,omitempty"`
	Replicas int      `yaml:"replicas,omitempty" json:"replicas,omitempty"`
	HopLimit int      `yaml:"hop_limit,omitempty" json:"hop_limit,omitempty"`
	// AntiEntropyInterval is the period between anti-entropy object
	// reconciliation rounds in full mode. Default 30s. Set to 0 to
	// disable. Has no effect in strong or eventual mode.
	AntiEntropyInterval time.Duration `yaml:"anti_entropy_interval,omitempty" json:"anti_entropy_interval,omitempty"`
	// BackfillLimit caps the number of keys backfilled per peer per
	// anti-entropy round. 0 (default) means no limit — all missing
	// keys are fetched in one round. Set to a positive value (e.g.
	// 1000) to prevent thundering herd when a new pod joins a cluster
	// with many cached objects.
	BackfillLimit int `yaml:"backfill_limit,omitempty" json:"backfill_limit,omitempty"`
	// TLS configures mTLS for peer-to-peer cluster communication.
	// When non-empty, peer-fetch and broadcast RPCs use TLS with client
	// certificates. Leave empty for plain HTTP (dev / single-node use).
	TLS ClusterTLS `yaml:"tls,omitempty" json:"tls,omitempty"`
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

// UpstreamPool is a named set of origin targets with a shared TLS
// profile and health policy.
type UpstreamPool struct {
	Name    string        `yaml:"name,omitempty" json:"name,omitempty"`
	Targets []string      `yaml:"targets,omitempty" json:"targets,omitempty"`
	TLS     UpstreamTLS   `yaml:"tls,omitempty" json:"tls,omitempty"`
	Health  HealthPolicy  `yaml:"health,omitempty" json:"health,omitempty"`
	Connect ConnectPolicy `yaml:"connect,omitempty" json:"connect,omitempty"`
}

// UpstreamTLS configures TLS to origin. insecure_skip_verify is
// refused at startup in release builds.
type UpstreamTLS struct {
	Enabled            bool     `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	ServerName         string   `yaml:"server_name,omitempty" json:"server_name,omitempty"`
	CABundle           string   `yaml:"ca_bundle,omitempty" json:"ca_bundle,omitempty"`
	ClientCert         string   `yaml:"client_cert,omitempty" json:"client_cert,omitempty"`
	ClientKey          string   `yaml:"client_key,omitempty" json:"client_key,omitempty"`
	MinVersion         string   `yaml:"min_version,omitempty" json:"min_version,omitempty"`
	ALPN               []string `yaml:"alpn,omitempty" json:"alpn,omitempty"`
	PinnedSPKISHA256   []string `yaml:"pinned_spki_sha256,omitempty" json:"pinned_spki_sha256,omitempty"`
	InsecureSkipVerify bool     `yaml:"insecure_skip_verify,omitempty" json:"insecure_skip_verify,omitempty"`
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
	Timeout        time.Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	KeepAlive      time.Duration `yaml:"keep_alive,omitempty" json:"keep_alive,omitempty"`
	MaxConnections int           `yaml:"max_connections,omitempty" json:"max_connections,omitempty"`
	// HedgeTimeout fires a duplicate request to the same pool when the
	// primary does not respond within this duration. Zero disables hedging.
	// Only applies to idempotent methods (GET, HEAD, OPTIONS).
	HedgeTimeout time.Duration `yaml:"hedge_timeout,omitempty" json:"hedge_timeout,omitempty"`
}

// Route declares a host/path match and its per-request behaviour.
type Route struct {
	// Name is the human-readable route label used in Prometheus metrics and
	// the operator dashboard. Defaults to host:path_prefix when empty.
	Name     string        `yaml:"name,omitempty" json:"name,omitempty"`
	Match    RouteMatch    `yaml:"match,omitempty" json:"match,omitempty"`
	Pool     string        `yaml:"pool,omitempty" json:"pool,omitempty"`
	Cache    RouteCache    `yaml:"cache,omitempty" json:"cache,omitempty"`
	Request  RouteRequest  `yaml:"request,omitempty" json:"request,omitempty"`
	Response RouteResponse `yaml:"response,omitempty" json:"response,omitempty"`
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
	// Propagate controls which bouine operations forward to Cloudflare.
	Propagate CloudflarePropagation `yaml:"propagate,omitempty" json:"propagate,omitempty"`
	// Async controls whether CF propagation blocks the admin response.
	// Defaults to true when omitted: the /v1/purge|ban|refresh response
	// returns immediately and CF invalidation fires in a background goroutine.
	// Set async: false to wait for CF confirmation (~50–300 ms extra latency).
	Async *bool `yaml:"async,omitempty" json:"async,omitempty"`
	// Timeout for individual CF API calls (default 10s).
	Timeout time.Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
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
	// RateLimitPerSecond caps the number of write requests per second
	// on the admin API. Zero (default) disables rate limiting.
	RateLimitPerSecond int `yaml:"rate_limit_per_second,omitempty" json:"rate_limit_per_second,omitempty"`
}

// ByteSize is a typed size in bytes, parsed from strings like "2Go"
// or "512Mo". It is implemented as int64 so it composes with stdlib
// arithmetic.
//
// Stable surface — the YAML representation is what matters.
type ByteSize int64
