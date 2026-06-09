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
	Listen Listen `yaml:"listen"`

	// TLS configures the data-plane TLS handshake. The control-plane
	// admin listener has its own minimal TLS hook (see internal/admin).
	TLS TLS `yaml:"tls"`

	// Storage controls the hot + warm tiers.
	Storage Storage `yaml:"storage"`

	// Cluster controls peer discovery and fan-out.
	Cluster Cluster `yaml:"cluster"`

	// UpstreamPools declares the origin / backend pools that routes
	// reference by name.
	UpstreamPools []UpstreamPool `yaml:"upstream_pools"`

	// Routes are matched in declaration order; the first match wins.
	Routes []Route `yaml:"routes"`

	// Admin controls the admin API security settings.
	Admin AdminConfig `yaml:"admin"`

	// Cloudflare configures optional invalidation propagation to the
	// downstream Cloudflare CDN. When zone_id and api_token are set,
	// purge/ban/refresh operations are forwarded to the CF edge.
	Cloudflare CloudflareConfig `yaml:"cloudflare"`

	// Tracing configures OpenTelemetry span export. Empty endpoint = no-op.
	Tracing TracingConfig `yaml:"tracing"`
}

// Listen enumerates the listener addresses. Empty strings disable.
type Listen struct {
	HTTP    string `yaml:"http"`
	HTTPS   string `yaml:"https"`
	Admin   string `yaml:"admin"`
	Cluster string `yaml:"cluster"`
}

// TLS configures the data-plane TLS handshake. Multiple certs are
// supported via SNI; the first matching cert wins.
type TLS struct {
	Certs        []TLSCert `yaml:"certs"`
	ALPN         []string  `yaml:"alpn"`
	MinVersion   string    `yaml:"min_version"`
	OCSPStapling string    `yaml:"ocsp_stapling"`
}

// TLSCert is a single cert/key pair plus its SNI matches.
type TLSCert struct {
	CertFile string   `yaml:"cert_file"`
	KeyFile  string   `yaml:"key_file"`
	SNI      []string `yaml:"sni"`
}

// Storage controls embedded hot + warm tiers. Phase 2+.
type Storage struct {
	HotMaxBytes  ByteSize `yaml:"hot_max_bytes"`
	WarmDir      string   `yaml:"warm_dir"`
	WarmMaxBytes ByteSize `yaml:"warm_max_bytes"`
	Eviction     string   `yaml:"eviction"` // "sieve"
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
	Enabled bool `yaml:"enabled"`
	// NodeName overrides the hostname used for gossip membership. When
	// empty, defaults to os.Hostname(). Required when running multiple
	// nodes on the same host (e.g. integration tests).
	NodeName string `yaml:"node_name"`
	// Mode determines the cluster consistency model. Accepted values:
	//   "strong"    — consistent hash ring, peer fetch on miss (default)
	//   "eventual"  — local cache, gossip invalidation, no peer fetch
	//   "full"      — local cache + full replication, gossip everything
	// Empty defaults to "strong" for backward compatibility.
	Mode     string   `yaml:"mode"`
	Join     []string `yaml:"join"`
	Replicas int      `yaml:"replicas"`
	HopLimit int      `yaml:"hop_limit"`
	// TLS configures mTLS for peer-to-peer cluster communication.
	// When non-empty, peer-fetch and broadcast RPCs use TLS with client
	// certificates. Leave empty for plain HTTP (dev / single-node use).
	TLS ClusterTLS `yaml:"tls"`
}

// ClusterTLS holds the mTLS configuration for cluster inter-node RPCs.
type ClusterTLS struct {
	// CABundle is the path to the CA certificate that signed all cluster
	// peer certificates. Required when TLS is enabled.
	CABundle string `yaml:"ca_bundle"`
	// CertFile is the path to this node's client certificate.
	CertFile string `yaml:"cert_file"`
	// KeyFile is the path to this node's private key.
	KeyFile string `yaml:"key_file"`
}

// UpstreamPool is a named set of origin targets with a shared TLS
// profile and health policy.
type UpstreamPool struct {
	Name    string        `yaml:"name"`
	Targets []string      `yaml:"targets"`
	TLS     UpstreamTLS   `yaml:"tls"`
	Health  HealthPolicy  `yaml:"health"`
	Connect ConnectPolicy `yaml:"connect"`
}

// UpstreamTLS configures TLS to origin. insecure_skip_verify is
// refused at startup in release builds.
type UpstreamTLS struct {
	Enabled            bool     `yaml:"enabled"`
	ServerName         string   `yaml:"server_name"`
	CABundle           string   `yaml:"ca_bundle"`
	ClientCert         string   `yaml:"client_cert"`
	ClientKey          string   `yaml:"client_key"`
	MinVersion         string   `yaml:"min_version"`
	ALPN               []string `yaml:"alpn"`
	PinnedSPKISHA256   []string `yaml:"pinned_spki_sha256"`
	InsecureSkipVerify bool     `yaml:"insecure_skip_verify"`
}

// HealthPolicy aggregates active + passive health checks.
type HealthPolicy struct {
	Active  ActiveHealthCheck  `yaml:"active"`
	Passive PassiveHealthCheck `yaml:"passive"`
}

// ActiveHealthCheck is the optional probe sent by the upstream pool.
type ActiveHealthCheck struct {
	Path                string        `yaml:"path"`
	Method              string        `yaml:"method"`
	Interval            time.Duration `yaml:"interval"`
	Timeout             time.Duration `yaml:"timeout"`
	HealthyThreshold    int           `yaml:"healthy_threshold"`
	UnhealthyThreshold  int           `yaml:"unhealthy_threshold"`
	ExpectedStatusCodes []int         `yaml:"expected_status_codes"`
}

// PassiveHealthCheck is the rolling-error-rate ejection policy.
type PassiveHealthCheck struct {
	Consecutive5xx int           `yaml:"consecutive_5xx"`
	EjectFor       time.Duration `yaml:"eject_for"`
}

// ConnectPolicy bounds dial behaviour.
type ConnectPolicy struct {
	Timeout        time.Duration `yaml:"timeout"`
	KeepAlive      time.Duration `yaml:"keep_alive"`
	MaxConnections int           `yaml:"max_connections"`
	// HedgeTimeout fires a duplicate request to the same pool when the
	// primary does not respond within this duration. Zero disables hedging.
	// Only applies to idempotent methods (GET, HEAD, OPTIONS).
	HedgeTimeout time.Duration `yaml:"hedge_timeout"`
}

// Route declares a host/path match and its per-request behaviour.
type Route struct {
	// Name is the human-readable route label used in Prometheus metrics and
	// the operator dashboard. Defaults to host:path_prefix when empty.
	Name     string        `yaml:"name"`
	Match    RouteMatch    `yaml:"match"`
	Pool     string        `yaml:"pool"`
	Cache    RouteCache    `yaml:"cache"`
	Request  RouteRequest  `yaml:"request"`
	Response RouteResponse `yaml:"response"`
}

// RouteMatch is the predicate for selecting a route.
type RouteMatch struct {
	Host       string `yaml:"host"`
	PathPrefix string `yaml:"path_prefix"`
}

// RouteCache is the per-route cache policy.
type RouteCache struct {
	Enabled              *bool         `yaml:"enabled"`
	TTLDefault           time.Duration `yaml:"ttl_default"`
	StaleWhileRevalidate time.Duration `yaml:"stale_while_revalidate"`
	StaleIfError         time.Duration `yaml:"stale_if_error"`
	// NegativeTTL caches error responses (404, 405, 410, 501) for
	// the configured duration. Zero disables negative caching.
	NegativeTTL time.Duration `yaml:"negative_ttl"`
	// JitterPercent adds a random ±N% to every TTL to prevent
	// synchronized expiry stampedes. Range 0–50; 0 disables.
	JitterPercent int `yaml:"jitter_percent"`
	// StayinAlive enables emergency stale mode: when the upstream is
	// unreachable or returns 5xx, serve the cached object regardless
	// of how long ago it expired. Keeps the route alive until the
	// upstream recovers.
	StayinAlive bool `yaml:"stayin_alive"`
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
	AllowSetCookie *bool `yaml:"allow_set_cookie"`
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
	TTLOverride time.Duration `yaml:"ttl_override"`
	// MaxObjectSize, when > 0, skips caching for responses whose body
	// exceeds this size. The response is still proxied to the client —
	// only storage is skipped. Prevents large downloads from evicting
	// useful cache entries. Zero (default) = no limit.
	MaxObjectSize ByteSize `yaml:"max_object_size"`
}

// RouteRequest is the per-route request-side header rewrite block.
type RouteRequest struct {
	HeaderSet    map[string]string `yaml:"header_set"`
	HeaderRemove []string          `yaml:"header_remove"`
	// StripPrefix removes this path prefix from the request URL before
	// forwarding to the upstream. The cache key uses the original path
	// so different routes with the same stripped path don't collide.
	// Must start with "/" when non-empty.
	StripPrefix string `yaml:"strip_prefix"`
}

// RouteResponse is the per-route response-side header rewrite block.
type RouteResponse struct {
	HeaderSet    map[string]string `yaml:"header_set"`
	HeaderRemove []string          `yaml:"header_remove"`
}

// CloudflareConfig configures Cloudflare Cache API invalidation propagation.
type CloudflareConfig struct {
	// ZoneID is the Cloudflare zone identifier (non-secret; visible in the
	// Cloudflare dashboard URL). Required when propagation is enabled.
	ZoneID string `yaml:"zone_id"`
	// APIToken must have the "Cache Purge" permission for this zone.
	// Inject via the CF_API_TOKEN environment variable; never hardcode.
	APIToken string `yaml:"api_token"`
	// Propagate controls which bouine operations forward to Cloudflare.
	Propagate CloudflarePropagation `yaml:"propagate"`
	// Async controls whether CF propagation blocks the admin response.
	// Defaults to true when omitted: the /v1/purge|ban|refresh response
	// returns immediately and CF invalidation fires in a background goroutine.
	// Set async: false to wait for CF confirmation (~50–300 ms extra latency).
	Async *bool `yaml:"async"`
	// Timeout for individual CF API calls (default 10s).
	Timeout time.Duration `yaml:"timeout"`
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
	Purge bool `yaml:"purge"`
	// Ban forwards POST /v1/ban to CF PurgeByTags/PurgeByPrefixes/PurgeByHostnames.
	Ban bool `yaml:"ban"`
	// Refresh forwards POST /v1/refresh to CF PurgeSingleFile.
	Refresh bool `yaml:"refresh"`
}

// TracingConfig configures OTel trace export.
type TracingConfig struct {
	// Endpoint is the OTLP/HTTP collector, e.g. "http://otel-collector:4318".
	// Empty disables tracing (no-op).
	Endpoint     string  `yaml:"endpoint"`
	ServiceName  string  `yaml:"service_name"`
	SamplingRate float64 `yaml:"sampling_rate"`
}

// AdminConfig controls admin API security.
type AdminConfig struct {
	// Token is the bearer token required on all admin write endpoints.
	// If empty, a random token is generated at startup and logged as
	// a WARN so operators are forced to notice it.
	Token string `yaml:"token"`
}

// ByteSize is a typed size in bytes, parsed from strings like "2Go"
// or "512Mo". It is implemented as int64 so it composes with stdlib
// arithmetic.
//
// Stable surface — the YAML representation is what matters.
type ByteSize int64
