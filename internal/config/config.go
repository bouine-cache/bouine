// Package config loads, validates, and exposes the bouine YAML
// configuration tree. It is the single source of truth for runtime
// settings (listeners, TLS, upstream pools, storage, cluster, routes).
//
// Hot reload is supported via fsnotify + SIGHUP (see reload.go).
// Additive changes only — removing or renaming a field requires a
// major version bump (see PLAN.md §13).
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

	// Storage controls the hot + warm tiers (phase 2+).
	Storage Storage `yaml:"storage"`

	// Cluster controls peer discovery and fan-out (phase 4+).
	Cluster Cluster `yaml:"cluster"`

	// UpstreamPools declares the origin / backend pools that routes
	// reference by name.
	UpstreamPools []UpstreamPool `yaml:"upstream_pools"`

	// Routes are matched in declaration order; the first match wins.
	Routes []Route `yaml:"routes"`

	// Admin controls the admin API security settings.
	Admin AdminConfig `yaml:"admin"`

	// Experimental holds opt-in feature flags that have not graduated
	// to the stable schema.
	Experimental Experimental `yaml:"experimental"`
}

// Listen enumerates the listener addresses. Empty strings disable.
type Listen struct {
	HTTP    string `yaml:"http"`
	HTTPS   string `yaml:"https"`
	HTTP3   string `yaml:"http3"`
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
	Reload       TLSReload `yaml:"reload"`
	HTTP3        TLSHTTP3  `yaml:"http3"`
}

// TLSCert is a single cert/key pair plus its SNI matches.
type TLSCert struct {
	CertFile string   `yaml:"cert_file"`
	KeyFile  string   `yaml:"key_file"`
	SNI      []string `yaml:"sni"`
}

// TLSReload toggles automatic reload sources.
type TLSReload struct {
	FSNotify bool `yaml:"fsnotify"`
	SIGHUP   bool `yaml:"sighup"`
}

// TLSHTTP3 carries HTTP/3 specific knobs. 0-RTT is always off by
// default; per-route opt-in lives on Route.
type TLSHTTP3 struct {
	Enable0RTT bool `yaml:"enable_0rtt"`
}

// Storage controls embedded hot + warm tiers. Phase 2+.
type Storage struct {
	HotMaxBytes  ByteSize `yaml:"hot_max_bytes"`
	WarmDir      string   `yaml:"warm_dir"`
	WarmMaxBytes ByteSize `yaml:"warm_max_bytes"`
	Eviction     string   `yaml:"eviction"` // "sieve" or "w-tinylfu"
}

// Cluster controls peer membership and fan-out. Phase 4+.
type Cluster struct {
	Enabled  bool     `yaml:"enabled"`
	Join     []string `yaml:"join"`
	Replicas int      `yaml:"replicas"`
	HopLimit int      `yaml:"hop_limit"`
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
// refused at startup in release builds (see PLAN.md §6.1).
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
}

// Route declares a host/path match and its per-request behaviour.
type Route struct {
	Match    RouteMatch    `yaml:"match"`
	Pool     string        `yaml:"pool"`
	Cache    RouteCache    `yaml:"cache"`
	Request  RouteRequest  `yaml:"request"`
	Response RouteResponse `yaml:"response"`
	HTTP3    RouteHTTP3    `yaml:"http3"`
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
}

// RouteRequest is the per-route request-side header rewrite block.
type RouteRequest struct {
	HeaderSet    map[string]string `yaml:"header_set"`
	HeaderRemove []string          `yaml:"header_remove"`
}

// RouteResponse is the per-route response-side header rewrite block.
type RouteResponse struct {
	HeaderSet    map[string]string `yaml:"header_set"`
	HeaderRemove []string          `yaml:"header_remove"`
}

// RouteHTTP3 carries per-route HTTP/3 toggles. 0-RTT is off unless
// explicitly enabled here AND globally allowed.
type RouteHTTP3 struct {
	Allow0RTT bool `yaml:"allow_0rtt"`
}

// Experimental holds unstable opt-in feature flags. Empty by default.
type Experimental struct{}

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
