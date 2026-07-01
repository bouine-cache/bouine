package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Defaults returns a Config populated with safe defaults. The
// "admin: :9000" listener is enabled so the daemon is operable even
// with an empty config file.
func Defaults() Config {
	return Config{
		Listen: Listen{
			Admin: ":9000",
		},
		TLS: TLS{
			ALPN:       []string{"h2", "http/1.1"},
			MinVersion: "1.2",
		},
		Cluster: Cluster{
			Mode:     ClusterModeStrong,
			HopLimit: 2,
		},
	}
}

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
func Parse(b []byte) (*Config, error) {
	cfg := Defaults()
	if len(strings.TrimSpace(string(b))) == 0 {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("yaml decode: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate runs cross-field checks. It is called by Load/Parse but is
// also useful from tests.
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

	return nil
}

// validateRoute checks a single route entry and normalises its fields.
func (c *Config) validateRoute(i int, pools map[string]struct{}) error {
	r := &c.Routes[i]
	if r.Pool == "" {
		return fmt.Errorf("config: route %d has no pool", i)
	}
	if _, ok := pools[r.Pool]; !ok {
		return fmt.Errorf("config: route %d references unknown pool %q", i, r.Pool)
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
	if r.Cache.TTLOverride < 0 {
		return fmt.Errorf("config: route %d ttl_override must be >= 0, got %v", i, r.Cache.TTLOverride)
	}
	if r.Cache.TTLDefault < 0 {
		return fmt.Errorf("config: route %d ttl_default must be >= 0, got %v", i, r.Cache.TTLDefault)
	}
	if r.Cache.StaleWhileRevalidate < 0 {
		return fmt.Errorf("config: route %d stale_while_revalidate must be >= 0, got %v", i, r.Cache.StaleWhileRevalidate)
	}
	if r.Cache.StaleIfError < 0 {
		return fmt.Errorf("config: route %d stale_if_error must be >= 0, got %v", i, r.Cache.StaleIfError)
	}
	if r.Cache.NegativeTTL < 0 {
		return fmt.Errorf("config: route %d negative_ttl must be >= 0, got %v", i, r.Cache.NegativeTTL)
	}
	if r.Cache.JitterPercent < 0 || r.Cache.JitterPercent > 50 {
		return fmt.Errorf("config: route %d jitter_percent must be 0–50, got %d", i, r.Cache.JitterPercent)
	}
	if r.Cache.MaxResponseBytes < 0 {
		return fmt.Errorf("config: route %d max_response_bytes must be >= 0, got %s", i, r.Cache.MaxResponseBytes)
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
	if p.Connect.HedgeTimeout < 0 {
		return fmt.Errorf("config: upstream pool %q connect.hedge_timeout must be >= 0, got %v", p.Name, p.Connect.HedgeTimeout)
	}
	return nil
}

// validateCluster checks and normalises cluster configuration.
func (c *Config) validateCluster() error {
	if c.Cluster.Enabled {
		c.Cluster.Mode = strings.TrimSpace(c.Cluster.Mode)
		switch c.Cluster.Mode {
		case ClusterModeStrong, ClusterModeEventual, ClusterModeFull:
			// valid
		case "":
			c.Cluster.Mode = ClusterModeStrong
		default:
			return fmt.Errorf("config: cluster.mode must be %q, %q, or %q, got %q",
				ClusterModeStrong, ClusterModeEventual, ClusterModeFull, c.Cluster.Mode)
		}
	} else if c.Cluster.Mode != "" && c.Cluster.Mode != ClusterModeStrong {
		return fmt.Errorf("config: cluster.mode %q requires cluster.enabled = true", c.Cluster.Mode)
	}
	if c.Cluster.Mode == "" {
		c.Cluster.Mode = ClusterModeStrong
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
