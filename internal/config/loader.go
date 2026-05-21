package config

import (
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
			Reload: TLSReload{
				FSNotify: true,
				SIGHUP:   true,
			},
		},
		Cluster: Cluster{
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
	// listener during phase 0 / 1 development.
	if c.Listen.HTTP == "" && c.Listen.HTTPS == "" &&
		c.Listen.HTTP3 == "" && c.Listen.Admin == "" {
		return errors.New("config: at least one listener must be configured")
	}

	// Upstream pool names must be unique.
	seen := make(map[string]struct{}, len(c.UpstreamPools))
	for _, p := range c.UpstreamPools {
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
	}

	// Every route must reference a declared pool.
	for i, r := range c.Routes {
		if r.Pool == "" {
			return fmt.Errorf("config: route %d has no pool", i)
		}
		if _, ok := seen[r.Pool]; !ok {
			return fmt.Errorf("config: route %d references unknown pool %q", i, r.Pool)
		}
	}

	return nil
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

// byteSizeUnits maps the uppercased unit suffix to its multiplier.
// Unknown units are rejected by parseByteSize.
var byteSizeUnits = map[string]float64{
	"":    1,
	"B":   1,
	"K":   1e3,
	"KB":  1e3,
	"KIB": 1 << 10,
	"KO":  1e3,
	"M":   1e6,
	"MB":  1e6,
	"MIB": 1 << 20,
	"MO":  1e6,
	"G":   1e9,
	"GB":  1e9,
	"GIB": 1 << 30,
	"GO":  1e9,
	"T":   1e12,
	"TB":  1e12,
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
