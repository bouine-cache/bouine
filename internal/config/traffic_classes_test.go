package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validTrafficClassesCfg(classes ...TrafficClass) *Config {
	return &Config{
		Listen:  Listen{Admin: ":9000"},
		Metrics: MetricsConfig{TrafficClasses: classes},
	}
}

func TestTrafficClasses_Valid(t *testing.T) {
	t.Parallel()
	cfg := validTrafficClassesCfg(
		TrafficClass{Name: "csr", Hosts: []string{"www.backmarket.fr", "www.backmarket.de"}},
		TrafficClass{Name: "ssr", Hosts: []string{"*.svc.cluster.local", "www.backmarket.*"}},
	)
	require.NoError(t, cfg.Validate())
}

func TestTrafficClasses_AbsentIsValid(t *testing.T) {
	t.Parallel()
	cfg := &Config{Listen: Listen{Admin: ":9000"}}
	require.NoError(t, cfg.Validate())
}

func TestTrafficClasses_TooManyRejected(t *testing.T) {
	t.Parallel()
	classes := make([]TrafficClass, MaxTrafficClasses+1)
	for i := range classes {
		classes[i] = TrafficClass{Name: "c", Hosts: []string{"h"}}
	}
	err := validTrafficClassesCfg(classes...).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most 8")
}

func TestTrafficClasses_NameValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		valid bool
	}{
		{"csr", true},
		{"c", true},
		{"ssr_internal", true},
		{"a2345678901234567890123456789012", true},   // 32 chars, cap
		{"a23456789012345678901234567890123", false}, // 33 chars
		{"", false},
		{"UpperCase", false},
		{"1leading", false},
		{"_leading", false},
		{"hyphen-not-allowed", false},
		{"dot.not", false},
	}
	for _, tt := range tests {
		err := validTrafficClassesCfg(TrafficClass{Name: tt.name, Hosts: []string{"h"}}).Validate()
		if tt.valid {
			assert.NoError(t, err, tt.name)
		} else {
			require.Error(t, err, tt.name)
			assert.Contains(t, err.Error(), "must match", tt.name)
		}
	}
}

func TestTrafficClasses_ReservedNameRejected(t *testing.T) {
	t.Parallel()
	err := validTrafficClassesCfg(TrafficClass{Name: "unclassified", Hosts: []string{"h"}}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved")
}

func TestTrafficClasses_DuplicateNameRejected(t *testing.T) {
	t.Parallel()
	err := validTrafficClassesCfg(
		TrafficClass{Name: "csr", Hosts: []string{"a"}},
		TrafficClass{Name: "csr", Hosts: []string{"b"}},
	).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate class name")
}

func TestTrafficClasses_NoHostsRejected(t *testing.T) {
	t.Parallel()
	err := validTrafficClassesCfg(TrafficClass{Name: "csr"}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no hosts")
}

func TestTrafficClasses_TooManyHostsRejected(t *testing.T) {
	t.Parallel()
	hosts := make([]string, MaxTrafficClassHosts+1)
	for i := range hosts {
		hosts[i] = "h"
	}
	err := validTrafficClassesCfg(TrafficClass{Name: "csr", Hosts: hosts}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than 64 hosts")
}

func TestTrafficClasses_HostPatternValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pattern string
		valid   bool
	}{
		{"www.example.com", true},
		{"*.svc.cluster.local", true},
		{"www.backmarket.*", true},
		{"www.backmarket*", true},
		{"", false},
		{"*", false},
		{"*evil.com", false},
		{"a*b.com", false},
		{"*a.example.com", false},
		{"www.*.com", false},
		{"*.*", false},
		{"**", false},
		{".*", false},  // no anchor: compiles to a prefix no host matches
		{"*.", false},  // no anchor: compiles to a suffix no real host matches
		{"www.", true}, // exact host with a trailing dot still has an anchor
	}
	for _, tt := range tests {
		assert.Equal(t, tt.valid, validTrafficClassHostPattern(tt.pattern), tt.pattern)
	}
}

func TestTrafficClasses_YAMLDecode(t *testing.T) {
	t.Parallel()
	yaml := `
listen:
  admin: ":9000"
metrics:
  traffic_classes:
    - name: csr
      hosts:
        - "www.backmarket.fr"
        - "www.backmarket.de"
    - name: ssr
      hosts:
        - "*.svc.cluster.local"
`
	var cfg Config
	require.NoError(t, yamlUnmarshal(t, []byte(yaml), &cfg))
	require.NoError(t, cfg.Validate())
	require.Len(t, cfg.Metrics.TrafficClasses, 2)
	assert.Equal(t, "csr", cfg.Metrics.TrafficClasses[0].Name)
	assert.Equal(t, "ssr", cfg.Metrics.TrafficClasses[1].Name)

	err := yamlUnmarshal(t, []byte(strings.Replace(yaml, "name: csr", "name: Csr", 1)), &Config{})
	require.NoError(t, err, "decoder accepts; Validate is the gate")
}
