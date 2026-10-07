package config

import (
	"fmt"
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
	// Unique names so the cap is the only finding; the duplicate-name
	// and collect-all behaviours have their own tests.
	classes := make([]TrafficClass, MaxTrafficClasses+1)
	for i := range classes {
		classes[i] = TrafficClass{Name: fmt.Sprintf("c%d", i), Hosts: []string{"h"}}
	}
	err := validTrafficClassesCfg(classes...).Validate()
	requireFieldError(t, err, "metrics.traffic_classes", "at most 8")
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
			requireFieldError(t, err, "metrics.traffic_classes[0].name", "must match")
		}
	}
}

func TestTrafficClasses_ReservedNameRejected(t *testing.T) {
	t.Parallel()
	err := validTrafficClassesCfg(TrafficClass{Name: "unclassified", Hosts: []string{"h"}}).Validate()
	requireFieldError(t, err, "metrics.traffic_classes[0].name", "reserved")
}

func TestTrafficClasses_DuplicateNameRejected(t *testing.T) {
	t.Parallel()
	err := validTrafficClassesCfg(
		TrafficClass{Name: "csr", Hosts: []string{"a"}},
		TrafficClass{Name: "csr", Hosts: []string{"b"}},
	).Validate()
	requireFieldError(t, err, "metrics.traffic_classes[1].name", "duplicate class name")
}

func TestTrafficClasses_NoHostsRejected(t *testing.T) {
	t.Parallel()
	err := validTrafficClassesCfg(TrafficClass{Name: "csr"}).Validate()
	requireFieldError(t, err, "metrics.traffic_classes[0].hosts", "no hosts")
}

func TestTrafficClasses_TooManyHostsRejected(t *testing.T) {
	t.Parallel()
	hosts := make([]string, MaxTrafficClassHosts+1)
	for i := range hosts {
		hosts[i] = "h"
	}
	err := validTrafficClassesCfg(TrafficClass{Name: "csr", Hosts: hosts}).Validate()
	requireFieldError(t, err, "metrics.traffic_classes[0].hosts", "more than 64 hosts")
}

// TestTrafficClasses_AllFindingsReportedAtOnce pins the errCollector
// behaviour (ADR follow-up to the early-return form): every invalid
// field across every class is reported in a single Validate call, each
// anchored to its own path, instead of only the first.
func TestTrafficClasses_AllFindingsReportedAtOnce(t *testing.T) {
	t.Parallel()
	cfg := validTrafficClassesCfg(
		TrafficClass{Name: "UPPER", Hosts: []string{"a", "*evil.com"}},
		TrafficClass{Name: "csr"},
		TrafficClass{Name: "csr", Hosts: []string{"b"}},
		TrafficClass{Name: "unclassified", Hosts: []string{"c"}},
	)
	var paths []string
	for _, fe := range fieldErrs(t, cfg.Validate()) {
		paths = append(paths, fe.Path)
	}
	assert.ElementsMatch(t, []string{
		"metrics.traffic_classes[0].name",
		"metrics.traffic_classes[0].hosts[1]",
		"metrics.traffic_classes[1].hosts",
		"metrics.traffic_classes[2].name",
		"metrics.traffic_classes[3].name",
	}, paths)
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
