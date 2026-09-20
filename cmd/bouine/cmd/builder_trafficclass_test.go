package cmd

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
)

// captureLogger collects Error records so tests can assert on what the
// engine logs at boot (AGENTS.md §12: the boundary logs, layers return).
type captureLogger struct {
	mu     sync.Mutex
	errors []string
}

func (l *captureLogger) Info(string, ...any)  {}
func (l *captureLogger) Warn(string, ...any)  {}
func (l *captureLogger) Debug(string, ...any) {}

func (l *captureLogger) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, msg)
}

// TestBuildRouter_LogsShadowedTrafficClassPatterns proves the wiring,
// not just the detector: a config whose later traffic class is fully
// shadowed must surface at Error level at boot while boot still
// proceeds (declaration order stays the precedence, ADR-0047).
func TestBuildRouter_LogsShadowedTrafficClassPatterns(t *testing.T) {
	t.Parallel()
	log := &captureLogger{}
	e := &engine{
		cfg: &config.Config{
			Metrics: config.MetricsConfig{
				TrafficClasses: []config.TrafficClass{
					{Name: "csr", Hosts: []string{"*.example.com"}},
					{Name: "ssr", Hosts: []string{"api.example.com"}},
				},
			},
		},
		logger: log,
	}
	rs := &runState{}
	e.buildRouter(rs)

	require.NotNil(t, rs.trafficClassify)
	require.Len(t, log.errors, 1)
	assert.Contains(t, log.errors[0], `"api.example.com" in class "ssr"`)
	assert.Contains(t, log.errors[0], `"*.example.com" in class "csr"`)

	// Soft failure: the classifier stays fully functional and keeps
	// first-match precedence.
	assert.Equal(t, "csr", rs.trafficClassify.Classify("api.example.com"))
}

// TestBuildRouter_NoShadowFindingIsSilent pins the quiet path: clean
// (or absent) traffic-class config logs no Error records at boot.
func TestBuildRouter_NoShadowFindingIsSilent(t *testing.T) {
	t.Parallel()
	log := &captureLogger{}
	e := &engine{
		cfg: &config.Config{
			Metrics: config.MetricsConfig{
				TrafficClasses: []config.TrafficClass{
					{Name: "csr", Hosts: []string{"*.example.com"}},
					{Name: "ssr", Hosts: []string{"*.svc.cluster.local"}},
				},
			},
		},
		logger: log,
	}
	rs := &runState{}
	e.buildRouter(rs)

	require.NotNil(t, rs.trafficClassify)
	for _, msg := range log.errors {
		assert.False(t, strings.Contains(msg, "traffic_class:"), "unexpected finding: %s", msg)
	}
}
