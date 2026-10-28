package cmd

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
)

// This file pins the engine-side near-cache wiring (ADR-0071): the
// feature's construction (one shared instance + registered metrics),
// its disabled default, and the inert-config warning — enabling
// experimental.near_cache without a live strong-mode cluster is a
// config the operator must hear about, not one the daemon silently
// ignores.

func nearWiringEngine(t *testing.T, mode config.ClusterMode, logs *bytes.Buffer) *engine {
	t.Helper()
	e := &engine{
		cfg: &config.Config{
			Cluster: config.Cluster{Mode: mode},
			Experimental: config.ExperimentalConfig{
				NearCache: &config.NearCacheConfig{
					Enabled:  true,
					MaxBytes: 32 << 20,
					MaxTTL:   45 * time.Second,
				},
			},
		},
		metrics: observability.NewMetrics(),
		logger:  slog.New(slog.NewTextHandler(logs, nil)),
	}
	return e
}

func TestBuildNearCache_DisabledIsNil(t *testing.T) {
	t.Parallel()
	logs := &bytes.Buffer{}
	e := &engine{
		cfg:     &config.Config{},
		metrics: observability.NewMetrics(),
		logger:  slog.New(slog.NewTextHandler(logs, nil)),
	}
	require.Nil(t, e.buildNearCache(true), "no near_cache block means no instance")
	require.NotContains(t, logs.String(), "near-cache", "nothing is logged for the default")
}

func TestBuildNearCache_StrongClusterIsSilent(t *testing.T) {
	t.Parallel()
	logs := &bytes.Buffer{}
	e := nearWiringEngine(t, config.ClusterModeStrong, logs)
	near := e.buildNearCache(true)
	require.NotNil(t, near)
	assert.Contains(t, logs.String(), "experimental near-cache enabled")
	assert.NotContains(t, logs.String(), "level=WARN", "a live strong cluster has nothing to warn about")
}

func TestBuildNearCache_WarnsWhenInert(t *testing.T) {
	t.Parallel()
	t.Run("eventual mode", func(t *testing.T) {
		t.Parallel()
		logs := &bytes.Buffer{}
		e := nearWiringEngine(t, config.ClusterModeEventual, logs)
		require.NotNil(t, e.buildNearCache(true))
		assert.Contains(t, logs.String(), "not in strong mode", "an inert feature must be visible")
	})
	t.Run("cluster init failed", func(t *testing.T) {
		t.Parallel()
		logs := &bytes.Buffer{}
		e := nearWiringEngine(t, config.ClusterModeStrong, logs)
		require.NotNil(t, e.buildNearCache(false))
		assert.Contains(t, logs.String(), "not in strong mode", "the warning covers a dead cluster too")
	})
	t.Run("single node", func(t *testing.T) {
		t.Parallel()
		logs := &bytes.Buffer{}
		e := nearWiringEngine(t, "", logs)
		require.NotNil(t, e.buildNearCache(false))
		assert.Contains(t, logs.String(), "not in strong mode")
	})
}

func TestBuildNearCache_MetricsRegistered(t *testing.T) {
	t.Parallel()
	logs := &bytes.Buffer{}
	e := nearWiringEngine(t, config.ClusterModeStrong, logs)
	require.NotNil(t, e.buildNearCache(true))
	count := 0
	families, err := e.metrics.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if strings.HasPrefix(mf.GetName(), "bouine_near_cache_") {
			count++
		}
	}
	assert.EqualValues(t, 3, count, "hits, admissions and invalidations counters must be registered")
}
