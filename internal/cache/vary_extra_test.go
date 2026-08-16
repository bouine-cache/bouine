package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeHeaderValue(t *testing.T) {
	t.Parallel()
	t.Run("no_comma_lowercase", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "gzip", normalizeHeaderValue("GZIP"))
	})
	t.Run("comma_separated_sorted", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "en,fr", normalizeHeaderValue("fr, en"))
	})
	t.Run("same_order_regardless_of_input", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, normalizeHeaderValue("en,FR"), normalizeHeaderValue("fr, en"))
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", normalizeHeaderValue(""))
	})
	t.Run("whitespace_trimmed", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "gzip", normalizeHeaderValue("  gzip  "))
	})
}

func TestVaryContainsStar(t *testing.T) {
	t.Parallel()
	t.Run("star_alone", func(t *testing.T) {
		t.Parallel()
		require.True(t, varyContainsStar("*"))
	})
	t.Run("star_with_spaces", func(t *testing.T) {
		t.Parallel()
		require.True(t, varyContainsStar(" * "))
	})
	t.Run("non_star", func(t *testing.T) {
		t.Parallel()
		require.False(t, varyContainsStar("Accept-Encoding"))
	})
	t.Run("accept_and_star", func(t *testing.T) {
		t.Parallel()
		require.True(t, varyContainsStar("Accept, *"))
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		require.False(t, varyContainsStar(""))
	})
}
