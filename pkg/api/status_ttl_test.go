package api

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusTTLPolicy_Resolution(t *testing.T) {
	t.Parallel()
	p, err := NewStatusTTLMap(map[string]time.Duration{
		"404": 30 * time.Second,
		"5xx": 10 * time.Second,
		"405": 0,
	})
	require.NoError(t, err)

	t.Run("table", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name   string
			status int
			want   time.Duration
		}{
			{"exact_code", 404, 30 * time.Second},
			{"class_member", 503, 10 * time.Second},
			{"class_lower_bound", 500, 10 * time.Second},
			{"class_upper_bound", 599, 10 * time.Second},
			{"outside_any_entry", 402, 0},
			{"uncovered_same_class_status", 410, 0},
			{"non_error_status", 200, 0},
		}
		for _, tt := range tests {
			assert.Equal(t, tt.want, p.TTL(tt.status), tt.name)
			assert.Equal(t, tt.want > 0, p.Cacheable(tt.status), tt.name)
		}
	})

	t.Run("exact_shadows_class", func(t *testing.T) {
		t.Parallel()
		// The "blanket + exception" idiom: 5xx at 10s, 503 at 30s.
		p, err := NewStatusTTLMap(map[string]time.Duration{
			"5xx": 10 * time.Second,
			"503": 30 * time.Second,
		})
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, p.TTL(503), "exact code shadows its class")
		assert.Equal(t, 10*time.Second, p.TTL(502), "other class members keep the class TTL")
	})

	t.Run("nil_policy_disables", func(t *testing.T) {
		t.Parallel()
		var p *StatusTTLPolicy
		assert.Equal(t, time.Duration(0), p.TTL(404))
		assert.False(t, p.Cacheable(404))
	})

	t.Run("empty_map_returns_nil_policy", func(t *testing.T) {
		t.Parallel()
		p, err := NewStatusTTLMap(map[string]time.Duration{})
		require.NoError(t, err)
		require.Nil(t, p)
	})

	t.Run("scalar_shorthand_expansion", func(t *testing.T) {
		t.Parallel()
		// negative_ttl: 30s decodes to the default error set — no more.
		p, err := NewStatusTTLMap(DefaultNegTTLMap(5 * time.Second))
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, p.TTL(404))
		assert.Equal(t, 5*time.Second, p.TTL(410))
		assert.Equal(t, time.Duration(0), p.TTL(503), "scalar shorthand must not leak to 5xx")
	})
}

func TestNewStatusTTLMap_Rejections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   map[string]time.Duration
	}{
		{"bad_key", map[string]time.Duration{"4o4": time.Second}},
		{"code_below_400", map[string]time.Duration{"302": time.Second}},
		{"code_out_of_range", map[string]time.Duration{"99": time.Second}},
		{"code_above_599", map[string]time.Duration{"600": time.Second}},
		{"negative_ttl", map[string]time.Duration{"404": -time.Second}},
		{"range_syntax_rejected", map[string]time.Duration{"500-599": time.Second}},
		{"unknown_class", map[string]time.Duration{"3xx": time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewStatusTTLMap(tt.in)
			require.Error(t, err)
		})
	}
}

func TestNewStatusTTLMap_Accepts(t *testing.T) {
	t.Parallel()
	p, err := NewStatusTTLMap(map[string]time.Duration{
		"404": 0,
		"400": time.Second,
		"5xx": time.Minute,
		"503": 0,
	})
	require.NoError(t, err)
	assert.False(t, p.Cacheable(404), "explicit zero disables")
	assert.Equal(t, time.Second, p.TTL(400))
	assert.Equal(t, time.Minute, p.TTL(502))
	assert.False(t, p.Cacheable(503), "exact zero shadows class for that code")
	assert.False(t, p.Cacheable(418), "4xx class not configured")

	// A zero class entry disables the whole class.
	p, err = NewStatusTTLMap(map[string]time.Duration{"5xx": 0})
	require.NoError(t, err)
	assert.False(t, p.Cacheable(503))
}

func TestDefaultNegTTLMap(t *testing.T) {
	t.Parallel()
	// The scalar shorthand expands to exactly the default error set —
	// nothing more, nothing less.
	m := DefaultNegTTLMap(30 * time.Second)
	require.Len(t, m, len(NegativeStatuses))
	for _, status := range NegativeStatuses {
		assert.Equal(t, 30*time.Second, m[strconv.Itoa(status)])
	}
	p, err := NewStatusTTLMap(m)
	require.NoError(t, err)
	assert.True(t, p.Cacheable(404))
	assert.True(t, p.Cacheable(405))
	assert.True(t, p.Cacheable(410))
	assert.True(t, p.Cacheable(501))
	assert.False(t, p.Cacheable(503), "scalar shorthand must not leak to 5xx")

	assert.Nil(t, DefaultNegTTLMap(0), "zero scalar disables negative caching")
	assert.Nil(t, DefaultNegTTLMap(-time.Second))
}

func TestParseStatusTTLKey(t *testing.T) {
	t.Parallel()
	lo, hi, err := ParseStatusTTLKey("5xx")
	require.NoError(t, err)
	assert.Equal(t, 500, lo)
	assert.Equal(t, 599, hi)

	lo, hi, err = ParseStatusTTLKey("4xx")
	require.NoError(t, err)
	assert.Equal(t, 400, lo)
	assert.Equal(t, 499, hi)

	lo, hi, err = ParseStatusTTLKey("404")
	require.NoError(t, err)
	assert.Equal(t, 404, lo)
	assert.Equal(t, 404, hi)

	_, _, err = ParseStatusTTLKey("abc")
	assert.Error(t, err)
	_, _, err = ParseStatusTTLKey("200")
	assert.Error(t, err, "2xx must be rejected: negative caching is for errors")
	_, _, err = ParseStatusTTLKey("500-599")
	assert.Error(t, err, "range syntax is replaced by class keys")
}

func TestStatusTTLPolicy_Entries(t *testing.T) {
	t.Parallel()
	p, err := NewStatusTTLMap(map[string]time.Duration{
		"5xx": 10 * time.Second,
		"404": time.Minute,
		"410": 0,
	})
	require.NoError(t, err)

	// Entries enumerate the configured policy, sorted by key — the
	// shape the operator wrote, exact codes and classes together.
	assert.Equal(t, []StatusTTLEntry{
		{Key: "404", TTL: time.Minute},
		{Key: "410", TTL: 0},
		{Key: "5xx", TTL: 10 * time.Second},
	}, p.Entries())

	// A scalar shorthand's policy enumerates its expansion.
	p, err = NewStatusTTLMap(DefaultNegTTLMap(30 * time.Second))
	require.NoError(t, err)
	assert.Equal(t, []StatusTTLEntry{
		{Key: "404", TTL: 30 * time.Second},
		{Key: "405", TTL: 30 * time.Second},
		{Key: "410", TTL: 30 * time.Second},
		{Key: "501", TTL: 30 * time.Second},
	}, p.Entries())

	var nilP *StatusTTLPolicy
	assert.Nil(t, nilP.Entries(), "nil policy has no entries")
}

func TestStatusTTLPolicy_CoversAnything(t *testing.T) {
	t.Parallel()
	p, err := NewStatusTTLMap(map[string]time.Duration{"404": 0})
	require.NoError(t, err)
	assert.False(t, p.CoversAnything(), "all-zero map caches nothing")

	p, err = NewStatusTTLMap(map[string]time.Duration{"5xx": 0, "404": 0})
	require.NoError(t, err)
	assert.False(t, p.CoversAnything(), "zero class and zero exact cache nothing")

	p, err = NewStatusTTLMap(map[string]time.Duration{"5xx": 0, "404": time.Second})
	require.NoError(t, err)
	assert.True(t, p.CoversAnything(), "one positive entry is enough")

	var nilP *StatusTTLPolicy
	assert.False(t, nilP.CoversAnything(), "nil policy covers nothing")
}
