package cache

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestBuildKeyFromURL_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, api.Key{}, BuildKeyFromURL("", nil))
}

func TestBuildKeyFromURL_Invalid(t *testing.T) {
	t.Parallel()
	// url.Parse rejects control characters.
	assert.Equal(t, api.Key{}, BuildKeyFromURL("ht\x00tp://invalid", nil))
}

func TestBuildKeyFromURL_Valid(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "http://example.com/foo?a=1&b=2", nil)
	expected := BuildKey(r, nil)
	assert.Equal(t, expected, BuildKeyFromURL("http://example.com/foo?a=1&b=2", nil))
}

func TestBuildKey_HTTPS_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "https://example.com/", nil)
	r2 := httptest.NewRequest("GET", "https://example.com:443/", nil)
	assert.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_NormaliseListHeader_NoComma(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "GZIP", normaliseListHeader("GZIP"))
	assert.Equal(t, "", normaliseListHeader(""))
}

func TestBuildKey_PolicySlowPath(t *testing.T) {
	t.Parallel()
	// Exercise the policy slow path (appendCanonicalQuerySlow) with keepParams.
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/search?q=test&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=test", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestBuildKey_PolicySlowPath_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=1&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestBuildKey_PolicySlowPath_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=1&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestBuildKey_PolicySlowPath_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=2", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestBuildKey_PercentEncodedNoPolicy(t *testing.T) {
	t.Parallel()
	// Percent-encoded params trigger the slow path.
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&b=2", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?b=2&a=1", nil)
	assert.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_MoreThan8Params(t *testing.T) {
	t.Parallel()
	// >8 params triggers the slow path.
	r1 := httptest.NewRequest("GET", "http://example.com/?a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?i=9&a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8", nil)
	assert.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
	require.NotEqual(t, api.Key{}, BuildKey(r1, nil))
}
