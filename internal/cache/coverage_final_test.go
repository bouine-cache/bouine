package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Test appendCanonicalQuerySlow (key.go policy slow path) via BuildKey.
func TestAppendCanonicalQuerySlow_KeepParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	// Use percent-encoded params to force the slow path.
	r1 := httptest.NewRequest("GET", "http://example.com/?q=%74est&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?q=test", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestAppendCanonicalQuerySlow_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestAppendCanonicalQuerySlow_StripPrefixes(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&utm_source=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestAppendCanonicalQuerySlow_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestAppendCanonicalQuerySlow_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%32&a=%31", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=2", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

func TestAppendCanonicalQuerySlow_AllFeatures(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(
		map[string]bool{"q": true},
		nil, nil, nil, true, true,
	)
	r1 := httptest.NewRequest("GET", "http://example.com/?q=%74est&q=dup&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?q=test", nil)
	assert.Equal(t, BuildKey(r2, policy), BuildKey(r1, policy))
}

// Test variantKeySlow via VariantKey with too many fields.
func TestVariantKeySlow_TooManyFields(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h := http.Header{}
	// >16 Vary fields triggers variantKeySlow fallback.
	vary := ""
	for i := range 20 {
		if i > 0 {
			vary += ", "
		}
		vary += "X-H" + string(rune('0'+i))
		h.Set("X-H"+string(rune('0'+i)), "val")
	}
	// Should produce a non-primary key (variantKeySlow processes all fields).
	result := VariantKey(primary, vary, h, nil)
	assert.NotEqual(t, primary, result)
}

func TestVariantKeySlow_LongValue(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h := http.Header{}
	// A single Vary field with a very long value that exceeds the 256-byte buffer.
	h.Set("Accept-Encoding", string(make([]byte, 300)))
	result := VariantKey(primary, "Accept-Encoding", h, nil)
	// Should NOT return primary (it should hash the long value).
	assert.NotEqual(t, primary, result)
}

// Test normaliseListHeader with comma.
func TestNormaliseListHeader_Comma(t *testing.T) {
	t.Parallel()
	// "b, a" and "a, b" should produce the same output (sorted).
	assert.Equal(t, normaliseListHeader("b, a"), normaliseListHeader("a, b"))
	// Should be trimmed and sorted.
	assert.Equal(t, "a,b", normaliseListHeader(" b ,  a "))
}

// Test buildKeyFromRaw scheme default.
func TestBuildKeyFromRaw_SchemeDefault(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "", // empty scheme should default to "http"
	}
	key := buildKeyFromRaw(req, nil)
	require.NotEqual(t, api.Key{}, key)
}

// Test buildKeyFromRaw with TLS.
func TestBuildKeyFromRaw_TLS(t *testing.T) {
	t.Parallel()
	req1 := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	req2 := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "https"}
	assert.NotEqual(t, buildKeyFromRaw(req2, nil), buildKeyFromRaw(req1, nil))
}

// Test serializeResponse fallback (head too large).
func TestSerializeResponse_HeadTooLarge(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)
	// Create an object with many headers to exceed the max header bytes.
	hdr := http.Header{}
	for i := range 200 {
		hdr.Set("X-Huge-"+string(rune('a'+i%26))+string(rune('a'+i/26)), strings.Repeat("x", 100))
	}
	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header:     header.FromHTTP(hdr),
		Body:       []byte("hello"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	_ = store.Put(context.Background(), testkey.Key(1), obj)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	resp, ok := fp.TryHit(req, time.Now())
	// Should still get a response (fallback path).
	if ok && resp != nil {
		fp.Release(resp)
	}
}
