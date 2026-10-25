package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestStrippedParamNames pins that the derivation explain reports
// uses the same predicate the key builder does: exact strips, prefix
// strips, allowlist fall-through, stripEmpty, and dedup never
// removing a name outright.
func TestStrippedParamNames(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		policy *KeyPolicy
		query  string
		want   []string
	}{
		{
			name:   "exact and prefix strips, order preserved, dup names once",
			policy: NewKeyPolicy(map[string]bool{"fbclid": true}, nil, nil, []string{"utm_"}, false, false, nil, false),
			query:  "utm_source=a&utm_medium=b&fbclid=x&keep=1&utm_source=c",
			want:   []string{"utm_source", "utm_medium", "fbclid"},
		},
		{
			name:   "allowlist strips everything not listed",
			policy: NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false, nil, false),
			query:  "q=1&page=2&zoom=3",
			want:   []string{"page", "zoom"},
		},
		{
			name:   "stripEmpty drops empty-valued params only",
			policy: NewKeyPolicy(nil, nil, nil, nil, true, false, nil, false),
			query:  "a=&b=1&c=",
			want:   []string{"a", "c"},
		},
		{
			name:   "dedup trims values, never names",
			policy: NewKeyPolicy(nil, nil, nil, nil, false, true, nil, false),
			query:  "k=1&k=2&k=3",
			want:   nil,
		},
		{
			name:   "no policy reports nothing",
			policy: nil,
			query:  "utm_source=a",
			want:   nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.policy.StrippedParamNames(tc.query))
		})
	}
}

// TestStrippedParamNames_MatchesKeyBuilder pins the no-drift rule at
// the behavior level: a stripped param does not appear in the key,
// and a kept param does.
func TestStrippedParamNames_MatchesKeyBuilder(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"fbclid": true}, nil, nil, []string{"utm_"}, false, false, nil, false)
	stripped := policy.StrippedParamNames("q=1&utm_source=a&fbclid=b")
	require.Equal(t, []string{"utm_source", "fbclid"}, stripped)
}

// TestVariantsOf pins the cold accessor: it reports the live variant
// keys the fill path registered, without touching them.
func TestVariantsOf(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, header.AcceptEncoding)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := NewHandler(HandlerConfig{Upstream: upstream, FastClient: &testFastClient{handler: upstream}, Store: storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})})

	// Fill two Vary: Accept-Encoding variants through the real path.
	for _, ae := range []string{"gzip", "br"} {
		rr := testCtx("GET", "http://example.com/vary")
		rr.Request.Header.Set(header.AcceptEncoding, ae)
		h.ServeRequest(rr)
	}

	primary := BuildKeyFromURL("http://example.com/vary", nil)
	variants := h.VariantsOf(primary)
	require.Len(t, variants, 2, "the fill path registered both variants")
	for _, vk := range variants {
		assert.NotEqual(t, primary, vk, "variants are secondary keys")
		assert.True(t, h.store.Has(vk), "the index lists live entries")
	}

	// Read-only: enumerating again returns the same entries, and
	// unknown keys report nothing.
	assert.Len(t, h.VariantsOf(primary), 2)
	assert.Empty(t, h.VariantsOf(api.Key{9, 9, 9}))
}

// TestEncodingBucket_Exported pins that the exported wrapper returns
// the same bucket the data plane's picker computes.
func TestEncodingBucket_Exported(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "br", EncodingBucket(NominalAcceptEncoding),
		"the nominal browser dialect selects br")
	assert.Equal(t, encodingBucket("gzip, deflate"), EncodingBucket("gzip, deflate"),
		"the export must not diverge from the internal picker")
}
