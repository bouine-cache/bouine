package cache

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// TestCookieValues_Reduction is the table-driven pin of the
// cookieValuesValue canonicalization (issue #870): positional
// "name=value" pairs over the sorted normalized name list, joined by
// "; ". Every rule here is load-bearing for cluster agreement (two
// nodes reducing the same request differently store/resolve variants
// under different keys).
func TestCookieValues_Reduction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		listed  []string
		cookies string
		want    string
	}{
		{
			name:    "basic values, sorted-name positional order",
			listed:  []string{"currency", "locale"},
			cookies: "locale=en; currency=eur; theme=dark",
			want:    "currency=eur; locale=en",
		},
		{
			name:    "value kept case-sensitive, name matched case-insensitively",
			listed:  []string{"locale"},
			cookies: "Locale=en-US",
			want:    "locale=en-US",
		},
		{
			name:    "value trimmed of surrounding OWS (RFC 6265 §5.2)",
			listed:  []string{"locale"},
			cookies: "locale =  en  ",
			want:    "locale=en",
		},
		{
			name:    "empty value is its own variant",
			listed:  []string{"locale"},
			cookies: "locale=",
			want:    "locale=",
		},
		{
			name:    "absent listed cookie hashes as empty value",
			listed:  []string{"locale", "currency"},
			cookies: "theme=dark",
			want:    "currency=; locale=",
		},
		{
			name:    "unlisted cookies ignored entirely",
			listed:  []string{"locale"},
			cookies: "a=1; locale=fr; b=2; c=3",
			want:    "locale=fr",
		},
		{
			name:    "duplicate listed name: last occurrence wins (RFC 6265 §5.4)",
			listed:  []string{"locale"},
			cookies: "locale=en; theme=x; locale=fr",
			want:    "locale=fr",
		},
		{
			name:    "name without value separator (flag-style cookie) is present with empty value",
			listed:  []string{"consent"},
			cookies: "consent",
			want:    "consent=",
		},
		{
			name:    "no Cookie header at all reduces to empty string",
			listed:  []string{"locale"},
			cookies: "",
			want:    "",
		},
		{
			name:    "quoted value kept verbatim including DQUOTEs (RFC 6265 §5.2 rule 6)",
			listed:  []string{"exp"},
			cookies: `exp="a=b"`,
			want:    `exp="a=b"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues(tt.listed)
			require.NotNil(t, policy)
			assert.Equal(t, tt.want, policy.cookieValuesValue(tt.cookies))
		})
	}
}

// TestCookieValues_ConfigOrderIndependent pins that the reduction — and
// the variant key built on it — does not depend on the order the names
// were listed in config: normalization sorts before the pairs are
// emitted, so every node reduces to the same bytes.
func TestCookieValues_ConfigOrderIndependent(t *testing.T) {
	t.Parallel()
	p1 := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale", "currency", "exp"})
	p2 := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"exp", "currency", "locale"})
	require.Equal(t, p1.CookieValuesVary(), p2.CookieValuesVary())

	primary := testkey.Key(31)
	slow := headerMap(header.Cookie, "locale=fr; currency=eur")
	require.Equal(t,
		VariantKey(primary, p1.CookieValuesVary(), slow, p1),
		VariantKey(primary, p2.CookieValuesVary(), slow, p2))
}

// TestCookieValues_NilAndEmptyGuards pins the nil-safety and the
// no-listing no-op: a policy without cookie_values reduces to "" and
// CookieValuesVary stays empty (the field never unions into Vary).
func TestCookieValues_NilAndEmptyGuards(t *testing.T) {
	t.Parallel()
	var nilPolicy *KeyPolicy
	assert.Equal(t, "", nilPolicy.CookieValuesVary(), "nil policy must be safe")
	assert.Equal(t, "", nilPolicy.cookieValuesValue("locale=fr"))

	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues(nil)
	assert.Equal(t, "", policy.CookieValuesVary())
	assert.Equal(t, "", policy.cookieValuesValue("locale=fr"))

	// An empty-list policy behaves exactly like none.
	assert.Equal(t, "", NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).
		WithCookieValues([]string{""}).CookieValuesVary())
}

// TestCookieValues_DerivedVaryShape pins the ADR-0071 derivedVary: the
// value field unions into the stored Vary exactly like the presence
// field and include_headers — sorted, deduplicated, ", "-joined — so
// every construction path reads one constant.
func TestCookieValues_DerivedVaryShape(t *testing.T) {
	t.Parallel()
	newBase := func() *KeyPolicy {
		return NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false)
	}
	tests := []struct {
		name   string
		policy *KeyPolicy
		want   string
	}{
		{
			name:   "values alone",
			policy: newBase().WithCookieValues([]string{"locale"}),
			want:   cookieValuesField,
		},
		{
			name:   "values + presence: both synthetic fields, sorted",
			policy: newBase().WithCookieValues([]string{"locale"}).WithCookiePresence([]string{"consent"}),
			want:   cookiePresenceField + ", " + cookieValuesField,
		},
		{
			name: "values + include_headers: union sorted",
			policy: NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Beta"}, false).
				WithCookieValues([]string{"locale"}),
			want: "x-beta, " + cookieValuesField,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.policy.derivedVary)
			require.Equal(t, tt.want, tt.policy.flightVary(), "flight key must hash the same declared dimensions")
		})
	}
}

// testHandlerCookieValues builds a Handler with cache.key.cookie_values
// enabled (issue #870): the listed cookie names contribute their values
// to the variant key.
func testHandlerCookieValues(t *testing.T, upstream fasthttp.RequestHandler, names []string) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
		Policy:     NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues(names),
	})
}

// Value-keyed variants: a request with locale=en must never receive the
// variant stored for locale=fr — and vice versa; the same value HITs.
// This is the Cloudflare cookie.include partitioning property.
func TestCookieValues_VariantsKeyedByValue(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerCookieValues(t, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("render-for-locale=" + string(ctx.Request.Header.Peek(header.Cookie))))
	}, []string{"locale"})

	en := testCtx("GET", "http://example.com/page")
	en.Request.Header.Set(header.Cookie, "locale=en")
	h.ServeRequest(en)
	require.Equal(t, "MISS", respHeader(en, header.XCache))

	fr := testCtx("GET", "http://example.com/page")
	fr.Request.Header.Set(header.Cookie, "locale=fr; theme=dark")
	h.ServeRequest(fr)
	assert.Equal(t, "MISS", respHeader(fr, header.XCache))
	assert.Equal(t, "render-for-locale=locale=fr; theme=dark", respBody(fr), "different value must be its own variant with its own body")

	// Same value, other unlisted cookies: HIT the same variant.
	en2 := testCtx("GET", "http://example.com/page")
	en2.Request.Header.Set(header.Cookie, "locale=en; analytics=1; session=abc")
	h.ServeRequest(en2)
	assert.Equal(t, "HIT", respHeader(en2, header.XCache))
	assert.Equal(t, "render-for-locale=locale=en", respBody(en2),
		"unlisted cookies must not fragment the value-keyed variant")

	// No cookie at all: the empty-value variant, its own MISS then HIT.
	anon := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon)
	assert.Equal(t, "MISS", respHeader(anon, header.XCache))
	anon2 := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon2)
	assert.Equal(t, "HIT", respHeader(anon2, header.XCache))
	assert.Equal(t, "render-for-locale=", respBody(anon2))
}

// Variant-key computation must be byte-identical across the three
// request representations (slow path header.Map, fasthttp Peek path,
// RawRequest path) or store/lookup and the peer gates diverge — the
// parity invariant ADR-0061 established, applied to cookie values.
func TestCookieValues_KeyParityAcrossPaths(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale", "currency"})
	primary := testkey.Key(41)
	vary := policy.CookieValuesVary()

	// Slow path: one joined Cookie line (fasthttp's All() shape).
	slow := headerMap(header.Cookie, "currency=eur; locale=en")
	kSlow := VariantKey(primary, vary, slow, policy)

	// Fasthttp shape: Peek returns the joined value.
	fastHdr := &fasthttp.RequestHeader{}
	fastHdr.Set(header.Cookie, "currency=eur; locale=en")
	kFast := VariantKeyFast(primary, vary, fastHdr, policy)

	// RawRequest shape: two separate Cookie lines (the h1parser keeps
	// them; the value reduction must see both).
	raw := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	raw.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "currency=eur"}
	raw.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "locale=en"}
	raw.NHeaders = 2
	kRaw := VariantKeyFromRaw(primary, vary, raw, policy)

	require.Equal(t, kSlow, kFast, "slow path and fasthttp Peek path must agree")
	require.Equal(t, kSlow, kRaw, "RawRequest path must agree despite split Cookie lines")
}

// The stored VaryKey assertion (BuildVaryKey — the peer-gate hash) must
// key on the same value pairs as the variant key (VariantKey), or peers
// reject every exchange of a value-keyed variant.
func TestCookieValues_VaryKeyAssertionParity(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale"})
	primary := testkey.Key(43)
	vary := policy.CookieValuesVary()
	slow := headerMap(header.Cookie, "locale=fr")
	vk := VariantKey(primary, vary, slow, policy)
	require.NotEqual(t, primary, vk, "a listed cookie with a value must move the variant key")

	assertion := BuildVaryKey(vary, slow, policy)
	require.NotEmpty(t, assertion)

	// A different value changes both hashes.
	slowEn := headerMap(header.Cookie, "locale=en")
	require.NotEqual(t, vk, VariantKey(primary, vary, slowEn, policy))
	require.NotEqual(t, assertion, BuildVaryKey(vary, slowEn, policy))

	// The same value with extra unlisted cookies keeps both hashes.
	slowExtra := headerMap(header.Cookie, "locale=fr; analytics=1")
	require.Equal(t, vk, VariantKey(primary, vary, slowExtra, policy))
	require.Equal(t, assertion, BuildVaryKey(vary, slowExtra, policy))
}

// Long cookie values must overflow the 256-byte variant-key stack
// buffer onto the allocating slow path and still hash identically from
// every adapter — the fallback is not a semantics change.
func TestCookieValues_OverflowPathParity(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale", "currency"})
	primary := testkey.Key(45)
	vary := policy.CookieValuesVary()
	longVal := strings.Repeat("x", 300)
	joined := "locale=" + longVal + "; currency=eur"

	slow := headerMap(header.Cookie, joined)
	fastHdr := &fasthttp.RequestHeader{}
	fastHdr.Set(header.Cookie, joined)
	raw := &api.RawRequest{
		Method: "GET", Path: "/", Host: "example.com", Scheme: "http", HTTPVersion: "HTTP/1.1",
	}
	raw.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "locale=" + longVal}
	raw.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "currency=eur"}
	raw.NHeaders = 2

	require.Equal(t,
		VariantKey(primary, vary, slow, policy),
		VariantKeyFast(primary, vary, fastHdr, policy),
		"slow and fast adapters must agree on the overflow path")
	require.Equal(t,
		VariantKey(primary, vary, slow, policy),
		VariantKeyFromRaw(primary, vary, raw, policy))
	require.NotEqual(t, primary, VariantKey(primary, vary, slow, policy))
}

// effectiveVary must union the synthetic value field into the origin's
// Vary (never replace it), exactly as it does for include_headers and
// the presence field — the stored VaryValue is what every store site,
// lookup, and 304 revalidation hashes.
func TestCookieValues_EffectiveVaryUnion(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale"})
	got := effectiveVary(headerMap(header.Vary, "Accept-Encoding"), policy)
	require.Equal(t, "accept-encoding, "+cookieValuesField, got)

	// Values + presence + include_headers: one sorted union, all fields.
	combined := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Beta"}, false).
		WithCookieValues([]string{"locale"}).
		WithCookiePresence([]string{"consent"})
	got = effectiveVary(headerMap(header.Vary, "Accept-Encoding"), combined)
	require.Equal(t, "accept-encoding, x-beta, "+cookiePresenceField+", "+cookieValuesField, got)

	// No cookie keying, no include: passthrough unchanged (origin
	// case preserved — the union path is what lowercases).
	plain := effectiveVary(headerMap(header.Vary, "Accept-Encoding"), nil)
	require.Equal(t, "Accept-Encoding", plain)
}
