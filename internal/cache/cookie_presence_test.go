package cache

import (
	"log/slog"
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

// testHandlerCookiePresence builds a Handler with
// cache.key.cookie_presence enabled (issue #768): the listed cookie
// names contribute presence bits to the variant key.
func testHandlerCookiePresence(t *testing.T, upstream fasthttp.RequestHandler, names []string) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
		Policy:     NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence(names),
	})
}

// Presence-keyed variants: a request whose listed cookie is present must
// never receive the variant stored for requests where it is absent —
// and vice versa. This is the personalized-SSR safety property: the
// consent-state (say) changes the origin render, so it must select its
// own variant.
func TestCookiePresence_VariantsKeyedByPresence(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerCookiePresence(t, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("render-for-consent=" + string(ctx.Request.Header.Peek(header.Cookie))))
	}, []string{"consent"})

	// Absent-cookie request fills the "consent absent" variant.
	anon := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Present-cookie request is a different variant: MISS, own body.
	present := testCtx("GET", "http://example.com/page")
	present.Request.Header.Set(header.Cookie, "consent=accepted; analytics=1")
	h.ServeRequest(present)
	assert.Equal(t, "MISS", respHeader(present, header.XCache))
	assert.Equal(t, "render-for-consent=consent=accepted; analytics=1", respBody(present))

	// Both variants now HIT for their own population.
	anon2 := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon2)
	assert.Equal(t, "HIT", respHeader(anon2, header.XCache))
	assert.Equal(t, "render-for-consent=", respBody(anon2), "absent variant must serve the absent render")

	present2 := testCtx("GET", "http://example.com/page")
	present2.Request.Header.Set(header.Cookie, "consent=rejected; other=x")
	h.ServeRequest(present2)
	assert.Equal(t, "HIT", respHeader(present2, header.XCache))
	assert.Equal(t, "render-for-consent=consent=accepted; analytics=1", respBody(present2),
		"present variant keyed on presence, not value: a different consent value shares the variant")
}

// Cookie VALUES must never affect the variant key: presence only. A
// different value for the same listed name is the same variant.
func TestCookiePresence_ValuesNotKeyed(t *testing.T) {
	t.Parallel()
	h := testHandlerCookiePresence(t, origin200("shared"), []string{"consent"})

	first := testCtx("GET", "http://example.com/val")
	first.Request.Header.Set(header.Cookie, "consent=yes")
	h.ServeRequest(first)
	require.Equal(t, "MISS", respHeader(first, header.XCache))

	// Different value, same presence: HIT the same variant.
	second := testCtx("GET", "http://example.com/val")
	second.Request.Header.Set(header.Cookie, "consent=no")
	h.ServeRequest(second)
	assert.Equal(t, "HIT", respHeader(second, header.XCache))
}

// Unlisted cookies must not fragment the cache: two requests differing
// only in an unlisted cookie share the variant.
func TestCookiePresence_UnlistedCookiesShareVariant(t *testing.T) {
	t.Parallel()
	h := testHandlerCookiePresence(t, origin200("shared"), []string{"consent"})

	first := testCtx("GET", "http://example.com/un")
	first.Request.Header.Set(header.Cookie, "analytics=1")
	h.ServeRequest(first)
	require.Equal(t, "MISS", respHeader(first, header.XCache))

	second := testCtx("GET", "http://example.com/un")
	second.Request.Header.Set(header.Cookie, "analytics=2; theme=dark")
	h.ServeRequest(second)
	assert.Equal(t, "HIT", respHeader(second, header.XCache))
}

// Variant-key computation must be byte-identical across the three
// request representations (slow path header.Map, fasthttp Peek path,
// RawRequest path) or store/lookup and the peer gates diverge — the
// parity invariant ADR-0051 established for encoding buckets, applied
// to cookie presence.
func TestCookiePresence_KeyParityAcrossPaths(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent", "session_id"})
	primary := testkey.Key(7)
	vary := policy.CookiePresenceVary()

	// Slow path: one joined Cookie line (fasthttp's All() shape).
	slow := headerMap(header.Cookie, "consent=yes; session_id=abc")
	kSlow := VariantKey(primary, vary, slow, policy)

	// Fasthttp shape: Peek returns the joined value.
	fastHdr := &fasthttp.RequestHeader{}
	fastHdr.Set(header.Cookie, "consent=yes; session_id=abc")
	kFast := VariantKeyFast(primary, vary, fastHdr, policy)

	// RawRequest shape: two separate Cookie lines (the h1parser keeps
	// them; presence must see both).
	raw := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	raw.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "consent=yes"}
	raw.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "session_id=abc"}
	raw.NHeaders = 2
	kRaw := VariantKeyFromRaw(primary, vary, raw, policy)

	require.Equal(t, kSlow, kFast, "slow path and fasthttp Peek path must agree")
	require.Equal(t, kSlow, kRaw, "RawRequest path must agree despite split Cookie lines")
}

// The presence-keyed Vary must be deterministic and config-order
// independent: the same names in a different config order produce the
// same Vary field and the same keys (the union is sorted).
func TestCookiePresence_ConfigOrderIndependent(t *testing.T) {
	t.Parallel()
	p1 := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent", "session_id"})
	p2 := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"session_id", "consent"})
	require.Equal(t, p1.CookiePresenceVary(), p2.CookiePresenceVary())

	primary := testkey.Key(9)
	slow := headerMap(header.Cookie, "session_id=x")
	require.Equal(t, VariantKey(primary, p1.CookiePresenceVary(), slow, p1),
		VariantKey(primary, p2.CookiePresenceVary(), slow, p2))
}

// The stored VaryKey assertion (BuildVaryKey — the peer-gate hash) must
// key on the same presence bits as the variant key (VariantKey), or
// peers reject every exchange of a presence-keyed variant.
func TestCookiePresence_VaryKeyAssertionParity(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent"})
	primary := testkey.Key(11)
	vary := policy.CookiePresenceVary()
	slow := headerMap(header.Cookie, "consent=1")
	vk := VariantKey(primary, vary, slow, policy)
	assertNotPrimary(t, vk, primary)

	assertion := BuildVaryKey(vary, slow, policy)
	require.NotEmpty(t, assertion)

	// A different presence changes both hashes.
	slowAbsent := headerMap(header.Cookie, "other=1")
	vk2 := VariantKey(primary, vary, slowAbsent, policy)
	assertion2 := BuildVaryKey(vary, slowAbsent, policy)
	require.NotEqual(t, vk, vk2)
	require.NotEqual(t, assertion, assertion2)

	// A different VALUE keeps both hashes.
	slowOtherValue := headerMap(header.Cookie, "consent=2")
	require.Equal(t, vk, VariantKey(primary, vary, slowOtherValue, policy))
	require.Equal(t, assertion, BuildVaryKey(vary, slowOtherValue, policy))
}

func assertNotPrimary(t *testing.T, k, primary api.Key) {
	t.Helper()
	require.NotEqual(t, primary, k, "variant key must differ from the primary key when a listed cookie is present")
}

// A presence-keyed route with refresh-before-expiry must keep the
// VaryValue/VaryKey pair consistent through a 304 background refresh
// (issue #768): the registry saves the Cookie header, the replayed
// conditional fetch carries it, and refreshFrom304 recomputes VaryKey
// from the same presence bits the store path hashed. Without the
// registry fix, the replayed request would be cookie-less, the bits
// would rehash as all-absent, and the peer gates would reject every
// refreshed object until TTL.
func TestCookiePresence_RefreshRegistryReplaysCookie(t *testing.T) {
	t.Parallel()
	origin := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("render"))
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream:            origin,
		FastClient:          &testFastClient{handler: origin},
		Store:               store,
		Logger:              slog.Default(),
		RefreshBeforeExpiry: true,
		Policy:              NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent"}),
	})

	cookied := testCtx("GET", "http://example.com/p")
	cookied.Request.Header.Set(header.Cookie, "consent=yes")
	h.ServeRequest(cookied)
	require.Equal(t, "MISS", respHeader(cookied, header.XCache))

	primaryKey := h.buildKey(cookied)
	ri := requestInfoFromCtx(cookied)
	variantKey := VariantKey(primaryKey, h.policy.CookiePresenceVary(), ri.Header, h.policy)

	// The variant must be registered for background refresh.
	entry := h.refreshRegistry.Lookup(variantKey)
	require.NotNil(t, entry, "presence-keyed variant must be refresh-registered")

	// The registered entry replays the Cookie header, and the VaryKey
	// recomputed from the replay equals the one computed from the
	// original request — the pair the peer gates compare.
	require.Equal(t, "consent=yes", entry.header.Get(header.Cookie))
	stored := BuildVaryKey(h.policy.CookiePresenceVary(), ri.Header, h.policy)
	replayed := BuildVaryKey(h.policy.CookiePresenceVary(), entry.header, h.policy)
	require.Equal(t, stored, replayed)
}
