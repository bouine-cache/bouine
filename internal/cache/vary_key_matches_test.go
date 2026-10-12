package cache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/server/h1parser"
	"github.com/bouine-cache/bouine/pkg/api"
)

// varyParityPolicies returns the KeyPolicy shapes that change the
// assertion hash: none, exclude_headers, cookie presence, verbatim
// Accept-Encoding, and all of them together.
func varyParityPolicies() map[string]*KeyPolicy {
	exclude := NewKeyPolicy(nil, nil, map[string]bool{"x-drop": true}, nil, false, false, nil, false)
	presence := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).
		WithCookiePresence([]string{"consent", "session_id"})
	verbatim := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false)
	verbatim.SetVerbatimAE(true)
	all := NewKeyPolicy(nil, nil, map[string]bool{"x-drop": true}, nil, false, false, nil, false).
		WithCookiePresence([]string{"consent"})
	all.SetVerbatimAE(true)
	return map[string]*KeyPolicy{
		"nil":      nil,
		"exclude":  exclude,
		"presence": presence,
		"verbatim": verbatim,
		"all":      all,
	}
}

func rawRequestFromWire(t *testing.T, headers string) *api.RawRequest {
	t.Helper()
	var req api.RawRequest
	wire := "GET /v HTTP/1.1\r\nHost: example.com\r\n" + headers + "\r\n"
	require.NoError(t, h1parser.ParseHeadersForTest([]byte(wire), &req))
	return &req
}

// TestVaryKeyMatchesParity pins varyKeyMatches and BuildVaryKey to the
// reference buildVaryKeySlow over both request views (the fast path's
// RawRequest and the slow path's header.Map) for every shape the
// zero-alloc hash handles itself and every shape it hands to the
// fallback.
func TestVaryKeyMatchesParity(t *testing.T) {
	t.Parallel()

	longVal := strings.Repeat("v", 300)
	manyFields := make([]string, maxVaryFields+1)
	for i := range manyFields {
		manyFields[i] = "x-f" + strings.Repeat("a", i)
	}
	cookiePresence := cookiePresenceField

	varies := []string{
		"",
		"*",
		"X-Region",
		"x-region",
		" b, a",
		"B,a",
		"X-Region, X-Region",
		"X-Region,",
		",",
		" ",
		"Accept-Encoding",
		"Accept-Language, Accept-Encoding, Accept",
		"X-Drop, X-Region",
		"X-Région",
		"X-Region,\u00a0X-Other",
		"\tX-Region\r",
		strings.Join(manyFields, ","),
		"X-Long",
		"X-Long, X-Region",
		cookiePresence,
		cookiePresence + ", X-Region",
		"Cookie",
	}
	headers := map[string]string{
		"none":            "",
		"basic":           "X-Region: US\r\nB: bee\r\nA: ay\r\n",
		"lowercase key":   "x-region: US\r\n",
		"trailing ows":    "X-Region: US \t\r\nX-Drop: d\r\n",
		"duplicate":       "X-Region: US\r\nX-Region: EU\r\n",
		"whitespace only": "X-Region:  \t\r\n",
		"encodings":       "Accept-Encoding: br, gzip\r\nAccept-Language: fr-CH, en;q=0.8\r\nAccept: text/html, */*\r\n",
		"long value":      "X-Long: " + longVal + "\r\nX-Region: US\r\n",
		"single cookie":   "Cookie: consent=1; other=2\r\n",
		"multi cookie":    "Cookie: consent=1\r\nCookie: session_id=x\r\n",
		"blank cookie":    "Cookie:  \t\r\nCookie: session_id=x \r\n",
		"cookie blank 2":  "Cookie: \t\r\nCookie: \r\n",
		"many fields":     "X-Fa: 1\r\nX-Faaaa: 2\r\n",
	}

	for policyName, policy := range varyParityPolicies() {
		for headerName, hdrs := range headers {
			req := rawRequestFromWire(t, hdrs)
			hm := reqHeaderMapFromRaw(req)
			for _, vary := range varies {
				want := buildVaryKeySlow(vary, hm, policy)
				if vary == "" || vary == "*" {
					want = vary
				}
				name := policyName + "/" + headerName + "/" + vary
				assert.Equal(t, want, BuildVaryKey(vary, hm, policy), name)
				assert.True(t, varyKeyMatches(vary, varyAssertSrc{r: req}, policy, want), "raw "+name)
				assert.True(t, varyKeyMatches(vary, varyAssertSrc{m: hm}, policy, want), "map "+name)
				assert.False(t, varyKeyMatches(vary, varyAssertSrc{r: req}, policy, want+"0"), "raw wrong "+name)
				assert.False(t, varyKeyMatches(vary, varyAssertSrc{m: hm}, policy, want+"0"), "map wrong "+name)
			}
		}
	}
}

// TestVaryKeyMatchesZeroAlloc pins the point of varyKeyMatches: both
// peer variant gates (the fast path's RawRequest view and the slow
// path's header.Map view) allocate nothing on custom-header Varies.
func TestVaryKeyMatchesZeroAlloc(t *testing.T) {
	req := rawRequestFromWire(t, "X-Region: US\r\nX-Device: mobile\r\n")
	hm := reqHeaderMapFromRaw(req)
	srcs := map[string]varyAssertSrc{"raw": {r: req}, "map": {m: hm}}
	for name, src := range srcs {
		for _, vary := range []string{"X-Region", "x-device, X-Region"} {
			want := buildVaryKeySlow(vary, hm, nil)
			allocs := testing.AllocsPerRun(100, func() {
				if !varyKeyMatches(vary, src, nil, want) {
					t.Fatal("gate mismatch")
				}
			})
			assert.Zero(t, allocs, name+" "+vary)
		}
	}
}

// FuzzVaryKeyMatchesParity checks varyKeyMatches over the RawRequest
// and map views against the reference buildVaryKeySlow for arbitrary
// Vary lists and request headers, under every policy shape.
func FuzzVaryKeyMatchesParity(f *testing.F) {
	f.Add("X-Region", "X-Region", "US", "Cookie", "consent=1")
	f.Add(" b, a", "B", "x \t", "a", "y")
	f.Add("B,a,", "b", "1", "A", "2")
	f.Add(cookiePresenceField+", X-Drop", "Cookie", " ", "Cookie", "session_id=1")
	f.Add("Accept-Encoding, Accept-Language", "accept-encoding", "br, gzip", "Accept-Language", "fr, en;q=0.5")
	f.Add("X-Région, x", "X", "v", "x", "w")
	f.Add(strings.Repeat("a,", 20), "A", strings.Repeat("z", 300), "a", "")

	policies := varyParityPolicies()
	f.Fuzz(func(t *testing.T, vary, k1, v1, k2, v2 string) {
		var req api.RawRequest
		req.Headers[0] = api.RawHeader{Key: k1, Value: v1}
		req.Headers[1] = api.RawHeader{Key: k2, Value: v2}
		req.Headers[2] = api.RawHeader{Key: k1, Value: v2}
		req.NHeaders = 3
		hm := reqHeaderMapFromRaw(&req)
		for name, policy := range policies {
			want := buildVaryKeySlow(vary, hm, policy)
			if vary == "" || vary == "*" {
				want = vary
			}
			if got := BuildVaryKey(vary, hm, policy); got != want {
				t.Fatalf("%s: BuildVaryKey(%q)=%q, want %q", name, vary, got, want)
			}
			if !varyKeyMatches(vary, varyAssertSrc{r: &req}, policy, want) {
				t.Fatalf("%s: raw view mismatch for vary %q", name, vary)
			}
			if !varyKeyMatches(vary, varyAssertSrc{m: hm}, policy, want) {
				t.Fatalf("%s: map view mismatch for vary %q", name, vary)
			}
		}
	})
}
