package cache

import (
	"bufio"
	"strings"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// BenchmarkGate_VaryKey_CookieValues gates the value-pair reduction on
// the variant-key hot path for value-keyed routes (issue #870): the
// slow-path header.Map form. Each iteration hashes the synthetic field:
// one reduced "name=value" pair string (two listed names, short values)
// plus the CookieAll read of the Cookie line. Budget 1 — the reduction
// string is the floor of the design (one allocation, stack-allocated
// pair values); the cost is value-keyed-route-only, every other route
// pays nothing (the field is absent from its Vary).
func BenchmarkGate_VaryKey_CookieValues(b *testing.B) {
	primary := testkey.Key(51)
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale", "currency"})
	hm := headerMap(header.Cookie, "locale=en; currency=eur; theme=dark")
	vary := policy.CookieValuesVary()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKey(primary, vary, hm, policy)
	}
}

// BenchmarkGate_VaryKey_CookieValuesFast is the fasthttp form: the
// slow-path lookup's pre-collection state (VariantKeyFast over a parsed
// fasthttp header). The PeekAll canonicalization costs one string
// conversion on top of the map form's reduction; budget 2 — the
// presence-tier ceiling (VaryKey_CookiePresenceFast).
func BenchmarkGate_VaryKey_CookieValuesFast(b *testing.B) {
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale", "currency"})
	primary := testkey.Key(52)
	vary := policy.CookieValuesVary()

	var fh fasthttp.RequestHeader
	wire := "GET / HTTP/1.1\r\nHost: example.com\r\nCookie: locale=en; currency=eur; theme=dark\r\n\r\n"
	if err := fh.Read(bufio.NewReader(strings.NewReader(wire))); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKeyFast(primary, vary, &fh, policy)
	}
}

// BenchmarkGate_VaryKey_CookieValuesRaw is the RawRequest form (the H1
// fast path's lookup): the h1parser keeps Cookie lines separately, so
// the reduction joins them (api.RawRequest.CookieValue) before hashing
// the pairs. Budget 4 — one more alloc than the Map form for the join,
// the presence-tier ceiling (VaryKey_CookiePresenceRaw).
func BenchmarkGate_VaryKey_CookieValuesRaw(b *testing.B) {
	primary := testkey.Key(53)
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookieValues([]string{"locale", "currency"})
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	req.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "locale=en"}
	req.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "currency=eur"}
	req.NHeaders = 2
	vary := policy.CookieValuesVary()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKeyFromRaw(primary, vary, req, policy)
	}
}

// BenchmarkGate_VaryKey_NoCookieKeyingZeroCost pins the invariant that
// routes WITHOUT cookie keying pay zero extra allocations for the
// synthetic-field dispatch: neither cookie field is present in their
// Vary, so neither reduction branch ever runs. The cookie-bearing
// request hashes exactly as it did before this feature — the same
// shape VaryKey_NoPresenceZeroCost pins for the presence field.
func BenchmarkGate_VaryKey_NoCookieKeyingZeroCost(b *testing.B) {
	primary := testkey.Key(54)
	hm := headerMap(header.Cookie, "locale=en; analytics=2")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKey(primary, "Cookie", hm, nil)
	}
}
