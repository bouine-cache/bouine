package cache

import (
	"bufio"
	"strings"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// BenchmarkGate_VaryKey_CookiePresenceFast gates the presence-bit
// computation on the slow-path lookup (VariantKeyFast over a parsed
// fasthttp header, pre-collection — the state ServeRequest's lookup
// sees). The PeekAll canonicalization costs one string conversion on
// top of the map shape's bits string; budget 2.
func BenchmarkGate_VaryKey_CookiePresenceFast(b *testing.B) {
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent", "analytics"})
	primary := testkey.Key(24)
	vary := policy.CookiePresenceVary()

	// Real parsed wire request (single Cookie line, the common browser
	// shape) — the pre-collection state the slow-path lookup sees.
	var fh fasthttp.RequestHeader
	wire := "GET / HTTP/1.1\r\nHost: example.com\r\nCookie: consent=yes; analytics=2; theme=dark\r\n\r\n"
	if err := fh.Read(bufio.NewReader(strings.NewReader(wire))); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKeyFast(primary, vary, &fh, policy)
	}
}
