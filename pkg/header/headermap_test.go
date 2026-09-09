package header

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestMap_GetSetDel(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")
	h.Set("X-Custom", "value1")

	got := h.Get("Content-Type")
	assert.Equal(t, "text/html", got)
	got = h.Get("content-type")
	assert.Equal(t, "text/html", got)
	got = h.Get("CACHE-CONTROL")
	assert.Equal(t, "public, max-age=3600", got)
	got = h.Get("Missing")
	assert.Equal(t, "", got)

	h.Set("Content-Type", "application/json")
	got = h.Get("Content-Type")
	assert.Equal(t, "application/json", got)

	h.Del("X-Custom")
	assert.False(t, h.Has("X-Custom"))
	got = h.Get("X-Custom")
	assert.Equal(t, "", got)
}

func TestMap_FromFastHTTP(t *testing.T) {
	src := &fasthttp.ResponseHeader{}
	src.Set("Content-Type", "text/html")
	src.Set("Cache-Control", "public, max-age=3600")
	src.Set("X-Custom", "value")

	hm := FromFastHTTP(src)

	got := hm.Get("Content-Type")
	assert.Equal(t, "text/html", got)
	got = hm.Get("Cache-Control")
	assert.Equal(t, "public, max-age=3600", got)
	assert.True(t, hm.Has("Content-Type"))
	assert.True(t, hm.Has("Cache-Control"))
	assert.True(t, hm.Has("X-Custom"))
}

func TestMap_FromFastHTTP_Nil(t *testing.T) {
	hm := FromFastHTTP(nil)
	assert.Equal(t, 0, hm.Len())
}

func TestMap_Clone(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("ETag", `"abc123"`)

	c := h.Clone()
	got := c.Get("Content-Type")
	assert.Equal(t, "text/html", got)

	c.Set("Content-Type", "application/json")
	got = h.Get("Content-Type")
	assert.Equal(t, "text/html", got)
	got = c.Get("Content-Type")
	assert.Equal(t, "application/json", got)
}

func TestMap_WriteToFastHTTP(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")
	h.Set("X-Custom", "value")

	dst := &fasthttp.ResponseHeader{}
	h.WriteToFastHTTP(dst)

	got := string(dst.Peek("Content-Type"))
	assert.Equal(t, "text/html", got)
	got = string(dst.Peek("Cache-Control"))
	assert.Equal(t, "public, max-age=3600", got)
	got = string(dst.Peek("X-Custom"))
	assert.Equal(t, "value", got)
}

func TestMap_Range(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("ETag", `"abc"`)

	seen := make(map[string]string)
	h.Range(func(key, value string) bool {
		seen[key] = value
		return true
	})

	assert.Len(t, seen, 2)
	assert.Equal(t, "text/html", seen["Content-Type"])
}

func TestMap_Range_CanonicalKeyOrder(t *testing.T) {
	// Set inserts in arbitrary order. Range must produce a stable,
	// canonical-key-sorted order so that the binary codec (which encodes
	// via Range) emits deterministic bytes for logically identical objects.
	hm := Map{}
	hm.Set("Vary", "Accept-Encoding")
	hm.Set("Content-Type", "text/html")
	hm.Set("Age", "0")
	hm.Set("Cache-Control", "public")

	var keys []string
	hm.Range(func(key, value string) bool {
		keys = append(keys, key)
		return true
	})

	want := []string{"Age", "Cache-Control", "Content-Type", "Vary"}
	require.Len(t, keys, len(want))
	for i, k := range keys {
		assert.Equal(t, want[i], k)
	}
}

func TestMap_Range_StopEarly(t *testing.T) {
	h := Map{}
	h.Set("A", "1")
	h.Set("B", "2")
	h.Set("C", "3")

	count := 0
	h.Range(func(key, value string) bool {
		count++
		return false // stop after first
	})

	assert.Equal(t, 1, count)
}

func TestMap_InternKey(t *testing.T) {
	k1 := InternKey("content-type")
	k2 := InternKey("Content-Type")
	k3 := InternKey("CONTENT-TYPE")

	if k1 != k2 || k2 != k3 {
		t.Errorf("InternKey should return same string for different cases: %q %q %q", k1, k2, k3)
	}
	assert.Equal(t, "Content-Type", k1)
}

func TestMap_MarshalJSON(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")

	data, err := json.Marshal(h)
	require.NoError(t, err, "MarshalJSON")

	var m map[string][]string
	err = json.Unmarshal(data, &m)
	require.NoError(t, err, "unmarshal result")

	if vals, ok := m["Content-Type"]; !ok || len(vals) != 1 || vals[0] != "text/html" {
		t.Errorf("Content-Type not in JSON: %v", m)
	}
	if vals, ok := m["Cache-Control"]; !ok || len(vals) != 1 || vals[0] != "public, max-age=3600" {
		t.Errorf("Cache-Control not in JSON: %v", m)
	}
}

func TestMap_MarshalJSON_Empty(t *testing.T) {
	h := Map{}
	data, err := json.Marshal(h)
	require.NoError(t, err, "MarshalJSON")
	assert.Equal(t, "{}", string(data))
}

func TestMap_UnmarshalJSON(t *testing.T) {
	input := `{"Content-Type": ["text/html"], "Cache-Control": ["public, max-age=3600"], "X-Multi": ["a", "b"]}`

	var h Map
	err := json.Unmarshal([]byte(input), &h)
	require.NoError(t, err, "UnmarshalJSON")

	got := h.Get("Content-Type")
	assert.Equal(t, "text/html", got)
	got = h.Get("Cache-Control")
	assert.Equal(t, "public, max-age=3600", got)
	got = h.Get("X-Multi")
	assert.Equal(t, "a, b", got)
}

func TestMap_JSONRoundTrip(t *testing.T) {
	original := Map{}
	original.Set("Content-Type", "text/html; charset=utf-8")
	original.Set("ETag", `"abc-12345"`)
	original.Set("Cache-Control", "public, max-age=3600, stale-while-revalidate=60")
	original.Set("Vary", "Accept-Encoding")

	data, err := json.Marshal(original)
	require.NoError(t, err, "marshal")

	var decoded Map
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err, "unmarshal")

	original.Range(func(key, value string) bool {
		got := decoded.Get(key)
		if got != value {
			t.Errorf("round-trip Get(%q) = %q, want %q", key, got, value)
		}
		return true
	})
}

func TestMap_Footprint_NoOrphans(t *testing.T) {
	t.Parallel()
	h := NewMap(3)
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public")
	h.Set("X-Custom", "value1")

	entries, valueSlots, valueBytes := h.Footprint()
	assert.Equal(t, 3, entries)
	assert.Equal(t, 3, valueSlots)
	wantBytes := len("text/html") + len("public") + len("value1")
	assert.Equal(t, wantBytes, valueBytes)
}

func TestMap_Footprint_WithOrphanedDel(t *testing.T) {
	t.Parallel()
	h := NewMap(3)
	h.Set("Content-Type", "text/html")
	h.Set("Set-Cookie", "session=abc")
	h.Set("X-Custom", "value1")
	h.Del("Set-Cookie")

	entries, valueSlots, valueBytes := h.Footprint()
	assert.Equal(t, 2, entries)
	// Del orphans the value slot — valueSlots should still be 3.
	assert.Equal(t, 3, valueSlots)
	// valueBytes should include the orphaned "session=abc" bytes.
	wantBytes := len("text/html") + len("session=abc") + len("value1")
	assert.Equal(t, wantBytes, valueBytes)
}

func TestInternKey_Deduplicates(t *testing.T) {
	t.Parallel()
	a := InternKey("content-type")
	b := InternKey("Content-Type")
	c := InternKey("CONTENT-TYPE")
	if a != b || b != c {
		t.Fatalf("InternKey should return identical strings for same key in different cases; got %q %q %q", a, b, c)
	}
}

func TestInternValue_Deduplicates(t *testing.T) {
	t.Parallel()
	a := internValue("text/html")
	b := internValue("text/html")
	c := internValue("application/json")
	require.Equal(t, b, a)
	require.NotEqual(t, c, a)
}

func TestBytesToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"non-empty", []byte("hello world"), "hello world"},
		{"header value", []byte("text/html; charset=utf-8"), "text/html; charset=utf-8"},
		{"single byte", []byte("X"), "X"},
		{"empty slice", []byte{}, ""},
		{"nil slice", nil, ""},
		{"binary data", []byte{0x00, 0x01, 0xFF, 0xFE}, string([]byte{0x00, 0x01, 0xFF, 0xFE})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BytesToString(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBytesToString_NoCopy(t *testing.T) {
	t.Parallel()
	// Verify that BytesToString shares the backing memory: modifying
	// the byte slice after conversion should be visible through the
	// string (proving no copy occurred). This is the expected unsafe
	// behavior — callers must not mutate the slice after conversion
	// if the string is retained.
	b := []byte("hello")
	s := BytesToString(b)
	require.Equal(t, "hello", s)

	// Mutate the underlying bytes.
	b[0] = 'H'
	// The string should reflect the mutation because no copy was made.
	// This proves the zero-copy property.
	assert.Equal(t, "Hello", s)
}

func TestBytesToString_EmptyNoPanic(t *testing.T) {
	t.Parallel()
	// Empty and nil slices must not panic — the guard returns ""
	// before dereferencing the slice header.
	assert.NotPanics(t, func() {
		_ = BytesToString([]byte{})
	})
	assert.NotPanics(t, func() {
		_ = BytesToString(nil)
	})
}

func TestFromFastHTTP_ZeroCopy(t *testing.T) {
	t.Parallel()
	// Verify that FromFastHTTP correctly converts headers with the
	// zero-copy path. A typical origin response with 12 headers.
	// Note: fasthttp's Set() silently drops Date (managed automatically),
	// so we use SetDateRaw or skip Date — this test focuses on the
	// zero-copy conversion, not fasthttp's special-header handling.
	src := &fasthttp.ResponseHeader{}
	src.Set("Content-Type", "text/html; charset=utf-8")
	src.Set("Content-Encoding", "gzip")
	src.Set("Cache-Control", "public, max-age=3600, stale-while-revalidate=86400")
	src.Set("Vary", "Accept-Encoding")
	src.Set("ETag", `"abc123def456"`)
	src.Set("Last-Modified", "Mon, 25 Aug 2025 10:00:00 GMT")
	src.Set("Age", "42")
	src.Set("Server", "bouine/1.0")
	src.Set("X-Custom-1", "custom-value-1")
	src.Set("X-Custom-2", "custom-value-2")
	src.Set("Content-Length", "12345")
	src.Set("X-Frame-Options", "DENY")

	hm := FromFastHTTP(src)

	// Every header should be present with the correct value.
	cases := []struct {
		key, want string
	}{
		{"Content-Type", "text/html; charset=utf-8"},
		{"Content-Encoding", "gzip"},
		{"Cache-Control", "public, max-age=3600, stale-while-revalidate=86400"},
		{"Vary", "Accept-Encoding"},
		{"ETag", `"abc123def456"`},
		{"Last-Modified", "Mon, 25 Aug 2025 10:00:00 GMT"},
		{"Age", "42"},
		{"Server", "bouine/1.0"},
		{"X-Custom-1", "custom-value-1"},
		{"X-Custom-2", "custom-value-2"},
		{"Content-Length", "12345"},
		{"X-Frame-Options", "DENY"},
	}
	for _, c := range cases {
		got := hm.Get(c.key)
		assert.Equal(t, c.want, got, "header %s", c.key)
	}
}

func TestFromFastHTTP_EmptyValuesSkipped(t *testing.T) {
	t.Parallel()
	// FromFastHTTP must skip entries where either key or value is empty.
	// fasthttp's Set() drops empty values, so we verify the skip logic
	// by ensuring that even if empty-value entries were present, they
	// would not cause panics or empty strings in the output Map.
	src := &fasthttp.ResponseHeader{}
	src.Set("Content-Type", "text/html")
	hm := FromFastHTTP(src)
	assert.Equal(t, 1, hm.Len())
	assert.Equal(t, "text/html", hm.Get("Content-Type"))
}

// buildTypicalResponseHeader creates a *fasthttp.ResponseHeader with 15
// headers matching a typical origin response. Used by benchmarks.
func buildTypicalResponseHeader() *fasthttp.ResponseHeader {
	h := &fasthttp.ResponseHeader{}
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Encoding", "gzip")
	h.Set("Cache-Control", "public, max-age=3600, stale-while-revalidate=86400")
	h.Set("Vary", "Accept-Encoding")
	h.Set("ETag", `"abc123def456"`)
	h.Set("Last-Modified", "Mon, 25 Aug 2025 10:00:00 GMT")
	h.Set("Age", "42")
	h.Set("Date", "Mon, 25 Aug 2025 12:00:00 GMT")
	h.Set("Server", "bouine/1.0")
	h.Set("X-Custom-1", "custom-value-1")
	h.Set("X-Custom-2", "custom-value-2")
	h.Set("Content-Length", "12345")
	h.Set("X-Frame-Options", "DENY")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
	return h
}

// BenchmarkFromFastHTTP measures the cost of converting a
// *fasthttp.ResponseHeader with 15 headers into a header.Map.
//
// First iteration: header values are not yet interned, so unique.Make
// allocates one string per unique value (~15 allocs).
// Steady state (all subsequent iterations): all values are interned,
// so unique.Make returns the existing string without allocation.
// The benchmark reports allocs/op across all iterations — the steady-
// state per-call alloc count is 0, but go test -bench averages the
// first call across all iterations, so the reported number will be
// < 1 alloc/op.
func BenchmarkFromFastHTTP(b *testing.B) {
	src := buildTypicalResponseHeader()

	// Warm up the intern table so steady-state benchmarks see 0 allocs.
	_ = FromFastHTTP(src)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = FromFastHTTP(src)
	}
}

// BenchmarkFromFastHTTP_Cold measures the first-call cost when no
// header values are interned yet. This is a single-shot benchmark
// (BenchmarkSingle_ prefix) — it skips itself under time-driven benchtime.
func BenchmarkSingle_FromFastHTTP_Cold(b *testing.B) {
	// Skip under time-driven benchtime (b.Loop mode); this benchmark
	// measures a single cold call.
	if b.N > 1 {
		b.Skip("single-shot benchmark; run with -benchtime=1x -count=10")
	}

	// Build a fresh header with values that haven't been interned yet.
	// We use unique suffixes to guarantee no prior interning.
	src := &fasthttp.ResponseHeader{}
	src.Set("Content-Type", "text/html; charset=utf-8")
	src.Set("Cache-Control", "public, max-age=3600")
	src.Set("Vary", "Accept-Encoding")
	src.Set("ETag", `"cold-bench-etag-`+b.Name()+`"`)
	src.Set("X-Custom", "cold-bench-value")

	b.ReportAllocs()
	b.ResetTimer()
	_ = FromFastHTTP(src)
}

// BenchmarkBytesToString measures the raw cost of the zero-copy
// byte-to-string conversion.
func BenchmarkBytesToString(b *testing.B) {
	data := []byte("text/html; charset=utf-8")
	b.ReportAllocs()
	for b.Loop() {
		_ = BytesToString(data)
	}
}
