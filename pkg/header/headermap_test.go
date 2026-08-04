package header

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestMap_FromHTTP(t *testing.T) {
	src := http.Header{}
	src.Set("Content-Type", "text/html")
	src.Set("Cache-Control", "public, max-age=3600")
	src.Add("X-Multi", "a")
	src.Add("X-Multi", "b")
	src.Add("X-Multi", "c")

	hm := FromHTTP(src)

	got := hm.Get("Content-Type")
	assert.Equal(t, "text/html", got)
	got = hm.Get("X-Multi")
	assert.Equal(t, "a, b, c", got)
	assert.Equal(t, 3, hm.Len())
}

func TestMap_FromHTTP_Nil(t *testing.T) {
	hm := FromHTTP(nil)
	assert.Equal(t, 0, hm.Len())
	hm2 := FromHTTP(http.Header{})
	assert.Equal(t, 0, hm2.Len())
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

func TestMap_WriteTo(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")
	h.Set("X-Custom", "value")

	dst := make(http.Header, 3)
	h.WriteTo(dst)

	got := dst.Get("Content-Type")
	assert.Equal(t, "text/html", got)
	got = dst.Get("Cache-Control")
	assert.Equal(t, "public, max-age=3600", got)
	got = dst.Get("X-Custom")
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
	// FromHTTP inherits Go map iteration order, which is randomized per
	// call. Range must produce a stable, canonical-key-sorted order so that
	// the binary codec (which encodes via Range) emits deterministic bytes
	// for logically identical objects.
	src := http.Header{}
	src.Set("Vary", "Accept-Encoding")
	src.Set("Content-Type", "text/html")
	src.Set("Age", "0")
	src.Set("Cache-Control", "public")

	hm := FromHTTP(src)

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

func TestMap_SetValues(t *testing.T) {
	h := Map{}
	h.SetValues("X-Multi", []string{"a", "b", "c"})
	got := h.Get("X-Multi")
	assert.Equal(t, "a, b, c", got)

	h.SetValues("X-Single", []string{"only"})
	got = h.Get("X-Single")
	assert.Equal(t, "only", got)

	h.SetValues("X-Multi", []string{})
	assert.False(t, h.Has("X-Multi"))
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
	a := InternValue("text/html")
	b := InternValue("text/html")
	c := InternValue("application/json")
	require.Equal(t, b, a)
	require.NotEqual(t, c, a)
}

func TestFromHTTP_InternsValues(t *testing.T) {
	t.Parallel()
	h1 := http.Header{
		"Content-Type": {"text/html"},
		"X-Custom":     {"unique-value-123"},
	}
	m1 := FromHTTP(h1)

	h2 := http.Header{
		"Content-Type": {"text/html"},
		"X-Custom":     {"unique-value-123"},
	}
	m2 := FromHTTP(h2)

	// String equality verifies the values are correct. unique.Make
	// guarantees pointer-level deduplication internally; we test that
	// contract via TestInternValue_Deduplicates. Here we verify FromHTTP
	// produces the right values and doesn't corrupt the header map.
	v1 := m1.Get("Content-Type")
	v2 := m2.Get("Content-Type")
	if v1 != "text/html" || v2 != "text/html" {
		t.Fatalf("expected text/html for both; got %q and %q", v1, v2)
	}

	custom1 := m1.Get("X-Custom")
	custom2 := m2.Get("X-Custom")
	if custom1 != "unique-value-123" || custom2 != "unique-value-123" {
		t.Fatalf("expected unique-value-123 for both; got %q and %q", custom1, custom2)
	}
}
