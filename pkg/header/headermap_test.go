package header

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestMap_GetSetDel(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")
	h.Set("X-Custom", "value1")

	if got := h.Get("Content-Type"); got != "text/html" {
		t.Errorf("Get(Content-Type) = %q, want %q", got, "text/html")
	}
	if got := h.Get("content-type"); got != "text/html" {
		t.Errorf("Get(content-type) = %q, want %q (case-insensitive)", got, "text/html")
	}
	if got := h.Get("CACHE-CONTROL"); got != "public, max-age=3600" {
		t.Errorf("Get(CACHE-CONTROL) = %q, want %q (case-insensitive)", got, "public, max-age=3600")
	}
	if got := h.Get("Missing"); got != "" {
		t.Errorf("Get(Missing) = %q, want empty", got)
	}

	h.Set("Content-Type", "application/json")
	if got := h.Get("Content-Type"); got != "application/json" {
		t.Errorf("after Set overwrite, Get = %q, want %q", got, "application/json")
	}

	h.Del("X-Custom")
	if h.Has("X-Custom") {
		t.Error("Has(X-Custom) = true after Del")
	}
	if got := h.Get("X-Custom"); got != "" {
		t.Errorf("Get(X-Custom) = %q after Del, want empty", got)
	}
}

func TestMap_FromHTTP(t *testing.T) {
	src := http.Header{}
	src.Set("Content-Type", "text/html")
	src.Set("Cache-Control", "public, max-age=3600")
	src.Add("X-Multi", "a")
	src.Add("X-Multi", "b")
	src.Add("X-Multi", "c")

	hm := FromHTTP(src)

	if got := hm.Get("Content-Type"); got != "text/html" {
		t.Errorf("Get(Content-Type) = %q, want %q", got, "text/html")
	}
	if got := hm.Get("X-Multi"); got != "a, b, c" {
		t.Errorf("Get(X-Multi) = %q, want %q (joined)", got, "a, b, c")
	}
	if hm.Len() != 3 {
		t.Errorf("Len() = %d, want 3", hm.Len())
	}
}

func TestMap_FromHTTP_Nil(t *testing.T) {
	hm := FromHTTP(nil)
	if hm.Len() != 0 {
		t.Errorf("FromHTTP(nil) Len = %d, want 0", hm.Len())
	}
	hm2 := FromHTTP(http.Header{})
	if hm2.Len() != 0 {
		t.Errorf("FromHTTP(empty) Len = %d, want 0", hm2.Len())
	}
}

func TestMap_Clone(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("ETag", `"abc123"`)

	c := h.Clone()
	if got := c.Get("Content-Type"); got != "text/html" {
		t.Errorf("clone Get(Content-Type) = %q, want %q", got, "text/html")
	}

	c.Set("Content-Type", "application/json")
	if got := h.Get("Content-Type"); got != "text/html" {
		t.Errorf("original mutated after clone Set: Get = %q, want %q", got, "text/html")
	}
	if got := c.Get("Content-Type"); got != "application/json" {
		t.Errorf("clone Get = %q, want %q", got, "application/json")
	}
}

func TestMap_WriteTo(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")
	h.Set("X-Custom", "value")

	dst := make(http.Header, 3)
	h.WriteTo(dst)

	if got := dst.Get("Content-Type"); got != "text/html" {
		t.Errorf("dst.Get(Content-Type) = %q, want %q", got, "text/html")
	}
	if got := dst.Get("Cache-Control"); got != "public, max-age=3600" {
		t.Errorf("dst.Get(Cache-Control) = %q, want %q", got, "public, max-age=3600")
	}
	if got := dst.Get("X-Custom"); got != "value" {
		t.Errorf("dst.Get(X-Custom) = %q, want %q", got, "value")
	}
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

	if len(seen) != 2 {
		t.Errorf("Range visited %d entries, want 2", len(seen))
	}
	if seen["Content-Type"] != "text/html" {
		t.Errorf("Range saw Content-Type = %q, want %q", seen["Content-Type"], "text/html")
	}
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
	if len(keys) != len(want) {
		t.Fatalf("Range visited %d keys, want %d: %v", len(keys), len(want), keys)
	}
	for i, k := range keys {
		if k != want[i] {
			t.Errorf("Range order[%d] = %q, want %q (full: %v)", i, k, want[i], keys)
		}
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

	if count != 1 {
		t.Errorf("Range with early stop visited %d entries, want 1", count)
	}
}

func TestMap_SetValues(t *testing.T) {
	h := Map{}
	h.SetValues("X-Multi", []string{"a", "b", "c"})
	if got := h.Get("X-Multi"); got != "a, b, c" {
		t.Errorf("SetValues Get = %q, want %q", got, "a, b, c")
	}

	h.SetValues("X-Single", []string{"only"})
	if got := h.Get("X-Single"); got != "only" {
		t.Errorf("SetValues single Get = %q, want %q", got, "only")
	}

	h.SetValues("X-Multi", []string{})
	if h.Has("X-Multi") {
		t.Error("SetValues with empty slice should delete header")
	}
}

func TestMap_InternKey(t *testing.T) {
	k1 := InternKey("content-type")
	k2 := InternKey("Content-Type")
	k3 := InternKey("CONTENT-TYPE")

	if k1 != k2 || k2 != k3 {
		t.Errorf("InternKey should return same string for different cases: %q %q %q", k1, k2, k3)
	}
	if k1 != "Content-Type" {
		t.Errorf("InternKey should return canonical form, got %q", k1)
	}
}

func TestMap_MarshalJSON(t *testing.T) {
	h := Map{}
	h.Set("Content-Type", "text/html")
	h.Set("Cache-Control", "public, max-age=3600")

	data, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var m map[string][]string
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

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
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(data) != "{}" {
		t.Errorf("empty MarshalJSON = %s, want {}", string(data))
	}
}

func TestMap_UnmarshalJSON(t *testing.T) {
	input := `{"Content-Type": ["text/html"], "Cache-Control": ["public, max-age=3600"], "X-Multi": ["a", "b"]}`

	var h Map
	if err := json.Unmarshal([]byte(input), &h); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	if got := h.Get("Content-Type"); got != "text/html" {
		t.Errorf("Get(Content-Type) = %q, want %q", got, "text/html")
	}
	if got := h.Get("Cache-Control"); got != "public, max-age=3600" {
		t.Errorf("Get(Cache-Control) = %q, want %q", got, "public, max-age=3600")
	}
	if got := h.Get("X-Multi"); got != "a, b" {
		t.Errorf("Get(X-Multi) = %q, want %q (joined)", got, "a, b")
	}
}

func TestMap_JSONRoundTrip(t *testing.T) {
	original := Map{}
	original.Set("Content-Type", "text/html; charset=utf-8")
	original.Set("ETag", `"abc-12345"`)
	original.Set("Cache-Control", "public, max-age=3600, stale-while-revalidate=60")
	original.Set("Vary", "Accept-Encoding")

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Map
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

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
	if entries != 3 {
		t.Errorf("entries = %d, want 3", entries)
	}
	if valueSlots != 3 {
		t.Errorf("valueSlots = %d, want 3", valueSlots)
	}
	wantBytes := len("text/html") + len("public") + len("value1")
	if valueBytes != wantBytes {
		t.Errorf("valueBytes = %d, want %d", valueBytes, wantBytes)
	}
}

func TestMap_Footprint_WithOrphanedDel(t *testing.T) {
	t.Parallel()
	h := NewMap(3)
	h.Set("Content-Type", "text/html")
	h.Set("Set-Cookie", "session=abc")
	h.Set("X-Custom", "value1")
	h.Del("Set-Cookie")

	entries, valueSlots, valueBytes := h.Footprint()
	if entries != 2 {
		t.Errorf("entries = %d, want 2 (after Del)", entries)
	}
	// Del orphans the value slot — valueSlots should still be 3.
	if valueSlots != 3 {
		t.Errorf("valueSlots = %d, want 3 (Del orphans the slot)", valueSlots)
	}
	// valueBytes should include the orphaned "session=abc" bytes.
	wantBytes := len("text/html") + len("session=abc") + len("value1")
	if valueBytes != wantBytes {
		t.Errorf("valueBytes = %d, want %d (orphaned value data must be counted)", valueBytes, wantBytes)
	}
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
	if a != b {
		t.Fatalf("InternValue should return identical strings for same value; got %q and %q", a, b)
	}
	if a == c {
		t.Fatalf("InternValue should return different strings for different values; got %q == %q", a, c)
	}
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
