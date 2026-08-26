package header

import (
	"encoding/json"
	"net/textproto"
	"slices"
	"strings"
	"unique"
	"unsafe"

	"github.com/valyala/fasthttp"
)

// canonicalHeaderKey returns the canonical MIME form of a header key:
// the first letter of each dash-separated word is uppercased, the rest
// is lowercased. This produces the same output as net/http.CanonicalHeaderKey
// without importing net/http on the hot path.
//
// Fast path: if the key is already in canonical form (the common case
// since all header constants are pre-canonicalized), the function
// returns the input string directly without any allocation.
func canonicalHeaderKey(key string) string {
	if isCanonical(key) {
		return key
	}
	return canonicalize(key)
}

// isCanonical reports whether key is already in canonical MIME header form.
// Inlineable so the compiler can optimize the common case where the key
// is a package-level constant (already canonical).
func isCanonical(key string) bool {
	upper := true
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case upper && c >= 'a' && c <= 'z':
			return false
		case upper:
			upper = false
		case c == '-':
			upper = true
		case c >= 'A' && c <= 'Z':
			return false
		}
	}
	return true
}

// canonicalize performs the actual canonicalization. Separated from
// canonicalHeaderKey so the slow path doesn't bloat the inlineable fast path.
func canonicalize(key string) string {
	b := []byte(key)
	upper := true
	for i, c := range b {
		switch {
		case upper && c >= 'a' && c <= 'z':
			b[i] = c - 32
			upper = false
		case upper:
			upper = false
		case c == '-':
			upper = true
		case c >= 'A' && c <= 'Z':
			b[i] = c + 32
		}
	}
	return string(b)
}

// Map is a compact, allocation-efficient replacement for http.Header
// in cached objects. It stores headers as a flat slice of key-offset pairs
// backed by a shared []string values slice, eliminating map bucket overhead
// for a typical 10-header response.
//
// Header keys and common values are interned via unique.Make (Go 1.23+)
// so all cached objects share the same string for common keys like
// "Content-Type" and common values like "text/html" — saving
// ~200 B/entry in keys and ~50 MB in values at 1.24M entries. Keys are
// stored in canonical MIME form and lookups are case-insensitive.
//
// Multi-value headers are joined with ", " at store time (RFC 9110 §5.2).
//
// The flat values design allows WriteToFastHTTP to populate a
// (map[string][]string) without any per-header allocations on the hit
// path: each entry's value is a sub-slice of the shared values slice,
// assigned by reference into the destination map.
//
// Map implements json.Marshaler/Unmarshaler so api.Object serializes
// to the same JSON wire format as when Header was http.Header — the
// cluster gossip replication protocol and admin API are unaffected.
type Map struct {
	entries []headerEntry
	values  []string
}

// headerEntry is a single key-offset pair in a Map.
type headerEntry struct {
	key string
	off int
}

// InternKey returns the shared, canonicalized form of key.
// Fast path: if the key is already canonical (common case for headers
// from fasthttp which are already normalized), skip canonicalHeaderKey
// and intern directly.
func InternKey(key string) string {
	if isCanonical(key) {
		return unique.Make(key).Value()
	}
	return unique.Make(canonicalHeaderKey(key)).Value()
}

// InternKeyCanonical interns a key that is already known to be canonical,
// skipping the isCanonical check. Used by FromFastHTTP and headerFromCtx
// where fasthttp guarantees normalized keys.
func InternKeyCanonical(key string) string {
	return unique.Make(key).Value()
}

// InternValue deduplicates header value strings across all cached objects.
func InternValue(s string) string {
	return unique.Make(s).Value()
}

// FromFastHTTP converts a *fasthttp.ResponseHeader into a Map.
// Multi-value headers are joined with ", " per RFC 9110 §5.2.
// Entries are sorted by canonical key for deterministic output.
//
// Zero-copy: header keys and values are converted from []byte to string
// via BytesToString (unsafe.String) without allocation. unique.Make then
// interns the string; if the value is already interned (common after
// warm-up: text/html, gzip, application/json, etc.), no allocation occurs
// at all. If it's a new value, unique.Make copies it into the intern
// table — a retained copy, not garbage, so it doesn't contribute to GC
// pressure.
func FromFastHTTP(h *fasthttp.ResponseHeader) Map {
	if h == nil || h.Len() == 0 {
		return Map{}
	}
	// Over-allocate by 4 to leave room for internal headers (X-Bouine-Path,
	// X-Bouine-Host) and Content-Length that buildObject adds after
	// construction, avoiding slice growth/rehashing.
	n := h.Len() + 4
	hm := Map{
		entries: make([]headerEntry, 0, n),
		values:  make([]string, 0, n),
	}
	for k, v := range h.All() {
		if len(k) == 0 || len(v) == 0 {
			continue
		}
		hm.values = append(hm.values, InternValue(BytesToString(v)))
		hm.entries = append(hm.entries, headerEntry{
			key: InternKeyCanonical(BytesToString(k)),
			off: len(hm.values) - 1,
		})
	}
	hm.SortEntries()
	return hm
}

// Get returns the value of the header with the given key. The key is
// canonicalized before lookup. Returns "" if the header is not present.
// Allocation-free when the input key is already in canonical form.
//
// Fast path: the key is compared directly against stored entries. Since
// all stored keys are canonical (interned via InternKey at store time)
// and all header constants in this package are pre-canonicalized, the
// direct comparison works without calling canonicalHeaderKey. If the
// key is not canonical, the caller must canonicalize it first.
func (h Map) Get(key string) string {
	if len(h.entries) == 0 {
		return ""
	}
	for i := range h.entries {
		if h.entries[i].key == key {
			return h.values[h.entries[i].off]
		}
	}
	// Fallback: canonicalize and retry (handles non-canonical caller keys).
	ck := canonicalHeaderKey(key)
	if ck == key {
		return "" // already canonical, just not present
	}
	for i := range h.entries {
		if h.entries[i].key == ck {
			return h.values[h.entries[i].off]
		}
	}
	return ""
}

// GetAll returns all values for the given key, joined with ", " per
// RFC 9111 §5.2 (multiple header field lines are equivalent to a
// comma-separated list). Returns "" if the header is not present.
func (h Map) GetAll(key string) string {
	var parts []string
	// Fast path: direct comparison (see Get for rationale).
	for i := range h.entries {
		if h.entries[i].key == key {
			parts = append(parts, h.values[h.entries[i].off])
		}
	}
	if len(parts) == 0 {
		// Fallback: canonicalize and retry.
		ck := canonicalHeaderKey(key)
		if ck == key {
			return ""
		}
		for i := range h.entries {
			if h.entries[i].key == ck {
				parts = append(parts, h.values[h.entries[i].off])
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, ", ")
}

// Set sets the header with the given key to the single value.
func (h *Map) Set(key, value string) {
	ck := InternKey(key)
	iv := InternValue(value)
	for i := range h.entries {
		if h.entries[i].key == ck {
			h.values[h.entries[i].off] = iv
			return
		}
	}
	h.insertSorted(ck, iv)
}

// SetValues sets the header with the given key to the provided values.
func (h *Map) SetValues(key string, vals []string) {
	if len(vals) == 0 {
		h.Del(key)
		return
	}
	var v string
	if len(vals) == 1 {
		v = vals[0]
	} else {
		v = strings.Join(vals, ", ")
	}
	h.Set(key, v)
}

// Del removes the header with the given key.
func (h *Map) Del(key string) {
	ck := canonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			h.entries = append(h.entries[:i], h.entries[i+1:]...)
			return
		}
	}
}

// Has reports whether the header with the given key exists.
func (h Map) Has(key string) bool {
	ck := canonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			return true
		}
	}
	return false
}

// AppendEntry adds a key-value pair without checking for duplicates.
func (h *Map) AppendEntry(key, value string) {
	h.values = append(h.values, InternValue(value))
	h.entries = append(h.entries, headerEntry{
		key: InternKey(key),
		off: len(h.values) - 1,
	})
}

// AppendEntryCanonical adds a key-value pair without checking for
// duplicates, skipping the canonicalization check on the key. Use when
// the key is already known canonical (e.g. from InternKeyCanonical or
// a package-level constant). The value is still interned.
func (h *Map) AppendEntryCanonical(key, value string) {
	h.values = append(h.values, InternValue(value))
	h.entries = append(h.entries, headerEntry{
		key: InternKeyCanonical(key),
		off: len(h.values) - 1,
	})
}

// SortEntries sorts the entries slice by canonical key in place.
// Uses slices.SortStableFunc (generic, no reflection) instead of
// sort.SliceStable (which allocates via reflectlite.Swapper).
func (h *Map) SortEntries() {
	if len(h.entries) <= 1 {
		return
	}
	slices.SortStableFunc(h.entries, func(a, b headerEntry) int {
		return strings.Compare(a.key, b.key)
	})
}

// insertSorted inserts a new entry in canonical-key order.
func (h *Map) insertSorted(key, value string) {
	h.values = append(h.values, value)
	off := len(h.values) - 1
	entry := headerEntry{key: key, off: off}
	for i := range h.entries {
		if h.entries[i].key > key {
			h.entries = append(h.entries, headerEntry{})
			copy(h.entries[i+1:], h.entries[i:])
			h.entries[i] = entry
			return
		}
	}
	h.entries = append(h.entries, entry)
}

// NewMap returns a Map pre-allocated for n entries.
func NewMap(n int) Map {
	return Map{
		entries: make([]headerEntry, 0, n),
		values:  make([]string, 0, n),
	}
}

// Len returns the number of distinct header keys.
func (h Map) Len() int {
	return len(h.entries)
}

// Footprint returns the heap footprint components of the Map for eviction
// accounting by internal/storage.objSize.
func (h Map) Footprint() (entries, valueSlots, valueBytes int) {
	var n int
	for i := range h.values {
		n += len(h.values[i])
	}
	return len(h.entries), len(h.values), n
}

// Clone returns a shallow copy of the Map.
func (h Map) Clone() Map {
	if len(h.entries) == 0 {
		return Map{}
	}
	entries := make([]headerEntry, len(h.entries))
	copy(entries, h.entries)
	values := make([]string, len(h.values))
	copy(values, h.values)
	return Map{entries: entries, values: values}
}

// WriteToFastHTTP copies all headers into a *fasthttp.ResponseHeader.
// This is the fasthttp-native version of WriteTo, used on the hit path
// to set response headers without going through net/http.Header.
//
// Uses SetCanonical instead of Set to skip normalizeHeaderKey (35% of
// serveObject CPU), since all stored keys are already canonical (interned
// via InternKey at store time).
//
// Hop-by-hop headers (Connection, KeepAlive, TE, Trailer, Upgrade) and
// internal headers (XBouinePath, XBouineHost, XBouineRoute) are skipped
// here so the caller doesn't need to Del them afterward. Date and
// Transfer-Encoding are also skipped (Date is set via SetDateRaw).
// Age is skipped (set dynamically per-request by serveObject).
func (h Map) WriteToFastHTTP(dst *fasthttp.ResponseHeader) {
	for i := range h.entries {
		key := h.entries[i].key
		switch key {
		case Date, TransferEncoding, Connection, KeepAlive,
			TE, Trailer, Upgrade, XBouinePath, XBouineHost,
			XBouineRoute, Age:
			continue
		}
		dst.SetCanonical(s2b(key), s2b(h.values[h.entries[i].off]))
	}
}

// BytesToString converts a []byte to string without copying the underlying
// bytes. The returned string shares its backing memory with b — it is only
// valid as long as b is not modified or garbage-collected.
//
// Safe for read-only use where the string is immediately consumed (e.g.
// passed to unique.Make which copies the content into its intern table).
// The zero-length case returns "" without dereferencing the slice header,
// avoiding an index-out-of-range panic on empty slices.
//
// does not outlive the caller's byte slice in any path that doesn't
// immediately copy it (unique.Make, InternKey, etc.).
//
//nolint:gosec // G103: unsafe.String is safe: the string is read-only and
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// S2b converts a string to []byte without allocation by pointing at
// the string's backing memory. Safe for read-only use (SetCanonical
// copies the bytes into its own buffer). This mirrors fasthttp's
// internal s2b function.
func S2b(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s)) //nolint:gosec // G103: read-only slice from string
}

// s2b is an alias for S2b for internal use.
func s2b(s string) []byte { return S2b(s) }

// httpKV mirrors fasthttp's internal argsKV struct layout.
// It is used by SetDateRaw to bypass the setSpecialHeader check
// that silently drops Date headers. The noValue field is required
// for memory layout compatibility even though it's never set.
type httpKV struct {
	key     []byte
	value   []byte
	noValue bool //nolint:unused // required for argsKV layout compatibility
}

// dateKey is a cached []byte("Date") to avoid per-call allocation in SetDateRaw.
var dateKey = []byte("Date")

// SetDateRaw sets the Date header on a *fasthttp.ResponseHeader by
// directly appending to its internal header map. This bypasses
// fasthttp's setSpecialHeader, which treats Date as "managed
// automatically" and silently discards any value passed to Set().
// The server must have NoDefaultDate set to true to prevent fasthttp
// from overwriting this value with its own auto-generated Date.
func SetDateRaw(dst *fasthttp.ResponseHeader, date string) {
	// ResponseHeader embeds header as its first field, and header's
	// first field is h []argsKV. Since httpKV has the same memory
	// layout as argsKV, we can safely reinterpret the slice.
	hp := (*[]httpKV)(unsafe.Pointer(dst)) //nolint:gosec // G103: controlled unsafe for fasthttp interop
	*hp = append(*hp, httpKV{key: dateKey, value: []byte(date)})
}

// At returns the key and value at index i. Panics if i >= Len().
func (h Map) At(i int) (string, string) {
	return h.entries[i].key, h.values[h.entries[i].off]
}

// Range iterates over all headers in canonical-key order, calling f for
// each. If f returns false, iteration stops.
func (h Map) Range(f func(key string, value string) bool) {
	for i := range h.entries {
		if !f(h.entries[i].key, h.values[h.entries[i].off]) {
			return
		}
	}
}

// MarshalJSON serializes the Map as a JSON object mapping canonical
// header keys to single-element string arrays.
func (h Map) MarshalJSON() ([]byte, error) {
	if len(h.entries) == 0 {
		return []byte("{}"), nil
	}
	m := make(map[string][]string, len(h.entries))
	for i := range h.entries {
		off := h.entries[i].off
		m[h.entries[i].key] = h.values[off : off+1 : off+1]
	}
	return json.Marshal(m)
}

// UnmarshalJSON deserializes a JSON object mapping header keys to string
// arrays into a Map. Multi-value arrays are joined with ", ".
func (h *Map) UnmarshalJSON(data []byte) error {
	var m map[string][]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	h.entries = make([]headerEntry, 0, len(m))
	h.values = make([]string, 0, len(m))
	for k, vals := range m {
		if len(vals) == 0 {
			continue
		}
		var v string
		if len(vals) == 1 {
			v = vals[0]
		} else {
			v = strings.Join(vals, ", ")
		}
		h.values = append(h.values, InternValue(v))
		h.entries = append(h.entries, headerEntry{
			key: InternKey(textproto.CanonicalMIMEHeaderKey(k)),
			off: len(h.values) - 1,
		})
	}
	h.SortEntries()
	return nil
}
