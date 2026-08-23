package header

import (
	"encoding/json"
	"net/textproto"
	"sort"
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
func InternKey(key string) string {
	return unique.Make(canonicalHeaderKey(key)).Value()
}

// InternValue deduplicates header value strings across all cached objects.
func InternValue(s string) string {
	return unique.Make(s).Value()
}

// FromFastHTTP converts a *fasthttp.ResponseHeader into a Map.
// Multi-value headers are joined with ", " per RFC 9110 §5.2.
// Entries are sorted by canonical key for deterministic output.
func FromFastHTTP(h *fasthttp.ResponseHeader) Map {
	if h == nil || h.Len() == 0 {
		return Map{}
	}
	hm := Map{
		entries: make([]headerEntry, 0, h.Len()),
		values:  make([]string, 0, h.Len()),
	}
	for k, v := range h.All() {
		if len(k) == 0 || len(v) == 0 {
			continue
		}
		hm.values = append(hm.values, InternValue(string(v)))
		hm.entries = append(hm.entries, headerEntry{
			key: InternKey(string(k)),
			off: len(hm.values) - 1,
		})
	}
	hm.SortEntries()
	return hm
}

// Get returns the value of the header with the given key. The key is
// canonicalized before lookup. Returns "" if the header is not present.
// Allocation-free when the input key is already in canonical form.
func (h Map) Get(key string) string {
	ck := canonicalHeaderKey(key)
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
	ck := canonicalHeaderKey(key)
	var parts []string
	for i := range h.entries {
		if h.entries[i].key == ck {
			parts = append(parts, h.values[h.entries[i].off])
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

// SortEntries sorts the entries slice by canonical key in place.
func (h *Map) SortEntries() {
	if len(h.entries) <= 1 {
		return
	}
	sort.SliceStable(h.entries, func(i, j int) bool {
		return h.entries[i].key < h.entries[j].key
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
// Date and Transfer-Encoding are skipped because fasthttp's Set()
// silently drops them as "managed automatically" headers. The caller
// must use SetDateRaw to set the Date header after calling this method.
func (h Map) WriteToFastHTTP(dst *fasthttp.ResponseHeader) {
	for i := range h.entries {
		off := h.entries[i].off
		key := h.entries[i].key
		// Skip Date and Transfer-Encoding — fasthttp's Set() silently
		// drops them via setSpecialHeader. Date is set separately via
		// SetDateRaw; Transfer-Encoding is a hop-by-hop header.
		if key == Date || key == TransferEncoding {
			continue
		}
		dst.Set(key, h.values[off])
	}
}

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
