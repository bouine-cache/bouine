package header

import (
	"encoding/json"
	"net/http"
	"net/textproto"
	"sort"
	"strings"
	"sync"
)

// Map is a compact, allocation-efficient replacement for http.Header
// in cached objects. It stores headers as a flat slice of key-offset pairs
// backed by a shared []string values slice, eliminating map bucket overhead
// for a typical 10-header response.
//
// Header keys are interned via a global sync.Map so all cached objects
// share the same string for common keys like "Content-Type" — saving
// ~200 B/entry at 1.24M entries. Keys are stored in canonical MIME form
// (http.CanonicalHeaderKey) and lookups are case-insensitive.
//
// Multi-value headers are joined with ", " at store time (RFC 9110 §5.2).
//
// The flat values design allows WriteTo to populate an http.Header
// (map[string][]string) without any per-header allocations on the hit
// path: each entry's value is a sub-slice of the shared values slice,
// assigned by reference into the destination map.
//
// Map implements json.Marshaler/Unmarshaler so api.Object serializes
// to the same JSON wire format as when Header was http.Header — the cluster
// gossip protocol and peer-fetch HTTP API are unaffected.
type Map struct {
	entries []headerEntry
	values  []string
}

// headerEntry is a single key-offset pair in a Map. The off field indexes
// into Map.values, so the actual string value lives in the flat values
// slice. This keeps each entry at 24 bytes (string header + int) instead
// of 32 bytes (two string headers), and enables zero-allocation WriteTo
// via values[off : off+1 : off+1] sub-slicing.
type headerEntry struct {
	key string
	off int
}

// keyIntern deduplicates canonical header key strings across all cached
// objects. The map is keyed by the canonical form; the value is the same
// string, shared by all Maps that use that key. This trades a small
// constant-time sync.Map lookup on the miss path (Set) for ~200 B/entry
// of string-data savings on the hot path.
var keyIntern sync.Map

// InternKey returns the shared, canonicalized form of key. If the key has
// not been seen before it is canonicalized and stored; subsequent calls
// with the same key (in any case) return the same string pointer.
func InternKey(key string) string {
	ck := http.CanonicalHeaderKey(key)
	if v, ok := keyIntern.Load(ck); ok {
		return v.(string)
	}
	keyIntern.Store(ck, ck)
	return ck
}

// FromHTTP converts an http.Header into a Map. The resulting Map
// does not share any underlying storage with h. Multi-value headers are
// joined with ", " per RFC 9110 §5.2.
//
// Entries are sorted by canonical key so that Range and the binary
// codec produce deterministic output without per-call allocation.
func FromHTTP(h http.Header) Map {
	if len(h) == 0 {
		return Map{}
	}
	hm := Map{
		entries: make([]headerEntry, 0, len(h)),
		values:  make([]string, 0, len(h)),
	}
	for k, vals := range h {
		if len(vals) == 0 {
			continue
		}
		var v string
		if len(vals) == 1 {
			v = vals[0]
		} else {
			v = strings.Join(vals, ", ")
		}
		hm.values = append(hm.values, v)
		hm.entries = append(hm.entries, headerEntry{
			key: InternKey(k),
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
	ck := http.CanonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			return h.values[h.entries[i].off]
		}
	}
	return ""
}

// Set sets the header with the given key to the single value. If the
// header already exists its value is replaced; otherwise a new entry is
// inserted in canonical-key order. The key is canonicalized and interned.
func (h *Map) Set(key, value string) {
	ck := InternKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			h.values[h.entries[i].off] = value
			return
		}
	}
	h.insertSorted(ck, value)
}

// SetValues sets the header with the given key to the provided values.
// If vals has one element, it is stored directly. If multiple values are
// provided they are joined with ", " per RFC 9110 §5.2.
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

// Del removes the header with the given key. No-op if the header is not
// present. The value slot in the values slice is orphaned (not reclaimed)
// to avoid shifting offsets of other entries; this is acceptable because
// Del is only called on the store path, not the hit path.
//
// Because the orphaned value string is not reclaimed, objSize (which
// counts active entries via Len, not len(values)) underestimates the
// true memory footprint by one string header + value bytes per Del.
// This is bounded by the number of Del calls per object and not worth
// reclaiming for.
func (h *Map) Del(key string) {
	ck := http.CanonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			h.entries = append(h.entries[:i], h.entries[i+1:]...)
			return
		}
	}
}

// Has reports whether the header with the given key exists.
func (h Map) Has(key string) bool {
	ck := http.CanonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			return true
		}
	}
	return false
}

// AppendEntry adds a key-value pair without checking for duplicates.
// Intended for bulk construction from a known-unique source (e.g. the
// binary codec decoder). The key is canonicalized and interned.
// Entries are appended in source order; call sortEntries after the
// bulk construction loop to restore canonical-key order.
func (h *Map) AppendEntry(key, value string) {
	h.values = append(h.values, value)
	h.entries = append(h.entries, headerEntry{
		key: InternKey(key),
		off: len(h.values) - 1,
	})
}

// SortEntries sorts the entries slice by canonical key in place.
// Called at the end of bulk construction (decodeObject) so that Range
// iterates in canonical-key order without per-call allocation.
// FromHTTP and Set already keep entries sorted; this is for callers
// that use AppendEntry in a loop.
func (h *Map) SortEntries() {
	if len(h.entries) <= 1 {
		return
	}
	sort.Slice(h.entries, func(i, j int) bool {
		return h.entries[i].key < h.entries[j].key
	})
}

// insertSorted inserts a new entry in canonical-key order. Used by Set
// when the key does not already exist.
func (h *Map) insertSorted(key, value string) {
	h.values = append(h.values, value)
	off := len(h.values) - 1
	entry := headerEntry{key: key, off: off}
	// Find the insertion point via linear scan (entries is typically
	// 10-15 elements; binary search would add complexity for no gain).
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

// Clone returns a shallow copy of the Map. The entry and values slices
// are newly allocated; the interned keys and immutable string value data
// are shared. Mutating the clone does not affect the original.
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

// WriteTo copies all headers into dst, converting each to the
// map[string][]string form expected by net/http. This replaces
// maps.Copy(dst, obj.Header) and produces the same result.
//
// This is the hit-path method and is allocation-free: each entry's
// value is a single-element sub-slice of the internal values slice,
// assigned by reference into dst.
func (h Map) WriteTo(dst http.Header) {
	for i := range h.entries {
		off := h.entries[i].off
		dst[h.entries[i].key] = h.values[off : off+1 : off+1]
	}
}

// Range iterates over all headers in canonical-key order, calling f for
// each. If f returns false, iteration stops. This replaces
// `for k, vs := range obj.Header` loops.
//
// Entries are kept sorted at construction time (FromHTTP, decodeObject,
// Set), so Range is a zero-allocation linear scan. Deterministic Range
// output makes the binary codec produce stable bytes for logically
// identical objects, which anti-entropy checksums rely on.
func (h Map) Range(f func(key string, value string) bool) {
	for i := range h.entries {
		if !f(h.entries[i].key, h.values[h.entries[i].off]) {
			return
		}
	}
}

// MarshalJSON serializes the Map as a JSON object mapping canonical
// header keys to single-element string arrays, matching the wire format
// of http.Header used by the cluster gossip protocol and peer-fetch API.
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
		h.values = append(h.values, v)
		h.entries = append(h.entries, headerEntry{
			key: InternKey(textproto.CanonicalMIMEHeaderKey(k)),
			off: len(h.values) - 1,
		})
	}
	return nil
}
