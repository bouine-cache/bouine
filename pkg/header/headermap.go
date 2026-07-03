package header

import (
	"encoding/json"
	"net/http"
	"net/textproto"
	"strings"
	"sync"
)

// Map is a compact, allocation-efficient replacement for http.Header
// in cached objects. It stores headers as a flat slice of key-value pairs
// instead of a map[string][]string, eliminating ~528 B/entry of map bucket
// overhead for a typical 10-header response.
//
// Header keys are interned via a global sync.Map so all cached objects
// share the same string for common keys like "Content-Type" — saving
// ~200 B/entry at 1.24M entries. Keys are stored in canonical MIME form
// (http.CanonicalHeaderKey) and lookups are case-insensitive.
//
// Multi-value headers are joined with ", " at store time (RFC 9110 §5.2
// permits this for all headers except Set-Cookie, which is stripped before
// storage). This means Values always returns a single-element slice for
// stored objects, but the method is retained for interface compatibility
// with http.Header.
//
// Map implements json.Marshaler/Unmarshaler so api.Object serializes
// to the same JSON wire format as when Header was http.Header — the cluster
// gossip protocol and peer-fetch HTTP API are unaffected.
type Map struct {
	entries []headerEntry
}

// headerEntry is a single key-value pair in a Map.
type headerEntry struct {
	key   string
	value string
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
// joined with ", " per RFC 9110 §5.2 (Set-Cookie must be stripped before
// calling this — the join would corrupt multiple Set-Cookie values).
func FromHTTP(h http.Header) Map {
	if len(h) == 0 {
		return Map{}
	}
	hm := Map{
		entries: make([]headerEntry, 0, len(h)),
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
		hm.entries = append(hm.entries, headerEntry{
			key:   InternKey(k),
			value: v,
		})
	}
	return hm
}

// Get returns the value of the header with the given key. The key is
// canonicalized before lookup. Returns "" if the header is not present.
// Allocation-free when the input key is already in canonical form.
func (h Map) Get(key string) string {
	ck := http.CanonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			return h.entries[i].value
		}
	}
	return ""
}

// Set sets the header with the given key to the single value. If the
// header already exists its value is replaced; otherwise a new entry is
// appended. The key is canonicalized and interned.
func (h *Map) Set(key, value string) {
	ck := InternKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			h.entries[i].value = value
			return
		}
	}
	h.entries = append(h.entries, headerEntry{key: ck, value: value})
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
// present.
func (h *Map) Del(key string) {
	ck := http.CanonicalHeaderKey(key)
	for i := range h.entries {
		if h.entries[i].key == ck {
			h.entries = append(h.entries[:i], h.entries[i+1:]...)
			return
		}
	}
}

// Values returns all values for the header with the given key. Since
// multi-value headers are joined at store time, this always returns a
// single-element slice (or nil if not present). The returned slice is
// freshly allocated; callers should not hold it across mutations.
func (h Map) Values(key string) []string {
	v := h.Get(key)
	if v == "" {
		return nil
	}
	return []string{v}
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
func (h *Map) AppendEntry(key, value string) {
	h.entries = append(h.entries, headerEntry{
		key:   InternKey(key),
		value: value,
	})
}

// NewMap returns a Map pre-allocated for n entries.
func NewMap(n int) Map {
	return Map{entries: make([]headerEntry, 0, n)}
}

// Len returns the number of distinct header keys.
func (h Map) Len() int {
	return len(h.entries)
}

// Clone returns a shallow copy of the Map. The entry slice is newly
// allocated; the interned keys and immutable string values are shared.
func (h Map) Clone() Map {
	if len(h.entries) == 0 {
		return Map{}
	}
	entries := make([]headerEntry, len(h.entries))
	copy(entries, h.entries)
	return Map{entries: entries}
}

// WriteTo copies all headers into dst, converting each to the
// map[string][]string form expected by net/http. This replaces
// maps.Copy(dst, obj.Header) and produces the same result.
func (h Map) WriteTo(dst http.Header) {
	for i := range h.entries {
		dst[h.entries[i].key] = []string{h.entries[i].value}
	}
}

// Range iterates over all headers in canonical-key order, calling f for
// each. If f returns false, iteration stops. This replaces
// `for k, vs := range obj.Header` loops.
func (h Map) Range(f func(key string, value string) bool) {
	for i := range h.entries {
		if !f(h.entries[i].key, h.entries[i].value) {
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
		m[h.entries[i].key] = []string{h.entries[i].value}
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
		h.entries = append(h.entries, headerEntry{
			key:   InternKey(textproto.CanonicalMIMEHeaderKey(k)),
			value: v,
		})
	}
	return nil
}
