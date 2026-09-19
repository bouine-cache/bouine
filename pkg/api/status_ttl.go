package api

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Status TTL policy shared by config (validation) and cache (lookup).
//
// The key format ("404", "5xx") is part of the operator-facing
// configuration surface, so the parser lives here: pkg/api is the only
// package both internal/config and internal/cache may import. Keeping
// one parser is what guarantees the two agree; a copy in each package
// is how they eventually won't.

// NegativeStatuses are the HTTP status codes in the legacy negative_ttl
// default set (404/405/410/501: Cloudflare's default plus the RFC 9211
// heuristic error codes). They are the statuses a plain
// `negative_ttl: 30s` scalar expands to.
var NegativeStatuses = [...]int{404, 405, 410, 501}

// Status classes accepted as negative_ttl map keys, alongside exact
// codes. Class keys exist for the one real-world blanket need — "cache
// all 5xx" — without general range syntax; an exact code shadows its
// class, giving the "blanket + exception" idiom
// ("5xx: 10s, 503: 30s").
const (
	class4xxLo, class4xxHi = 400, 499
	class5xxLo, class5xxHi = 500, 599
)

// ParseStatusTTLKey parses a negative_ttl map key: a single status code
// ("404") or a class ("4xx", "5xx"). Exact codes must be error statuses
// (400-599) — negative caching exists to absorb origin failures; a
// 2xx/3xx entry would be indistinguishable from a normal cache fill but
// is excluded from proactive refresh, silently degrading the best
// content.
func ParseStatusTTLKey(key string) (lo, hi int, err error) {
	switch key {
	case "4xx":
		return class4xxLo, class4xxHi, nil
	case "5xx":
		return class5xxLo, class5xxHi, nil
	}
	code, perr := strconv.Atoi(key)
	if perr != nil {
		return 0, 0, fmt.Errorf("invalid key %q: must be a status code, 4xx, or 5xx", key)
	}
	if err := ValidateStatusTTLCode(code); err != nil {
		return 0, 0, err
	}
	return code, code, nil
}

// ValidateStatusTTLCode rejects status codes outside the 400-599 error
// range.
func ValidateStatusTTLCode(code int) error {
	if code < 400 || code > 599 {
		return fmt.Errorf("status code %d out of range 400-599", code)
	}
	return nil
}

// NewStatusTTLMap parses and validates a status→TTL map: keys are
// parsed by ParseStatusTTLKey, values must be >= 0 (zero explicitly
// disables caching for that status). An exact code and its class may
// coexist — the exact entry shadows the class for that one status;
// the classes are a fixed set, so no other overlap is possible. Nil or
// empty returns a nil policy (no negative caching).
//
// This is the single authority: internal/config calls it during
// Validate, and every consumer receives the resulting policy instead
// of constructing its own.
func NewStatusTTLMap(statusTTL map[string]time.Duration) (*StatusTTLPolicy, error) {
	if len(statusTTL) == 0 {
		return nil, nil
	}
	p := &StatusTTLPolicy{
		exact:   make(map[int]time.Duration, len(statusTTL)),
		classes: make(map[int]time.Duration, 2),
	}
	for key, ttl := range statusTTL {
		lo, hi, err := ParseStatusTTLKey(key)
		if err != nil {
			return nil, fmt.Errorf("negative_ttl[%s]: %w", key, err)
		}
		if ttl < 0 {
			return nil, fmt.Errorf("negative_ttl[%s]: must be >= 0, got %v", key, ttl)
		}
		if lo == hi {
			if _, dup := p.exact[lo]; dup {
				return nil, fmt.Errorf("negative_ttl: duplicate entry for status %d", lo)
			}
			p.exact[lo] = ttl
			continue
		}
		cls := classOf(lo)
		if _, dup := p.classes[cls]; dup {
			return nil, fmt.Errorf("negative_ttl: duplicate class entry %q", key)
		}
		p.classes[cls] = ttl
	}
	return p, nil
}

func classOf(lo int) int { return lo / 100 }

// DefaultNegTTLMap expands a scalar negative_ttl into the explicit map
// the policy consumes: every status of the legacy default set at the
// given duration. `negative_ttl: 30s` and `negative_ttl: {404: 30s,
// 405: 30s, 410: 30s, 501: 30s}` are the same policy; the scalar is
// just shorthand for the latter.
func DefaultNegTTLMap(d time.Duration) map[string]time.Duration {
	if d <= 0 {
		return nil
	}
	m := make(map[string]time.Duration, len(NegativeStatuses))
	for _, status := range NegativeStatuses {
		m[strconv.Itoa(status)] = d
	}
	return m
}

// StatusTTLPolicy resolves per-status negative-caching TTLs. It is
// immutable after construction and nil-safe: a nil *StatusTTLPolicy
// means no negative caching. Resolution order per status: exact match >
// its class (4xx/5xx) > not covered. An entry with a zero TTL
// explicitly disables caching for that status — including a class
// entry of zero, which disables the whole class.
type StatusTTLPolicy struct {
	exact   map[int]time.Duration
	classes map[int]time.Duration // keyed by class digit (4, 5)
}

// TTL returns the negative-caching TTL for status, 0 when status is not
// covered by the policy.
func (s *StatusTTLPolicy) TTL(status int) time.Duration {
	if s == nil {
		return 0
	}
	if d, ok := s.exact[status]; ok {
		return d
	}
	return s.classes[status/100]
}

// Cacheable reports whether status should be negative-cached.
func (s *StatusTTLPolicy) Cacheable(status int) bool {
	return s.TTL(status) > 0
}

// StatusTTLEntry is one configured policy entry, in operator-facing
// key form: a status class ("4xx", "5xx") or an exact code ("404").
type StatusTTLEntry struct {
	Key string
	TTL time.Duration
}

// Entries enumerates the configured policy entries, sorted by key:
// exact codes and class entries together, the same shape the operator
// wrote (or the scalar shorthand expanded to). Nil-safe: a nil policy
// has no entries. This is the enumeration surface — consumers that
// need to render or iterate the policy (dashboard, insights) go
// through it instead of re-walking a raw map.
func (s *StatusTTLPolicy) Entries() []StatusTTLEntry {
	if s == nil || (len(s.exact) == 0 && len(s.classes) == 0) {
		return nil
	}
	entries := make([]StatusTTLEntry, 0, len(s.exact)+len(s.classes))
	for code, ttl := range s.exact {
		entries = append(entries, StatusTTLEntry{Key: strconv.Itoa(code), TTL: ttl})
	}
	for cls, ttl := range s.classes {
		entries = append(entries, StatusTTLEntry{Key: strconv.Itoa(cls) + "xx", TTL: ttl})
	}
	slices.SortFunc(entries, func(a, b StatusTTLEntry) int {
		return strings.Compare(a.Key, b.Key)
	})
	return entries
}

// CoversAnything reports whether the policy caches at least one error
// status: an all-zero map disables negative caching entirely. A nil
// policy covers nothing.
func (s *StatusTTLPolicy) CoversAnything() bool {
	if s == nil {
		return false
	}
	for _, ttl := range s.exact {
		if ttl > 0 {
			return true
		}
	}
	for _, ttl := range s.classes {
		if ttl > 0 {
			return true
		}
	}
	return false
}
