package cache

import (
	"time"
)

// Directives holds the parsed Cache-Control directives from either a
// request or a response. Zero values mean the directive was absent.
type Directives struct {
	NoStore              bool
	NoCache              bool
	Private              bool
	Public               bool
	MustRevalidate       bool
	ProxyRevalidate      bool
	Immutable            bool
	NoTransform          bool
	OnlyIfCached         bool
	MaxAge               time.Duration
	MaxAgeSet            bool
	SMaxAge              time.Duration
	SMaxAgeSet           bool
	MinFresh             time.Duration
	MinFreshSet          bool
	MaxStale             time.Duration
	MaxStaleSet          bool
	StaleWhileRevalid    time.Duration
	StaleWhileRevalidSet bool
	StaleIfError         time.Duration
	StaleIfErrorSet      bool
}

// ParseCacheControl parses a Cache-Control header value into
// Directives. Zero-alloc: scans the header bytes in place without
// allocating slices or substrings.
func ParseCacheControl(header string) Directives {
	var d Directives
	i := 0
	for i < len(header) {
		i = skipDelimiters(header, i)
		if i >= len(header) {
			break
		}
		var key, val string
		key, val, i = scanToken(header, i)
		applyDirective(&d, key, val)
	}
	return d
}

func skipDelimiters(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == ',' || s[i] == '\t') {
		i++
	}
	return i
}

func scanToken(s string, i int) (key, val string, next int) {
	keyStart := i
	for i < len(s) && s[i] != '=' && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
		i++
	}
	key = s[keyStart:i]

	if i < len(s) && s[i] == '=' {
		i++
		val, i = scanValue(s, i)
	}
	return key, val, i
}

func scanValue(s string, i int) (string, int) {
	if i < len(s) && s[i] == '"' {
		i++
		start := i
		for i < len(s) && s[i] != '"' {
			i++
		}
		val := s[start:i]
		if i < len(s) {
			i++
		}
		return val, i
	}
	start := i
	for i < len(s) && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
		i++
	}
	return s[start:i], i
}

func applyDirective(d *Directives, key, val string) {
	if applyBoolDirective(d, key) {
		return
	}
	applyDurDirective(d, key, val)
}

func applyBoolDirective(d *Directives, key string) bool {
	switch {
	case eqFold(key, "no-store"):
		d.NoStore = true
	case eqFold(key, "no-cache"):
		d.NoCache = true
	case eqFold(key, "private"):
		d.Private = true
	case eqFold(key, "public"):
		d.Public = true
	case eqFold(key, "must-revalidate"):
		d.MustRevalidate = true
	case eqFold(key, "proxy-revalidate"):
		d.ProxyRevalidate = true
	case eqFold(key, "immutable"):
		d.Immutable = true
	case eqFold(key, "no-transform"):
		d.NoTransform = true
	case eqFold(key, "only-if-cached"):
		d.OnlyIfCached = true
	default:
		return false
	}
	return true
}

func applyDurDirective(d *Directives, key, val string) {
	switch {
	case eqFold(key, "max-age"):
		parseDur(&d.MaxAge, &d.MaxAgeSet, val)
	case eqFold(key, "s-maxage"):
		parseDur(&d.SMaxAge, &d.SMaxAgeSet, val)
	case eqFold(key, "min-fresh"):
		parseDur(&d.MinFresh, &d.MinFreshSet, val)
	case eqFold(key, "max-stale"):
		if val == "" {
			d.MaxStale = time.Duration(1<<63 - 1)
			d.MaxStaleSet = true
		} else {
			parseDur(&d.MaxStale, &d.MaxStaleSet, val)
		}
	case eqFold(key, "stale-while-revalidate"):
		parseDur(&d.StaleWhileRevalid, &d.StaleWhileRevalidSet, val)
	case eqFold(key, "stale-if-error"):
		parseDur(&d.StaleIfError, &d.StaleIfErrorSet, val)
	}
}

// parseDur parses seconds from a string without allocating.
func parseDur(dur *time.Duration, set *bool, val string) {
	n, ok := parseIntNoAlloc(val)
	if ok {
		*dur = time.Duration(n) * time.Second
		*set = true
	}
}

// parseIntNoAlloc parses a non-negative decimal integer without
// allocating (avoids strconv.ParseInt which may allocate error).
func parseIntNoAlloc(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var n int64
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// eqFold is a case-insensitive ASCII comparison that avoids allocating
// (unlike strings.EqualFold which may allocate for Unicode).
func eqFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// FreshnessLifetime computes the freshness lifetime of a response
// per RFC 9111 §4.2.1.
func FreshnessLifetime(respCC Directives, header func(string) string) (time.Duration, bool) {
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge, true
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge, true
	}
	if exp := header("Expires"); exp != "" {
		expTime, err := time.Parse(time.RFC1123, exp)
		if err != nil {
			return 0, false
		}
		dateTime, err := time.Parse(time.RFC1123, header("Date"))
		if err != nil {
			return 0, false
		}
		return expTime.Sub(dateTime), true
	}
	return 0, false
}
