package cache

import (
	"time"

	"github.com/bouine-cache/bouine/pkg/header"
)

// Directives holds the parsed Cache-Control directives from either a
// request or a response. Zero values mean the directive was absent.
type Directives struct {
	NoCacheFields        string // comma-separated field names from no-cache="…"
	MaxAge               time.Duration
	StaleIfError         time.Duration
	StaleWhileRevalid    time.Duration
	MaxStale             time.Duration
	MinFresh             time.Duration
	SMaxAge              time.Duration
	MaxAgeSet            bool
	SMaxAgeSet           bool
	MinFreshSet          bool
	MaxStaleSet          bool
	StaleWhileRevalidSet bool
	StaleIfErrorSet      bool
	MustRevalidate       bool
	ProxyRevalidate      bool
	Immutable            bool
	NoTransform          bool
	OnlyIfCached         bool
	MustUnderstand       bool
	NoStore              bool
	Public               bool
	Private              bool
	NoCache              bool
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

// ParseCacheControlBytes parses a Cache-Control header value from a []byte
// without converting to string first. Used on the bypass path where the
// header value comes from fasthttp's Peek (zero-copy []byte).
func ParseCacheControlBytes(cc []byte) Directives {
	var d Directives
	i := 0
	for i < len(cc) {
		i = skipDelimitersBytes(cc, i)
		if i >= len(cc) {
			break
		}
		var key, val []byte
		key, val, i = scanTokenBytes(cc, i)
		applyDirectiveBytes(&d, key, val)
	}
	return d
}

func skipDelimitersBytes(s []byte, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == ',' || s[i] == '\t') {
		i++
	}
	return i
}

func scanTokenBytes(s []byte, i int) (key, val []byte, next int) {
	keyStart := i
	for i < len(s) && s[i] != '=' && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
		i++
	}
	key = s[keyStart:i]

	if i < len(s) && s[i] == '=' {
		i++
		val, i = scanValueBytes(s, i)
	}
	return key, val, i
}

func scanValueBytes(s []byte, i int) ([]byte, int) {
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

func applyDirectiveBytes(d *Directives, key, val []byte) {
	if eqFoldBytes(key, []byte("no-cache")) && len(val) > 0 {
		d.NoCacheFields = string(val)
		return
	}
	if applyBoolDirectiveBytes(d, key) {
		return
	}
	applyDurDirectiveBytes(d, key, val)
}

func applyBoolDirectiveBytes(d *Directives, key []byte) bool {
	switch {
	case eqFoldBytes(key, []byte("no-store")):
		d.NoStore = true
	case eqFoldBytes(key, []byte("no-cache")):
		d.NoCache = true
	case eqFoldBytes(key, []byte("private")):
		d.Private = true
	case eqFoldBytes(key, []byte("public")):
		d.Public = true
	case eqFoldBytes(key, []byte("must-revalidate")):
		d.MustRevalidate = true
	case eqFoldBytes(key, []byte("proxy-revalidate")):
		d.ProxyRevalidate = true
	case eqFoldBytes(key, []byte("immutable")):
		d.Immutable = true
	case eqFoldBytes(key, []byte("no-transform")):
		d.NoTransform = true
	case eqFoldBytes(key, []byte("only-if-cached")):
		d.OnlyIfCached = true
	case eqFoldBytes(key, []byte("must-understand")):
		d.MustUnderstand = true
	default:
		return false
	}
	return true
}

func applyDurDirectiveBytes(d *Directives, key, val []byte) {
	switch {
	case eqFoldBytes(key, []byte("max-age")):
		parseDurBytes(&d.MaxAge, &d.MaxAgeSet, val)
	case eqFoldBytes(key, []byte("s-maxage")):
		parseDurBytes(&d.SMaxAge, &d.SMaxAgeSet, val)
	case eqFoldBytes(key, []byte("min-fresh")):
		parseDurBytes(&d.MinFresh, &d.MinFreshSet, val)
	case eqFoldBytes(key, []byte("max-stale")):
		if len(val) == 0 {
			d.MaxStale = time.Duration(1<<63 - 1)
			d.MaxStaleSet = true
		} else {
			parseDurBytes(&d.MaxStale, &d.MaxStaleSet, val)
		}
	case eqFoldBytes(key, []byte("stale-while-revalidate")):
		parseDurBytes(&d.StaleWhileRevalid, &d.StaleWhileRevalidSet, val)
	case eqFoldBytes(key, []byte("stale-if-error")):
		parseDurBytes(&d.StaleIfError, &d.StaleIfErrorSet, val)
	}
}

func parseDurBytes(dur *time.Duration, set *bool, val []byte) {
	n, ok := parseIntBytes(val)
	if !ok {
		return
	}
	d := time.Duration(n) * time.Second
	if !*set || d > *dur {
		*dur = d
		*set = true
	}
}

func parseIntBytes(s []byte) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var n int64
	found := false
	for i := range s {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
		found = true
	}
	return n, found
}

func eqFoldBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
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
	// RFC 9111 §5.2.2.4: no-cache with a quoted field list means "strip
	// those headers when serving from cache" — different from bare no-cache
	// which requires full revalidation.
	if eqFold(key, "no-cache") && val != "" {
		d.NoCacheFields = val
		return
	}
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
	case eqFold(key, "must-understand"):
		d.MustUnderstand = true
	default:
		return false
	}
	return true
}

func applyDurDirective(d *Directives, key, val string) {
	switch {
	case eqFold(key, "max-age"):
		// RFC 9111 §5.2.2.1: ignore max-age with non-numeric value (e.g. "a3600").
		// parseIntNoAlloc returns (0,false) for values starting with a letter.
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
// For freshness directives (max-age, s-maxage), the LARGEST value among
// duplicates wins so that caches can serve the freshest possible response
// when origins send conflicting values (optimal behaviour per cache-tests).
func parseDur(dur *time.Duration, set *bool, val string) {
	n, ok := parseIntNoAlloc(val)
	if !ok {
		return
	}
	d := time.Duration(n) * time.Second
	if !*set || d > *dur {
		*dur = d
		*set = true
	}
}

// parseIntNoAlloc parses a non-negative decimal integer without
// allocating. Tolerant: stops at the first non-digit character so
// "100a" → 100 and "3600.0" → 3600 (RFC 9111 requires integers but
// real-world servers send trailing garbage and decimals).
func parseIntNoAlloc(s string) (int64, bool) {
	if len(s) == 0 {
		return 0, false
	}
	var n int64
	found := false
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
		found = true
	}
	return n, found
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
// per RFC 9111 §4.2.1. When CDN-Cache-Control is present it takes
// precedence over Cache-Control for shared-cache TTL decisions
// (RFC 9211).
func FreshnessLifetime(respCC Directives, getHdr func(string) string) (time.Duration, bool) {
	// CDN-Cache-Control takes precedence when present.
	if cdnCC := getHdr(header.CDNCacheControl); cdnCC != "" {
		cdnD := ParseCacheControl(cdnCC)
		if cdnD.MaxAgeSet {
			return cdnD.MaxAge, true
		}
		if cdnD.NoStore || cdnD.Private {
			return 0, true // blocked by CDN directive
		}
		// CDN-CC present but no TTL directive — treat as expired.
		return 0, true
	}
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge, true
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge, true
	}
	if exp := getHdr(header.Expires); exp != "" {
		expTime := parseHTTPDate(exp)
		if expTime.IsZero() {
			// Invalid Expires → treat as no freshness information,
			// not as explicitly expired, so heuristic can still apply.
			return 0, false
		}
		dateStr := getHdr(header.Date)
		dateTime := parseHTTPDate(dateStr)
		if dateTime.IsZero() {
			return 0, false
		}
		return expTime.Sub(dateTime), true
	}
	return 0, false
}

// FreshnessLifetimeH is like FreshnessLifetime but takes header.Map
// directly so it can detect multiple Expires headers (which are
// invalid per RFC 9110 §5.3) and read CDN-Cache-Control.
func FreshnessLifetimeH(respCC Directives, h header.Map) (time.Duration, bool) {
	// CDN-Cache-Control takes precedence when present (RFC 9211).
	if cdnCC := mergeHeaderValues(h, header.CDNCacheControl); cdnCC != "" {
		cdnD := ParseCacheControl(cdnCC)
		if cdnD.MaxAgeSet {
			return cdnD.MaxAge, true
		}
		if cdnD.NoStore || cdnD.Private {
			return 0, true
		}
		return 0, true
	}
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge, true
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge, true
	}
	expiresVal := h.Get(header.Expires)
	if expiresVal == "" {
		return 0, false
	}
	expTime := parseHTTPDate(expiresVal)
	if expTime.IsZero() {
		// Syntactically invalid Expires → treat as no freshness info
		// (RFC 9111 §4.2.1: don't use invalid Expires in calculations).
		return 0, false
	}
	dateStr := h.Get(header.Date)
	dateTime := parseHTTPDate(dateStr)
	if dateTime.IsZero() {
		// RFC 9111 §4.2.1: if Date is absent or invalid, use the current
		// time as a proxy for the response date so Expires-based freshness
		// can still be computed.
		dateTime = time.Now()
	}
	return expTime.Sub(dateTime), true
}
