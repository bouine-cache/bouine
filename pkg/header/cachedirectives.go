package header

import "time"

// cachedirectives.go holds the RFC 9111 §5.2 Cache-Control tokenizer.
// It lives in the pkg/header shared kernel (not internal/cache) so the
// storage layer can re-derive an Object's transient gate flags from the
// stored header map when decoding blobs whose wire version predates the
// flags byte (see internal/storage/codec.go and ADR-0053).
//
// The tokenizer is zero-alloc: it scans the header value in place and
// never materializes substrings for directive keys or values.

// Directives holds the parsed Cache-Control directives from either a
// request or a response. Zero values mean the directive was absent.
//
// Stable: internal/cache re-exports this type as cache.Directives so
// existing call sites keep compiling; new code in lower layers should
// use header.Directives directly.
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
//
// Stable: internal/cache re-exports this function; see ADR-0053 for the
// move history.
func ParseCacheControl(headerValue string) Directives {
	var d Directives
	i := 0
	for i < len(headerValue) {
		i = skipCCDelimiters(headerValue, i)
		if i >= len(headerValue) {
			break
		}
		var key, val string
		key, val, i = scanCCToken(headerValue, i)
		applyCCDirective(&d, key, val)
	}
	return d
}

// ParseCacheControlBytes parses a Cache-Control header value from a
// []byte without converting to string first. Used on hot paths where
// the header value comes from fasthttp's Peek (zero-copy []byte).
//
// Stable: internal/cache re-exports this function.
func ParseCacheControlBytes(cc []byte) Directives {
	var d Directives
	i := 0
	for i < len(cc) {
		i = skipCCDelimitersBytes(cc, i)
		if i >= len(cc) {
			break
		}
		var key, val []byte
		key, val, i = scanCCTokenBytes(cc, i)
		applyCCDirectiveBytes(&d, key, val)
	}
	return d
}

func skipCCDelimitersBytes(s []byte, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == ',' || s[i] == '\t') {
		i++
	}
	return i
}

func scanCCTokenBytes(s []byte, i int) (key, val []byte, next int) {
	keyStart := i
	for i < len(s) && s[i] != '=' && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
		i++
	}
	key = s[keyStart:i]

	if i < len(s) && s[i] == '=' {
		i++
		val, i = scanCCValueBytes(s, i)
	}
	return key, val, i
}

func scanCCValueBytes(s []byte, i int) ([]byte, int) {
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

func applyCCDirectiveBytes(d *Directives, key, val []byte) {
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

// eqFoldBytes is a case-insensitive ASCII comparison over byte slices
// that never allocates.
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

func skipCCDelimiters(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == ',' || s[i] == '\t') {
		i++
	}
	return i
}

func scanCCToken(s string, i int) (key, val string, next int) {
	keyStart := i
	for i < len(s) && s[i] != '=' && s[i] != ',' && s[i] != ' ' && s[i] != '\t' {
		i++
	}
	key = s[keyStart:i]

	if i < len(s) && s[i] == '=' {
		i++
		val, i = scanCCValue(s, i)
	}
	return key, val, i
}

func scanCCValue(s string, i int) (string, int) {
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

func applyCCDirective(d *Directives, key, val string) {
	// RFC 9111 §5.2.2.4: no-cache with a quoted field list means "strip
	// those headers when serving from cache" — different from bare
	// no-cache which requires full revalidation.
	if EqFold(key, "no-cache") && val != "" {
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
	case EqFold(key, "no-store"):
		d.NoStore = true
	case EqFold(key, "no-cache"):
		d.NoCache = true
	case EqFold(key, "private"):
		d.Private = true
	case EqFold(key, "public"):
		d.Public = true
	case EqFold(key, "must-revalidate"):
		d.MustRevalidate = true
	case EqFold(key, "proxy-revalidate"):
		d.ProxyRevalidate = true
	case EqFold(key, "immutable"):
		d.Immutable = true
	case EqFold(key, "no-transform"):
		d.NoTransform = true
	case EqFold(key, "only-if-cached"):
		d.OnlyIfCached = true
	case EqFold(key, "must-understand"):
		d.MustUnderstand = true
	default:
		return false
	}
	return true
}

func applyDurDirective(d *Directives, key, val string) {
	switch {
	case EqFold(key, "max-age"):
		// RFC 9111 §5.2.2.1: ignore max-age with non-numeric value (e.g. "a3600").
		// parseIntNoAlloc returns (0,false) for values starting with a letter.
		parseDurStr(&d.MaxAge, &d.MaxAgeSet, val)
	case EqFold(key, "s-maxage"):
		parseDurStr(&d.SMaxAge, &d.SMaxAgeSet, val)
	case EqFold(key, "min-fresh"):
		parseDurStr(&d.MinFresh, &d.MinFreshSet, val)
	case EqFold(key, "max-stale"):
		if val == "" {
			d.MaxStale = time.Duration(1<<63 - 1)
			d.MaxStaleSet = true
		} else {
			parseDurStr(&d.MaxStale, &d.MaxStaleSet, val)
		}
	case EqFold(key, "stale-while-revalidate"):
		parseDurStr(&d.StaleWhileRevalid, &d.StaleWhileRevalidSet, val)
	case EqFold(key, "stale-if-error"):
		parseDurStr(&d.StaleIfError, &d.StaleIfErrorSet, val)
	}
}

// parseDurStr parses seconds from a string without allocating.
// For freshness directives (max-age, s-maxage), the LARGEST value among
// duplicates wins so that caches can serve the freshest possible response
// when origins send conflicting values (optimal behaviour per cache-tests).
func parseDurStr(dur *time.Duration, set *bool, val string) {
	n, ok := ParseIntNoAlloc(val)
	if !ok {
		return
	}
	d := time.Duration(n) * time.Second
	if !*set || d > *dur {
		*dur = d
		*set = true
	}
}

// ParseIntNoAlloc parses a non-negative decimal integer without
// allocating. Tolerant: stops at the first non-digit character so
// "100a" → 100 and "3600.0" → 3600 (RFC 9111 requires integers but
// real-world servers send trailing garbage and decimals).
//
// Exported: internal/cache and internal/storage share it for age and
// duration header parsing.
func ParseIntNoAlloc(s string) (int64, bool) {
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

// EqFold is a case-insensitive ASCII comparison that avoids
// allocating (unlike strings.EqualFold which may allocate for Unicode).
//
// Exported: shared by internal/cache and this package's tokenizer call
// sites (ADR-0053).
func EqFold(a, b string) bool {
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
