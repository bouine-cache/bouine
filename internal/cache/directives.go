package cache

import (
	"strconv"
	"strings"
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
// Directives. Unknown directives are silently ignored per RFC 9111
// §5.2.3.
func ParseCacheControl(header string) Directives {
	var d Directives
	for _, token := range strings.Split(header, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		key, val, _ := strings.Cut(token, "=")
		key = strings.TrimSpace(strings.ToLower(key))
		val = strings.TrimSpace(strings.Trim(val, "\""))
		applyDirective(&d, key, val)
	}
	return d
}

func applyDirective(d *Directives, key, val string) {
	if applyBoolDirective(d, key) {
		return
	}
	applyDurDirective(d, key, val)
}

func applyBoolDirective(d *Directives, key string) bool {
	switch key {
	case "no-store":
		d.NoStore = true
	case "no-cache":
		d.NoCache = true
	case "private":
		d.Private = true
	case "public":
		d.Public = true
	case "must-revalidate":
		d.MustRevalidate = true
	case "proxy-revalidate":
		d.ProxyRevalidate = true
	case "immutable":
		d.Immutable = true
	case "no-transform":
		d.NoTransform = true
	case "only-if-cached":
		d.OnlyIfCached = true
	default:
		return false
	}
	return true
}

func applyDurDirective(d *Directives, key, val string) {
	switch key {
	case "max-age":
		parseDur(&d.MaxAge, &d.MaxAgeSet, val)
	case "s-maxage":
		parseDur(&d.SMaxAge, &d.SMaxAgeSet, val)
	case "min-fresh":
		parseDur(&d.MinFresh, &d.MinFreshSet, val)
	case "max-stale":
		if val == "" {
			d.MaxStale = time.Duration(1<<63 - 1)
			d.MaxStaleSet = true
		} else {
			parseDur(&d.MaxStale, &d.MaxStaleSet, val)
		}
	case "stale-while-revalidate":
		parseDur(&d.StaleWhileRevalid, &d.StaleWhileRevalidSet, val)
	case "stale-if-error":
		parseDur(&d.StaleIfError, &d.StaleIfErrorSet, val)
	}
}

func parseDur(dur *time.Duration, set *bool, val string) {
	if secs, err := strconv.ParseInt(val, 10, 64); err == nil {
		*dur = time.Duration(secs) * time.Second
		*set = true
	}
}

// FreshnessLifetime computes the freshness lifetime of a response
// per RFC 9111 §4.2.1. The caller supplies the response headers.
// Returns the lifetime and whether it was explicitly set.
func FreshnessLifetime(respCC Directives, header func(string) string) (time.Duration, bool) {
	// s-maxage takes priority for shared caches (RFC 9111 §5.2.2.10).
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge, true
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge, true
	}
	// Expires header fallback (RFC 9111 §5.3).
	if exp := header("Expires"); exp != "" {
		expTime, err := time.Parse(time.RFC1123, exp)
		if err != nil {
			return 0, false
		}
		dateStr := header("Date")
		dateTime, err := time.Parse(time.RFC1123, dateStr)
		if err != nil {
			return 0, false
		}
		return expTime.Sub(dateTime), true
	}
	return 0, false
}
