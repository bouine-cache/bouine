package cloudflare

import (
	"regexp"
	"strings"
)

// metaCharPattern matches any regexp metacharacter that prevents treating
// the regex as a plain literal value.
var metaCharPattern = regexp.MustCompile(`[+*?()|\[\]{}\\$]`)

// MapResult holds the result of mapping a bouine invalidation to one or more
// Cloudflare purge operations.
type MapResult struct {
	// URLs are exact URLs to purge (PurgeSingleFile).
	URLs []string
	// Tags are cache tags to purge (PurgeByTags).
	Tags []string
	// Prefixes are URL prefixes to purge (PurgeByPrefixes).
	Prefixes []string
	// Hosts are hostnames to purge (PurgeByHostnames).
	Hosts []string
	// Skipped is true when the invalidation could not be mapped to a CF
	// operation (e.g. a non-literal regex). The caller should log a warning.
	Skipped bool
	// SkipReason explains why the invalidation was skipped.
	SkipReason string
}

// MapURL maps a bouine purge-by-URL to CF PurgeSingleFile.
func MapURL(url string) MapResult {
	if url == "" {
		return MapResult{Skipped: true, SkipReason: "empty URL"}
	}
	return MapResult{URLs: []string{url}}
}

// MapSurrogateKey maps a bouine ban-by-surrogate-key to CF PurgeByTags.
func MapSurrogateKey(key string) MapResult {
	if key == "" {
		return MapResult{Skipped: true, SkipReason: "empty surrogate key"}
	}
	return MapResult{Tags: []string{key}}
}

// MapPathRegex attempts to map a bouine ban-by-path-regex to CF
// PurgeByPrefixes. Succeeds only when the regex is a plain literal prefix
// (optionally anchored with ^). Returns Skipped=true for patterns that
// contain metacharacters, since CF does not support regex-based purging.
func MapPathRegex(pattern string) MapResult {
	if pattern == "" {
		return MapResult{Skipped: true, SkipReason: "empty path regex"}
	}

	// Strip leading ^ anchor — CF prefixes are implied anchors.
	literal := strings.TrimPrefix(pattern, "^")

	// Reject any remaining metacharacters.
	if metaCharPattern.MatchString(literal) {
		return MapResult{
			Skipped:    true,
			SkipReason: "path_regex contains metacharacters — cannot map to CF prefix: " + pattern,
		}
	}

	// Also strip trailing $ anchor if present (literal suffix match).
	if strings.HasSuffix(literal, "$") {
		return MapResult{
			Skipped:    true,
			SkipReason: "path_regex is a suffix anchor (ends with $) — cannot map to CF prefix: " + pattern,
		}
	}

	if literal == "" {
		// "^" alone matches everything — use prefix "/" which purges all paths.
		literal = "/"
	}

	return MapResult{Prefixes: []string{literal}}
}

// MapHostRegex attempts to map a bouine ban-by-host-regex to CF
// PurgeByHostnames. Succeeds only when the pattern is a literal hostname
// (with optional escaped dots). Returns Skipped=true for patterns with
// regex metacharacters.
func MapHostRegex(pattern string) MapResult {
	if pattern == "" {
		return MapResult{Skipped: true, SkipReason: "empty host regex"}
	}

	// Unescape literal dot "\." → ".".
	literal := strings.ReplaceAll(pattern, `\.`, ".")

	// Reject if any metacharacters remain.
	if metaCharPattern.MatchString(literal) {
		return MapResult{
			Skipped:    true,
			SkipReason: "host_regex contains metacharacters — cannot map to CF hostname: " + pattern,
		}
	}

	// Reject anchors.
	if strings.ContainsAny(literal, "^$") {
		return MapResult{
			Skipped:    true,
			SkipReason: "host_regex contains anchors — cannot map to CF hostname: " + pattern,
		}
	}

	return MapResult{Hosts: []string{literal}}
}

// MergeResults combines multiple MapResults into one.
// If any result is Skipped, the merged result carries the first skip reason.
func MergeResults(results ...MapResult) MapResult {
	var merged MapResult
	for _, r := range results {
		merged.URLs = append(merged.URLs, r.URLs...)
		merged.Tags = append(merged.Tags, r.Tags...)
		merged.Prefixes = append(merged.Prefixes, r.Prefixes...)
		merged.Hosts = append(merged.Hosts, r.Hosts...)
		if r.Skipped && !merged.Skipped {
			merged.Skipped = true
			merged.SkipReason = r.SkipReason
		}
	}
	return merged
}
