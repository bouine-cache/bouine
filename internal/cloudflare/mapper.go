package cloudflare

import (
	"regexp"
	"strings"
)

// metaCharPattern matches any regexp metacharacter that prevents treating
// the regex as a plain literal value.
var metaCharPattern = regexp.MustCompile(`[+*?()|\[\]{}\\$]`)

// Skip reason categories for metric labels. Using fixed categories instead
// of raw user-supplied patterns prevents unbounded metric cardinality.
const (
	SkipCategoryEmpty        = "empty_input"
	SkipCategoryPathMetachar = "path_regex_metacharacters"
	SkipCategoryPathSuffix   = "path_regex_suffix_anchor"
	SkipCategoryHostMetachar = "host_regex_metacharacters"
	SkipCategoryHostAnchor   = "host_regex_anchors"
	SkipCategoryCompoundBan  = "compound_ban"
)

// SkipCategory maps a full SkipReason string to a fixed metric label value.
// The full reason (which may contain user-supplied patterns) goes into logs;
// the category goes into Prometheus labels to keep cardinality bounded.
func SkipCategory(reason string) string {
	switch {
	case strings.Contains(reason, "compound ban"):
		return SkipCategoryCompoundBan
	case strings.Contains(reason, "path_regex is a suffix anchor"):
		return SkipCategoryPathSuffix
	case strings.Contains(reason, "path_regex contains metacharacters"):
		return SkipCategoryPathMetachar
	case strings.Contains(reason, "host_regex contains metacharacters"):
		return SkipCategoryHostMetachar
	case strings.Contains(reason, "host_regex contains anchors"):
		return SkipCategoryHostAnchor
	case strings.Contains(reason, "empty"):
		return SkipCategoryEmpty
	default:
		return "unknown"
	}
}

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

	// A trailing $ anchor means exact-match, not prefix-match. Check this
	// before the metacharacter rejection below ($ is itself a metacharacter)
	// so we can give a more specific skip reason.
	if strings.HasSuffix(literal, "$") {
		return MapResult{
			Skipped:    true,
			SkipReason: "path_regex is a suffix anchor (ends with $) — cannot map to CF prefix: " + pattern,
		}
	}

	// Reject any remaining metacharacters.
	if metaCharPattern.MatchString(literal) {
		return MapResult{
			Skipped:    true,
			SkipReason: "path_regex contains metacharacters — cannot map to CF prefix: " + pattern,
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

	// Reject ^ anchor ($ is already caught by the metacharacter check above).
	if strings.Contains(literal, "^") {
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
