package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/bouine-cache/bouine/pkg/api"
)

// maxFetchTimeout is the upper bound for fetch_timeout. It must stay
// strictly below internal/server.safetyNetWriteTimeout so the write
// deadline never fires during an origin fetch. The two constants are
// duplicated across packages because the layering rules (L1 and L2
// cannot depend on each other) prevent a shared import. If you change
// one, change the other.
const maxFetchTimeout = 5 * time.Minute

// maxFetchWaitTimeout is the upper bound for fetch_wait_timeout. The
// wait bound exists to absorb sub-second fetch-queue bursts, not to
// queue through a sustained overload: when arrival rate exceeds the
// semaphore's drain rate, no finite wait drains the queue, so a longer
// bound only holds goroutines (and their connections) longer before
// shedding them (issue #562). 1s keeps the fetch-queue shed binding
// before the cruder connection-limit shed at realistic pod rates, and
// stays coherent with the Retry-After: 1 sent to shed clients.
const maxFetchWaitTimeout = 1 * time.Second

// defaultAdminIdleTimeout mirrors admin.DefaultAdminIdleTimeout (300s).
// Duplicated because config is a leaf package and cannot import
// internal/admin. If you change one, change the other.
const defaultAdminIdleTimeout = 300 * time.Second

// MaxPeerFetchConcurrency mirrors cluster.MaxPeerFetchConcurrency (128).
// Duplicated because config is a leaf package and cannot import
// internal/cluster. If you change one, change the other.
const MaxPeerFetchConcurrency = 128

// maxReadTimeout is the upper bound for listen.read_timeout. It must
// stay strictly below internal/server.safetyNetWriteTimeout so the
// safety net, not the read deadline, bounds a request's total lifetime.
// Duplicated across packages because the layering rules (config is a
// leaf) prevent a shared import. If you change one, change the other.
const maxReadTimeout = 5 * time.Minute

// Defaults returns a Config populated with safe defaults. The
// "admin: :9000" listener is enabled so the daemon is operable even
// with an empty config file.
func Defaults() Config {
	return Config{
		Listen: Listen{
			Admin: ":9000",
		},
		TLS: TLS{
			MinVersion: TLSVersion12,
		},
		Cluster: Cluster{
			Mode:     ClusterModeStrong,
			HopLimit: 2,
		},
		Admin: AdminConfig{
			DrainDuration: 10 * time.Second,
		},
	}
}

// defaultHotMaxBytesRatio is the fraction of GOMEMLIMIT used to derive
// the hot store budget when hot_max_bytes is unset. 75% leaves headroom
// for RSS overhead and lets the Go GC run without aggressive cycles
// near GOMEMLIMIT (issue #161).
const defaultHotMaxBytesRatio = 75

// defaultWarmMaxEntriesRatio is the fraction of GOMEMLIMIT used to derive
// the warm index entry cap when warm_max_entries is unset. 15% leaves
// headroom for the hot store (75%), Go runtime overhead, and GC
// fragmentation. At 14 GiB GOMEMLIMIT, 15% = ~16 M entries (~2 GiB heap).
const defaultWarmMaxEntriesRatio = 15

// defaultStreamingBufferRatio is the fraction of GOMEMLIMIT used to derive
// the streaming tee buffer cap when max_streaming_buffer_bytes is unset.
// 7% leaves headroom for the hot store (75%), warm index (15%), Go runtime
// overhead, and fasthttp response buffers (bytebufferpool). At 768 MiB
// GOMEMLIMIT, 7% = 53 MiB, bounding peak tee buffer allocations to ~106 MiB
// (bytes.Buffer doubles) — below the OOMKill threshold with margin for the
// in-loop cap to act before all 32 fetchSem slots fill. At 14 GiB
// GOMEMLIMIT, 7% = ~1 GiB, generous enough that the cap rarely triggers
// in normal traffic.
const defaultStreamingBufferRatio = 7

// Load reads a YAML file from path, applies Defaults, and validates.
// Strict mode rejects unknown fields so typos surface immediately.
func Load(path string) (*Config, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", path, err)
	}

	raw, err := os.ReadFile(abs) //nolint:gosec // configured path, see threat-model T15
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", abs, err)
	}
	return Parse(raw)
}

// Parse decodes YAML bytes into a Config, applying Defaults underneath
// and rejecting unknown keys. An empty input is valid and yields a
// config equal to Defaults().
//
// Environment variable interpolation is applied before YAML decoding:
// ${VAR} is replaced with the value of VAR from the process environment.
// ${VAR:-default} provides a fallback when VAR is unset or empty.
// Interpolation applies only to ${NAME} where NAME is shaped like an
// environment variable name (a letter or underscore, then letters,
// digits, or underscores); other braced sequences (e.g. ${1}, a
// path_rewrite capture-group reference) are left untouched. Literal
// dollar signs can be escaped as $$.
func Parse(b []byte) (*Config, error) {
	expanded := expandEnvVars(b)
	cfg := Defaults()
	if len(strings.TrimSpace(string(expanded))) > 0 {
		dec := yaml.NewDecoder(strings.NewReader(string(expanded)))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("yaml decode: %w", err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Derive hot_max_bytes from GOMEMLIMIT when unset so the budget
	// adapts to the runtime memory limit of the deployment (issue #161).
	// Runs for both empty and populated configs so an empty config file
	// in a container with GOMEMLIMIT still gets an eviction budget.
	cfg.Storage.ResolveHotMaxBytes(os.Getenv("GOMEMLIMIT"))
	cfg.Storage.ResolveWarmMaxEntries(os.Getenv("GOMEMLIMIT"))
	// Derive max_streaming_buffer_bytes from GOMEMLIMIT when unset so
	// the streaming tee buffer cap adapts to the pod's memory limit
	// (PR #524 OOMKill fix). Per-route like the other cache fields.
	for i := range cfg.Routes {
		cfg.Routes[i].Cache.ResolveMaxStreamingBufferBytes(os.Getenv("GOMEMLIMIT"))
	}
	return &cfg, nil
}

// Validate runs cross-field checks, reporting every invalid field at
// once (each failure is a *FieldError; multiple failures are joined
// with errors.Join). It is called by Load/Parse but is also useful
// from tests.
//
//nolint:gocyclo // 22: validation is a flat checklist of independent fields
func (c *Config) Validate() error {
	ec := &errCollector{}

	// At least one listener must be enabled. Admin is OK as a sole
	// listener when no TLS is configured.
	if c.Listen.HTTP == "" && c.Listen.HTTPS == "" &&
		c.Listen.Admin == "" {
		ec.addf("listen", "at least one listener must be configured")
	}

	// Upstream pool names must be unique.
	seen := make(map[string]struct{}, len(c.UpstreamPools))
	for i := range c.UpstreamPools {
		p := &c.UpstreamPools[i]
		path := fmt.Sprintf("upstream_pools[%d]", i)
		if p.Name == "" {
			ec.addf(path+".name", "must be non-empty")
		} else if _, dup := seen[p.Name]; dup {
			ec.addf(path+".name", "is a duplicate (pool %q is declared twice)", p.Name)
		} else {
			seen[p.Name] = struct{}{}
		}
		if len(p.Targets) == 0 {
			ec.addf(path+".targets", "must list at least one target (pool %q)", p.Name)
		}
		validatePoolDurations(ec, i, p)
	}

	for i := range c.Routes {
		c.validateRoute(ec, i, seen)
	}

	c.validateCluster(ec)

	// SO_REUSEPORT is only supported on Linux. The config package is a
	// leaf and cannot import internal/platform, so we check GOOS directly.
	// platform.ReusePortSupported mirrors this check.
	if c.Listen.ReusePort != nil && *c.Listen.ReusePort && runtime.GOOS != "linux" {
		ec.addf("listen.reuse_port", "is only supported on Linux")
	}

	if c.Listen.IdleTimeout < 0 {
		ec.addf("listen.idle_timeout", "must be >= 0, got %v", c.Listen.IdleTimeout)
	}

	if c.Listen.ReadTimeout < 0 {
		ec.addf("listen.read_timeout", "must be >= 0, got %v", c.Listen.ReadTimeout)
	}
	if c.Listen.ReadTimeout >= maxReadTimeout {
		ec.addf("listen.read_timeout", "must be < %v (data plane safety-net WriteTimeout), got %v", maxReadTimeout, c.Listen.ReadTimeout)
	}

	// The reactor multiplexes fast-path hit serving; without the fast
	// path it has nothing to serve and would silently no-op. The
	// listener wiring also gates on H1FastPath, so reject the
	// combination early at load time instead of logging a warning at
	// startup.
	if c.Experimental.H1Reactor && !c.Experimental.H1FastPath {
		ec.addf("experimental.h1_reactor", "requires experimental.h1_fast_path")
	}

	// The peer branch only runs inside the fast path's TryHit; without
	// h1_fast_path the flag would silently no-op. Reject early at load
	// time instead of logging a warning at startup.
	if c.Experimental.H1FastPeerPath && !c.Experimental.H1FastPath {
		ec.addf("experimental.h1_fast_peer_path", "requires experimental.h1_fast_path")
	}

	// GOGC must be -1 (off) or a positive percentage. Zero is invalid
	// (would trigger GC on every allocation) and negative values other
	// than -1 are meaningless.
	if c.GOGC != nil && *c.GOGC != -1 && *c.GOGC <= 0 {
		ec.addf("gogc", "must be -1 (off) or a positive percentage")
	}

	return ec.err()
}

// expandEnvVars replaces ${VAR} and ${VAR:-default} patterns in the
// raw config bytes with values from the process environment. $$ is
// expanded to a literal $ to allow escaping.
func expandEnvVars(b []byte) []byte {
	s := string(b)
	var sb strings.Builder
	sb.Grow(len(s))
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '$' && s[i+1] == '$' {
			sb.WriteByte('$')
			i += 2
			continue
		}
		if i+1 < len(s) && s[i] == '$' && s[i+1] == '{' {
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				sb.WriteByte(s[i])
				i++
				continue
			}
			expr := s[i+2 : i+2+end]
			name := expr
			defVal := ""
			if idx := strings.Index(expr, ":-"); idx >= 0 {
				name = expr[:idx]
				defVal = expr[idx+2:]
			}
			// Only env-var-shaped names interpolate. A braced run that
			// starts with a digit ($1, ${1}, ${01}) or is otherwise not a
			// plausible environment variable name is a path_rewrite
			// capture-group reference or a typo, not an env lookup: POSIX
			// env names never start with a digit. Leaving it verbatim keeps
			// the documented `${1}x` template syntax intact through the
			// loader; before this gate it was silently replaced with the
			// (usually empty) value of an env var that cannot exist.
			if !isEnvName(name) {
				sb.WriteString(s[i : i+2+end+1])
				i += 2 + end + 1
				continue
			}
			val := os.Getenv(name)
			if val == "" {
				val = defVal
			}
			sb.WriteString(val)
			i += 2 + end + 1
			continue
		}
		sb.WriteByte(s[i])
		i++
	}
	return []byte(sb.String())
}

// isEnvName reports whether s is shaped like a POSIX environment
// variable name: a letter or underscore, then letters, digits, or
// underscores. Environment-variable interpolation is restricted to
// these names so braced capture-group references (`${1}`, `${01}`) and
// other non-name runs survive the loader verbatim (see expandEnvVars).
func isEnvName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') {
			continue
		}
		if i > 0 && '0' <= b && b <= '9' {
			continue
		}
		return false
	}
	return true
}

// ResolveHotMaxBytes derives the hot store memory budget from the
// GOMEMLIMIT value (as exported by the Go runtime) when hot_max_bytes
// is not explicitly configured. This keeps SIEVE eviction headroom
// below the Go runtime soft memory limit so the GC does not enter a
// death spiral as the cache fills (issue #161).
//
// When hot_max_bytes is set explicitly it is kept as-is (operator
// override). When GOMEMLIMIT is empty or unparseable, HotMaxBytes is
// left unchanged so the HotStore falls back to its internal default.
// In practice the Go runtime rejects an invalid GOMEMLIMIT before the
// process starts, so the unparseable branch is unreachable for the env
// var; it is tolerated only so unit tests can pass synthetic values.
func (s *Storage) ResolveHotMaxBytes(goMemLimit string) {
	if s.HotMaxBytes > 0 {
		return
	}
	raw := strings.TrimSpace(goMemLimit)
	if raw == "" {
		return
	}
	n, err := parseByteSize(raw)
	if err != nil || n <= 0 {
		return
	}
	s.HotMaxBytes = ByteSize(n * int64(defaultHotMaxBytesRatio) / 100)
}

// ResolveWarmMaxEntries derives the warm index entry cap from the
// GOMEMLIMIT value when warm_max_entries is not explicitly configured.
// This bounds the Go heap cost of the warm index (map[uint64]warmLoc +
// SIEVE entries) to a percentage of the runtime memory limit, preventing
// unbounded heap growth that leads to OOMKill (exit 137).
//
// When warm_max_entries is set explicitly it is kept as-is (operator
// override). When GOMEMLIMIT is empty or unparseable, WarmMaxEntries is
// left unchanged (zero = unlimited). The 160 constant is inlined from
// warm.EstimatedWarmLocHeapBytes to avoid a circular import.
func (s *Storage) ResolveWarmMaxEntries(goMemLimit string) {
	if s.WarmMaxEntries > 0 {
		return
	}
	raw := strings.TrimSpace(goMemLimit)
	if raw == "" {
		return
	}
	n, err := parseByteSize(raw)
	if err != nil || n <= 0 {
		return
	}
	// 160 = warm.EstimatedWarmLocHeapBytes. Inlined to avoid circular
	// import (config -> warm -> storage). Update both if warmLoc changes.
	s.WarmMaxEntries = n * int64(defaultWarmMaxEntriesRatio) / (100 * 160)
}

// ResolveMaxStreamingBufferBytes derives the streaming tee buffer cap
// from the GOMEMLIMIT value when max_streaming_buffer_bytes is not
// explicitly configured. This bounds the total memory held in live
// streaming buffers during concurrent miss-fetches, preventing the
// OOMKill that occurs under slow-origin events when all fetchSem slots
// fill with buffered responses (PR #524).
//
// When max_streaming_buffer_bytes is set explicitly it is kept as-is
// (operator override). When GOMEMLIMIT is empty or unparseable, the
// field is left unchanged so the handler falls back to its built-in
// default (64 MiB).
func (c *RouteCache) ResolveMaxStreamingBufferBytes(goMemLimit string) {
	if c.MaxStreamingBufferBytes > 0 {
		return
	}
	raw := strings.TrimSpace(goMemLimit)
	if raw == "" {
		return
	}
	n, err := parseByteSize(raw)
	if err != nil || n <= 0 {
		return
	}
	c.MaxStreamingBufferBytes = ByteSize(n * int64(defaultStreamingBufferRatio) / 100)
}

// validateRoute checks a single route entry and normalises its fields.
// A route must specify exactly one of Pool or Static.Root.
func (c *Config) validateRoute(ec *errCollector, i int, pools map[string]struct{}) {
	r := &c.Routes[i]
	prefix := fmt.Sprintf("routes[%d]", i)
	hasPool := r.Pool != ""
	hasStatic := r.Static.Root != ""
	if hasPool && hasStatic {
		ec.addf(prefix, "has both pool and static.root — specify exactly one")
	}
	if !hasPool && !hasStatic {
		ec.addf(prefix, "has no pool or static.root")
	}
	if hasPool {
		if _, ok := pools[r.Pool]; !ok {
			ec.addf(prefix+".pool", "references unknown pool %q", r.Pool)
		}
	}
	if hasStatic {
		validateStatic(ec, prefix+".static", r.Static)
	}
	// Auto-derive Route.Name when empty so Prometheus metrics and the
	// dashboard have consistent route labels without requiring operators
	// to name every route.
	if r.Name == "" {
		switch {
		case r.Match.Host != "":
			r.Name = r.Match.Host + ":" + r.Match.PathPrefix
		case r.Match.PathPrefix != "":
			r.Name = r.Match.PathPrefix
		default:
			r.Name = "_catch-all"
		}
	}
	for j, m := range r.Match.Methods {
		up := strings.ToUpper(strings.TrimSpace(m))
		if !isKnownHTTPMethod(up) {
			ec.addf(fmt.Sprintf("%s.match.methods[%d]", prefix, j), "unknown HTTP method %q", m)
			continue
		}
		r.Match.Methods[j] = up
	}
	if sp := r.Request.StripPrefix; sp != "" && !strings.HasPrefix(sp, "/") {
		ec.addf(prefix+".request.strip_prefix", "must start with '/', got %q", sp)
	}
	validatePathRewrite(ec, prefix+".request", r.Request)
	validateRouteCache(ec, prefix+".cache", &r.Cache)
}

// validatePathRewrite validates the request.path_rewrite block: both
// fields required together (a half-configured rewrite is an operator
// mistake, not a no-op), mutually exclusive with strip_prefix (two
// mechanisms rewriting the same origin path cannot be reasoned about),
// compiled here so a bad pattern fails startup instead of the first
// request, size-capped, free of raw control bytes, and every template
// reference resolvable against the pattern's capture groups.
func validatePathRewrite(ec *errCollector, reqPath string, req RouteRequest) {
	pw := req.PathRewrite
	if pw.Match == "" && pw.Replace == "" {
		return
	}
	basePath := reqPath + ".path_rewrite"
	if pw.Match == "" || pw.Replace == "" {
		ec.addf(basePath, "requires both match and replace")
		return
	}
	if req.StripPrefix != "" {
		ec.addf(basePath, "is mutually exclusive with strip_prefix — specify exactly one")
	}
	if len(pw.Match) > MaxPathRewritePatternBytes {
		ec.addf(basePath+".match", "exceeds %d bytes", MaxPathRewritePatternBytes)
	}
	if len(pw.Replace) > MaxPathRewritePatternBytes {
		ec.addf(basePath+".replace", "exceeds %d bytes", MaxPathRewritePatternBytes)
	}
	// Raw control bytes can never appear in a request path (the data
	// plane rejects them at parse time), so a template carrying them
	// can only produce a corrupted origin request. Escaped forms in
	// the pattern (`\x0d`) stay legal — they simply never match.
	rejectControlBytes(ec, basePath+".match", pw.Match)
	rejectControlBytes(ec, basePath+".replace", pw.Replace)
	// The replace template is a literal, not a regex: bytes that cannot
	// appear in an origin-form request-target can only corrupt the
	// origin-bound request. A raw space breaks the request line
	// (fasthttp writes it verbatim); a raw '?' splices a second query
	// delimiter in front of the preserved original query; a raw '#' is
	// a fragment marker. In the match pattern these stay legal (space
	// and '?' are regex syntax there — a literal '?' in match simply
	// never matches, since the query is split off first).
	rejectNonTargetBytes(ec, basePath+".replace", pw.Replace)
	// Compile now (RE2 — linear time, no backtracking, so an
	// operator-supplied pattern cannot ReDoS the data plane). This is
	// a correctness gate, not a compile-cache: cache.NewPathRewrite
	// recompiles for the handler.
	re, err := regexp.Compile(pw.Match)
	if err != nil {
		ec.addf(basePath+".match", "is not a valid regular expression: %v", err)
		return
	}
	// Every $reference in the template must resolve to a group of the
	// pattern. Go's Expand silently expands an unknown reference to
	// the empty string — the classic `$1x` typo (reference to a group
	// named "1x") would corrupt every rewritten path with no error
	// anywhere. Reject it here, at config load.
	validateTemplateRefs(ec, basePath+".replace", re, pw.Replace)
}

// rejectControlBytes rejects raw C0 control bytes in a path_rewrite
// string. Escaped regex syntax is untouched: only literal bytes < 0x20
// (plus DEL) are checked, which is exactly the set a request path
// cannot carry.
func rejectControlBytes(ec *errCollector, path, s string) {
	for j := 0; j < len(s); j++ {
		if s[j] < 0x20 || s[j] == 0x7f {
			ec.addf(path, "contains a raw control byte (0x%02x) at offset %d — escape it or remove it", s[j], j)
			return
		}
	}
}

// rejectNonTargetBytes rejects raw bytes that cannot appear in an
// origin-form request-target and are therefore always a mistake in the
// literal replace template: space (breaks the origin request line),
// '?' (the engine re-appends the original query itself, so a template
// '?' splices a second delimiter into it), and '#' (fragment marker:
// fasthttp drops it from the parsed path while sending it raw).
func rejectNonTargetBytes(ec *errCollector, path, s string) {
	for j := 0; j < len(s); j++ {
		if s[j] == ' ' || s[j] == '?' || s[j] == '#' {
			ec.addf(path, "contains %q at offset %d — a request-target cannot carry it raw (write %%20 for a space; the query string is never modified by the template)", s[j], j)
			return
		}
	}
}

// validateTemplateRefs checks every $-reference in the replace template
// against the compiled pattern's capture groups. References:
//
//	$$       literal dollar
//	$1, $2…  index (0 = whole match)
//	$name    named group (?P<name>…)
//	${name}  braces disambiguate from following word bytes
//
// Go's Expand takes the longest word-run after $ as the group name, so
// `$1x` is a lookup of group "1x" — an unknown reference that expands
// to the empty string. Fail loudly at load instead.
func validateTemplateRefs(ec *errCollector, path string, re *regexp.Regexp, template string) {
	numGroups := re.NumSubexp()
	names := re.SubexpNames() // index 0 = "", names for (?P<name>…) groups
	for j := 0; j < len(template); j++ {
		if template[j] != '$' {
			continue
		}
		if j+1 < len(template) && template[j+1] == '$' {
			j++ // $$ literal dollar — skip both
			continue
		}
		// Collect the reference: optional '{' … '}', else the longest
		// word-run ([A-Za-z0-9_]).
		ref, next := "", j+1
		if next < len(template) && template[next] == '{' {
			end := strings.IndexByte(template[next:], '}')
			if end < 0 {
				ec.addf(path, "has unterminated '${' reference at offset %d", j)
				return
			}
			ref, next = template[next+1:next+end], next+end+1
		} else {
			for next < len(template) && isWordByte(template[next]) {
				next++
			}
			ref = template[j+1 : next]
		}
		if ref == "" {
			ec.addf(path, "has a lone '$' at offset %d — use $$ for a literal dollar", j)
			return
		}
		resolveTemplateRef(ec, path, ref, numGroups, names)
		j = next - 1
	}
}

// resolveTemplateRef checks one parsed reference against the pattern.
func resolveTemplateRef(ec *errCollector, path, ref string, numGroups int, names []string) {
	if allDigits(ref) {
		// Go's Expand disallows leading-zero indexes: extract() marks
		// them as names ("01" is a group name, not group 1), and an
		// unknown name expands to the empty string. Reject the padded
		// form rather than silently dropping text from every rewritten
		// path — the same failure class as an out-of-range index.
		if len(ref) > 1 && ref[0] == '0' {
			ec.addf(path, "references $%s — leading-zero group indexes are not valid (Go expands them to the empty string); write the index without padding", ref)
			return
		}
		n, err := strconv.Atoi(ref)
		if err != nil || n > numGroups {
			ec.addf(path, "references group $%s but the pattern has only %d capture group(s)", ref, numGroups)
		}
		return
	}
	for _, name := range names {
		if name == ref {
			return
		}
	}
	ec.addf(path, "references unknown group %q — the pattern defines no (?P<%s>...) group", ref, ref)
}

// isWordByte reports whether b is a byte that Go's Expand treats as
// part of a reference name ([A-Za-z0-9_]).
func isWordByte(b byte) bool {
	return b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9')
}

// allDigits reports whether s is non-empty and all decimal digits.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for j := 0; j < len(s); j++ {
		if s[j] < '0' || s[j] > '9' {
			return false
		}
	}
	return true
}

// validateStatic validates a StaticConfig block.
func validateStatic(ec *errCollector, path string, sc StaticConfig) {
	if !filepath.IsAbs(sc.Root) {
		ec.addf(path+".root", "must be an absolute path, got %q", sc.Root)
	}
	if sc.MaxFileSize < 0 {
		ec.addf(path+".max_file_size", "must be >= 0, got %s", sc.MaxFileSize)
	}
	for j, idx := range sc.Index {
		if strings.Contains(idx, "/") {
			ec.addf(fmt.Sprintf("%s.index[%d]", path, j), "must not contain '/', got %q", idx)
		}
	}
}

func validateRouteCache(ec *errCollector, path string, rc *RouteCache) {
	if rc.TTLOverride < 0 {
		ec.addf(path+".ttl_override", "must be >= 0, got %v", rc.TTLOverride)
	}
	if rc.TTLDefault < 0 {
		ec.addf(path+".ttl_default", "must be >= 0, got %v", rc.TTLDefault)
	}
	if rc.StaleWhileRevalidate < 0 {
		ec.addf(path+".stale_while_revalidate", "must be >= 0, got %v", rc.StaleWhileRevalidate)
	}
	if rc.StaleIfError < 0 {
		ec.addf(path+".stale_if_error", "must be >= 0, got %v", rc.StaleIfError)
	}
	validateStatusTTL(ec, path+".negative_ttl", &rc.NegativeTTL)
	if rc.JitterPercent < 0 || rc.JitterPercent > 50 {
		ec.addf(path+".jitter_percent", "must be 0–50, got %d", rc.JitterPercent)
	}
	if rc.MaxResponseBytes < 0 {
		ec.addf(path+".max_response_bytes", "must be >= 0, got %s", rc.MaxResponseBytes)
	}
	if rc.MaxStreamingBufferBytes < 0 {
		ec.addf(path+".max_streaming_buffer_bytes", "must be >= 0, got %s", rc.MaxStreamingBufferBytes)
	}
	if rc.MaxFetchConcurrency < 0 {
		ec.addf(path+".max_fetch_concurrency", "must be >= 0, got %d", rc.MaxFetchConcurrency)
	}
	if rc.FetchTimeout < 0 {
		ec.addf(path+".fetch_timeout", "must be >= 0, got %v", rc.FetchTimeout)
	}
	if rc.FetchTimeout >= maxFetchTimeout {
		ec.addf(path+".fetch_timeout", "must be < %v (data plane safety-net WriteTimeout), got %v", maxFetchTimeout, rc.FetchTimeout)
	}
	if rc.FetchWaitTimeout < 0 {
		ec.addf(path+".fetch_wait_timeout", "must be >= 0, got %v", rc.FetchWaitTimeout)
	}
	// An unbounded wait recreates the goroutine pileup this knob exists
	// to prevent (issue #562): every handler parks holding a connection.
	if rc.FetchWaitTimeout > maxFetchWaitTimeout {
		ec.addf(path+".fetch_wait_timeout", "must be <= %v, got %v", maxFetchWaitTimeout, rc.FetchWaitTimeout)
	}
	validateRouteKey(ec, path+".key", rc.Key)
	validateRefreshConfig(ec, path, *rc)
}

//nolint:gocyclo // 22: validation is a flat checklist of independent fields
func validateRefreshConfig(ec *errCollector, path string, rc RouteCache) {
	if rc.RefreshBeforeExpiry {
		if rc.TTLDefault <= 0 && rc.TTLOverride <= 0 {
			ec.addf(path+".refresh_before_expiry", "requires ttl_default or ttl_override > 0")
		}
	}
	if rc.RefreshMarginPercent < 0 || rc.RefreshMarginPercent > 50 {
		ec.addf(path+".refresh_margin_percent", "must be 0-50, got %d", rc.RefreshMarginPercent)
	}
	if rc.RefreshConcurrency < 0 || rc.RefreshConcurrency > 64 {
		ec.addf(path+".refresh_concurrency", "must be 0-64, got %d", rc.RefreshConcurrency)
	}
	if rc.RefreshTimeout < 0 || rc.RefreshTimeout > 120*time.Second {
		ec.addf(path+".refresh_timeout", "must be 0-120s, got %v", rc.RefreshTimeout)
	}
	if rc.RefreshMinHits < 0 {
		ec.addf(path+".refresh_min_hits", "must be >= 0, got %d", rc.RefreshMinHits)
	}
	if rc.RefreshPersistCycles < 0 {
		ec.addf(path+".refresh_persist_cycles", "must be >= 0, got %d", rc.RefreshPersistCycles)
	}
	if rc.RefreshPersistCycles > 0 && rc.RefreshMinHits <= 0 {
		ec.addf(path+".refresh_persist_cycles", "requires refresh_min_hits > 0")
	}
	if rc.RefreshMinScore < 0 {
		ec.addf(path+".refresh_min_score", "must be >= 0, got %d", rc.RefreshMinScore)
	}
	if rc.RefreshMinScore > 0 && rc.RefreshMinHits <= 0 {
		ec.addf(path+".refresh_min_score", "requires refresh_min_hits > 0")
	}
	if rc.RefreshMaxRPS < 0 || rc.RefreshMaxRPS > 10000 {
		ec.addf(path+".refresh_max_rps", "must be 0 or 1-10000, got %d", rc.RefreshMaxRPS)
	}
	if rc.RefreshReactiveFirst {
		if rc.StaleWhileRevalidate <= 0 {
			ec.addf(path+".refresh_reactive_first", "requires stale_while_revalidate > 0")
		}
		if rc.RefreshMinHits <= 0 {
			ec.addf(path+".refresh_reactive_first", "requires refresh_min_hits > 0")
		}
	}
}

// validateRouteKey validates cache key construction fields on a route.
func validateRouteKey(ec *errCollector, path string, rk RouteKey) {
	if len(rk.KeepQueryParams) > 0 {
		if len(rk.StripQueryParams) > 0 {
			ec.addf(path+".keep_query_params", "is mutually exclusive with strip_query_params")
		}
		if len(rk.StripQueryPrefix) > 0 {
			ec.addf(path+".keep_query_params", "is mutually exclusive with strip_query_prefix")
		}
	}
	if len(rk.StripQueryPrefix) > 16 {
		ec.addf(path+".strip_query_prefix", "capped at 16 entries, got %d", len(rk.StripQueryPrefix))
	}
	for j, p := range rk.StripQueryPrefix {
		if p == "" {
			ec.addf(fmt.Sprintf("%s.strip_query_prefix[%d]", path, j), "must be non-empty")
		}
	}
	validateIncludeHeaders(ec, path, rk)
}

// validateIncludeHeaders validates cache.key.include_headers: capped at
// 16 entries (mirrors strip_query_prefix), every entry must be a single
// RFC 9110 token (§5.6.2: tchar only, so one comma-free header name — "x,y"
// would be one union field to effectiveVary but two Vary fields to the
// variant-key builders, i.e. a knob whose meaning depends on the
// reader), no "*" (a wildcard Vary is unkeyable and would explode the
// variant space), no case-insensitive duplicates, and no overlap with
// exclude_headers — the overlap check is load-bearing, not cosmetic: an
// excluded header force-included into the key would silently collapse
// variants. Every comparison runs on the trimmed entry: NewKeyPolicy
// trims before storing, so " *", "x ", or " x,y" must be rejected here
// or validation and the stored policy disagree (issue #632 review).
func validateIncludeHeaders(ec *errCollector, path string, rk RouteKey) {
	if len(rk.IncludeHeaders) > 16 {
		ec.addf(path+".include_headers", "capped at 16 entries, got %d", len(rk.IncludeHeaders))
	}
	seen := make(map[string]bool, len(rk.IncludeHeaders))
	for j, raw := range rk.IncludeHeaders {
		h := strings.TrimSpace(raw)
		entryPath := fmt.Sprintf("%s.include_headers[%d]", path, j)
		if h == "" {
			ec.addf(entryPath, "must be a non-empty header name")
			continue
		}
		lower := strings.ToLower(h)
		if lower == "*" {
			ec.addf(entryPath, `must not be "*": a wildcard Vary is unkeyable (RFC 9111 §4.1)`)
			continue
		}
		if !isHTTPToken(lower) {
			ec.addf(entryPath, "(%q) must be a single RFC 9110 §5.1 header name: one comma-free token, no whitespace or separators", h)
			continue
		}
		if seen[lower] {
			ec.addf(entryPath, "(%s) is a duplicate (comparison is case-insensitive)", h)
			continue
		}
		seen[lower] = true
	}
	for j, raw := range rk.ExcludeHeaders {
		if seen[strings.ToLower(strings.TrimSpace(raw))] {
			ec.addf(fmt.Sprintf("%s.exclude_headers[%d]", path, j), "(%s) is also listed in include_headers: an excluded header must not participate in the key", raw)
		}
	}
}

// isHTTPToken reports whether s is a valid RFC 9110 §5.6.2 token:
// one or more tchar (visible ASCII excluding separators) — the shape
// of a single header field name.
func isHTTPToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isTchar(s[i]) {
			return false
		}
	}
	return true
}

// isTchar reports whether c is an RFC 9110 §5.6.2 tchar: ALPHA, DIGIT,
// or one of "!#$%&'*+-.^_`|~". Everything else (space, comma, colon,
// separators, non-ASCII) fails the token check.
func isTchar(c byte) bool {
	return tcharTable[c]
}

// tcharTable is the RFC 9110 §5.6.2 tchar bit set, indexed by byte.
var tcharTable = [256]bool{
	'0': true, '1': true, '2': true, '3': true, '4': true,
	'5': true, '6': true, '7': true, '8': true, '9': true,
	'A': true, 'B': true, 'C': true, 'D': true, 'E': true,
	'F': true, 'G': true, 'H': true, 'I': true, 'J': true,
	'K': true, 'L': true, 'M': true, 'N': true, 'O': true,
	'P': true, 'Q': true, 'R': true, 'S': true, 'T': true,
	'U': true, 'V': true, 'W': true, 'X': true, 'Y': true,
	'Z': true,
	'a': true, 'b': true, 'c': true, 'd': true, 'e': true,
	'f': true, 'g': true, 'h': true, 'i': true, 'j': true,
	'k': true, 'l': true, 'm': true, 'n': true, 'o': true,
	'p': true, 'q': true, 'r': true, 's': true, 't': true,
	'u': true, 'v': true, 'w': true, 'x': true, 'y': true,
	'z': true,
	'!': true, '#': true, '$': true, '%': true, '&': true,
	'\'': true, '*': true, '+': true, '-': true, '.': true,
	'^': true, '_': true, '`': true, '|': true, '~': true,
}

func validatePoolDurations(ec *errCollector, i int, p *UpstreamPool) {
	base := fmt.Sprintf("upstream_pools[%d]", i)
	if p.Health.Active.Interval < 0 {
		ec.addf(base+".health.active.interval", "pool %q: must be >= 0, got %v", p.Name, p.Health.Active.Interval)
	}
	if p.Health.Active.Timeout < 0 {
		ec.addf(base+".health.active.timeout", "pool %q: must be >= 0, got %v", p.Name, p.Health.Active.Timeout)
	}
	if p.Health.Passive.EjectFor < 0 {
		ec.addf(base+".health.passive.eject_for", "pool %q: must be >= 0, got %v", p.Name, p.Health.Passive.EjectFor)
	}
	if p.Connect.Timeout < 0 {
		ec.addf(base+".connect.timeout", "pool %q: must be >= 0, got %v", p.Name, p.Connect.Timeout)
	}
	if p.Connect.KeepAlive < 0 {
		ec.addf(base+".connect.keep_alive", "pool %q: must be >= 0, got %v", p.Name, p.Connect.KeepAlive)
	}
	if p.Connect.MaxIdleConnDuration < 0 {
		ec.addf(base+".connect.max_idle_conn_duration", "pool %q: must be >= 0, got %v", p.Name, p.Connect.MaxIdleConnDuration)
	}
	if p.Connect.ResponseHeaderTimeout < 0 {
		ec.addf(base+".connect.response_header_timeout", "pool %q: must be >= 0, got %v", p.Name, p.Connect.ResponseHeaderTimeout)
	}
	// This knob is the fallback origin-fetch bound for every route on the
	// pool that does not set its own cache.fetch_timeout. The same
	// safety-net ordering that applies to route fetch_timeout (the data
	// plane's 5-minute WriteTimeout must be able to outlive the fetch)
	// must hold here, or an inherited default aborts the client
	// connection before the origin wait gives up.
	if p.Connect.ResponseHeaderTimeout >= maxFetchTimeout {
		ec.addf(base+".connect.response_header_timeout", "pool %q: must be < %v (data plane safety-net WriteTimeout), got %v", p.Name, maxFetchTimeout, p.Connect.ResponseHeaderTimeout)
	}
	if p.Connect.MaxConnections < 0 {
		ec.addf(base+".connect.max_connections", "pool %q: must be >= 0, got %v", p.Name, p.Connect.MaxConnections)
	}
	if p.Connect.HedgeTimeout < 0 {
		ec.addf(base+".connect.hedge_timeout", "pool %q: must be >= 0, got %v", p.Name, p.Connect.HedgeTimeout)
	}
}

// validateCluster checks and normalises cluster configuration. The
// cluster is considered enabled when Listen.Cluster is non-empty.
func (c *Config) validateCluster(ec *errCollector) {
	if c.Listen.Cluster != "" {
		c.Cluster.Mode = ClusterMode(strings.TrimSpace(string(c.Cluster.Mode)))
		switch c.Cluster.Mode {
		case ClusterModeStrong, ClusterModeEventual:
			// valid
		case "":
			c.Cluster.Mode = ClusterModeStrong
		default:
			ec.addf("cluster.mode", "must be %q or %q, got %q",
				ClusterModeStrong, ClusterModeEventual, c.Cluster.Mode)
		}
	} else if c.Cluster.Mode != "" && c.Cluster.Mode != ClusterModeStrong {
		ec.addf("cluster.mode", "%q requires listen.cluster to be set", c.Cluster.Mode)
	}
	if c.Cluster.Mode == "" {
		c.Cluster.Mode = ClusterModeStrong
	}
	if c.Cluster.HandoffQueueDepth < 0 {
		ec.addf("cluster.handoff_queue_depth", "must be >= 0 (0 = default), got %d",
			c.Cluster.HandoffQueueDepth)
	}
	if c.Cluster.HandoffQueueDepth > maxHandoffQueueDepth {
		ec.addf("cluster.handoff_queue_depth", "must be <= %d, got %d (each slot costs a pointer + message header per peer)",
			maxHandoffQueueDepth, c.Cluster.HandoffQueueDepth)
	}
	c.validatePeerFetchConfig(ec)
	c.validateClusterStorage(ec)
	validateEvictionAlgorithm(ec, &c.Storage)
}

// validateClusterStorage checks the cluster storage sync settings.
// Extracted from validateCluster to keep cyclomatic complexity under
// the gocyclo limit.
func (c *Config) validateClusterStorage(ec *errCollector) {
	if c.Storage.WarmSyncInterval < -1 {
		ec.addf("storage.warm_sync_interval", "must be >= -1 (-1 = disabled), got %v", c.Storage.WarmSyncInterval)
	}
	if c.Storage.WarmSyncBatchSize < 0 {
		ec.addf("storage.warm_sync_batch_size", "must be >= 0, got %v", c.Storage.WarmSyncBatchSize)
	}
	if c.Storage.WALSyncInterval < -1 {
		ec.addf("storage.wal_sync_interval", "must be >= -1 (-1 = synchronous mode), got %v", c.Storage.WALSyncInterval)
	}
	if c.Storage.TombstoneQueueSize < 0 {
		ec.addf("storage.tombstone_queue_size", "must be >= 0 (0 = default 65536), got %v", c.Storage.TombstoneQueueSize)
	}
	if c.Storage.TombstoneDrainInterval < -1 {
		ec.addf("storage.tombstone_drain_interval", "must be >= -1 (-1 = disabled), got %v", c.Storage.TombstoneDrainInterval)
	}
}

// validatePeerFetchConfig checks the peer fetch pipelining settings
// (ADR-0039). Extracted from validateCluster to keep cyclomatic
// complexity under the gocyclo limit.
func (c *Config) validatePeerFetchConfig(ec *errCollector) {
	if c.Cluster.PeerMaxConnsPerHost < 0 {
		ec.addf("cluster.peer_max_conns_per_host", "must be >= 0 (0 = default 8), got %d",
			c.Cluster.PeerMaxConnsPerHost)
	}
	if c.Cluster.PeerMaxIdleConnDuration < 0 {
		ec.addf("cluster.peer_max_idle_conn_duration", "must be >= 0 (0 = default 120s), got %v",
			c.Cluster.PeerMaxIdleConnDuration)
	}
	if c.Cluster.PeerFetchConcurrency < 0 {
		ec.addf("cluster.peer_fetch_concurrency", "must be >= 0 (0 = default 4), got %d",
			c.Cluster.PeerFetchConcurrency)
	}
	if c.Cluster.PeerFetchConcurrency > MaxPeerFetchConcurrency {
		ec.addf("cluster.peer_fetch_concurrency", "must be <= %d, got %d",
			MaxPeerFetchConcurrency, c.Cluster.PeerFetchConcurrency)
	}
	if c.Cluster.BanTTL < 0 {
		ec.addf("cluster.ban_ttl", "must be >= 0 (0 = default 24h), got %v",
			c.Cluster.BanTTL)
	}
	if c.Cluster.BanTTL > 0 && c.Cluster.BanTTL < time.Second {
		ec.addf("cluster.ban_ttl", "must be >= 1s when set, got %v",
			c.Cluster.BanTTL)
	}
	if c.Admin.IdleTimeout < 0 {
		ec.addf("admin.idle_timeout", "must be >= 0 (0 = default 300s), got %v",
			c.Admin.IdleTimeout)
	}
	// The peer client must close idle connections before the admin
	// server reaps them; otherwise the first peer RPC on a
	// server-reaped connection fails with EOF or broken pipe and the
	// fetch falls back to origin. Only enforced when both values are
	// explicitly set: the built-in defaults (120s client / 300s server)
	// already satisfy the ordering.
	if id := c.Cluster.PeerMaxIdleConnDuration; id > 0 {
		adminIdle := c.Admin.IdleTimeout
		if adminIdle <= 0 {
			adminIdle = defaultAdminIdleTimeout
		}
		if id >= adminIdle {
			ec.addf("cluster.peer_max_idle_conn_duration",
				"(%v) must be below admin.idle_timeout (%v): the client must close idle peer connections before the admin server reaps them, or peer RPCs fail with EOF/broken pipe",
				id, adminIdle)
		}
	}
}

// validateEvictionAlgorithm checks the eviction policy selection for
// both tiers. The shared EvictionAlgorithm sets the default for both
// tiers; HotEvictionAlgorithm and WarmEvictionAlgorithm override it
// per-tier. All three accept the zero value, EvictionSieve, or
// EvictionCachaner.
//
// This function is a pure check — it does not mutate s.
func validateEvictionAlgorithm(ec *errCollector, s *Storage) {
	for _, algo := range []struct {
		path  string
		value EvictionAlgorithm
	}{
		{"storage.eviction_algorithm", s.EvictionAlgorithm},
		{"storage.hot_eviction_algorithm", s.HotEvictionAlgorithm},
		{"storage.warm_eviction_algorithm", s.WarmEvictionAlgorithm},
	} {
		switch algo.value {
		case "", EvictionSieve, EvictionCachaner:
			// valid
		default:
			ec.addf(algo.path, `must be %q or %q, got %q`, EvictionSieve, EvictionCachaner, algo.value)
		}
	}
}

// isKnownHTTPMethod returns true for standard HTTP methods accepted in
// route match.methods. Non-standard methods are rejected at parse time.
func isKnownHTTPMethod(m string) bool {
	switch m {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE":
		return true
	}
	return false
}

// ---- ByteSize YAML unmarshalling ----

// UnmarshalYAML implements yaml.Unmarshaler for ByteSize. Accepted
// forms: an integer (bytes) or a string suffixed with B/KB/KiB/MB/MiB/
// GB/GiB/TB/TiB/Ko/Mo/Go/To (case-insensitive).
func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		raw := strings.TrimSpace(value.Value)
		if raw == "" {
			*b = 0
			return nil
		}
		n, err := parseByteSize(raw)
		if err != nil {
			return fmt.Errorf("config: invalid byte size %q: %w", raw, err)
		}
		*b = ByteSize(n)
		return nil
	default:
		return fmt.Errorf("config: byte size must be scalar, got kind %d", value.Kind)
	}
}

// Bytes returns the value as a plain int64.
func (b ByteSize) Bytes() int64 { return int64(b) }

// IsZero reports whether the ByteSize is zero so yaml.v3 omitempty works.
func (b ByteSize) IsZero() bool { return int64(b) == 0 }

// MarshalYAML emits ByteSize as its human-readable string form (e.g. "2Go")
// so the YAML representation matches what operators write in config files.
func (b ByteSize) MarshalYAML() (interface{}, error) {
	if b == 0 {
		return 0, nil
	}
	return b.String(), nil
}

// MarshalJSON emits ByteSize as its human-readable string form so the
// JSON representation in the dashboard matches the YAML representation.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	if b == 0 {
		return []byte("0"), nil
	}
	return json.Marshal(b.String())
}

// String returns a human-readable representation of the byte size.
func (b ByteSize) String() string {
	v := int64(b)
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.0fGo", float64(v)/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.0fMo", float64(v)/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.0fKo", float64(v)/(1<<10))
	default:
		return fmt.Sprintf("%dB", v)
	}
}

// byteSizeUnits maps the uppercased unit suffix to its multiplier.
// Unknown units are rejected by parseByteSize.
var byteSizeUnits = map[string]float64{
	"":    1,
	"B":   1,
	"K":   1e3,
	"KB":  1e3,
	"KI":  1 << 10,
	"KIB": 1 << 10,
	"KO":  1e3,
	"M":   1e6,
	"MB":  1e6,
	"MI":  1 << 20,
	"MIB": 1 << 20,
	"MO":  1e6,
	"G":   1e9,
	"GB":  1e9,
	"GI":  1 << 30,
	"GIB": 1 << 30,
	"GO":  1e9,
	"T":   1e12,
	"TB":  1e12,
	"TI":  1 << 40,
	"TIB": 1 << 40,
	"TO":  1e12,
}

func parseByteSize(s string) (int64, error) {
	// Numeric-only — treat as bytes.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}

	i := 0
	for i < len(s) && (s[i] == '-' || s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	num := strings.TrimSpace(s[:i])
	unit := strings.ToUpper(strings.TrimSpace(s[i:]))
	if num == "" {
		return 0, fmt.Errorf("missing number")
	}
	val, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", num, err)
	}
	mult, ok := byteSizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	return int64(val * mult), nil
}

// validateStatusTTL resolves the route's negative-caching policy via
// api.NewStatusTTLMap — the single parser and overlap checker — and
// stores the built policy on the config, consuming the raw map the
// decoder left behind. A nil raw map means the policy is already
// resolved (programmatic construction) or absent (no negative caching);
// rebuilding would clobber it. After Validate, the policy is the only
// stored form: consumers (cache handler, builder, dashboard) read it
// via Policy() and its enumeration methods; the raw map never escapes
// the config layer.
func validateStatusTTL(ec *errCollector, path string, n *NegativeTTLConfig) {
	if n.raw == nil {
		return
	}
	p, err := api.NewStatusTTLMap(n.raw)
	if err != nil {
		ec.addf(path, "%v", err)
		return
	}
	n.policy = p
	n.raw = nil
}

// ---- negative_ttl YAML unmarshalling ----

// NegativeTTLConfig is the decoded form of the negative_ttl config
// key, which accepts two shapes under one name so operators keep a
// single mental slot for negative caching:
//
//	negative_ttl: 30s                           # default-set shorthand
//	negative_ttl: {404: 1m, 5xx: 10s, 410: 0} # per-status map
//
// Both forms normalize to one map at decode time: the scalar expands
// to the default set (404/405/410/501) via api.DefaultNegTTLMap, the
// map is the complete policy (no implicit fallback statuses). Validate
// then builds the *api.StatusTTLPolicy from that map and drops the
// map: the policy is the single stored representation, and its
// enumeration methods (Entries, CoversAnything) are how consumers see
// the policy — no second shape to drift against. Key format, bounds,
// and overlaps are validated by Validate via api.NewStatusTTLMap;
// only the shape is decided here.
type NegativeTTLConfig struct {
	// raw holds the decoded map between UnmarshalYAML and Validate;
	// nil afterwards. It never leaves the config layer.
	raw map[string]time.Duration
	// policy is the resolved *api.StatusTTLPolicy, built once by
	// Validate. Nil when the route has no negative caching.
	policy *api.StatusTTLPolicy
}

// Policy returns the resolved negative-caching policy, built once by
// Validate via api.NewStatusTTLMap. Nil when the route has no
// negative caching. Consumers must not construct the policy
// themselves: a config that passed Validate already has it.
func (n NegativeTTLConfig) Policy() *api.StatusTTLPolicy {
	return n.policy
}

// IsZero reports whether any negative-caching policy is configured, so
// yaml.v3 omitempty drops the key when empty. A policy resolved by
// Validate also counts as configured.
func (n NegativeTTLConfig) IsZero() bool {
	return len(n.raw) == 0 && n.policy == nil
}

// durationValue decodes one TTL scalar. yaml.v3's native
// time.Duration decoding rejects bare numbers like `0` or `30` (a
// plain int, not "30s"), so entries decode through the same
// scalar-or-parse path the ByteSize type uses: numbers are treated as
// seconds, strings as duration literals ("30s", "1m"). Both
// negative_ttl forms share it, so `negative_ttl: 30` and
// `negative_ttl: {404: 30}` mean the same 30 seconds.
type durationValue time.Duration

func (d *durationValue) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("invalid duration %q: must be a scalar", value.Value)
	}
	var dur time.Duration
	if err := value.Decode(&dur); err != nil {
		// Not a duration literal; a bare number means seconds
		// (mirrors "0" disabling a status in the map example).
		var secs float64
		if ferr := value.Decode(&secs); ferr != nil || secs < 0 {
			return fmt.Errorf("invalid duration %q", value.Value)
		}
		dur = time.Duration(secs * float64(time.Second))
	}
	*d = durationValue(dur)
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler for the negative_ttl key:
// a duration scalar (shorthand for the default error statuses), or a
// map of status codes/classes to durations (complete policy).
func (n *NegativeTTLConfig) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		var d durationValue
		if err := value.Decode(&d); err != nil {
			return fmt.Errorf("config: negative_ttl must be a duration or a status map: %w", err)
		}
		n.raw = api.DefaultNegTTLMap(time.Duration(d))
		return nil
	case yaml.MappingNode:
		var raw map[string]durationValue
		if err := value.Decode(&raw); err != nil {
			return fmt.Errorf("config: negative_ttl must be a duration or a status map: %w", err)
		}
		n.raw = make(map[string]time.Duration, len(raw))
		for k, v := range raw {
			n.raw[k] = time.Duration(v)
		}
		return nil
	default:
		return fmt.Errorf("config: negative_ttl must be a duration or a status map, got YAML kind %d", value.Kind)
	}
}

// NegTTLScalar builds the scalar shorthand's policy programmatically
// (tests, SDK): d applies to the default error set. Non-positive d
// yields no negative caching. Delegates to NegTTLMap over the default
// expansion — one construction path for both forms, so they can never
// disagree about what a scalar expands to.
func NegTTLScalar(d time.Duration) NegativeTTLConfig {
	// An error here is impossible: DefaultNegTTLMap emits only keys
	// this parser accepts (exact codes of the default set, d > 0).
	// Swallowing rather than panicking keeps the constructor total,
	// matching the config-file path where d <= 0 means "no policy".
	neg, _ := NegTTLMap(api.DefaultNegTTLMap(d))
	return neg
}

// NegTTLMap builds a per-status policy programmatically. Keys use
// the same "404" / "5xx" format as YAML. An invalid map is rejected
// here, not deferred to Validate — an unrepresentable policy is a
// programming error, and returning an error keeps the caller from
// storing a config that could never load.
func NegTTLMap(m map[string]time.Duration) (NegativeTTLConfig, error) {
	p, err := api.NewStatusTTLMap(m)
	if err != nil {
		return NegativeTTLConfig{}, err
	}
	return NegativeTTLConfig{policy: p}, nil
}

// MarshalYAML emits the map form, rebuilt from the policy's entries:
// it round-trips through UnmarshalYAML with the same semantics (the
// scalar shorthand is canonical only at input).
func (n NegativeTTLConfig) MarshalYAML() (any, error) {
	m := make(map[string]time.Duration, len(n.policy.Entries()))
	for _, e := range n.policy.Entries() {
		m[e.Key] = e.TTL
	}
	return m, nil
}

// MarshalJSON emits the map form for the dashboard's JSON surfaces,
// rebuilt from the policy's entries. json.Marshal renders an empty
// map as {} and omits it via omitempty; the policy — not a stored
// raw map — is the single representation.
func (n NegativeTTLConfig) MarshalJSON() ([]byte, error) {
	m := make(map[string]time.Duration, len(n.policy.Entries()))
	for _, e := range n.policy.Entries() {
		m[e.Key] = e.TTL
	}
	return json.Marshal(m)
}
