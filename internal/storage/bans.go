package storage

import (
	"strings"
	"sync"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// banSnapshot is a compiled, O(1)-rejectable view of the active ban
// list. Registration keeps the ordered activeBan slice (source of
// truth, used for eager scans); matchesActiveBan consults this
// snapshot instead of walking the slice.
//
// Soundness argument (why rejection is safe): every ban requires at
// least one non-empty condition — a host pattern, a path pattern, or a
// surrogate key (admin validation rejects all-empty expressions). A
// literal ban with host pattern H can only match an object whose
// X-Bouine-Host equals H; an anchored-prefix ban ^P can only match a
// host/path with that prefix. So when an object's host, path, and
// surrogate keys appear in none of the sets/tries, no literal or
// prefix ban can match it and only the opaqueBans need evaluation.
//
// The check is allocation-free: header values and surrogate keys are
// read once and compared against immutable set members.
type banSnapshot struct {
	// hosts/paths/surrogates map each literal condition to the ban's
	// exemption time (expr.CreatedAt as compiled into the predicate;
	// zero means the ban has no exemption — always subject). An object
	// matches the ban iff its value equals the literal AND it is
	// subject (StoredAt not after the exemption time).
	hosts      map[string]time.Time
	paths      map[string]time.Time
	surrogates map[string]time.Time
	// hostPrefixes and pathPrefixes hold anchored prefix patterns
	// ("^/foo/", "^api\\."). A subject matches only if its host/path
	// starts with the literal prefix; non-matching subjects can be
	// rejected without regexp evaluation.
	hostPrefixes []prefixBan
	pathPrefixes []prefixBan
	// opaqueBans are the bans that cannot be rejected cheaply: regex
	// patterns that are not pure anchored literals, or bans carrying
	// multiple conditions (host AND path AND surrogate) where each
	// individual condition must be checked in combination. They are
	// evaluated in registration (oldest-first) order via the stored
	// predicate, after the cheap sets fail to reject.
	opaqueBans []activeBan
}

// prefixBan is an anchored-prefix pattern compiled to its literal
// prefix, so membership testing is strings.HasPrefix instead of
// regexp evaluation.
type prefixBan struct {
	exemptAfter time.Time
	pred        banPredicate
	prefix      string
}

// compileBanSnapshot derives the O(1) view from an ordered ban list.
// bans must be pruned of expired entries first.
func compileBanSnapshot(bans []activeBan) *banSnapshot {
	snap := &banSnapshot{
		hosts:      make(map[string]time.Time, len(bans)),
		paths:      make(map[string]time.Time, len(bans)),
		surrogates: make(map[string]time.Time, len(bans)),
	}
	for _, b := range bans {
		switch classifyBan(b.pattern) {
		case banClassLiteralHost:
			snap.hosts[literalHostPattern(b.pattern.hostRegex)] = b.exemptAfter
		case banClassLiteralPath:
			snap.paths[literalPathPattern(b.pattern.pathRegex)] = b.exemptAfter
		case banClassLiteralSurrogate:
			snap.surrogates[b.pattern.surrogateKey] = b.exemptAfter
		case banClassPrefixHost:
			snap.hostPrefixes = append(snap.hostPrefixes, prefixBan{
				prefix:      anchoredPrefix(b.pattern.hostRegex),
				pred:        b.pred,
				exemptAfter: b.exemptAfter,
			})
		case banClassPrefixPath:
			snap.pathPrefixes = append(snap.pathPrefixes, prefixBan{
				prefix:      anchoredPrefix(b.pattern.pathRegex),
				pred:        b.pred,
				exemptAfter: b.exemptAfter,
			})
		default:
			snap.opaqueBans = append(snap.opaqueBans, b)
		}
	}
	return snap
}

// literalHostPattern normalizes a literal-classified host pattern to
// the exact string it matches: plain literals map to themselves,
// anchored-exact patterns map to their unescaped body.
func literalHostPattern(pattern string) string {
	if isAnchoredExact(pattern) {
		return anchoredExact(pattern)
	}
	return pattern
}

// literalPathPattern mirrors literalHostPattern for paths.
func literalPathPattern(pattern string) string {
	if isAnchoredExact(pattern) {
		return anchoredExact(pattern)
	}
	return pattern
}

// matches reports whether obj is subject to any ban in the snapshot.
// The cheap checks run first: literal set membership for host, path,
// and surrogate keys, then anchored-prefix HasPrefix scans. Only when
// every cheap check fails to reject does it fall through to the
// opaqueBans predicate walk.
func (s *banSnapshot) matches(obj *api.Object) bool {
	if s == nil {
		return false
	}
	if s.literalMatch(obj) {
		return true
	}
	if s.prefixMatch(obj) {
		return true
	}
	// Opaque bans: full predicate walk. The StoredAt/CreatedAt
	// exemption check inside the predicate already handles staleness.
	for _, b := range s.opaqueBans {
		if b.pred(obj) {
			return true
		}
	}
	return false
}

// literalMatch evaluates the literal set checks: host, path, and
// surrogate-key membership with per-ban exemption times. These are the
// O(1) rejections; a miss here rules out every literal-classified ban.
func (s *banSnapshot) literalMatch(obj *api.Object) bool {
	if len(s.hosts) > 0 {
		if exempt, hit := s.hosts[obj.Header.Get(header.XBouineHost)]; hit && subjectTo(obj, exempt) {
			return true
		}
	}
	if len(s.paths) > 0 {
		if exempt, hit := s.paths[obj.Header.Get(header.XBouinePath)]; hit && subjectTo(obj, exempt) {
			return true
		}
	}
	return len(s.surrogates) > 0 && surrogateListed(obj.SurrogateKeys, s.surrogates, obj)
}

// prefixMatch evaluates the anchored-prefix bans: HasPrefix is a
// sound pre-filter, and on a hit the full predicate applies (the
// pattern may carry multiple conditions).
func (s *banSnapshot) prefixMatch(obj *api.Object) bool {
	if len(s.hostPrefixes) > 0 {
		host := obj.Header.Get(header.XBouineHost)
		for _, pb := range s.hostPrefixes {
			if strings.HasPrefix(host, pb.prefix) && subjectTo(obj, pb.exemptAfter) && pb.pred(obj) {
				return true
			}
		}
	}
	if len(s.pathPrefixes) > 0 {
		path := obj.Header.Get(header.XBouinePath)
		for _, pb := range s.pathPrefixes {
			if strings.HasPrefix(path, pb.prefix) && subjectTo(obj, pb.exemptAfter) && pb.pred(obj) {
				return true
			}
		}
	}
	return false
}

// surrogateListed reports whether any of keys is in the banned map,
// honoring the per-ban exemption time.
func surrogateListed(keys []string, banned map[string]time.Time, obj *api.Object) bool {
	for _, k := range keys {
		if exempt, hit := banned[k]; hit && subjectTo(obj, exempt) {
			return true
		}
	}
	return false
}

// subjectTo reports whether obj is subject to a ban whose exemption
// time is exempt (the ban's original expr.CreatedAt; zero means the
// ban has no exemption and everything is subject). Objects stored
// after the exemption time are not subject (RFC 9111 §4.4).
func subjectTo(obj *api.Object, exempt time.Time) bool {
	return exempt.IsZero() || !obj.StoredAt.After(exempt)
}

// banClass classifies how a ban's pattern conditions can be checked.
type banClass int

const (
	// banClassOpaque means the ban needs full predicate evaluation.
	banClassOpaque banClass = iota
	// banClassLiteralHost: single literal host pattern, no other
	// conditions — membership via hosts set.
	banClassLiteralHost
	// banClassLiteralPath: single literal path pattern, no other
	// conditions — membership via paths set.
	banClassLiteralPath
	// banClassLiteralSurrogate: single surrogate key, no patterns —
	// membership via surrogates set.
	banClassLiteralSurrogate
	// banClassPrefixHost: single anchored-prefix host pattern.
	banClassPrefixHost
	// banClassPrefixPath: single anchored-prefix path pattern.
	banClassPrefixPath
)

// classifyBan inspects the compiled ban's pattern fields. A ban with
// exactly one non-empty condition is classified by that condition;
// multi-condition bans are opaque (each set check alone cannot decide
// them, and misclassifying them would wrongly reject... rather, would
// wrongly MATCH objects whose other conditions fail — a soundness
// break — so they take the predicate path).
func classifyBan(e banPattern) banClass {
	n := 0
	var class banClass
	if e.hostRegex != "" {
		n++
		class = banClassLiteralHost
	}
	if e.pathRegex != "" {
		n++
		class = banClassLiteralPath
	}
	if e.surrogateKey != "" {
		n++
		class = banClassLiteralSurrogate
	}
	if n != 1 {
		return banClassOpaque
	}
	switch class {
	case banClassLiteralHost:
		if isBanLiteral(e.hostRegex) || isAnchoredExact(e.hostRegex) {
			return banClassLiteralHost
		}
		if isAnchoredPrefix(e.hostRegex) {
			return banClassPrefixHost
		}
	case banClassLiteralPath:
		if isBanLiteral(e.pathRegex) || isAnchoredExact(e.pathRegex) {
			return banClassLiteralPath
		}
		if isAnchoredPrefix(e.pathRegex) {
			return banClassPrefixPath
		}
	}
	return banClassOpaque
}

// isAnchoredPrefix reports whether pattern is a regexp of the form
// ^literal where literal consists only of literal characters and
// escaped characters (\. is the common case in hostnames). Such a
// pattern matches exactly the strings with the given literal prefix,
// so strings.HasPrefix is a sound equivalent for rejection (and a
// sound pre-filter for matching).
func isAnchoredPrefix(pattern string) bool {
	if len(pattern) < 2 || pattern[0] != '^' || pattern[len(pattern)-1] == '$' {
		return false
	}
	_, ok := literalOfBody(pattern[1:])
	return ok
}

// anchoredPrefix strips the ^ anchor and unescapes the body. Callers
// must have verified isAnchoredPrefix.
func anchoredPrefix(pattern string) string {
	lit, _ := literalOfBody(pattern[1:])
	return lit
}

// isAnchoredExact reports whether pattern is ^literal$ where the body
// consists only of literal and escaped characters. Such a pattern
// matches exactly one string, so map equality is a sound equivalent.
func isAnchoredExact(pattern string) bool {
	if len(pattern) < 3 || pattern[0] != '^' || pattern[len(pattern)-1] != '$' {
		return false
	}
	_, ok := literalOfBody(pattern[1 : len(pattern)-1])
	return ok
}

// anchoredExact strips ^/$ and unescapes the body. Callers must have
// verified isAnchoredExact.
func anchoredExact(pattern string) string {
	lit, _ := literalOfBody(pattern[1 : len(pattern)-1])
	return lit
}

// regexpMetachars lists the characters that carry special meaning when
// unescaped in a Go regexp.
const regexpMetachars = `\.+*?()|[]{}^$`

// literalOfBody scans a pattern body (anchors stripped) and returns
// its literal interpretation: every unescaped ordinary character and
// every escaped character (\\. is the hostname case) contributes its
// literal byte. It reports ok=false when the body contains an
// unescaped metacharacter — the pattern then has real regex operators
// and cannot be represented as a literal or prefix.
func literalOfBody(body string) (string, bool) {
	var b strings.Builder
	b.Grow(len(body))
	for i := 0; i < len(body); i++ {
		c := body[i]
		if c == '\\' {
			if i+1 >= len(body) {
				// Trailing lone backslash: not a valid literal shape.
				return "", false
			}
			b.WriteByte(body[i+1])
			i++
			continue
		}
		if strings.IndexByte(regexpMetachars, c) >= 0 {
			return "", false
		}
		b.WriteByte(c)
	}
	return b.String(), true
}

// banListState couples the authoritative ordered ban list with its
// compiled snapshot. All mutations go through rebuild().
type banListState struct {
	snap *banSnapshot
	list []activeBan
	mu   sync.Mutex
}

// register appends (or refreshes) a ban and rebuilds the snapshot.
// The rebuild is O(len(list)) which is bounded by banListCap; under a
// storm of distinct bans this is amortized by the coalesced eager
// scan — one rebuild per registration batch is the steady state.
func (b *banListState) register(expr api.BanExpr, pred banPredicate, createdAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pat := patternOf(expr)
	// exemptAfter mirrors the predicate's own exemption semantics: the
	// ban's ORIGINAL CreatedAt (possibly zero = no exemption), not the
	// normalized registration time used for TTL accounting.
	exemptAfter := expr.CreatedAt
	refreshed := false
	list := make([]activeBan, 0, min(len(b.list)+1, banListCap))
	now := time.Now()
	for _, ban := range b.list {
		if now.Sub(ban.created) >= banTTL {
			continue
		}
		if ban.pattern == pat {
			ban.created = createdAt
			ban.exemptAfter = exemptAfter
			refreshed = true
		}
		list = append(list, ban)
	}
	if !refreshed {
		if len(list) >= banListCap {
			copy(list, list[1:])
			list = list[:len(list)-1]
		}
		list = append(list, activeBan{pred: pred, created: createdAt, exemptAfter: exemptAfter, pattern: pat})
	}
	b.list = list
	b.snap = compileBanSnapshot(list)
}

// pruneExpired drops bans older than banTTL and rebuilds when anything
// was dropped. Called by the TTL reaper each tick so a quiet day after
// a storm does not keep taxing hits for the full 24 h window.
func (b *banListState) pruneExpired(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.list[:0:0]
	dropped := false
	for _, ban := range b.list {
		if now.Sub(ban.created) >= banTTL {
			dropped = true
			continue
		}
		kept = append(kept, ban)
	}
	if dropped {
		b.list = kept
		b.snap = compileBanSnapshot(kept)
	}
	return dropped
}

// snapshot returns the current compiled view for lock-free reads.
// The returned pointer is immutable after publication.
func (b *banListState) snapshot() *banSnapshot {
	b.mu.Lock()
	snap := b.snap
	b.mu.Unlock()
	return snap
}

// len reports the active ban count (tests and metrics).
func (b *banListState) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.list)
}
