package storage

import (
	"strings"
	"sync"
	"sync/atomic"
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
// after the exemption time are not subject — RFC 9111 §4.4 invalidation
// only removes responses that existed at invalidation time.
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

// banFlushDelay bounds how long a registration may stay unpublished.
// A rebuild composes fresh maps sized by the list (O(list) transient
// garbage), so compiling per registration at storm rates (~600 bans/s
// against a list at banListCap) generates hundreds of MB/s of garbage.
// Registrations arriving more than banFlushDelay apart flush
// synchronously (the common single-ban case: enforced when Ban
// returns, no window); a denser burst coalesces into one rebuild per
// window on the deferred timer. Same tradeoff family as the eager
// scan's banScanCoalesceWindow, which see. Worst case, a ban is
// enforced by the lazy lookup path up to banFlushDelay after Ban
// returns — invalidation is lazy by design (RFC 9111 §4.4), and where
// one runs, the eager scan reclaims matching hot-tier entries
// immediately.
const banFlushDelay = 50 * time.Millisecond

// banListState couples the authoritative ordered ban list with its
// compiled snapshot. The snapshot is published copy-on-write (RCU):
// lookups load it with a plain atomic load and never take mu, never
// compile. Mutations mark the state dirty; the publication is either
// synchronous (sparse registrations) or deferred to a coalescing
// timer (storms), so a batch of N registrations costs one O(list)
// compile, not N — and, decisively, that compile never runs on the
// hit path. This is what removes the whole-plane stall under ban
// storms: previously every dirty lookup rebuilt the snapshot while
// holding mu, serializing all hits and peer-put receivers behind one
// compile.
type banListState struct {
	// snap is the last published immutable snapshot. Readers load it
	// without locking (see snapshot).
	snap atomic.Pointer[banSnapshot]
	list []activeBan
	// ttl is how long a ban stays in the list before prune paths drop
	// it. Zero applies defaultBanTTL. Configured per store via
	// HotConfig.BanTTL (invalidation.ban_ttl) so operators can bound the
	// blast radius of an over-broad ban.
	ttl time.Duration
	// dirty records that list changed since snap was compiled.
	dirty bool
	// flushing records a pending deferred flush; one timer is armed at
	// a time (register's CAS on it).
	flushing atomic.Bool
	// flushDelay overrides banFlushDelay in tests (a huge value pins
	// deferral deterministically; zero means the default).
	flushDelay time.Duration
	mu         sync.Mutex

	// registered/rebuilds/lastRebuildNanos/lastPublishNano feed the
	// bouine_ban_* metrics via HotStore.Stats (lastPublishNano also
	// separates isolated registrations from bursts in register).
	// Atomic so register and the flush timer stay off mu's critical
	// section for reporting.
	registered       atomic.Int64
	rebuilds         atomic.Int64
	lastRebuildNanos atomic.Int64
	lastPublishNano  atomic.Int64
}

func (b *banListState) ttlOrDefault() time.Duration {
	if b.ttl <= 0 {
		return defaultBanTTL
	}
	return b.ttl
}

func (b *banListState) flushDelayOrDefault() time.Duration {
	if b.flushDelay <= 0 {
		return banFlushDelay
	}
	return b.flushDelay
}

// register appends (or refreshes) a ban and marks the snapshot dirty
// without compiling it. Publication follows immediately when the last
// rebuild is older than flushDelayOrDefault (isolated bans: enforced
// when Ban returns, no visibility window); during bursts it is
// deferred to the coalescing timer so a storm costs one O(list)
// compile per window, never per ban and never per lookup.
//
// The list is mutated in place with zero allocations: expired bans are
// dropped and a matching pattern refreshed during one scan, a new
// pattern appends, and a full list evicts the oldest entry by shifting
// in place.
func (b *banListState) register(expr api.BanExpr, pred banPredicate, createdAt time.Time) {
	b.mu.Lock()
	pat := patternOf(expr)
	// exemptAfter mirrors the predicate's own exemption semantics: the
	// ban's ORIGINAL CreatedAt (possibly zero = no exemption), not the
	// normalized registration time used for TTL accounting.
	exemptAfter := expr.CreatedAt
	now := time.Now()
	pruned := b.list[:0]
	refreshed := false
	for _, ban := range b.list {
		if now.Sub(ban.created) >= b.ttlOrDefault() {
			continue
		}
		if ban.pattern == pat {
			ban.created = createdAt
			ban.exemptAfter = exemptAfter
			refreshed = true
		}
		pruned = append(pruned, ban)
	}
	b.list = pruned
	if refreshed {
		b.dirty = true
	} else {
		ban := activeBan{pred: pred, created: createdAt, exemptAfter: exemptAfter, pattern: pat}
		if len(b.list) >= banListCap {
			copy(b.list, b.list[1:])
			b.list[len(b.list)-1] = ban
		} else {
			b.list = append(b.list, ban)
		}
		b.dirty = true
	}
	b.mu.Unlock()

	b.registered.Add(1)
	if time.Since(b.lastPublished()) >= b.flushDelayOrDefault() {
		b.flushSnapshot()
		return
	}
	b.scheduleFlush()
}

// scheduleFlush arms the deferred rebuild unless one is already
// pending, so a storm coalesces into a single timer.
func (b *banListState) scheduleFlush() {
	if b.flushing.CompareAndSwap(false, true) {
		time.AfterFunc(b.flushDelayOrDefault(), b.flushSnapshot)
	}
}

// flushSnapshot publishes a fresh snapshot if the list changed since
// the last publication. Runs on the deferred timer and synchronously
// from register; safe concurrently with any register (a racing timer
// either observes the registration's dirty flag or the registrant
// flushes it itself — see register).
func (b *banListState) flushSnapshot() {
	b.flushing.Store(false)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dirty {
		b.rebuildLocked(time.Now())
	}
}

// lastPublished reports when the snapshot was last compiled, used to
// separate isolated registrations (flush now) from bursts (defer).
func (b *banListState) lastPublished() time.Time {
	if b.rebuilds.Load() == 0 {
		return time.Time{}
	}
	return time.Unix(0, b.lastPublishNano.Load())
}

// pruneExpired rebuilds the state as of now, dropping bans older than
// banTTL. Called by the TTL reaper each tick so a quiet day after a
// storm does not keep taxing hits for the full 24 h window. Returns
// whether any ban was dropped.
func (b *banListState) pruneExpired(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	before := len(b.list)
	b.rebuildLocked(now)
	return len(b.list) != before
}

// snapshot returns the last published compiled view. Lock-free by
// design: the hit path never takes mu and never compiles — a slow or
// storm-driven rebuild cannot stall lookups (the RCU publication). A
// registration becomes visible when its flush publishes: immediately
// for sparse registrations, within flushDelayOrDefault during bursts.
func (b *banListState) snapshot() *banSnapshot {
	return b.snap.Load()
}

// rebuildLocked prunes bans older than the configured TTL and
// recompiles the snapshot from the surviving list, publishing it with
// one atomic store and clearing the dirty flag. The filter reuses the
// list's backing array. The mutex must be held.
func (b *banListState) rebuildLocked(now time.Time) {
	start := time.Now()
	pruned := b.list[:0]
	for _, ban := range b.list {
		if now.Sub(ban.created) >= b.ttlOrDefault() {
			continue
		}
		pruned = append(pruned, ban)
	}
	b.list = pruned
	b.snap.Store(compileBanSnapshot(b.list))
	b.dirty = false
	b.rebuilds.Add(1)
	b.lastRebuildNanos.Store(time.Since(start).Nanoseconds())
	b.lastPublishNano.Store(start.UnixNano())
}

// len reports the active ban count (tests and metrics).
func (b *banListState) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.list)
}
