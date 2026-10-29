package storage

import (
	"hash/maphash"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// banSnapshot is a compiled, O(1)-rejectable view of the active
// non-surrogate bans (host, path, prefix, regex, multi-condition).
// Surrogate-key-only bans — the high-rate tag-invalidation shape — are
// not compiled here: they live in the incrementally maintained
// surrogateSet, so a storm of tag bans never recompiles this view.
// Registration keeps the ordered list (source of truth, used for
// eager scans); matchesActiveBan consults this snapshot and the
// surrogate set instead of walking the list.
//
// Soundness argument (why rejection is safe): every ban requires at
// least one non-empty condition — a host pattern, a path pattern, or a
// surrogate key (admin validation rejects all-empty expressions). A
// literal ban with host pattern H can only match an object whose
// X-Bouine-Host equals H; an anchored-prefix ban ^P can only match a
// host/path with that prefix. So when an object's host and path
// appear in none of the sets/tries, no literal or prefix ban can match
// it and only the opaqueBans need evaluation.
//
// The check is allocation-free: header values are read once and
// compared against immutable set members.
type banSnapshot struct {
	// hosts/paths map each literal condition to the ban's exemption
	// time (the normalized registration instant from registerBan: the
	// original CreatedAt when set, otherwise the moment the ban was
	// registered). An object matches the ban iff its value equals the
	// literal AND it is subject (StoredAt not after the exemption
	// time).
	hosts map[string]time.Time
	paths map[string]time.Time
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
	opaqueBans []opaqueBan
}

// prefixBan is an anchored-prefix pattern compiled to its literal
// prefix, so membership testing is strings.HasPrefix instead of
// regexp evaluation.
type prefixBan struct {
	exemptAfter time.Time
	pred        banPredicate
	prefix      string
}

// opaqueBan is a ban evaluated by full predicate, with its exemption
// instant captured at compile time (a refresh marks the snapshot dirty
// and recompiles it).
type opaqueBan struct {
	exemptAfter time.Time
	pred        banPredicate
}

// compileBanSnapshot derives the O(1) view from the non-surrogate
// bans, skipping any already past ttl at now (they are pruned from the
// list by the next sweep; they must not keep matching meanwhile).
func compileBanSnapshot(bans []*activeBan, now time.Time, ttl time.Duration) *banSnapshot {
	snap := &banSnapshot{}
	for _, b := range bans {
		if now.Sub(b.created) >= ttl {
			continue
		}
		exempt := b.exemptAfter()
		switch classifyBan(b.pattern) {
		case banClassLiteralHost:
			if snap.hosts == nil {
				snap.hosts = make(map[string]time.Time)
			}
			snap.hosts[literalHostPattern(b.pattern.hostRegex)] = exempt
		case banClassLiteralPath:
			if snap.paths == nil {
				snap.paths = make(map[string]time.Time)
			}
			snap.paths[literalPathPattern(b.pattern.pathRegex)] = exempt
		case banClassPrefixHost:
			snap.hostPrefixes = append(snap.hostPrefixes, prefixBan{
				prefix:      anchoredPrefix(b.pattern.hostRegex),
				pred:        b.pred,
				exemptAfter: exempt,
			})
		case banClassPrefixPath:
			snap.pathPrefixes = append(snap.pathPrefixes, prefixBan{
				prefix:      anchoredPrefix(b.pattern.pathRegex),
				pred:        b.pred,
				exemptAfter: exempt,
			})
		default:
			// Opaque, and defensively any surrogate-only ban that
			// reached the compiler: the predicate walk is always sound.
			snap.opaqueBans = append(snap.opaqueBans, opaqueBan{pred: b.pred, exemptAfter: exempt})
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
// The cheap checks run first: literal set membership for host and
// path, then anchored-prefix HasPrefix scans. Only when every cheap
// check fails to reject does it fall through to the opaqueBans
// predicate walk.
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
	// Opaque bans: full predicate walk, gated by the same exemption
	// rule as the cheap sets. The predicate's own CreatedAt check only
	// fires when the ORIGINAL expr.CreatedAt was non-zero; for bans
	// registered without one (the admin API path), exemptAfter carries
	// the normalized registration time instead.
	for _, b := range s.opaqueBans {
		if subjectTo(obj, b.exemptAfter) && b.pred(obj) {
			return true
		}
	}
	return false
}

// literalMatch evaluates the literal set checks: host and path
// membership with per-ban exemption times. These are the O(1)
// rejections; a miss here rules out every literal-classified ban.
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
	return false
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

// subjectTo reports whether obj is subject to a ban whose exemption
// time is exempt (the ban's normalized registration instant; zero
// would mean the ban has no exemption and everything is subject).
// Objects stored after the exemption time are not subject — RFC 9111
// §4.4 invalidation only removes responses that existed at
// invalidation time.
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
	// membership via the surrogateSet (never compiled into the
	// snapshot).
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
	case banClassLiteralSurrogate:
		// A surrogate key is an exact token, never a pattern: a
		// surrogate-only ban is always a literal set member.
		return banClassLiteralSurrogate
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

// banListState is the lazy ban state: the authoritative ordered ban
// list plus the two read-side views the hit path consults without the
// mutex — the compiled snapshot of non-surrogate bans and the
// surrogate set.
//
// Cost model (all writers hold mu; readers never do):
//   - register: O(1) amortized. Dedup is a map lookup, eviction drops
//     the list head by reslicing, expired heads are popped as they
//     surface. A surrogate-only ban touches one surrogate-set shard; any
//     other ban marks the snapshot dirty.
//   - snapshot rebuild: O(non-surrogate bans), lazy (next read after a
//     dirty mark), so tag-invalidation storms never pay it.
//   - pruneExpired (reaper tick): O(list) sweep, off the hit and
//     registration paths.
//
// A ban whose TTL lapses while it is not at the list head keeps
// matching until the next reaper sweep or until it reaches the head.
// That only over-invalidates (the ban was active and still exempts
// objects stored after it); it never under-invalidates.
type banListState struct {
	// snap is published atomically so the hit path reads it without
	// the mutex: every cache hit (hot and warm tiers, fast path
	// included) consults it, and the mutex serializes all cores on one
	// cache line where an uncontended load does not (issue #757).
	snap atomic.Pointer[banSnapshot]
	// index maps each active pattern to its ban: O(1) dedup of
	// re-issued bans, and the membership test for list-derived views.
	index map[banPattern]*activeBan
	// surr enforces surrogate-only bans, the high-rate shape.
	surr surrogateSet
	// list holds every active ban in registration order (oldest
	// first). Evicting the head reslices (O(1)); append's growth
	// reallocates and drops the consumed prefix, so the backing array
	// stays bounded at a small multiple of the cap.
	list []*activeBan
	// rest is the non-surrogate subset of list, in registration order:
	// the only input of the snapshot compile.
	rest []*activeBan
	// ttl is how long a ban stays in the list before prune paths drop
	// it. Zero applies DefaultBanTTL. Configured per store via
	// HotConfig.BanTTL (invalidation.ban_ttl) so operators can bound the
	// blast radius of an over-broad ban.
	ttl time.Duration
	// capacity bounds the list. Zero applies banListCap.
	capacity int
	mu       sync.Mutex
	// dirty records that rest changed since snap was compiled. Atomic
	// so the lock-free snapshot() can check it without the mutex.
	dirty atomic.Bool
}

func (b *banListState) ttlOrDefault() time.Duration {
	if b.ttl <= 0 {
		return DefaultBanTTL
	}
	return b.ttl
}

func (b *banListState) capOrDefault() int {
	if b.capacity <= 0 {
		return banListCap
	}
	return b.capacity
}

// setTTL replaces the configured ban TTL at runtime (the hot-config
// surface, `bouine config set cluster.ban_ttl`). The mutex makes the
// swap atomic with the prune and register paths that read it; a new
// TTL also re-bounds bans already in the list on their next prune.
func (b *banListState) setTTL(d time.Duration) {
	b.mu.Lock()
	b.ttl = d
	b.mu.Unlock()
}

// currentTTL reports the configured TTL; zero means the built-in
// default (DefaultBanTTL) applies.
func (b *banListState) currentTTL() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ttl
}

// register adds (or refreshes) a ban in O(1) amortized time.
//
// A re-issued pattern refreshes the existing ban in place: its TTL
// anchor and exemption instant move to createdAt, keeping its list
// position. A new pattern appends; a full list evicts its head (the
// oldest registration). Expired bans at the head are popped first.
//
// Surrogate-only bans are published to the surrogate set immediately,
// so the very next lookup enforces them. Other bans mark the snapshot
// dirty; the rebuild happens on the next snapshot() read, which
// amortizes a registration batch into one compile — enforcement is
// unchanged, the first lookup after registration reads a fresh
// snapshot and sees the new ban.
func (b *banListState) register(expr api.BanExpr, pred banPredicate, createdAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pat := patternOf(expr)
	// exemptAfter is the normalized createdAt: the ban's original
	// CreatedAt when set, or the registration instant otherwise (the
	// admin API path — registerBan normalizes zero to time.Now()).
	// Objects stored after it are not subject to the ban (RFC 9111
	// §4.4: invalidation only removes responses that existed at
	// invalidation time). A zero value here would mean "no exemption
	// ever", which turned any over-broad ban (e.g. host_regex ".*")
	// into a permanent poison for every later fill.
	exemptAfter := createdAt
	b.popExpiredHeadLocked(time.Now())
	if ban := b.index[pat]; ban != nil {
		ban.created = createdAt
		ban.setExemptAfter(exemptAfter)
		if !ban.surrogate {
			b.dirty.Store(true)
		}
		return
	}
	for len(b.list) >= b.capOrDefault() {
		b.evictHeadLocked()
	}
	ban := &activeBan{
		pred:      pred,
		created:   createdAt,
		pattern:   pat,
		surrogate: classifyBan(pat) == banClassLiteralSurrogate,
	}
	ban.setExemptAfter(exemptAfter)
	if b.index == nil {
		b.index = make(map[banPattern]*activeBan)
	}
	b.index[pat] = ban
	b.list = append(b.list, ban)
	if ban.surrogate {
		b.surr.put(pat.surrogateKey, ban, b.capOrDefault())
		return
	}
	b.rest = append(b.rest, ban)
	b.dirty.Store(true)
}

// popExpiredHeadLocked evicts expired bans from the head of the list.
// Amortized O(1): each ban is popped at most once. The mutex must be
// held.
func (b *banListState) popExpiredHeadLocked(now time.Time) {
	ttl := b.ttlOrDefault()
	for len(b.list) > 0 && now.Sub(b.list[0].created) >= ttl {
		b.evictHeadLocked()
	}
}

// evictHeadLocked drops the oldest ban from the list and every view.
// The mutex must be held and the list non-empty.
func (b *banListState) evictHeadLocked() {
	ban := b.list[0]
	b.list[0] = nil // release the ban for GC; the slot is unreachable after the reslice
	b.list = b.list[1:]
	delete(b.index, ban.pattern)
	if ban.surrogate {
		b.surr.remove(ban.pattern.surrogateKey, ban)
		return
	}
	// rest keeps list order and the list head is the oldest ban of
	// all, so an evicted non-surrogate head is rest's head too: reslice
	// it off in O(1), like the list.
	if len(b.rest) > 0 && b.rest[0] == ban {
		b.rest[0] = nil
		b.rest = b.rest[1:]
	} else {
		b.rest = slices.DeleteFunc(b.rest, func(r *activeBan) bool { return r == ban })
	}
	b.dirty.Store(true)
}

// pruneExpired sweeps every ban older than the TTL out of the list
// and its views, then recompiles the snapshot. Called by the TTL
// reaper each tick so a quiet day after a storm does not keep taxing
// hits for the full 24 h window. O(list), off the hit path.
func (b *banListState) pruneExpired(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ttl := b.ttlOrDefault()
	kept := b.list[:0]
	restChanged := false
	for _, ban := range b.list {
		if now.Sub(ban.created) < ttl {
			kept = append(kept, ban)
			continue
		}
		delete(b.index, ban.pattern)
		if ban.surrogate {
			b.surr.remove(ban.pattern.surrogateKey, ban)
		} else {
			restChanged = true
		}
	}
	clear(b.list[len(kept):])
	b.list = kept
	if restChanged {
		b.rest = slices.DeleteFunc(b.rest, func(r *activeBan) bool {
			return b.index[r.pattern] != r
		})
	}
	b.rebuildLocked(now)
}

// emptyBanSnapshot is the shared zero-view served before the first
// compile (a zero-value banListState never rebuilt its snapshot).
var emptyBanSnapshot = &banSnapshot{}

// snapshot returns the current compiled view without ever parking the
// caller on the mutex. Clean (steady) state: one atomic load. When
// registrations have marked the state dirty, exactly one caller wins the
// TryLock and recompiles while the rest keep serving the previous
// snapshot — enforcement lags by at most one rebuild (~µs) during a
// registration batch, where the previous design parked every concurrent
// hit behind that same rebuild. The TryLock loser's fallback chain
// covers the pre-first-compile zero value.
//
// The amortization contract is preserved: a batch of N registrations
// still costs one rebuild, now paid by whichever hit first observes the
// dirty flag instead of by all of them (issue #757).
func (b *banListState) snapshot() *banSnapshot {
	if s := b.snap.Load(); s != nil && !b.dirty.Load() {
		return s
	}
	if b.mu.TryLock() {
		if b.dirty.Load() || b.snap.Load() == nil {
			b.rebuildLocked(time.Now())
		}
		b.mu.Unlock()
	}
	if s := b.snap.Load(); s != nil {
		return s
	}
	return emptyBanSnapshot
}

// rebuildLocked recompiles the snapshot from the non-surrogate bans
// and clears the dirty flag. O(len(rest)). The mutex must be held.
func (b *banListState) rebuildLocked(now time.Time) {
	b.snap.Store(compileBanSnapshot(b.rest, now, b.ttlOrDefault()))
	b.dirty.Store(false)
}

// matches reports whether obj is subject to any active ban: the
// compiled snapshot first, then one surrogate-set lookup per surrogate
// key on obj. Lock-free and allocation-free.
func (b *banListState) matches(obj *api.Object) bool {
	return b.snapshot().matches(obj) || b.surr.matches(obj)
}

// len reports the active ban count (tests and metrics).
func (b *banListState) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.list)
}

// descriptors returns the active bans obj is subject to, in
// registration (oldest-first) order, shaped for the wire (issue #814's
// explain plane). Read-only: no pruning, no eviction, no dirty
// marking — the same exemption rule and predicates the hit path
// enforces, asked as a question instead of applied as an action.
func (b *banListState) descriptors(obj *api.Object, now time.Time) []BanDescriptor {
	if obj == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ttl := b.ttlOrDefault()
	var out []BanDescriptor
	for _, ban := range b.list {
		if now.Sub(ban.created) >= ttl {
			continue
		}
		if !subjectTo(obj, ban.exemptAfter()) || !ban.pred(obj) {
			continue
		}
		out = append(out, BanDescriptor{
			HostRegex:    ban.pattern.hostRegex,
			PathRegex:    ban.pattern.pathRegex,
			SurrogateKey: ban.pattern.surrogateKey,
			CreatedAt:    ban.created,
			ExpiresAt:    ban.created.Add(ttl),
		})
	}
	return out
}

// surrogateSetMinShards is the surrogate set's minimum shard count.
// The table is sized to about two bans per shard at the list's
// capacity (see newSurrogateTable), so both a registration's shard
// copy and a lookup's binary search stay O(1) at any cap.
const surrogateSetMinShards = 1024

// surrogateSetSeed keys the shard hash. Process-wide: shard placement
// only needs to be stable within one process.
var surrogateSetSeed = maphash.MakeSeed()

// surrogateEntry maps one surrogate key to its ban.
type surrogateEntry struct {
	ban *activeBan
	key string
}

// surrogateShard is one immutable shard: entries sorted by key. A
// small sorted slice instead of a map keeps the read to one hash
// (shard selection) plus a short scan over contiguous memory, and makes
// a copy-on-write update a single allocation.
type surrogateShard []surrogateEntry

func compareSurrogateEntry(e surrogateEntry, key string) int {
	return strings.Compare(e.key, key)
}

// find returns the ban for key, or nil. Shards hold ~2 entries, so a
// linear scan (length check first, then bytes) beats a binary search's
// call overhead; larger shards fall back to the search.
func (sh surrogateShard) find(key string) *activeBan {
	if len(sh) <= 8 {
		for i := range sh {
			if sh[i].key == key {
				return sh[i].ban
			}
		}
		return nil
	}
	if i, ok := slices.BinarySearchFunc(sh, key, compareSurrogateEntry); ok {
		return sh[i].ban
	}
	return nil
}

// surrogateSet enforces surrogate-only bans with lock-free,
// allocation-free reads. Each shard is an immutable sorted slice
// published through an atomic pointer; writers (serialized by
// banListState.mu) copy the one shard they change and swap it in, so a
// registration costs one small copy instead of a full snapshot
// recompile, and is visible to the very next read. A refresh rewrites
// the shared ban's atomic exemption instant and copies nothing.
type surrogateSet struct {
	table atomic.Pointer[surrogateTable]
}

// surrogateTable is the shard array, allocated on the first put and
// sized from the list capacity; it is never resized.
type surrogateTable struct {
	shards []atomic.Pointer[surrogateShard]
	mask   uint64
}

func newSurrogateTable(capacity int) *surrogateTable {
	n := surrogateSetMinShards
	for n < capacity/2 {
		n <<= 1
	}
	return &surrogateTable{
		shards: make([]atomic.Pointer[surrogateShard], n),
		mask:   uint64(n - 1), //nolint:gosec // n is a positive power of two
	}
}

func (t *surrogateTable) shard(key string) *atomic.Pointer[surrogateShard] {
	return &t.shards[maphash.String(surrogateSetSeed, key)&t.mask]
}

// load returns the shard for key (nil when empty).
func (t *surrogateTable) load(key string) surrogateShard {
	if p := t.shard(key).Load(); p != nil {
		return *p
	}
	return nil
}

// put publishes key → ban, allocating the table for capacity on first
// use. Caller holds banListState.mu and guarantees key is not already
// present (the pattern index dedups first).
func (s *surrogateSet) put(key string, ban *activeBan, capacity int) {
	t := s.table.Load()
	if t == nil {
		t = newSurrogateTable(capacity)
		s.table.Store(t)
	}
	old := t.load(key)
	i, _ := slices.BinarySearchFunc(old, key, compareSurrogateEntry)
	next := make(surrogateShard, len(old)+1)
	copy(next, old[:i])
	next[i] = surrogateEntry{key: key, ban: ban}
	copy(next[i+1:], old[i:])
	t.shard(key).Store(&next)
}

// remove unpublishes key if it still maps to ban. Caller holds
// banListState.mu.
func (s *surrogateSet) remove(key string, ban *activeBan) {
	t := s.table.Load()
	if t == nil {
		return
	}
	old := t.load(key)
	i, ok := slices.BinarySearchFunc(old, key, compareSurrogateEntry)
	if !ok || old[i].ban != ban {
		return
	}
	if len(old) == 1 {
		t.shard(key).Store(nil)
		return
	}
	next := make(surrogateShard, len(old)-1)
	copy(next, old[:i])
	copy(next[i:], old[i+1:])
	t.shard(key).Store(&next)
}

// matches reports whether any of obj's surrogate keys has an active
// ban obj is subject to.
func (s *surrogateSet) matches(obj *api.Object) bool {
	if len(obj.SurrogateKeys) == 0 {
		return false
	}
	t := s.table.Load()
	if t == nil {
		return false
	}
	for _, k := range obj.SurrogateKeys {
		if ban := t.load(k).find(k); ban != nil && subjectToNanos(obj, ban.exemptNanos.Load()) {
			return true
		}
	}
	return false
}

// subjectToNanos is subjectTo for an exemption instant encoded as by
// activeBan.setExemptAfter (noExemption = zero time). It compares
// time.Time values rather than obj.StoredAt.UnixNano(), which is
// undefined for a zero StoredAt.
func subjectToNanos(obj *api.Object, exempt int64) bool {
	return exempt == noExemption || !obj.StoredAt.After(time.Unix(0, exempt))
}
