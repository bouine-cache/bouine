package cmd

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// explainURL recomputes the full cache decision chain for rawURL
// against this node's stored view (issue #814). Read-only by
// construction: every store read goes through the side-effect-free
// Probe (the same surface bouine keys uses, issue #816), variant
// enumeration rides the handler's variant index, and ban details come
// from the lazy registry. The request context is the pinned nominal
// request: browser-dialect Accept-Encoding, no other Vary-affecting
// headers. The context parameter is the admin ExplainFn contract
// (Config.ExplainFn); explain's store reads go through Probe, which
// does not take one.
func explainURL(_ context.Context, rawURL string, rs *runState) admin.ExplainResult {
	now := time.Now()
	result := admin.ExplainResult{URL: rawURL}
	_, h, policy := routeForURL(rs, rawURL)
	if h != nil {
		result.Route = admin.ExplainRoute{Name: h.RouteName(), Pool: h.PoolName()}
	}
	key := cache.BuildKeyFromURL(rawURL, policy)
	result.Key.Hex = key.Hex()
	result.Key.Derivation = keyDerivation(rawURL, policy)
	if key.IsZero() {
		// cachecheck's contract: a zero key means the route does not
		// cache, so there is no chain to explain.
		result.Verdict = "bypass"
		result.CacheResult = "BYPASS"
		return result
	}
	nominal := header.NewMap(1)
	nominal.Set(header.AcceptEncoding, cache.NominalAcceptEncoding)

	st, present, banned := explainResolve(rs, key, policy, nominal, &result.Variant)
	explainStoredVariants(rs, h, key, policy, &result.Variant)
	result.Stored = explainStored(st, present)
	result.Stored.VariantsPresent = len(result.Variant.StoredVariants)
	result.Cacheable = explainCacheability(h, st, present, nominal)
	result.Bans = explainBans(rs, rawURL, key, st, present)
	result.Cluster = explainCluster(rs, key)
	explainVerdict(&result, st, present, banned, now)
	return result
}

// explainResolve mirrors the data plane's two-step lookup (the
// primary carries the stored Vary metadata; the nominal request's
// variant key selects the bucket) on top of the side-effect-free
// Probe. The variant section fills as a side effect: the stored Vary
// and the nominal bucket. Returns the resolved entry's state (nil on
// variant-absent, not-stored, or banned-present — a lazily banned
// entry is a miss on the next lookup, which is the question explain
// answers).
func explainResolve(rs *runState, key api.Key, policy *cache.KeyPolicy, nominal header.Map, out *admin.ExplainVariant) (st api.KeyState, present, banned bool) {
	primary, ok := rs.store.Probe(key)
	if !ok {
		return api.KeyState{}, false, false
	}
	if primary.Banned {
		// A lazily banned entry is a miss on the next lookup (the
		// data plane's Get evicts it) — but it physically sits in the
		// tier, so the freshness data below still explains what was
		// there: a full life under a banned headline is the answer.
		return primary, true, true
	}
	if primary.Vary == "" {
		return primary, true, false
	}
	out.Vary = primary.Vary
	if isAcceptEncodingOnly(primary.Vary) {
		out.RequestBucket = cache.EncodingBucket(cache.NominalAcceptEncoding)
	}
	vk := cache.VariantKey(key, primary.Vary, nominal, policy)
	if vk == key {
		return primary, true, false
	}
	out.RequestKeyHex = vk.Hex()
	variant, ok := rs.store.Probe(vk)
	out.Matched = ok && !variant.Banned
	if !ok {
		// Variant absent: the resolver stays for validators, but the
		// response can never be selected for this request.
		return api.KeyState{}, false, false
	}
	if variant.Banned {
		return variant, true, true
	}
	return variant, true, false
}

// explainStoredVariants enumerates the live variants of the primary
// key through the handler's variant index, resolving each with
// Probe so index staleness (evicted-but-listed entries)
// self-corrects — O(variants of this key), never a store scan.
// Bucket labels probe the fixed encoding-bucket space against the
// stored assertion key; non-encoding Vary keeps its hex only.
func explainStoredVariants(rs *runState, h *cache.Handler, key api.Key, policy *cache.KeyPolicy, out *admin.ExplainVariant) {
	if h == nil {
		return
	}
	now := time.Now()
	for _, vk := range h.VariantsOf(key) {
		vst, ok := rs.store.Probe(vk)
		if !ok || vst.Banned {
			continue
		}
		sv := admin.ExplainStoredVariant{
			KeyHex:   vk.Hex(),
			Tier:     vst.Tier,
			Fresh:    explainStateFresh(vst, now),
			Age:      explainStateAge(vst, now),
			Lifetime: lifetimeString(vst),
		}
		if out.Vary != "" {
			sv.Bucket = probeVariantBucket(out.Vary, vst.VaryKey, policy)
		}
		out.StoredVariants = append(out.StoredVariants, sv)
	}
}

// probeVariantBucket resolves the human label of a stored variant by
// testing the known encoding buckets against the variant's stored
// assertion key — the same BuildVaryKey that stamped it, so the
// label cannot lie. O(4) hash computations, cold path. Empty when
// the Vary is not Accept-Encoding alone.
func probeVariantBucket(vary, varyKey string, policy *cache.KeyPolicy) string {
	if !isAcceptEncodingOnly(vary) {
		return ""
	}
	for _, cand := range []string{"br", "zstd", "gzip", "identity"} {
		m := header.NewMap(1)
		m.Set(header.AcceptEncoding, cand)
		if cache.BuildVaryKey(vary, m, policy) == varyKey {
			return cache.EncodingBucket(cand)
		}
	}
	return ""
}

// explainStored reports the resolved entry's stored view: tier,
// size, and the KeepGrace protection flag. Absent entries report
// present=false; the variant section carries the variant count.
func explainStored(st api.KeyState, present bool) admin.ExplainStored {
	out := admin.ExplainStored{Present: present}
	if !present {
		return out
	}
	out.Tier = st.Tier
	out.SizeBytes = st.SizeBytes
	out.Protected = st.KeepGrace
	return out
}

// explainCacheability re-derives whether this route would store a
// response like the stored one — the same IsCacheableWithDefault
// call the fill path makes, evaluated on the stored status and the
// merged Cache-Control the codec carries (ADR-0053). Nothing
// stored (or no matching route) means "unknown": the verdict is a
// property of a response, and explain never fetches one.
func explainCacheability(h *cache.Handler, st api.KeyState, present bool, nominal header.Map) admin.ExplainCacheability {
	out := admin.ExplainCacheability{Verdict: "unknown"}
	if !present || h == nil {
		return out
	}
	out.Directives = st.CacheControl
	respHeader := header.NewMap(1)
	respHeader.Set(header.CacheControl, st.CacheControl)
	if h.CacheableWithRouteDefaults(int(st.StatusCode), nominal, respHeader) {
		out.Verdict = "cacheable"
	} else {
		out.Verdict = "not-cacheable"
	}
	return out
}

// explainBans reports the active lazy bans the URL is subject to.
// With the entry present, its stored surrogate keys participate too
// (the host and path the matcher reads are the request's, which
// explain's URL supplies). When the eager ban scan already deleted
// the entry, a URL-shaped candidate with a zero StoredAt stands in
// — subject to every active ban, which is exactly the "would this
// ban have invalidated this URL" question. Surrogate-key bans
// cannot match a URL alone; they surface only while the entry is
// still stored.
func explainBans(rs *runState, rawURL string, key api.Key, st api.KeyState, present bool) []admin.ExplainBan {
	candidate := urlCandidateObject(rawURL, key)
	if candidate == nil {
		return nil
	}
	if present {
		// The entry's stored-at makes the candidate subject to exactly
		// the bans the real object is subject to — same exemption
		// rule, same predicates; a now() stamp would read as a post-ban
		// fill and wrongly exempt it.
		if storedAt, err := time.Parse(time.RFC3339, st.StoredAt); err == nil {
			candidate.StoredAt = storedAt
		}
		candidate.SurrogateKeys = st.SurrogateKeys
	}
	matches := rs.store.MatchingBans(candidate)
	if len(matches) == 0 {
		return nil
	}
	out := make([]admin.ExplainBan, 0, len(matches))
	for _, m := range matches {
		out = append(out, admin.ExplainBan{
			HostRegex:    m.HostRegex,
			PathRegex:    m.PathRegex,
			SurrogateKey: m.SurrogateKey,
			CreatedAt:    admin.FormatExplainTime(m.CreatedAt),
			ExpiresAt:    admin.FormatExplainTime(m.ExpiresAt),
		})
	}
	return out
}

// explainCluster reports the ring owner for the key: the peer name
// and whether this node is it. No ring (cluster disabled or empty)
// means this node owns everything.
func explainCluster(rs *runState, key api.Key) admin.ExplainCluster {
	if rs.clusterNode == nil {
		return admin.ExplainCluster{Local: true}
	}
	owner := rs.clusterNode.Owner(key)
	if owner.Name == "" {
		return admin.ExplainCluster{Local: true}
	}
	return admin.ExplainCluster{
		OwnerPeer: owner.Name + " (" + owner.DataAddr + ")",
		Local:     rs.clusterNode.IsLocal(key),
	}
}

// explainVerdict folds freshness into the final verdict and reason,
// in the order the data plane would experience them: a lazily
// banned entry is a miss on next lookup (explainResolve already
// dropped it, and the ban descriptors carry the why), an expired
// one a miss, a fresh one a hit, a stale one inside its swr/sie
// window a stale-hit. The freshness data is always reported when an
// entry exists — a banned entry's remaining life explains that
// freshness was not the problem.
func explainVerdict(result *admin.ExplainResult, st api.KeyState, present, banned bool, now time.Time) {
	if present {
		result.TTL = explainTTL(st, now)
	}
	switch {
	case banned:
		// The data plane evicts on the next lookup, so the honest
		// verdict is a miss; the freshness data still reports the
		// entry's life — freshness was not the problem.
		result.Verdict = "miss"
		result.CacheResult = "MISS"
		result.Reason = "banned"
	case !present && len(result.Bans) > 0:
		result.Verdict = "miss"
		result.CacheResult = "MISS"
		result.Reason = "banned"
	case !present && result.Variant.RequestKeyHex != "":
		result.Verdict = "miss"
		result.CacheResult = "MISS"
		result.Reason = "variant-absent"
	case !present:
		result.Verdict = "miss"
		result.CacheResult = "MISS"
		result.Reason = "not-stored"
	case result.Cacheable.Verdict == "not-cacheable":
		result.Verdict = "miss"
		result.CacheResult = "MISS"
		result.Reason = "not-cacheable"
	case result.TTL.Verdict == "fresh":
		result.Verdict = "hit"
		result.CacheResult = "HIT"
	case result.TTL.Verdict == "stale-swr" || result.TTL.Verdict == "stale-sie":
		result.Verdict = "stale-hit"
		result.CacheResult = "STALE"
	default:
		result.Verdict = "miss"
		result.CacheResult = "MISS"
		result.Reason = "expired"
	}
}

// explainTTL renders the freshness lifecycle from the probed state:
// verdict per zone and the remaining life inside the current zone
// (the CLI bar's remaining fuel). KeyState carries raw facts only;
// the age math and the clock live here, per its documented
// contract.
func explainTTL(st api.KeyState, now time.Time) admin.ExplainTTL {
	ttl := time.Duration(st.TTLSeconds) * time.Second
	swr := time.Duration(st.SWRSeconds) * time.Second
	sie := time.Duration(st.SIESeconds) * time.Second
	storedAt, err := time.Parse(time.RFC3339, st.StoredAt)
	if err != nil {
		return admin.ExplainTTL{}
	}
	age := now.Sub(storedAt)
	expiry := storedAt.Add(ttl)
	out := admin.ExplainTTL{
		Age:       age.Truncate(time.Second).String(),
		Lifetime:  ttl.Truncate(time.Second).String(),
		SWR:       swr.Truncate(time.Second).String(),
		SIE:       sie.Truncate(time.Second).String(),
		ExpiresAt: admin.FormatExplainTime(expiry),
	}
	switch {
	case age < ttl:
		out.Verdict = "fresh"
		out.FreshFor = expiry.Sub(now).Truncate(time.Second).String()
	case swr > 0 && age < ttl+swr:
		out.Verdict = "stale-swr"
		out.FreshFor = expiry.Add(swr).Sub(now).Truncate(time.Second).String()
	case sie > 0 && age < ttl+sie:
		out.Verdict = "stale-sie"
		out.FreshFor = expiry.Add(sie).Sub(now).Truncate(time.Second).String()
	default:
		out.Verdict = "dead"
	}
	return out
}

// explainStateFresh reports whether a probed entry is within its ttl
// window at now — the same comparison Object.Fresh makes, on the
// probe's second-granular facts.
func explainStateFresh(st api.KeyState, now time.Time) bool {
	storedAt, err := time.Parse(time.RFC3339, st.StoredAt)
	if err != nil {
		return false
	}
	return now.Sub(storedAt) < time.Duration(st.TTLSeconds)*time.Second
}

// explainStateAge renders a probed entry's age as a Go duration
// string for the stored-variant freshness row.
func explainStateAge(st api.KeyState, now time.Time) string {
	storedAt, err := time.Parse(time.RFC3339, st.StoredAt)
	if err != nil {
		return ""
	}
	return now.Sub(storedAt).Truncate(time.Second).String()
}

// lifetimeString renders a probed entry's stamped lifetime.
func lifetimeString(st api.KeyState) string {
	return (time.Duration(st.TTLSeconds) * time.Second).Truncate(time.Second).String()
}

// keyDerivation reports the inputs an operator can replay by hand:
// host, path, raw query, and the query policy's stripped names —
// computed with the policy's own predicate so the report cannot
// drift from the key the data plane computed.
func keyDerivation(rawURL string, policy *cache.KeyPolicy) []admin.ExplainKeyStep {
	u, err := url.Parse(rawURL)
	if err != nil {
		return []admin.ExplainKeyStep{{Step: "url", Value: rawURL}}
	}
	steps := []admin.ExplainKeyStep{
		{Step: "host", Value: u.Host},
		{Step: "path", Value: u.Path},
	}
	if u.RawQuery != "" {
		steps = append(steps, admin.ExplainKeyStep{Step: "query", Value: u.RawQuery})
	}
	if stripped := policy.StrippedParamNames(u.RawQuery); len(stripped) > 0 {
		steps = append(steps, admin.ExplainKeyStep{Step: "query_policy", Stripped: stripped})
	}
	return steps
}

// urlCandidateObject synthesizes the object a URL-shaped ban
// predicate evaluates against: the stored host/path header pair, the
// computed key, and a zero StoredAt (subject to every active ban —
// the eager scan has usually deleted the real entry already).
func urlCandidateObject(rawURL string, key api.Key) *api.Object {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	m := header.NewMap(2)
	m.Set(header.XBouineHost, u.Host)
	m.Set(header.XBouinePath, u.Path)
	return &api.Object{Key: key, Header: m}
}

// isAcceptEncodingOnly reports whether a stored Vary value names
// Accept-Encoding alone — the only shape whose bucket label the
// fixed candidate space can derive.
func isAcceptEncodingOnly(vary string) bool {
	return strings.EqualFold(strings.TrimSpace(vary), header.AcceptEncoding)
}
