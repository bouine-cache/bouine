package cmd

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/observability"
)

// routeForURL resolves the route a URL's host+path matches, via the
// same first-match-wins walk policyForURL uses, and returns the
// matched label, its cache handler, and the handler's key policy.
// label/handler/policy are zero when no route matches (the default
// host-ful key policy then applies, and an unmatched URL is by
// definition un-stored).
func routeForURL(rs *runState, rawURL string) (label string, handler *cache.Handler, policy *cache.KeyPolicy) {
	if rs == nil || rs.router == nil {
		return "", nil, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "", nil, nil
	}
	label = rs.router.MatchByHostPath(u.Host, u.Path)
	if label == "" {
		return "", nil, nil
	}
	for _, h := range rs.handlers {
		if h.RouteName() == label {
			return label, h, h.KeyPolicy()
		}
	}
	return label, nil, nil
}

// keysFindFn builds the GET /v1/keys/find provider (bouine keys find,
// issue #816): compute the primary key for a URL exactly the way the
// data plane does — the matched route's key policy, falling back to
// the default host-ful policy — then report this node's state via the
// side-effect-free storage probe. No storage enumeration anywhere:
// the deterministic key makes "look up this URL" a single map probe.
// With peer=true in cluster mode the ring owner is queried over the
// peer channel so the operator sees both ends of the miss.
func (e *engine) keysFindFn(rs *runState) func(ctx context.Context, rawURL string, peer bool) (admin.KeysFindResult, error) {
	return func(ctx context.Context, rawURL string, peer bool) (admin.KeysFindResult, error) {
		label, handler, policy := routeForURL(rs, rawURL)
		key := cache.BuildKeyFromURL(rawURL, policy)
		if key.IsZero() {
			return admin.KeysFindResult{}, fmt.Errorf("unparseable url %q", rawURL)
		}
		result := admin.KeysFindResult{
			URL:         rawURL,
			KeyHex:      key.Hex(),
			Route:       label,
			QueryPolicy: policy.QuerySummary(),
		}
		if u, err := url.Parse(rawURL); err == nil {
			result.Host = u.Host
			result.Path = u.Path
		}
		result.Local, _ = rs.store.Probe(key)
		if !result.Local.Present {
			result.AbsentReason = "no_entry"
		}
		if handler != nil {
			result.Variants = handler.VariantCount(key)
		}
		if rs.clusterNode != nil {
			if owner := rs.clusterNode.Owner(key); owner.Name != "" {
				ownerCopy := owner
				result.Owner = &ownerCopy
				if peer && rs.peerFetcher != nil && !rs.clusterNode.IsLocal(key) {
					st, err := rs.peerFetcher.QueryKeyState(ctx, owner, key)
					switch {
					case err != nil:
						result.OwnerError = err.Error()
					default:
						result.OwnerState = &st
					}
				}
			}
		}
		return result, nil
	}
}

// keysTopFn builds the GET /v1/keys/top and /v1/keys/recent provider
// (bouine keys top/recent, issue #816): the URL ring's recorded
// traffic, most requests first ("top") or latest activity first
// ("recent"). The ring is a sampled request-traffic view — it records
// non-HIT results over URL prefixes — not a storage inventory, which
// is why the endpoint is named after the ring, not the store.
func (e *engine) keysTopFn(rs *runState) func(kind, class string, limit int) admin.KeysTopResult {
	return func(kind, class string, limit int) admin.KeysTopResult {
		var entries []observability.URLTopStat
		if kind == "recent" {
			entries = rs.rings.URL.RecentURLs(limit, class)
		} else {
			entries = rs.rings.URL.TopURLs(limit, class)
		}
		result := admin.KeysTopResult{
			Kind:       kind,
			Class:      class,
			Limit:      limit,
			SampleRate: rs.rings.URL.SampleRate(),
			Entries:    make([]admin.KeysTopEntry, 0, len(entries)),
		}
		for _, st := range entries {
			entry := admin.KeysTopEntry{
				Prefix:      st.Prefix,
				Route:       st.Route,
				Requests:    st.Requests,
				Misses:      st.Misses,
				Stale:       st.Stale,
				Revalidated: st.Revalidated,
				Bypass:      st.Bypass,
			}
			if !st.LastSeen.IsZero() {
				entry.LastSeen = st.LastSeen.UTC().Format(time.RFC3339)
			}
			result.Entries = append(result.Entries, entry)
		}
		return result
	}
}
