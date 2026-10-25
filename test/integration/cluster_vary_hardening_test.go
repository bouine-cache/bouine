//go:build integration

package integration_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// This file is the cache-poisoning hardening suite for the
// metadata-only Vary resolver (ADR-0068): the cluster-shaped gaps the
// first production-readiness review identified. Every test drives a
// 3-node strong-mode cluster and asserts the cross-variant isolation
// invariant — a response's body must always match the selecting
// headers of the request that produced it, across cold nodes,
// restarts, purges, origin Vary rotations, conditional revalidation,
// and variant sprays past the MaxVariants cap.

// TestStrong_TwoStepColdNodeConvergence pins the ADR-0068 two-step
// resolution end-to-end plus its consumer-side observability: a node
// with no local Vary context for a URL converges through the owner
// (blank primary-key ask → resolver metadata → variant-key ask) and
// increments bouine_peer_fetch_resolver_answers_total{side="consumer"}.
// A restarted node must re-converge the same way and never serve a
// foreign variant while doing so.
func TestStrong_TwoStepColdNodeConvergence(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})
	path := "/vary-multiline?x=twostep"
	get := func(n int, lang string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Language": lang,
			"X-Region":        "FR",
		})
	}

	// Warm ONLY the fr variant (from node 0). The it variant is then
	// stored nowhere, so any other node requesting it holds no local
	// Vary context for the URL.
	driver.RetryUntil(t, 15*time.Second, 200*time.Millisecond, func() bool {
		return driver.XCache(get(0, "fr-FR,fr;q=0.9")) == "HIT"
	})
	time.Sleep(1500 * time.Millisecond) // let peer-put deliver resolver + variant to owners

	// Cold nodes 1 and 2 request the never-stored it variant. At most
	// one node owns the primary key, so at least one of these requests
	// must complete the ADR-0068 two-step — provable via the
	// consumer-side counter, which the rollout runbook uses as the
	// convergence canary.
	for _, n := range []int{1, 2} {
		resp := get(n, "it-IT,it;q=0.9")
		require.Contains(t, string(resp.Body), "lang=it-IT",
			"cold node %d must serve the it variant, never a foreign body", n)
	}
	var twoSteps float64
	for _, n := range s.AliveNodes() {
		twoSteps += s.MetricValueWithLabels(t, n,
			"bouine_peer_fetch_resolver_answers_total", `side="consumer"`)
	}
	require.Greater(t, twoSteps, float64(0),
		"at least one cold node must converge via the two-step resolution")
	for _, n := range []int{1, 2} {
		driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
			return driver.XCache(get(n, "it-IT,it;q=0.9")) == "HIT"
		})
	}

	// Restart node 2: its in-memory state is gone. The re-request must
	// converge to the correct body — two-step when node 2 is a
	// non-owner, an owner-local refill otherwise — never a foreign
	// variant on the way.
	s.KillNode(t, 2)
	s.RestartNode(t, 2)
	s.WaitNodeReady(t, 2, 30*time.Second)
	var sawForeign bool
	driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
		resp := get(2, "it-IT,it;q=0.9")
		body := string(resp.Body)
		if !strings.Contains(body, "lang=it-IT") {
			sawForeign = true
		}
		return driver.XCache(resp) == "HIT" && strings.Contains(body, "lang=it-IT")
	})
	require.False(t, sawForeign,
		"the restarted node must never serve a foreign variant while converging")
}

// TestStrong_FleetRestartWarmRouting pins ADR-0068's warm-back contract
// at fleet scale: after every node restarts, the reloaded warm tier
// must restore variant routing — the resolver (always warm-backed) and
// the fat variants (>64 KiB, over the hot/warm admission threshold)
// reload, and the full request matrix is served with ZERO origin
// refetches, correct bytes, and a Content-Length that matches the body
// (the metadata-only resolver must never surface as an empty 200).
func TestStrong_FleetRestartWarmRouting(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", WarmTier: true})
	path := "/vary-big?x=warmrestart"
	get := func(n int, lang string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Language": lang,
		})
	}
	wantBody := func(lang string) []byte {
		return driver.DeterministicPayload("varybig-"+lang, 96)
	}

	// Warm two >64 KiB variants, then let peer-put deliver them (and
	// the resolver) to their owners so the warm tier of every relevant
	// node holds the family.
	for _, lang := range []string{"fr", "it"} {
		driver.RetryUntil(t, 20*time.Second, 200*time.Millisecond, func() bool {
			return driver.XCache(get(0, lang)) == "HIT"
		})
	}
	time.Sleep(1500 * time.Millisecond)

	// Restart the entire fleet. In-memory state is gone everywhere;
	// only the per-node warm directories survive.
	for n := range 3 {
		s.KillNode(t, n)
	}
	for n := range 3 {
		s.RestartNode(t, n)
	}
	for n := range 3 {
		s.WaitNodeReady(t, n, 30*time.Second)
	}

	// Convergence pass: every matrix cell must come back as a HIT
	// (warm-reloaded resolver routing to warm-reloaded variants,
	// possibly via the two-step across nodes).
	for _, n := range s.AliveNodes() {
		for _, lang := range []string{"fr", "it"} {
			driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
				return driver.XCache(get(n, lang)) == "HIT"
			})
		}
	}

	// Zero-refetch sweep: a second full pass must serve every variant
	// entirely from the reloaded tiers — no origin fetch, exact bytes,
	// Content-Length backed by the bytes actually served.
	before := s.OriginRequests()
	for _, n := range s.AliveNodes() {
		for _, lang := range []string{"fr", "it"} {
			resp := get(n, lang)
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, wantBody(lang), resp.Body,
				"node %d lang %s: warm reload must restore the variant bytes", n, lang)
			cl, err := strconv.Atoi(resp.Header.Get("Content-Length"))
			require.NoError(t, err, "node %d lang %s: Content-Length must be present", n, lang)
			require.Equal(t, len(resp.Body), cl,
				"node %d lang %s: Content-Length must match the served bytes", n, lang)
			require.Equal(t, "HIT", driver.XCache(resp))
		}
	}
	require.Equal(t, before, s.OriginRequests(),
		"the post-restart matrix must be served entirely from the reloaded warm tier")
}

// TestStrong_PurgePeerPutVariantsPastCap pins the ADR-0068 cap-pressure
// purge fix at cluster level: peer-put registration always succeeds,
// including past the MaxVariants cap, because the write-to-owner RPC
// stores the variant before the registration callback runs. Eight
// variants over three owners force some owner past a cap of 2
// (pigeonhole), and the cluster-wide URL purge must delete every
// variant — including the past-cap registrations — on every node.
func TestStrong_PurgePeerPutVariantsPastCap(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", MaxVariants: 2})
	path := "/vary-multiline?x=pastcap"
	langs := []string{"fr", "it", "de", "es", "pt", "nl", "sv", "pl"}
	get := func(n int, lang string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Language": lang,
			"X-Region":        "EU",
		})
	}

	// Warm all 8 variants. Fill attempts rotate across nodes: the
	// primary's owner enforces the cap against a live local set (its
	// set is never pruned away — it holds the primary), while
	// non-owners prune their sets on every fill (their tracked variants
	// are never stored locally, so the liveness sweep empties them), so
	// the RetryUntil over a rotating node set always converges on a
	// non-owner fill that peer-puts the variant to its owner.
	fillLang := func(lang string) bool {
		for _, n := range []int{1, 2, 0} {
			if driver.XCache(get(n, lang)) == "HIT" {
				return true
			}
		}
		return false
	}
	for _, lang := range langs {
		driver.RetryUntil(t, 20*time.Second, 200*time.Millisecond, func() bool {
			return fillLang(lang)
		})
	}

	// Every variant is servable from EVERY node with its own body —
	// including the ones some owner registered past its cap.
	for _, lang := range langs {
		for _, n := range s.AliveNodes() {
			resp := get(n, lang)
			require.Contains(t, string(resp.Body), "lang="+lang,
				"variant %s via node %d must serve its own body, never a foreign one", lang, n)
		}
	}

	// Cluster-wide URL purge (RFC 9111 §4.4): the broadcast must reach
	// every peer-put variant, past-cap ones included.
	s.Purge(t, 0, "http://"+driver.CrossNodeHost+path)

	// Post-purge, the first request for every variant must be a fresh
	// MISS — a surviving past-cap registration would serve a stale HIT.
	for _, lang := range langs {
		var sawMiss bool
		for i := 0; i < 40 && !sawMiss; i++ {
			if driver.XCache(get(0, lang)) == "MISS" {
				sawMiss = true
			} else {
				time.Sleep(200 * time.Millisecond)
			}
		}
		require.True(t, sawMiss,
			"purge must invalidate variant %s, including past-cap peer-put registrations", lang)
	}
	// The fresh refills carry the right identities.
	for _, lang := range langs {
		resp := get(0, lang)
		require.Contains(t, string(resp.Body), "lang="+lang)
	}
}

// TestStrong_VaryDriftRotation pins ADR-0058 drift detection at cluster
// level: when the origin rotates its Vary declaration (X-A → X-B), the
// cache must never serve a body from the old declaration's world once
// convergence completes — the drift purge retires the old resolver and
// its variants, and the post-drift matrix serves only gen-2 content.
func TestStrong_VaryDriftRotation(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})
	path := "/vary-drift?x=rotation"
	get := func(n int, a, b string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"X-A": a,
			"X-B": b,
		})
	}

	// World 1: Vary X-A. Warm the (a=1) variant.
	driver.RetryUntil(t, 15*time.Second, 200*time.Millisecond, func() bool {
		return driver.XCache(get(0, "1", "1")) == "HIT"
	})

	// Rotate the origin's declaration: Vary X-B, gen-2 bodies.
	s.SetVaryDrift(true)

	// Converge: every node serving the new world's (a=1, b=2) request
	// must answer gen=2 — the stale x-a=1 variant is revalidated away
	// by the drift purge, never served fresh again.
	for _, n := range s.AliveNodes() {
		driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
			resp := get(n, "1", "2")
			return strings.Contains(string(resp.Body), "gen=2")
		})
	}

	// Stability sweep: every node × every header combo — including the
	// old world's exact selecting headers — serves only gen=2 bodies.
	combos := [][2]string{{"1", "2"}, {"1", "1"}, {"9", "2"}}
	for _, n := range s.AliveNodes() {
		for _, c := range combos {
			resp := get(n, c[0], c[1])
			require.Contains(t, string(resp.Body), "gen=2",
				"node %d combo a=%s b=%s: post-drift content must come from the new declaration", n, c[0], c[1])
			require.NotContains(t, string(resp.Body), "gen=1",
				"node %d combo a=%s b=%s: a stale pre-drift body is cache poisoning", n, c[0], c[1])
		}
	}
}

// TestStrong_Resolver304Coherence pins conditional-client coherence
// against the resolver path end-to-end: If-None-Match revalidation on
// Vary URLs must answer 304 for the client's own validated variant and
// 200 with the CORRECT variant's body otherwise — a conditional client
// must never be handed a foreign variant's content through the
// validator-reuse path (RFC 9111 §4.3).
func TestStrong_Resolver304Coherence(t *testing.T) {
	s := sharedCluster(t, "strong")
	path := "/vary-etag?x=304"
	get := func(n int, lang, inm string) *driver.Response {
		headers := map[string]string{"Accept-Language": lang}
		if inm != "" {
			headers["If-None-Match"] = inm
		}
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, headers)
	}

	// Warm the fr variant (ETag "ve-fr") from node 0.
	driver.RetryUntil(t, 15*time.Second, 200*time.Millisecond, func() bool {
		return driver.XCache(get(0, "fr", "")) == "HIT"
	})
	time.Sleep(1500 * time.Millisecond)

	// A cold node requesting the it variant while presenting the FR
	// tag: the origin must not 304 it (ve-it is the current
	// representation for it), so the client gets a 200 with it's own
	// body — never fr's, never a 304 misapplied.
	resp := get(1, "it", `"ve-fr"`)
	require.Equal(t, 200, resp.StatusCode,
		"a foreign tag does not validate the it representation: the origin must answer 200")
	require.Contains(t, string(resp.Body), "lang=it",
		"the conditional fill must serve the it body, never a foreign variant")
	require.NotContains(t, string(resp.Body), "lang=fr")

	// Own-tag conditionals through every node for both variants. In a
	// split-ownership layout (the common case: the primary's owner and
	// a variant's owner are independent ring assignments) even the
	// variant's owner serves via the two-step, and the peer path
	// answers 200 — spec-correct for the same validator. A 304 may
	// appear only where a node holds primary and variant together, so
	// this e2e pins the poisoning invariant, not the 304 synthesis
	// (that is the unit matrix's job: TestHandler_VaryMiss_Ambiguous304
	// and friends). Every answer must be a 304 with no body or a 200
	// with the requested variant's own body — never a foreign one.
	for _, lang := range []string{"it", "fr"} {
		tag := `"ve-` + lang + `"`
		for _, n := range s.AliveNodes() {
			r := get(n, lang, tag)
			switch r.StatusCode {
			case 304:
				require.Empty(t, string(r.Body), "a 304 must not carry a body")
			case 200:
				require.Contains(t, string(r.Body), "lang="+lang,
					"a conditional 200 must carry the %s body, never a foreign one", lang)
			default:
				t.Fatalf("unexpected status %d for the %s conditional", r.StatusCode, lang)
			}
		}
	}
}

// TestEventual_VaryCapSpray pins the MaxVariants DoS bound under a
// variant spray (RFC 9110 §12.5.5). Eventual mode is the deterministic
// shape: every node caches independently, so one node spraying a
// couple dozen attacker-chosen Accept-Language values at a URL with a
// cap of 2 must (1) store only its first `cap` variants, refetching
// every refused one from origin on EVERY request (bounded storage,
// unbounded correctness), (2) trip bouine_vary_cap_hits_total on the
// spraying node, and (3) never serve a foreign body on any request.
// The strong-mode past-cap peer-put registrations are pinned by
// TestStrong_PurgePeerPutVariantsPastCap.
func TestEventual_VaryCapSpray(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "eventual", MaxVariants: 2})
	path := "/vary-multiline?x=spray"
	langs := []string{
		"fr", "it", "de", "es", "pt", "nl", "sv", "pl",
		"da", "fi", "no", "cs", "el", "hu", "ro", "sk",
	}
	get := func(n int, lang string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Language": lang,
			"X-Region":        "EU",
		})
	}

	// Spray every language through node 0. Every response — stored,
	// or cap-refused-then-refetched — must carry the requested
	// variant's own body.
	for _, lang := range langs {
		resp := get(0, lang)
		require.Equal(t, 200, resp.StatusCode)
		require.Contains(t, string(resp.Body), "lang="+lang,
			"sprayed variant %s must serve its own body, never a foreign one", lang)
	}

	// The bound fired on the spraying node: its live local set holds
	// the primary (the resolver) plus exactly `cap` variants, so every
	// further fill was refused.
	require.Greater(t, s.MetricValue(t, 0, "bouine_vary_cap_hits_total"), float64(0),
		"the variant spray must trip the MaxVariants cap on the spraying node")

	// Bounded storage, unbounded correctness: the first `cap` variants
	// stay cached (HIT), every refused one is refetched from origin on
	// every request (MISS) — never stored, never a stale or foreign
	// body, and never served empty.
	for i, lang := range langs {
		resp := get(0, lang)
		require.Contains(t, string(resp.Body), "lang="+lang,
			"re-requested variant %s must serve its own body", lang)
		if i < 2 {
			require.Equal(t, "HIT", driver.XCache(resp),
				"variant %s is inside the cap and must stay cached", lang)
		} else {
			require.Equal(t, "MISS", driver.XCache(resp),
				"variant %s is past the cap: it must be refetched every time, never stored", lang)
		}
	}

	require.Len(t, s.AliveNodes(), 3, "the spray must not kill any node")
}
