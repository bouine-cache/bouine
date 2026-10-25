package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// explainFixtures returns the pinned decision cases from issue #814
// as wire documents. The renderer consumes the document only — no
// clock — so every case's bytes are deterministic.
func explainFixtures() map[string]*bouineapi.ExplainResult {
	hit := &bouineapi.ExplainResult{
		Verdict: "hit", CacheResult: "HIT",
		Key: bouineapi.ExplainKey{Hex: "9f3ab41c77e2d5a8", Derivation: []bouineapi.ExplainKeyStep{
			{Step: "host", Value: "shop.example.com"},
			{Step: "path", Value: "/assets/app.4f2c.js"},
			{Step: "query", Value: "v=4"},
		}},
		Route:     bouineapi.ExplainRoute{Name: "static", Pool: "origins-static"},
		Cacheable: bouineapi.ExplainCacheability{Verdict: "cacheable", Directives: "public, max-age=86400"},
		TTL: bouineapi.ExplainTTL{
			Verdict: "fresh", FreshFor: "21h13m0s", Age: "2h47m0s",
			Lifetime: "24h0m0s", SWR: "5m0s", SIE: "30m0s",
			ExpiresAt: "2026-10-26T04:03:11Z",
		},
		Stored:  bouineapi.ExplainStored{Present: true, Tier: "hot", SizeBytes: 86118},
		Cluster: bouineapi.ExplainCluster{OwnerPeer: "bouine-2 (10.0.0.2:9010)"},
	}
	stale := &bouineapi.ExplainResult{
		Verdict: "stale-hit", CacheResult: "STALE",
		Key:   bouineapi.ExplainKey{Hex: "1c6e09b3a4d2f877"},
		Route: bouineapi.ExplainRoute{Name: "api", Pool: "origins-api"},
		TTL: bouineapi.ExplainTTL{
			Verdict: "stale-swr", FreshFor: "2m48s0s", Age: "3m12s0s",
			Lifetime: "1m0s", SWR: "5m0s", ExpiresAt: "2026-10-25T06:17:00Z",
		},
		Stored:  bouineapi.ExplainStored{Present: true, Tier: "warm", SizeBytes: 12390},
		Cluster: bouineapi.ExplainCluster{Local: true},
	}
	expired := &bouineapi.ExplainResult{
		Verdict: "miss", CacheResult: "MISS", Reason: "expired",
		Key:   bouineapi.ExplainKey{Hex: "7b0d5c92e1f4a833"},
		Route: bouineapi.ExplainRoute{Name: "api", Pool: "origins-api"},
		TTL: bouineapi.ExplainTTL{
			Verdict: "dead", Age: "1m45s0s", Lifetime: "1m0s",
			ExpiresAt: "2026-10-25T06:16:00Z",
		},
		Stored:  bouineapi.ExplainStored{Present: true, Tier: "warm", SizeBytes: 3277},
		Cluster: bouineapi.ExplainCluster{OwnerPeer: "bouine-2 (10.0.0.2:9010)"},
	}
	banned := &bouineapi.ExplainResult{
		Verdict: "miss", CacheResult: "MISS", Reason: "banned",
		Key:   bouineapi.ExplainKey{Hex: "6b2d9a0e5c7f1a3b"},
		Route: bouineapi.ExplainRoute{Name: "shop", Pool: "origins-shop"},
		TTL: bouineapi.ExplainTTL{
			Verdict: "fresh", FreshFor: "48m0s", Age: "12m0s", Lifetime: "1h0m0s",
			ExpiresAt: "2026-10-25T07:04:00Z",
		},
		Stored: bouineapi.ExplainStored{Present: true, Tier: "hot", SizeBytes: 131328},
		Bans: []bouineapi.ExplainBan{{
			PathRegex: "^/p/products/42", CreatedAt: "2026-10-25T05:54:00Z",
			ExpiresAt: "2026-10-26T05:54:00Z",
		}},
		Cluster: bouineapi.ExplainCluster{OwnerPeer: "bouine-3 (10.0.0.3:9010)"},
	}
	variantAbsent := &bouineapi.ExplainResult{
		Verdict: "miss", CacheResult: "MISS", Reason: "variant-absent",
		Key:   bouineapi.ExplainKey{Hex: "4e8f1a2b7c3d9e6f"},
		Route: bouineapi.ExplainRoute{Name: "images", Pool: "origins-img"},
		Variant: bouineapi.ExplainVariant{
			Vary: "Accept-Encoding", RequestBucket: "br", RequestKeyHex: "abc12345",
			StoredVariants: []bouineapi.ExplainStoredVariant{
				{KeyHex: "11111111", Bucket: "gzip", Tier: "warm", Fresh: true, Age: "3h12m0s", Lifetime: "168h0m0s"},
			},
		},
		TTL:     bouineapi.ExplainTTL{},
		Stored:  bouineapi.ExplainStored{Present: false},
		Cluster: bouineapi.ExplainCluster{OwnerPeer: "bouine-2 (10.0.0.2:9010)"},
	}
	return map[string]*bouineapi.ExplainResult{
		"hit": hit, "stale": stale, "expired": expired,
		"banned": banned, "variant-absent": variantAbsent,
	}
}

// TestExplainRender_FreshHitGolden pins the full plain block byte for
// byte for one representative case: the piped output contract scripts
// and tests diff against.
func TestExplainRender_FreshHitGolden(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	doc := explainFixtures()["hit"]
	doc.URL = "https://shop.example.com/assets/app.4f2c.js"
	want := strings.Join([]string{
		"HIT · fresh · hot tier",
		"https://shop.example.com/assets/app.4f2c.js",
		"  ├ttl 24h┼ swr 5m ┼ sie 30m ┤",
		"  ░██████▏" + strings.Repeat("░", 20),
		"",
		"  key         9f3ab41c77e2d5a8",
		"    host shop.example.com · path /assets/app.4f2c.js · query v=4",
		"  route       static · pool origins-static",
		"  variant     none (no Vary)",
		"  cacheable   yes · Cache-Control: public, max-age=86400",
		"  freshness   fresh for 21h13m · age 2h47m / ttl 24h",
		"    expires at 2026-10-26T04:03:11Z",
		"  stored      hot tier · 84.1KiB · 0 variants · not protected",
		"  bans        none matched",
		"  cluster     key owned by bouine-2 (10.0.0.2:9010) · entry stored on this node",
		"",
	}, "\n")
	assert.Equal(t, want, renderExplainPlain(doc))
}

// TestExplainRender_Headlines pins the verdict headline — line one —
// for every pinned decision case.
func TestExplainRender_Headlines(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"hit":            "HIT · fresh · hot tier",
		"stale":          "HIT · stale · within swr window (5m)",
		"expired":        "MISS · expired · no swr/sie window",
		"banned":         "MISS · banned",
		"variant-absent": "MISS · no stored variant for this request",
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out := renderExplainPlain(explainFixtures()[name])
			assert.Equal(t, want, firstLine(out))
		})
	}
}

// TestExplainRender_StripShape pins the strip's geometry: ruler and
// bar are exactly explainTrackCells wide, the ruler carries the armed
// zone labels, and the fill sits in the current zone.
func TestExplainRender_StripShape(t *testing.T) {
	t.Parallel()
	for name, doc := range explainFixtures() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			lines := strings.Split(renderExplainPlain(doc), "\n")
			var ruler, bar string
			for i, l := range lines {
				if strings.HasPrefix(l, "  ├") && i+1 < len(lines) {
					ruler, bar = lines[i], lines[i+1]
					break
				}
			}
			if doc.TTL.Verdict == "" && name != "variant-absent" {
				assert.Empty(t, ruler, "no lifecycle, no strip")
				return
			}
			require.NotEmpty(t, ruler, "the strip renders when there is a lifecycle")
			assert.Equal(t, explainTrackCells, lipgloss.Width(ruler)-2, "ruler is exactly the track width")
			assert.Equal(t, explainTrackCells, lipgloss.Width(bar)-2, "bar is exactly the track width")
		})
	}
}

// TestExplainRender_StaleFillInWindow pins the remaining-fuel
// semantics on the stale case: the ttl segment sits empty and the
// amber zone's fill lives inside the swr segment.
func TestExplainRender_StaleFillInWindow(t *testing.T) {
	t.Parallel()
	lines := strings.Split(renderExplainPlain(explainFixtures()["stale"]), "\n")
	var ruler, bar string
	for i, l := range lines {
		if strings.HasPrefix(l, "  ├") {
			ruler, bar = strings.TrimPrefix(l, "  "), strings.TrimPrefix(lines[i+1], "  ")
			break
		}
	}
	require.NotEmpty(t, ruler)
	swrStart := strings.Index(ruler, "┼ swr")
	require.NotEqual(t, -1, swrStart, "the swr segment is labeled")
	// Everything before the swr segment is track: the ttl zone is spent.
	assert.NotContains(t, bar[:swrStart], "█", "the ttl segment sits empty in the swr zone")
	assert.Contains(t, bar[swrStart:], "█", "the fill sits inside the swr segment")
}

// TestExplainRender_NoANSIPlain pins the piped contract: zero escape
// codes in plain rendering, for every verdict.
func TestExplainRender_NoANSIPlain(t *testing.T) {
	t.Parallel()
	for name, doc := range explainFixtures() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.NotContains(t, renderExplainPlain(doc), "\x1b")
		})
	}
}

// TestExplainRender_StyledMatchesPlainUnderNoColor pins the
// degradation ladder's bottom rung: the TTY stylist under NO_COLOR
// produces byte-identical output to the plain stylist, so the two
// code paths cannot drift.
func TestExplainRender_StyledMatchesPlainUnderNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	for name, doc := range explainFixtures() {
		styled := renderExplain(doc, tuiExplainStylist(lipgloss.NewRenderer(io.Discard), newTuiPalette(lipgloss.NewRenderer(io.Discard))))
		assert.Equal(t, renderExplainPlain(doc), styled, "case %q", name)
	}
}

// TestExplainAnimation_FinalFrameEqualsStatic pins the acceptance
// criterion: the animation's final frame is byte-identical to the
// static render, so the flourish can never change what a pipe would
// have seen. Also pins that the first frame starts empty.
func TestExplainAnimation_FinalFrameEqualsStatic(t *testing.T) {
	t.Parallel()
	doc := explainFixtures()["hit"]
	meta, ok := newExplainBarMeta(doc)
	require.True(t, ok)
	st := plainExplainStylist()

	m := &explainBarModel{stylist: st, meta: meta}
	first := m.View()
	assert.NotContains(t, first, "█", "the ease starts from an empty fill")

	for range explainEaseSteps {
		_, _ = m.Update(explainBarTickMsg{})
	}
	final := m.View()
	meta.fillScale = 1
	assert.Equal(t, renderExplainBarStrip(meta, st), final,
		"the final frame is the static strip, byte for byte")
}

// TestExplainAnimation_EaseIsMonotonic pins that the fill grows
// monotonically toward the final state — no spring overshoot that
// would flash a fuller bar than the truth.
func TestExplainAnimation_EaseIsMonotonic(t *testing.T) {
	t.Parallel()
	doc := explainFixtures()["hit"]
	_, ok := newExplainBarMeta(doc)
	require.True(t, ok)
	prev := -1.0
	for step := range explainEaseSteps {
		scale := explainEaseOut(float64(step) / float64(explainEaseSteps-1))
		assert.GreaterOrEqual(t, scale, prev)
		prev = scale
	}
	assert.InDelta(t, 1.0, prev, 1e-9)
}

// explainStub serves the hit fixture at /v1/explain.
func explainStub(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/explain", r.URL.Path)
		require.Equal(t, "https://shop.example.com/x?b=1&a=2", r.URL.Query().Get("url"),
			"the URL travels percent-encoded in the query")
		doc := explainFixtures()["hit"]
		doc.URL = "https://shop.example.com/x?b=1&a=2"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestExplainCmd_TextOutput(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	server := explainStub(t)
	stdout, _, err := run(t, "explain", "--server", server, "https://shop.example.com/x?b=1&a=2")
	require.NoError(t, err)
	assert.Contains(t, stdout, "HIT · fresh")
	assert.Contains(t, stdout, "  key         9f3ab41c77e2d5a8")
	assert.NotContains(t, stdout, "\x1b", "piped output never carries escape codes")
}

func TestExplainCmd_JSONOutput(t *testing.T) {
	t.Parallel()
	server := explainStub(t)
	stdout, _, err := run(t, "explain", "--server", server, "--format", "json", "https://shop.example.com/x?b=1&a=2")
	require.NoError(t, err)
	var doc bouineapi.ExplainResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &doc))
	assert.Equal(t, "hit", doc.Verdict)
	assert.Equal(t, "fresh", doc.TTL.Verdict)
}

func TestExplainCmd_RequiresExactlyOneURL(t *testing.T) {
	t.Parallel()
	_, _, err := run(t, "explain")
	assert.Error(t, err)
	_, _, err = run(t, "explain", "a", "b")
	assert.Error(t, err)
}

func firstLine(s string) string {
	return strings.SplitN(s, "\n", 2)[0]
}
