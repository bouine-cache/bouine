package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// doc is a shorthand builder for row-branch fixtures: the fields the
// row renderers read, nothing else.
func doc(verdict, reason string, mutate func(*bouineapi.ExplainResult)) bouineapi.ExplainResult {
	d := bouineapi.ExplainResult{
		Verdict:     verdict,
		CacheResult: "MISS",
		Reason:      reason,
		URL:         "http://example.com/x",
	}
	switch verdict {
	case "hit":
		d.CacheResult = "HIT"
	case "stale-hit":
		d.CacheResult = "STALE"
	}
	if mutate != nil {
		mutate(&d)
	}
	return d
}

// rendered renders a single fixture and returns the block split into
// trimmed lines.
func rendered(d bouineapi.ExplainResult) []string {
	return strings.Split(strings.TrimRight(renderExplainPlain(&d), "\n"), "\n")
}

func rowOf(t *testing.T, lines []string, label string) string {
	t.Helper()
	for _, l := range lines {
		if strings.HasPrefix(l, "  "+label+" ") || strings.HasPrefix(l, "  "+label+"  ") {
			return strings.TrimRight(l, " ")
		}
	}
	t.Fatalf("row %q not found in block:\n%s", label, strings.Join(lines, "\n"))
	return ""
}

// TestExplainRows_RouteVariant pins every route-row branch: named
// route with pool, pool-less route, and the unmatched default.
func TestExplainRows_RouteVariant(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "  route       shop · pool origins-shop",
		rowOf(t, rendered(doc("hit", "", func(d *bouineapi.ExplainResult) {
			d.Route = bouineapi.ExplainRoute{Name: "shop", Pool: "origins-shop"}
		})), "route"))
	assert.Equal(t, "  route       static · no pool",
		rowOf(t, rendered(doc("hit", "", func(d *bouineapi.ExplainResult) {
			d.Route = bouineapi.ExplainRoute{Name: "static"}
		})), "route"))
	assert.Equal(t, "  route       no matching route · default key policy",
		rowOf(t, rendered(doc("miss", "not-stored", nil)), "route"))
}

// TestExplainRows_VariantBranches pins the variant row: no Vary, a
// Vary with no stored variants, and a matched bucket.
func TestExplainRows_VariantBranches(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "  variant     none (no Vary)",
		rowOf(t, rendered(doc("hit", "", nil)), "variant"))

	noStored := rendered(doc("miss", "variant-absent", func(d *bouineapi.ExplainResult) {
		d.Variant = bouineapi.ExplainVariant{
			Vary: "Accept-Encoding", RequestBucket: "br", RequestKeyHex: "abc",
		}
	}))
	assert.Equal(t, "  variant     Vary: Accept-Encoding", rowOf(t, noStored, "variant"))
	assert.Contains(t, strings.Join(noStored, "\n"), "    request bucket br · stored: none",
		"the continuation reports an empty index view")

	matched := rendered(doc("hit", "", func(d *bouineapi.ExplainResult) {
		d.Variant = bouineapi.ExplainVariant{
			Vary: "Accept-Encoding", RequestBucket: "br", Matched: true,
			StoredVariants: []bouineapi.ExplainStoredVariant{
				{KeyHex: "1111", Bucket: "br"}, {KeyHex: "2222", Bucket: "gzip"},
			},
		}
	}))
	assert.Contains(t, strings.Join(matched, "\n"), "    request bucket br · stored: br, gzip")
}

// TestExplainRows_CacheableBranches pins the cacheability row's three
// verdicts and the directives passthrough.
func TestExplainRows_CacheableBranches(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "  cacheable   yes · Cache-Control: public, max-age=86400",
		rowOf(t, rendered(doc("hit", "", func(d *bouineapi.ExplainResult) {
			d.Cacheable = bouineapi.ExplainCacheability{Verdict: "cacheable", Directives: "public, max-age=86400"}
		})), "cacheable"))
	assert.Equal(t, "  cacheable   no · Cache-Control: private",
		rowOf(t, rendered(doc("miss", "not-cacheable", func(d *bouineapi.ExplainResult) {
			d.Cacheable = bouineapi.ExplainCacheability{Verdict: "not-cacheable", Directives: "private"}
		})), "cacheable"))
	assert.Equal(t, "  cacheable   unknown · nothing stored to evaluate",
		rowOf(t, rendered(doc("miss", "not-stored", nil)), "cacheable"))
}

// TestExplainRows_FreshnessBranches pins every freshness row shape:
// fresh, swr, sie, dead-with-entry, and the three nothing-stored
// reasons (variant-absent reports the freshest stored variant).
func TestExplainRows_FreshnessBranches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		d     bouineapi.ExplainResult
		want  string
		wantC string // continuation; empty to skip
	}{
		{
			name: "fresh with armed windows",
			d: doc("hit", "", func(d *bouineapi.ExplainResult) {
				d.TTL = bouineapi.ExplainTTL{Verdict: "fresh", FreshFor: "21h13m0s", Age: "2h47m0s", Lifetime: "24h0m0s", SWR: "5m0s", SIE: "30m0s", ExpiresAt: "2026-10-26T04:03:11Z"}
			}),
			want:  "  freshness   fresh for 21h13m · age 2h47m / ttl 24h",
			wantC: "    expires at 2026-10-26T04:03:11Z",
		},
		{
			name: "stale in swr window",
			d: doc("stale-hit", "", func(d *bouineapi.ExplainResult) {
				d.TTL = bouineapi.ExplainTTL{Verdict: "stale-swr", FreshFor: "2m48s0s", Age: "3m12s0s", Lifetime: "1m0s", SWR: "5m0s"}
			}),
			want:  "  freshness   stale since 2m12s · swr window open for 2m48s more",
			wantC: "    age 3m12s · next request revalidates in background",
		},
		{
			name: "stale in sie window",
			d: doc("stale-hit", "", func(d *bouineapi.ExplainResult) {
				d.TTL = bouineapi.ExplainTTL{Verdict: "stale-sie", FreshFor: "4m30s0s", Age: "5m30s0s", Lifetime: "1m0s", SIE: "5m0s"}
			}),
			want:  "  freshness   stale since 4m30s · sie window open for 4m30s more",
			wantC: "    age 5m30s · served only when the origin errors",
		},
		{
			name: "dead entry still present",
			d: doc("miss", "expired", func(d *bouineapi.ExplainResult) {
				d.TTL = bouineapi.ExplainTTL{Verdict: "dead", Age: "1m45s0s", Lifetime: "1m0s"}
				d.Stored = bouineapi.ExplainStored{Present: true, Tier: "warm"}
			}),
			want:  "  freshness   expired 45s ago · age 1m45s / ttl 1m",
			wantC: "    expired entry still present · next request goes to origin",
		},
		{
			name: "variant absent reports stored variants",
			d: doc("miss", "variant-absent", func(d *bouineapi.ExplainResult) {
				d.Variant = bouineapi.ExplainVariant{
					Vary: "Accept-Encoding", RequestBucket: "br",
					StoredVariants: []bouineapi.ExplainStoredVariant{
						{KeyHex: "1111", Bucket: "gzip", Fresh: true, Age: "3h12m0s", Lifetime: "168h0m0s"},
					},
				}
			}),
			want: "  freshness   stored variants fresh for 164h48m (age 3h12m / ttl 168h, wrong variant)",
		},
		{
			name: "banned nothing to report",
			d:    doc("miss", "banned", nil),
			want: "  freshness   none · entry invalidated by ban",
		},
		{
			name: "never stored",
			d:    doc("miss", "not-stored", nil),
			want: "  freshness   nothing stored for this key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lines := rendered(tc.d)
			assert.Equal(t, tc.want, rowOf(t, lines, "freshness"))
			if tc.wantC != "" {
				assert.Contains(t, strings.Join(lines, "\n"), tc.wantC)
			}
		})
	}
}

// TestExplainRows_StoredBranches pins the stored row: present with and
// without protection, and the three absent stories.
func TestExplainRows_StoredBranches(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "  stored      hot tier · 84.1KiB · 1 variants · protected",
		rowOf(t, rendered(doc("hit", "", func(d *bouineapi.ExplainResult) {
			d.Stored = bouineapi.ExplainStored{Present: true, Tier: "hot", SizeBytes: 86118, Protected: true}
			d.Variant = bouineapi.ExplainVariant{StoredVariants: []bouineapi.ExplainStoredVariant{{KeyHex: "1"}}}
		})), "stored"))
	assert.Equal(t, "  stored      none · invalidated by ban (lazily dropped on lookup)",
		rowOf(t, rendered(doc("miss", "banned", nil)), "stored"))
	assert.Equal(t, "  stored      none for this request · resolver present, variant absent",
		rowOf(t, rendered(doc("miss", "variant-absent", func(d *bouineapi.ExplainResult) {
			d.Variant = bouineapi.ExplainVariant{Vary: "Accept-Encoding", RequestKeyHex: "abc"}
		})), "stored"))
	assert.Equal(t, "  stored      none · nothing stored for this key",
		rowOf(t, rendered(doc("miss", "not-stored", nil)), "stored"))
}

// TestExplainRows_BansAndCluster pins the ban descriptor line (multi-
// condition bans join with &&) and both cluster shapes.
func TestExplainRows_BansAndCluster(t *testing.T) {
	t.Parallel()
	lines := rendered(doc("miss", "banned", func(d *bouineapi.ExplainResult) {
		d.Bans = []bouineapi.ExplainBan{{
			HostRegex: "shop.example.com", PathRegex: "^/p/", SurrogateKey: "products",
			CreatedAt: "2026-10-25T05:54:00Z", ExpiresAt: "2026-10-26T05:54:00Z",
		}}
		d.Cluster = bouineapi.ExplainCluster{OwnerPeer: "bouine-2 (10.0.0.2:9010)"}
		d.Stored = bouineapi.ExplainStored{Present: true, Tier: "hot", SizeBytes: 100}
	}))
	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, "  bans        1 matched")
	assert.Contains(t, joined,
		"    host shop.example.com && path ^/p/ && surrogate products · placed 2026-10-25T05:54:00Z · expires 2026-10-26T05:54:00Z")
	assert.Contains(t, joined, "  cluster     key owned by bouine-2 (10.0.0.2:9010) · entry stored on this node")

	local := rendered(doc("hit", "", func(d *bouineapi.ExplainResult) {
		d.Cluster = bouineapi.ExplainCluster{Local: true}
	}))
	assert.Contains(t, strings.Join(local, "\n"), "  cluster     this node owns the key")
}

// TestExplainHeadline_ExpiredWindowsExhausted pins the headline
// variant where the windows existed but ran out — distinct from the
// never-armed case.
func TestExplainHeadline_ExpiredWindowsExhausted(t *testing.T) {
	t.Parallel()
	out := renderExplainPlain(&bouineapi.ExplainResult{
		Verdict: "miss", CacheResult: "MISS", Reason: "expired",
		TTL: bouineapi.ExplainTTL{Verdict: "dead", SWR: "5m0s", SIE: "30m0s"},
	})
	assert.Equal(t, "MISS · expired · windows exhausted", firstLine(out))
}

// TestExplainBarRuler_LabelFit pins the ruler's degradation ladder:
// centered label with padding when it fits, flush when it just fits,
// dropped entirely when the window labels eat the track.
func TestExplainBarRuler_LabelFit(t *testing.T) {
	t.Parallel()
	// ttl 1h with one 5m window: ttl segment centers the label.
	meta := explainBarMeta{zone: "fresh", ttl: time.Hour, remaining: time.Hour, swr: 5 * time.Minute, fillScale: 1}
	ruler := explainBarRuler(meta, explainBarGeometryFor(meta))
	assert.Equal(t, 28, lipgloss.Width(ruler))
	assert.Contains(t, ruler, " ttl 1h ")
	assert.Contains(t, ruler, "┼ swr 5m ")

	// Huge window labels squeeze the ttl segment below its label
	// length: the label drops, the zone geometry survives.
	meta = explainBarMeta{
		zone: "fresh", ttl: time.Hour, remaining: time.Hour, fillScale: 1,
		swr: 90 * time.Minute, sie: 105 * time.Minute,
	}
	ruler = explainBarRuler(meta, explainBarGeometryFor(meta))
	assert.Equal(t, 28, lipgloss.Width(ruler))
	assert.NotContains(t, ruler, "ttl", "the label cannot fit and drops")
	assert.Contains(t, ruler, "swr 1h30m")
}

// TestExplainBarFill_ZonesAndClamps pins the fill math per zone and
// the fraction clamps: sie fills inside its segment, remaining
// beyond the window clamps to full, negative to empty.
func TestExplainBarFill_ZonesAndClamps(t *testing.T) {
	t.Parallel()
	st := plainExplainStylist()

	// Fresh fill inside the ttl segment.
	meta := explainBarMeta{zone: "fresh", ttl: time.Hour, remaining: 30 * time.Minute, fillScale: 1}
	bar := explainBarFill(meta, explainBarGeometryFor(meta), st)
	require.Equal(t, explainTrackCells, lipgloss.Width(bar))
	assert.Contains(t, bar, "█", "half the ttl window is fuel")

	// Over-full clamps to the segment width: no runaway fill.
	meta.remaining = 2 * time.Hour
	bar = explainBarFill(meta, explainBarGeometryFor(meta), st)
	assert.Equal(t, explainTrackCells, lipgloss.Width(bar))

	// Negative remaining renders the dead bar: all track.
	meta = explainBarMeta{zone: "fresh", ttl: time.Hour, remaining: -time.Minute, fillScale: 1}
	bar = explainBarFill(meta, explainBarGeometryFor(meta), st)
	assert.NotContains(t, bar, "█", "a dead entry has no fuel")
}

// TestExplainBarFill_SIEZone pins the third zone: the fill lives
// inside the sie segment, past both boundaries.
func TestExplainBarFill_SIEZone(t *testing.T) {
	t.Parallel()
	meta := explainBarMeta{
		zone: "sie", ttl: time.Minute, swr: 5 * time.Minute, sie: 10 * time.Minute,
		remaining: 5 * time.Minute, // half the sie window left
		fillScale: 1,
	}
	g := explainBarGeometryFor(meta)
	require.Equal(t, "sie", meta.zone)
	ruler := explainBarRuler(meta, g)
	bar := explainBarFill(meta, g, plainExplainStylist())
	sieAt := strings.Index(ruler, "┼ sie")
	require.NotEqual(t, -1, sieAt)
	// Everything before the sie segment is track: both windows are spent.
	assert.NotContains(t, bar[:sieAt], "█")
	assert.Contains(t, bar[sieAt:], "█")
}

// TestExplainHelpers pins the pure helpers: duration formatting and
// parsing, the gradient endpoints, the ease clamps, and the freshest
// stored variant preference.
func TestExplainHelpers(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "2h47m", compactDur(2*time.Hour+47*time.Minute))
	assert.Equal(t, "5m", compactDur(5*time.Minute))
	assert.Equal(t, "45s", compactDur(45*time.Second))
	assert.Equal(t, "0s", compactDur(0))
	assert.Equal(t, "0s", compactDur(-time.Second))

	assert.Equal(t, time.Minute, parseDur("1m0s"))
	assert.Equal(t, time.Duration(0), parseDur("not-a-duration"))
	assert.Equal(t, time.Duration(0), parseDur("-5m"))

	assert.Equal(t, "#4ade80", explainGradientColor("fresh", 0, 4), "the fill starts bright")
	assert.Equal(t, "#16a34a", explainGradientColor("fresh", 3, 4), "and settles deep")
	assert.Equal(t, "#16a34a", explainGradientColor("fresh", 5, 1), "single-cell fills take the end anchor")
	assert.Equal(t, "#808080", explainGradientColor("unknown-zone", 0, 4), "unknown zones stay gray")
	assert.Equal(t, "#f87171", explainGradientColor("sie", 0, 2), "the sie fill starts bright red")
	assert.Equal(t, "#fbbf24", explainGradientColor("swr", 0, 3), "the swr fill starts amber")

	assert.InDelta(t, 0, explainEaseOut(-1), 1e-9)
	assert.InDelta(t, 1, explainEaseOut(2), 1e-9)

	fresh := bouineapi.ExplainStoredVariant{Fresh: true, Age: "1m0s", Lifetime: "10m0s"}
	stale := bouineapi.ExplainStoredVariant{Age: "1h0m0s", Lifetime: "100h0m0s"}
	result := bouineapi.ExplainResult{}
	result.Variant.StoredVariants = []bouineapi.ExplainStoredVariant{stale, fresh}
	got := freshestStoredVariant(&result)
	require.NotNil(t, got)
	assert.Equal(t, fresh, *got, "fresh beats stale regardless of raw remaining")
	assert.Nil(t, freshestStoredVariant(&bouineapi.ExplainResult{}))
}
