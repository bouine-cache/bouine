package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// explainTrackCells is the freshness bar's fixed track width: never
// terminal-adaptive, so piped output stays diff-stable (issue #814).
const explainTrackCells = 28

// explainStylist abstracts the only difference between the two
// rendering modes: color. The plain stylist (piped output, --format
// text) is the identity for every hook; the TTY stylist wires the
// status palette and the bar's per-cell gradient. The layout is
// written once against these hooks, so plain and styled bytes cannot
// drift: the golden tests pin the plain bytes and assert that the
// styled output stripped of ANSI equals them.
type explainStylist struct {
	verdict func(verdict string, s string) string
	healthy func(s string) string
	sick    func(s string) string
	stale   func(s string) string
	dim     func(s string) string
	// fillCell returns the styler for fill cell idx of fillCount in
	// the given zone ("fresh", "swr", "sie", "dead"). idx runs
	// 0..fillCount-1 over full cells; the partial edge cell uses
	// fillCount-1.
	fillCell func(zone string, idx, fillCount int) func(string) string
}

// plainExplainStylist renders with zero styling: every hook is the
// identity, so the output is pure layout bytes — what a pipe sees.
func plainExplainStylist() explainStylist {
	id := func(s string) string { return s }
	return explainStylist{
		verdict: func(_, s string) string { return s },
		healthy: id,
		sick:    id,
		stale:   id,
		dim:     id,
		fillCell: func(_ string, _, _ int) func(string) string {
			return id
		},
	}
}

// tuiExplainStylist wires the status palette and a truecolor
// gradient over r's renderer, so NO_COLOR / TERM=dumb degrade the
// same bytes to plain automatically.
func tuiExplainStylist(r *lipgloss.Renderer, p tuiPalette) explainStylist {
	return explainStylist{
		verdict: func(verdict string, s string) string {
			switch verdict {
			case "hit":
				return p.healthy.Render(s)
			case "stale-hit":
				return p.stale.Render(s)
			case "miss":
				return p.sick.Render(s)
			default:
				return p.dim.Render(s)
			}
		},
		healthy: func(s string) string { return p.healthy.Render(s) },
		sick:    func(s string) string { return p.sick.Render(s) },
		stale:   func(s string) string { return p.stale.Render(s) },
		dim:     func(s string) string { return p.dim.Render(s) },
		fillCell: func(zone string, idx, fillCount int) func(string) string {
			c := explainGradientColor(zone, idx, fillCount)
			return func(s string) string {
				return r.NewStyle().Foreground(lipgloss.Color(c)).Render(s)
			}
		},
	}
}

// explainGradAnchors holds the truecolor lerp endpoints per zone:
// the fill interpolates from the bright anchor to the deep one as it
// approaches the zone's end. Hand-rolled per-cell lipgloss styling —
// no new dependency (issue #814).
var explainGradAnchors = map[string][2]string{
	"fresh": {"#4ade80", "#16a34a"},
	"swr":   {"#fbbf24", "#d97706"},
	"sie":   {"#f87171", "#b91c1c"},
}

// explainGradientColor returns the hex color for fill cell idx of
// fillCount in zone. t=0 at the fill's start, 1 at its end.
func explainGradientColor(zone string, idx, fillCount int) string {
	anchors, ok := explainGradAnchors[zone]
	if !ok || fillCount <= 1 || idx >= fillCount-1 {
		if !ok {
			return "#808080"
		}
		return anchors[1]
	}
	t := float64(idx) / float64(fillCount-1)
	var fa, ta [3]uint8
	for i := 0; i < 3; i++ {
		fa[i] = hexByte(anchors[0], i*2+1)
		ta[i] = hexByte(anchors[1], i*2+1)
	}
	mixed := [3]uint8{}
	for i := 0; i < 3; i++ {
		mixed[i] = uint8(float64(fa[i]) + t*(float64(ta[i])-float64(fa[i])))
	}
	return fmt.Sprintf("#%02x%02x%02x", mixed[0], mixed[1], mixed[2])
}

// hexByte parses two hex digits out of a #rrggbb string at position
// pos. Callers pass fixed offsets; malformed input yields 0.
func hexByte(hexStr string, pos int) uint8 {
	if len(hexStr) < pos+2 {
		return 0
	}
	hi := hexNibble(hexStr[pos])
	lo := hexNibble(hexStr[pos+1])
	return hi<<4 | lo
}

func hexNibble(c byte) uint8 {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// explainBarMeta carries the parsed lifecycle inputs the freshness
// bar gauges. remaining is the fuel left in the current zone; swr
// and sie of 0 mean the zone is not armed and gets no ruler segment.
type explainBarMeta struct {
	zone      string // fresh | swr | sie | dead
	ttl       time.Duration
	swr       time.Duration
	sie       time.Duration
	remaining time.Duration
	// fillScale eases the fill for the TTY animation; 1.0 is the
	// steady state and must byte-equal the static render.
	fillScale float64
}

// newExplainBarMeta extracts the bar's inputs from the wire document.
// The strip gauges the resolved object's lifecycle; on a
// variant-absent miss it gauges the freshest stored variant (what IS
// cached for the key) — ttl only, the variant's windows are not on
// the wire. Nothing to gauge (bypass, never stored, banned entry
// already deleted) reports ok=false and no strip renders.
func newExplainBarMeta(doc *bouineapi.ExplainResult) (explainBarMeta, bool) {
	if doc.TTL.Verdict != "" {
		zone := "fresh"
		switch doc.TTL.Verdict {
		case "stale-swr":
			zone = "swr"
		case "stale-sie":
			zone = "sie"
		case "dead":
			zone = "dead"
		}
		return explainBarMeta{
			zone:      zone,
			ttl:       parseDur(doc.TTL.Lifetime),
			swr:       parseDur(doc.TTL.SWR),
			sie:       parseDur(doc.TTL.SIE),
			remaining: parseDur(doc.TTL.FreshFor),
			fillScale: 1,
		}, true
	}
	best, bestFresh, bestRemaining := -1, false, time.Duration(-1)
	for i, sv := range doc.Variant.StoredVariants {
		remaining := parseDur(sv.Lifetime) - parseDur(sv.Age)
		if best == -1 || (sv.Fresh && !bestFresh) || (sv.Fresh == bestFresh && remaining > bestRemaining) {
			best, bestFresh, bestRemaining = i, sv.Fresh, remaining
		}
	}
	if best == -1 {
		return explainBarMeta{}, false
	}
	sv := doc.Variant.StoredVariants[best]
	zone := "dead"
	if sv.Fresh {
		zone = "fresh"
	}
	return explainBarMeta{
		zone:      zone,
		ttl:       parseDur(sv.Lifetime),
		remaining: parseDur(sv.Lifetime) - parseDur(sv.Age),
		fillScale: 1,
	}, true
}

// parseDur reads a Go duration string from the wire document; empty
// or malformed means 0.
func parseDur(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

// explainBarGeometry is the strip's shared column layout: segment
// widths and the bar-cell offset where each zone's fill starts. The
// ruler and the fill line compute from the same numbers so they
// cannot misalign. Bar cell 0 corresponds to the ruler's ├ column;
// zone fills start inside their own segment (after its boundary tick).
type explainBarGeometry struct {
	ttlW      int
	freshFill int // start cell of the ttl zone's fill
	swrW      int
	swrFill   int
	sieW      int
	sieFill   int
}

func explainBarGeometryFor(meta explainBarMeta) explainBarGeometry {
	g := explainBarGeometry{freshFill: 1}
	if meta.swr > 0 {
		g.swrW = lipgloss.Width(" swr " + compactDur(meta.swr) + " ")
	}
	if meta.sie > 0 {
		g.sieW = lipgloss.Width(" sie " + compactDur(meta.sie) + " ")
	}
	boundaries := 0
	if meta.swr > 0 {
		boundaries++
	}
	if meta.sie > 0 {
		boundaries++
	}
	g.ttlW = explainTrackCells - 2 - boundaries - g.swrW - g.sieW
	cur := g.ttlW + 1 // the ┼ column after the ttl segment, or ┤
	if g.swrW > 0 {
		g.swrFill = cur + 1
		cur = g.swrFill + g.swrW - 1
	}
	if g.sieW > 0 {
		g.sieFill = cur + 2
	}
	return g
}

// renderExplainBarStrip renders the two-line freshness strip: a dim
// zone ruler, then the fill bar. Fill semantics are remaining fuel,
// not elapsed time: the fill shows the life left in the *current*
// zone, inside that zone's ruler segment — fresh is green, swr
// amber, sie red, dead empty. meta.fillScale eases the fill for the
// TTY animation; 1.0 must byte-equal the static render.
func renderExplainBarStrip(meta explainBarMeta, st explainStylist) string {
	g := explainBarGeometryFor(meta)
	return "  " + st.dim(explainBarRuler(meta, g)) + "\n  " + explainBarFill(meta, g, st) + "\n"
}

// explainBarRuler lays out the zone ruler: exactly explainTrackCells
// wide. Window segments are label-sized with one space padding each
// side; the ttl segment absorbs the remainder. When the ttl label
// cannot fit with padding it flushes left; when it cannot fit at
// all it drops (durations still live in the freshness row).
func explainBarRuler(meta explainBarMeta, g explainBarGeometry) string {
	var b strings.Builder
	b.WriteString("├")
	ttlLabel := "ttl " + compactDur(meta.ttl)
	switch {
	case g.ttlW >= lipgloss.Width(ttlLabel)+2:
		left := (g.ttlW - lipgloss.Width(ttlLabel) - 2) / 2
		right := g.ttlW - 2 - left - lipgloss.Width(ttlLabel)
		b.WriteString(strings.Repeat("─", left) + " " + ttlLabel + " " + strings.Repeat("─", right))
	case g.ttlW >= lipgloss.Width(ttlLabel):
		b.WriteString(ttlLabel)
	default:
		b.WriteString(strings.Repeat("─", g.ttlW))
	}
	if g.swrW > 0 {
		b.WriteString("┼ swr " + compactDur(meta.swr) + " ")
	}
	if g.sieW > 0 {
		b.WriteString("┼ sie " + compactDur(meta.sie) + " ")
	}
	b.WriteString("┤")
	return b.String()
}

// explainBarFill renders the 28-cell fill line from the geometry:
// gradient cells inside the current zone, an eighth-block partial at
// the fill edge, dim track elsewhere. The partial edge cell takes
// the gradient's end color.
func explainBarFill(meta explainBarMeta, g explainBarGeometry, st explainStylist) string {
	start, width, frac := 0, 0, 0.0
	switch meta.zone {
	case "fresh":
		start, width = g.freshFill, g.ttlW
		if meta.ttl > 0 {
			frac = float64(meta.remaining) / float64(meta.ttl)
		}
	case "swr":
		start, width = g.swrFill, g.swrW
		if meta.swr > 0 {
			frac = float64(meta.remaining) / float64(meta.swr)
		}
	case "sie":
		start, width = g.sieFill, g.sieW
		if meta.sie > 0 {
			frac = float64(meta.remaining) / float64(meta.sie)
		}
	}
	frac *= meta.fillScale
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	fillCells := frac * float64(width)
	full := int(fillCells)
	eighths := int((fillCells - float64(full)) * 8)
	var b strings.Builder
	for i := 0; i < explainTrackCells; i++ {
		switch {
		case i >= start && i < start+full:
			b.WriteString(st.fillCell(meta.zone, i-start, full+1)("█"))
		case i == start+full && eighths > 0:
			b.WriteString(st.fillCell(meta.zone, full, full+1)(eighthRunes[eighths-1]))
		default:
			b.WriteString(st.dim("░"))
		}
	}
	return b.String()
}

// eighthRunes are the block elements from 1/8 to 7/8 fill, the
// sub-cell precision at the fill edge.
var eighthRunes = []string{"▏", "▎", "▍", "▌", "▋", "▊", "▉"}

// compactDur renders a duration for ruler and row labels: zero
// components dropped ("2h47m", "5m", "30s", "0s").
func compactDur(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	var b strings.Builder
	if h := int(d / time.Hour); h > 0 {
		fmt.Fprintf(&b, "%dh", h)
		d %= time.Hour
	}
	if m := int(d / time.Minute); m > 0 {
		fmt.Fprintf(&b, "%dm", m)
		d %= time.Minute
	}
	if s := int(d / time.Second); s > 0 || b.Len() == 0 {
		fmt.Fprintf(&b, "%ds", s)
	}
	return b.String()
}
