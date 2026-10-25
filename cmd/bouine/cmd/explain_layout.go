package cmd

import (
	"fmt"
	"strings"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// explainLabelW is the label column: rows indent 2, labels pad to
// 12, values start at column 15 — the layout issue #814 pins.
const explainLabelW = 12

// renderExplainText writes the plain (piped) rendering: zero
// styling, pure layout bytes.
func renderExplainText(b *strings.Builder, doc *bouineapi.ExplainResult) {
	b.WriteString(renderExplain(doc, plainExplainStylist()))
}

// renderExplainStyled is provided by the command wiring: the TTY
// frame uses the status palette and the bar gradient, which degrade
// to the same bytes under NO_COLOR, TERM=dumb, and non-color
// profiles. See renderExplainTTY in explain.go.

// renderExplain assembles the whole document: the verdict headline,
// the URL, the freshness strip (when there is a lifecycle to gauge),
// and the aligned row block. Pure function of (doc, stylist).
func renderExplain(doc *bouineapi.ExplainResult, st explainStylist) string {
	var b strings.Builder
	verdict, headline := explainHeadline(doc)
	b.WriteString(st.verdict(verdict, headline))
	b.WriteString("\n")
	b.WriteString(doc.URL)
	b.WriteString("\n")
	if meta, ok := newExplainBarMeta(doc); ok {
		b.WriteString(renderExplainBarStrip(meta, st))
	}
	b.WriteString("\n")
	explainRows(&b, doc, st)
	return b.String()
}

// explainHeadline builds the `VERDICT · why` line — the answer on
// line one for a varnishlog reader — and the verdict kind that
// colors it.
func explainHeadline(doc *bouineapi.ExplainResult) (verdict, headline string) {
	switch doc.Verdict {
	case "hit":
		headline = "HIT · fresh"
		if doc.Stored.Tier != "" {
			headline += " · " + doc.Stored.Tier + " tier"
		}
		return "hit", headline
	case "stale-hit":
		if doc.TTL.Verdict == "stale-swr" {
			return "stale-hit", "HIT · stale · within swr window (" + compactDur(parseDur(doc.TTL.SWR)) + ")"
		}
		return "stale-hit", "HIT · stale · within sie window (" + compactDur(parseDur(doc.TTL.SIE)) + ")"
	case "bypass":
		return "bypass", "BYPASS · route does not cache"
	}
	switch doc.Reason {
	case "banned":
		return "miss", "MISS · banned"
	case "expired":
		if parseDur(doc.TTL.SWR) > 0 || parseDur(doc.TTL.SIE) > 0 {
			return "miss", "MISS · expired · windows exhausted"
		}
		return "miss", "MISS · expired · no swr/sie window"
	case "variant-absent":
		return "miss", "MISS · no stored variant for this request"
	case "not-cacheable":
		return "miss", "MISS · not cacheable"
	default:
		return "miss", "MISS · not stored"
	}
}

// explainRows writes the block: one row per JSON-document branch, in
// document order, so text and --format json map 1:1. Every row is
// always present; empty branches render an explicit empty value.
func explainRows(b *strings.Builder, doc *bouineapi.ExplainResult, st explainStylist) {
	explainKeyRow(b, doc, st)
	explainRow(b, st, "route", explainRouteValue(doc))
	explainVariantRow(b, doc, st)
	explainRow(b, st, "cacheable", explainCacheableValue(doc))
	explainFreshnessRow(b, doc, st)
	explainRow(b, st, "stored", explainStoredValue(doc))
	explainBansRow(b, doc, st)
	explainRow(b, st, "cluster", explainClusterValue(doc))
}

// explainRow writes one aligned row: two-space indent, label padded
// to explainLabelW, value at the fixed column.
func explainRow(b *strings.Builder, st explainStylist, label, value string) {
	fmt.Fprintf(b, "  %-*s%s\n", explainLabelW, label, value)
}

// explainCont writes a continuation line under its parent row: the
// 4-space child indent issue #814 pins.
func explainCont(b *strings.Builder, st explainStylist, text string) {
	b.WriteString("    " + st.dim(text) + "\n")
}

func explainKeyRow(b *strings.Builder, doc *bouineapi.ExplainResult, st explainStylist) {
	explainRow(b, st, "key", doc.Key.Hex)
	var parts []string
	for _, step := range doc.Key.Derivation {
		switch step.Step {
		case "host", "path":
			parts = append(parts, step.Step+" "+step.Value)
		case "query":
			parts = append(parts, "query "+step.Value)
		case "query_policy":
			explainCont(b, st, "query stripped "+strings.Join(step.Stripped, ", "))
		}
	}
	if len(parts) > 0 {
		explainCont(b, st, strings.Join(parts, " · "))
	}
}

func explainRouteValue(doc *bouineapi.ExplainResult) string {
	if doc.Route.Name == "" {
		return "no matching route · default key policy"
	}
	if doc.Route.Pool == "" {
		return doc.Route.Name + " · no pool"
	}
	return doc.Route.Name + " · pool " + doc.Route.Pool
}

func explainVariantRow(b *strings.Builder, doc *bouineapi.ExplainResult, st explainStylist) {
	if doc.Variant.Vary == "" {
		explainRow(b, st, "variant", "none (no Vary)")
		return
	}
	explainRow(b, st, "variant", "Vary: "+doc.Variant.Vary)
	labels := make([]string, 0, len(doc.Variant.StoredVariants))
	for _, sv := range doc.Variant.StoredVariants {
		label := sv.Bucket
		if label == "" && len(sv.KeyHex) >= 8 {
			label = sv.KeyHex[:8]
		}
		labels = append(labels, label)
	}
	value := "request bucket " + doc.Variant.RequestBucket
	if doc.Variant.RequestBucket == "" {
		value = "request key " + doc.Variant.RequestKeyHex
	}
	switch {
	case len(labels) == 0:
		value += " · stored: none"
	case !doc.Variant.Matched:
		value += " · stored: " + strings.Join(labels, ", ") + " (no " + doc.Variant.RequestBucket + " variant)"
	default:
		value += " · stored: " + strings.Join(labels, ", ")
	}
	explainCont(b, st, value)
}

func explainCacheableValue(doc *bouineapi.ExplainResult) string {
	switch doc.Cacheable.Verdict {
	case "cacheable":
		return "yes · Cache-Control: " + doc.Cacheable.Directives
	case "not-cacheable":
		return "no · Cache-Control: " + doc.Cacheable.Directives
	default:
		return "unknown · nothing stored to evaluate"
	}
}

func explainFreshnessRow(b *strings.Builder, doc *bouineapi.ExplainResult, st explainStylist) {
	value, cont := explainFreshnessValue(doc)
	explainRow(b, st, "freshness", value)
	if cont != "" {
		explainCont(b, st, cont)
	}
}

func explainFreshnessValue(doc *bouineapi.ExplainResult) (value, cont string) {
	ttl, age := parseDur(doc.TTL.Lifetime), parseDur(doc.TTL.Age)
	stale := age - ttl
	switch doc.TTL.Verdict {
	case "fresh":
		value = "fresh for " + compactDur(parseDur(doc.TTL.FreshFor)) +
			" · age " + compactDur(age) + " / ttl " + compactDur(ttl)
		if doc.TTL.ExpiresAt != "" {
			cont = "expires at " + doc.TTL.ExpiresAt
		}
	case "stale-swr":
		value = "stale since " + compactDur(stale) + " · swr window open for " +
			compactDur(parseDur(doc.TTL.FreshFor)) + " more"
		cont = "age " + compactDur(age) + " · next request revalidates in background"
	case "stale-sie":
		value = "stale since " + compactDur(stale) + " · sie window open for " +
			compactDur(parseDur(doc.TTL.FreshFor)) + " more"
		cont = "age " + compactDur(age) + " · served only when the origin errors"
	case "dead":
		value = "expired " + compactDur(stale) + " ago · age " + compactDur(age) +
			" / ttl " + compactDur(ttl)
		if doc.Stored.Present {
			cont = "expired entry still present · next request goes to origin"
		}
	default:
		switch doc.Reason {
		case "variant-absent":
			if sv := freshestStoredVariant(doc); sv != nil {
				return "stored variants fresh for " + compactDur(parseDur(sv.Lifetime)-parseDur(sv.Age)) +
					" (age " + compactDur(parseDur(sv.Age)) + " / ttl " + compactDur(parseDur(sv.Lifetime)) +
					", wrong variant)", ""
			}
			return "no object for this request", ""
		case "banned":
			return "none · entry invalidated by ban", ""
		default:
			return "nothing stored for this key", ""
		}
	}
	return value, cont
}

// freshestStoredVariant returns the stored variant with the most
// remaining life (fresh preferred), or nil.
func freshestStoredVariant(doc *bouineapi.ExplainResult) *bouineapi.ExplainStoredVariant {
	var best *bouineapi.ExplainStoredVariant
	for i := range doc.Variant.StoredVariants {
		sv := &doc.Variant.StoredVariants[i]
		if best == nil || (sv.Fresh && !best.Fresh) ||
			(sv.Fresh == best.Fresh && parseDur(sv.Lifetime)-parseDur(sv.Age) > parseDur(best.Lifetime)-parseDur(best.Age)) {
			best = sv
		}
	}
	return best
}

func explainStoredValue(doc *bouineapi.ExplainResult) string {
	switch {
	case doc.Stored.Present:
		value := doc.Stored.Tier + " tier · " + humanBytes(doc.Stored.SizeBytes) +
			" · " + fmt.Sprintf("%d", len(doc.Variant.StoredVariants)) + " variants"
		if doc.Stored.Protected {
			return value + " · protected"
		}
		return value + " · not protected"
	case doc.Reason == "banned":
		return "none · invalidated by ban (lazily dropped on lookup)"
	case doc.Reason == "variant-absent":
		return "none for this request · resolver present, variant absent"
	default:
		return "none · nothing stored for this key"
	}
}

func explainBansRow(b *strings.Builder, doc *bouineapi.ExplainResult, st explainStylist) {
	if len(doc.Bans) == 0 {
		explainRow(b, st, "bans", "none matched")
		return
	}
	explainRow(b, st, "bans", fmt.Sprintf("%d matched", len(doc.Bans)))
	for _, ban := range doc.Bans {
		explainCont(b, st, explainBanLine(ban))
	}
}

// explainBanLine renders one matched ban: its pattern, when it was
// placed, and when it expires. now is fixed by the caller so tests
// stay deterministic.
func explainBanLine(ban bouineapi.ExplainBan) string {
	var parts []string
	if ban.HostRegex != "" {
		parts = append(parts, "host "+ban.HostRegex)
	}
	if ban.PathRegex != "" {
		parts = append(parts, "path "+ban.PathRegex)
	}
	if ban.SurrogateKey != "" {
		parts = append(parts, "surrogate "+ban.SurrogateKey)
	}
	return strings.Join(parts, " && ") + " · placed " + ban.CreatedAt +
		" · expires " + ban.ExpiresAt
}

func explainClusterValue(doc *bouineapi.ExplainResult) string {
	if doc.Cluster.Local {
		return "this node owns the key"
	}
	value := "key owned by " + doc.Cluster.OwnerPeer
	if doc.Stored.Present {
		value += " · entry stored on this node"
	}
	return value
}
