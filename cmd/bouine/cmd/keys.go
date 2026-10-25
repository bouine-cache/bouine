package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// keys command flags.
const (
	flagKeysLimit = "limit"
	flagKeysClass = "class"
	flagKeysPeer  = "peer"
)

func newKeysCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "keys",
		Short: "Cache-key queries",
		Long: "keys answers cache questions without storage enumeration. find computes " +
			"the cache key for a URL exactly the way the data plane does (the matched " +
			"route's key policy) and reports the object's state on this node. top and " +
			"recent replay the URL ring's sampled non-HIT traffic — what the node is " +
			"missing on lately. The listing is a request-traffic view, not a storage " +
			"inventory: URL→key is deterministic, key→URL is impossible (the hash is " +
			"irreversible by design).",
	}

	c.AddCommand(newKeysFindCmd())
	c.AddCommand(newKeysTopCmd())
	c.AddCommand(newKeysRecentCmd())
	return c
}

func newKeysFindCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "find <url>",
		Short: "Report the cache state of a URL",
		Long: "find computes the cache key for the URL with the matched route's key policy — " +
			"the same key the data plane computes — and reports the object's state on this " +
			"node: tier, size, TTL, variant fan-out, protection, and ban status. With " +
			"--peer (cluster mode) the ring owner is queried over the peer channel too; " +
			"a peer failure is reported, never hidden, and never masks the local answer. " +
			"Ages are derived from the reported fill time.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			peer, err := cmd.Flags().GetBool(flagKeysPeer)
			if err != nil {
				return err
			}
			res, err := adminClient(cmd).KeysFind(cmd.Context(), args[0], peer)
			if err != nil {
				return err
			}
			return printResult(cmd, renderKeysFind(res), res)
		},
	}
	c.Flags().Bool(flagKeysPeer, false, "also query the ring owner over the peer channel (cluster mode)")
	return c
}

func newKeysTopCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "top [--limit N] [--class miss|stale|revalidated|bypass]",
		Short: "Most-requested URL prefixes — what the node is missing on",
		Long: "top lists the URL prefixes with the most recorded requests, with their " +
			"cache-result breakdown, from the node's URL ring. The ring samples non-HIT " +
			"requests (url_ring_sample_rate) and keeps the first three path segments " +
			"per prefix, so the listing answers where the misses concentrate — not what " +
			"is stored. Node-local: it reflects this pod's traffic only.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			limit, err := cmd.Flags().GetInt(flagKeysLimit)
			if err != nil {
				return err
			}
			class, err := cmd.Flags().GetString(flagKeysClass)
			if err != nil {
				return err
			}
			res, err := adminClient(cmd).KeysTop(cmd.Context(), limit, class)
			if err != nil {
				return err
			}
			return printResult(cmd, renderKeysTop(res), res)
		},
	}
	c.Flags().Int(flagKeysLimit, 20, "maximum prefixes to list (1..512)")
	c.Flags().String(flagKeysClass, "", "keep only prefixes with traffic of one cache result: miss, stale, revalidated, bypass")
	return c
}

func newKeysRecentCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "recent [--limit N] [--class miss|stale|revalidated|bypass]",
		Short: "Most recently active URL prefixes",
		Long: "recent lists the URL prefixes by the time of their most recent recorded " +
			"request, newest first, from the node's URL ring. Same sampling and " +
			"node-local semantics as top — use it to see what just started missing.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			limit, err := cmd.Flags().GetInt(flagKeysLimit)
			if err != nil {
				return err
			}
			class, err := cmd.Flags().GetString(flagKeysClass)
			if err != nil {
				return err
			}
			res, err := adminClient(cmd).KeysRecent(cmd.Context(), limit, class)
			if err != nil {
				return err
			}
			return printResult(cmd, renderKeysTop(res), res)
		},
	}
	c.Flags().Int(flagKeysLimit, 20, "maximum prefixes to list (1..512)")
	c.Flags().String(flagKeysClass, "", "keep only prefixes with traffic of one cache result: miss, stale, revalidated, bypass")
	return c
}

// renderKeysFind renders the find report: the derivation block, then
// the local state, then the owner's answer when one was requested.
func renderKeysFind(res *bouineapi.KeysFindResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "url:          %s\n", res.URL)
	fmt.Fprintf(&b, "key:          %s\n", res.KeyHex)
	if res.Route != "" {
		fmt.Fprintf(&b, "route:        %s\n", res.Route)
	}
	if res.Host != "" {
		fmt.Fprintf(&b, "host/path:    %s%s\n", res.Host, res.Path)
	}
	fmt.Fprintf(&b, "query policy: %s\n", res.QueryPolicy)
	if res.Owner != nil {
		addr := res.Owner.AdminAddr
		if addr == "" {
			addr = res.Owner.Addr
		}
		fmt.Fprintf(&b, "owner:        %s (%s)\n", res.Owner.Name, addr)
	}
	if !res.Local.Present && res.AbsentReason != "" {
		fmt.Fprintf(&b, "local:        absent (%s)\n", res.AbsentReason)
	} else {
		b.WriteString(renderKeyState("local", res.Local, res.Variants))
	}
	if res.OwnerState != nil {
		b.WriteString(renderKeyState("owner state", *res.OwnerState, -1))
	}
	if res.OwnerError != "" {
		fmt.Fprintf(&b, "owner state:  query failed: %s\n", res.OwnerError)
	}
	return strings.TrimRight(b.String(), "\n")
}

// renderKeyState renders one KeyState block: tier, size, freshness,
// identity, and lifecycle flags. variants >= 0 adds the tracked
// variant count (the local block knows it; the peer block does not).
func renderKeyState(label string, st api.KeyState, variants int) string {
	if !st.Present {
		return fmt.Sprintf("%s: absent\n", label)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s", label, keyStateHead(st))
	if flags := keyStateFlags(st, variants); len(flags) > 0 {
		fmt.Fprintf(&b, "\n              %s", strings.Join(flags, "; "))
	}
	b.WriteString("\n")
	return b.String()
}

// keyStateHead renders the state block's first line: the tier and the
// non-zero object facts, comma-joined.
func keyStateHead(st api.KeyState) string {
	var b strings.Builder
	b.WriteString(st.Tier)
	if st.SizeBytes > 0 {
		fmt.Fprintf(&b, ", %d bytes", st.SizeBytes)
	}
	if st.TTLSeconds > 0 {
		fmt.Fprintf(&b, ", ttl %s", keysSeconds(st.TTLSeconds))
	}
	if st.StoredAt != "" {
		fmt.Fprintf(&b, ", age %s", keysAge(st.StoredAt))
	}
	if st.StatusCode > 0 {
		fmt.Fprintf(&b, ", status %d", st.StatusCode)
	}
	if st.Hits > 0 {
		fmt.Fprintf(&b, ", hits %d", st.Hits)
	}
	if st.WindowHits > 0 {
		fmt.Fprintf(&b, ", window hits %d", st.WindowHits)
	}
	if st.Pool != "" {
		fmt.Fprintf(&b, ", pool %s", st.Pool)
	}
	return b.String()
}

// keyStateFlags renders the state block's second line: identity and
// lifecycle markers, semicolon-joined.
func keyStateFlags(st api.KeyState, variants int) []string {
	var flags []string
	if st.Vary != "" {
		flags = append(flags, "vary: "+st.Vary)
	}
	if st.VaryKey != "" {
		flags = append(flags, "variant "+st.VaryKey)
	}
	if variants >= 0 {
		flags = append(flags, fmt.Sprintf("%d tracked variants", variants))
	}
	if st.Banned {
		flags = append(flags, "BANNED: matches an active ban; the next lookup evicts it")
	}
	if st.KeepGrace {
		flags = append(flags, "keep-grace (reaper-spared)")
	}
	if st.BackedToWarm {
		flags = append(flags, "backed to warm")
	}
	if st.Protected {
		flags = append(flags, "protected warm copy")
	}
	if len(st.SurrogateKeys) > 0 {
		flags = append(flags, "surrogate keys: "+strings.Join(st.SurrogateKeys, ","))
	}
	return flags
}

// renderKeysTop renders the ring listing as an aligned table with a
// summary line. Sample rate 0 means every non-HIT request records.
func renderKeysTop(res *bouineapi.KeysTopResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d prefixes", res.Kind, len(res.Entries))
	if res.Class != "" {
		fmt.Fprintf(&b, " (class %s)", res.Class)
	}
	if res.SampleRate > 0 {
		fmt.Fprintf(&b, ", ring sample rate 1-in-%d", res.SampleRate)
	} else {
		b.WriteString(", ring sample rate: every non-HIT request")
	}
	b.WriteString(", non-HIT traffic view\n")
	if len(res.Entries) == 0 {
		return strings.TrimRight(b.String(), "\n")
	}
	prefW, routeW := lipgloss.Width("PREFIX"), lipgloss.Width("ROUTE")
	for _, e := range res.Entries {
		prefW = max(prefW, lipgloss.Width(e.Prefix))
		routeW = max(routeW, lipgloss.Width(e.Route))
	}
	fmt.Fprintf(&b, "%-*s %9s %9s %9s %9s %9s  %-*s  %s\n",
		prefW, "PREFIX", "REQ", "MISS", "STALE", "REVAL", "BYPASS", routeW, "ROUTE", "LAST SEEN")
	for _, e := range res.Entries {
		lastSeen := "-"
		if e.LastSeen != "" {
			lastSeen = e.LastSeen
		}
		fmt.Fprintf(&b, "%-*s %9d %9d %9d %9d %9d  %-*s  %s\n",
			prefW, e.Prefix, e.Requests, e.Misses, e.Stale, e.Revalidated, e.Bypass, routeW, e.Route, lastSeen)
	}
	return strings.TrimRight(b.String(), "\n")
}

// keysSeconds renders a seconds count as a compact duration string.
func keysSeconds(secs int64) string {
	if secs <= 0 {
		return "-"
	}
	return (time.Duration(secs) * time.Second).String()
}

// keysAge renders the age of a fill from its RFC 3339 timestamp.
// Client-clock derived: a display convenience, not a server fact.
func keysAge(storedAt string) string {
	if storedAt == "" {
		return "-"
	}
	t, err := time.Parse(time.RFC3339, storedAt)
	if err != nil {
		return "-"
	}
	return time.Since(t).Round(time.Second).String()
}
