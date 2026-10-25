package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// tuiPaletteFor builds a palette over a non-terminal writer, so the
// color profile resolves to Ascii and frames are deterministic in
// tests (no ANSI bytes).
func tuiPaletteFor(w io.Writer) tuiPalette {
	return newTuiPalette(lipgloss.NewRenderer(w))
}

func TestRenderStatusFrame_InlineHasSectionsAndBars(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	report := statusFixture()
	frame := renderStatusFrame(tuiPaletteFor(&buf), &report, nil, frameMeta{width: 100})
	for _, want := range []string{
		"bouine-0", "v0.6.0", "READY",
		" CACHE\n    hit 80.0%", "█", "░",
		"ORIGINS", "10.0.0.1:80", "SICK",
		"passive errors 3", "probe streak 3", "last change 1d ago",
		" CLUSTER\n    peer-fetch", "bouine-0", "live",
		"INSIGHTS", "[high] Hit rate is low",
	} {
		assert.Contains(t, frame, want)
	}
	assert.NotContains(t, frame, "pe:", "counter labels must be human-readable")
	assert.NotContains(t, frame, "hop-limit", "hop-limit was dropped from the cluster line")
	assert.NotContains(t, frame, "q quit", "inline frame has no key footer")
	assert.NotContains(t, frame, "drift", "no deltas without a previous report")
	assert.NotContains(t, frame, "\x1b[", "ascii profile emits no escape sequences")
	assert.NotContains(t, frame, "vv0.6.0", "version is not double-prefixed")
}

// TestRenderStatusFrame_TierBarAlignmentAndUnlimited pins the
// layout contract from the live review: hot and warm bars start at
// the same column, an unbudgeted tier renders an empty bar labeled
// unlimited (not a full bar), and the fill percentage carries its
// percent sign.
func TestRenderStatusFrame_TierBarAlignmentAndUnlimited(t *testing.T) {
	t.Parallel()
	report := statusFixture()
	report.Cache.Hot = bouineapi.StatusTier{Entries: 5, Bytes: 1024, MaxBytes: 4096}
	report.Cache.Warm = bouineapi.StatusTier{Entries: 9, Bytes: 2048} // no budget
	frame := renderStatusFrame(tuiPaletteFor(io.Discard), &report, nil, frameMeta{width: 100})

	hotLine := tierBarLine(t, frame, "hot")
	warmLine := tierBarLine(t, frame, "warm")
	require.Contains(t, hotLine, "1.0KiB / 4.0KiB (25.0%)", "budgeted detail keeps its percent sign")
	require.Contains(t, warmLine, "2.0KiB / unlimited", "unbudgeted tier is labeled unlimited")
	require.NotContains(t, warmLine, "█", "unbudgeted tier renders an empty bar")
	require.Equal(t, strings.IndexAny(hotLine, "█░"), strings.IndexAny(warmLine, "█░"),
		"hot and warm bars must start at the same column")
}

func tierBarLine(t *testing.T, frame, name string) string {
	t.Helper()
	for _, line := range strings.Split(frame, "\n") {
		if strings.Contains(line, name+" ") && strings.ContainsAny(line, "█░") {
			return line
		}
	}
	t.Fatalf("no tier bar line for %s", name)
	return ""
}

func TestRenderStatusFrame_WatchFooterAndDeltas(t *testing.T) {
	t.Parallel()
	prev := statusFixture()
	cur := statusFixture()
	cur.Cache.Hits = 260
	cur.Cache.Misses = 40
	cur.Cache.HitRatio = 0.867
	frame := renderStatusFrame(tuiPaletteFor(io.Discard), &cur, &prev, frameMeta{
		interval: 2 * time.Second, cluster: true, keys: true, width: 100,
	})
	assert.Contains(t, frame, "100/s", "per-interval request rate")
	assert.Contains(t, frame, "+6.7% drift", "hit-ratio drift between frames")
	assert.Contains(t, frame, "cluster on", "cluster toggle state in header")
	assert.Contains(t, frame, "q quit · r refresh · c cluster (on)", "key footer")
}

// TestRenderStatusFrame_InsightActionAlignment pins the aligned
// dash column: with insight titles of different lengths, every
// " - action" must start at the same column.
func TestRenderStatusFrame_InsightActionAlignment(t *testing.T) {
	t.Parallel()
	report := statusFixture()
	report.Insights = []bouineapi.StatusInsight{
		{ID: "one", Severity: "high", Title: "Pool api has 1/1 targets unhealthy", Action: "/dashboard/config"},
		{ID: "two", Severity: "med", Title: "No default TTL", Action: "/dashboard/config"},
		{ID: "three", Severity: "low", Title: "A considerably longer insight title than the rest", Action: "/act"},
	}
	frame := renderStatusFrame(tuiPaletteFor(io.Discard), &report, nil, frameMeta{width: 120})
	var cols []int
	for _, line := range strings.Split(frame, "\n") {
		if i := strings.Index(line, " - "); i >= 0 {
			cols = append(cols, i)
		}
	}
	require.Len(t, cols, 3, "one dash column per insight row")
	for _, col := range cols[1:] {
		require.Equal(t, cols[0], col, "all insight rows align their dash column")
	}
}

func TestRenderStatusFrame_NotReadyAndFetchError(t *testing.T) {
	t.Parallel()
	report := statusFixture()
	report.Node.Ready = false
	frame := renderStatusFrame(tuiPaletteFor(io.Discard), &report, nil,
		frameMeta{width: 100, fetchEr: errStub{"dial tcp: refused"}})
	assert.Contains(t, frame, "NOT-READY")
	assert.Contains(t, frame, "fetch error: dial tcp: refused")
}

func TestRenderBar_Clamps(t *testing.T) {
	t.Parallel()
	p := tuiPaletteFor(io.Discard)
	assert.Equal(t, strings.Repeat("█", 20), renderBar(p, 2.0, 20))
	assert.Equal(t, strings.Repeat("░", 20), renderBar(p, -1, 20))
	assert.Equal(t, strings.Repeat("█", 5)+strings.Repeat("░", 5), renderBar(p, 0.5, 10))
}

func newTestTUI(t *testing.T) *statusTUI {
	t.Helper()
	return newStatusTUI(nil, t.Context(), lipgloss.NewRenderer(io.Discard), false, 2*time.Second)
}

func TestStatusTUI_QuitKeys(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"q", "esc", "ctrl+c"} {
		model := newTestTUI(t)
		_, cmd := model.Update(tea.KeyMsg{Type: keyMsgType(key), Runes: []rune(key)})
		require.NotNil(t, cmd, "%s must quit", key)
		msg := cmd()
		_, ok := msg.(tea.QuitMsg)
		assert.True(t, ok, "%s must produce tea.QuitMsg", key)
	}
}

func TestStatusTUI_ClusterToggleAndRefresh(t *testing.T) {
	t.Parallel()
	model := newTestTUI(t)
	_, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	require.NotNil(t, cmd, "c toggles cluster and refetches")
	assert.True(t, model.cluster)

	fresh := newTestTUI(t)
	_, cmd = fresh.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	assert.NotNil(t, cmd, "r forces a refresh")
	assert.True(t, fresh.pending, "refresh marks an in-flight fetch")

	_, cmd = fresh.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	assert.Nil(t, cmd, "no duplicate in-flight fetch")
}

func TestStatusTUI_StatusMsgTracksPrevAndErrors(t *testing.T) {
	t.Parallel()
	model := newTestTUI(t)
	assert.Contains(t, model.View(), "connecting...", "before first fetch")

	first := statusFixture()
	_, cmd := model.Update(statusMsg{report: &first, at: time.Unix(100, 0).UTC()})
	assert.Nil(t, cmd)
	require.NotNil(t, model.report)
	assert.Nil(t, model.prev, "first frame has no previous")

	second := statusFixture()
	second.Cache.Hits = 300
	_, _ = model.Update(statusMsg{report: &second, at: time.Unix(102, 0).UTC()})
	require.NotNil(t, model.prev)
	assert.Equal(t, int64(80), model.prev.Cache.Hits, "previous frame tracked for deltas")

	// A failed fetch keeps the last good frame and surfaces the error.
	_, _ = model.Update(statusMsg{err: errStub{"boom"}, at: time.Unix(104, 0).UTC()})
	assert.NotNil(t, model.report, "last good report kept")
	assert.Contains(t, model.View(), "fetch error: boom")
}

func TestStatusTUI_WindowResize(t *testing.T) {
	t.Parallel()
	model := newTestTUI(t)
	_, _ = model.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	assert.Equal(t, 120, model.width)
	assert.Equal(t, 40, model.height)
}

// TestStatusRouting_TTYOneShotIsStyled pins the TTY routing: with the
// terminal probe overridden, a one-shot prints the styled inline
// frame (bars, sections) and not the plain line format or key footer.
func TestStatusRouting_TTYOneShotIsStyled(t *testing.T) {
	server := statusStub(t)
	orig := statusIsTTY
	statusIsTTY = func(uintptr) bool { return true }
	t.Cleanup(func() { statusIsTTY = orig })

	stdout, _, err := run(t, "status", "--server", server)
	require.NoError(t, err)
	assert.Contains(t, stdout, "CACHE")
	assert.Contains(t, stdout, "█", "tier bars render")
	assert.Contains(t, stdout, "ORIGINS")
	assert.NotContains(t, stdout, "q quit", "no TUI footer in one-shot")
	assert.NotContains(t, stdout, "hit ratio 80.0%", "plain format is for pipes only")
}

type errStub struct{ msg string }

func (e errStub) Error() string { return e.msg }

// keyMsgType maps the key names used in the quit-key test to tea key
// types; KeyMsg normalization differs per name.
func keyMsgType(name string) tea.KeyType {
	switch name {
	case "q":
		return tea.KeyRunes
	case "esc":
		return tea.KeyEsc
	default:
		return tea.KeyCtrlC
	}
}
