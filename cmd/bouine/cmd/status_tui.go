package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// statusIsTTY is the terminal probe; a var so tests can route the
// non-TTY paths without a pty.
var statusIsTTY = func(fd uintptr) bool {
	return term.IsTerminal(int(fd))
}

// tuiPalette holds the semantic styles. Colors degrade automatically
// with the renderer's profile (NO_COLOR, TERM=dumb, pipes).
type tuiPalette struct {
	title    lipgloss.Style
	dim      lipgloss.Style
	healthy  lipgloss.Style
	sick     lipgloss.Style
	stale    lipgloss.Style
	high     lipgloss.Style
	med      lipgloss.Style
	low      lipgloss.Style
	barFill  lipgloss.Style
	barEmpty lipgloss.Style
}

func newTuiPalette(r *lipgloss.Renderer) tuiPalette {
	return tuiPalette{
		title:    r.NewStyle().Bold(true),
		dim:      r.NewStyle().Faint(true),
		healthy:  r.NewStyle().Foreground(lipgloss.Color("46")),
		sick:     r.NewStyle().Foreground(lipgloss.Color("196")),
		stale:    r.NewStyle().Foreground(lipgloss.Color("220")),
		high:     r.NewStyle().Foreground(lipgloss.Color("196")),
		med:      r.NewStyle().Foreground(lipgloss.Color("220")),
		low:      r.NewStyle().Foreground(lipgloss.Color("46")),
		barFill:  r.NewStyle().Foreground(lipgloss.Color("46")),
		barEmpty: r.NewStyle().Faint(true),
	}
}

// statusTickMsg re-arms the poll loop; statusMsg carries one fetch.
type statusTickMsg struct{}

type statusMsg struct {
	report *bouineapi.StatusReport
	err    error
	at     time.Time
}

// statusTUI is the bubbletea model for `bouine status --watch` on a
// terminal. It is a plain state struct: View and the frame renderer
// are pure functions of it, which is what keeps the TUI testable
// without a pty.
type statusTUI struct { //nolint:govet // fieldalignment: TUI state grouped by role; cold-path UI struct
	client  *bouineapi.Client
	ctx     context.Context
	palette tuiPalette
	report  *bouineapi.StatusReport
	prev    *bouineapi.StatusReport
	fetchEr error
	cluster bool
	pending bool
	now     time.Time
	width   int
	height  int
	cycle   time.Duration
}

func newStatusTUI(client *bouineapi.Client, ctx context.Context, r *lipgloss.Renderer, cluster bool, interval time.Duration) *statusTUI {
	return &statusTUI{
		client:  client,
		ctx:     ctx,
		palette: newTuiPalette(r),
		cluster: cluster,
		width:   80,
		height:  24,
		cycle:   interval,
	}
}

func (m *statusTUI) Init() tea.Cmd {
	return tea.Batch(m.fetch(), tea.Tick(m.cycle, func(time.Time) tea.Msg { return statusTickMsg{} }))
}

func (m *statusTUI) fetch() tea.Cmd {
	if m.pending {
		return nil
	}
	m.pending = true
	client, ctx, cluster := m.client, m.ctx, m.cluster
	return func() tea.Msg {
		report, err := client.Status(ctx, cluster)
		return statusMsg{report: report, err: err, at: time.Now()}
	}
}

func (m *statusTUI) tick() tea.Cmd {
	return tea.Tick(m.cycle, func(time.Time) tea.Msg { return statusTickMsg{} })
}

func (m *statusTUI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case statusTickMsg:
		return m, tea.Batch(m.fetch(), m.tick())
	case statusMsg:
		m.pending = false
		m.now = msg.at
		if msg.err != nil {
			// Keep the last good report on screen; the fetch error is
			// surfaced in the header so a flapping admin endpoint does
			// not blank the dashboard.
			m.fetchEr = msg.err
			return m, nil
		}
		m.fetchEr = nil
		m.prev = m.report
		m.report = msg.report
		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "c":
			m.cluster = !m.cluster
			return m, m.fetch()
		case "r":
			return m, m.fetch()
		}
	}
	return m, nil
}

func (m *statusTUI) View() string {
	if m.report == nil && m.fetchEr != nil {
		return m.palette.sick.Render("error: " + m.fetchEr.Error())
	}
	if m.report == nil {
		return m.palette.dim.Render("connecting...")
	}
	return renderStatusFrame(m.palette, m.report, m.prev, frameMeta{
		interval: m.cycle,
		cluster:  m.cluster,
		keys:     true,
		fetchEr:  m.fetchEr,
		width:    m.width,
	})
}

// runStatusTUI runs the fullscreen alternate-screen program. The
// command context (SIGINT via NotifyContext) quits the program, and
// the SDK now honors cancellation mid-exchange, so a Ctrl-C during an
// in-flight frame aborts promptly.
func runStatusTUI(ctx context.Context, client *bouineapi.Client, cluster bool, interval time.Duration) error {
	renderer := lipgloss.NewRenderer(os.Stdout)
	prog := tea.NewProgram(newStatusTUI(client, ctx, renderer, cluster, interval), tea.WithAltScreen())
	ctxDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			prog.Quit()
		case <-ctxDone:
		}
	}()
	_, err := prog.Run()
	close(ctxDone)
	return err
}

// renderInlineFrame prints the styled frame in the normal buffer for
// a one-shot status on a terminal: no fullscreen, no raw mode, exit
// immediately. The writer is a parameter so the routing tests can
// capture the frame; the profile follows the writer.
func renderInlineFrame(w io.Writer, report *bouineapi.StatusReport, width int) error {
	palette := newTuiPalette(lipgloss.NewRenderer(w))
	_, err := fmt.Fprintln(w, renderStatusFrame(palette, report, nil, frameMeta{width: width}))
	return err
}

// inlineWidth probes the terminal width, defaulting to 80 when
// stdout is not sizeable (tests, exotic consoles).
func inlineWidth() int {
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
		return w
	}
	return 80
}
