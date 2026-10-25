package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// explainEaseSteps is the fill animation's frame budget at ~30 fps:
// ~300 ms of ease-out, then the program quits on the final frame
// (issue #814's flag-less TTY flourish).
const (
	explainEaseSteps = 9
	explainEaseFPS   = 30
)

func newExplainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "explain <url>",
		Short: "Explain the cache decision chain for one URL",
		Long: "Recomputes the full cache decision chain for one URL against " +
			"the node's stored view: key derivation, route, variant bucket, " +
			"cacheability, freshness lifecycle, bans, and ring ownership. " +
			"Read-only on the server. Text mode renders a decision tree with " +
			"a freshness bar; --format json emits the raw document.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := outputFormat(cmd)
			if err != nil {
				return err
			}
			// Ctrl-C must abort the in-flight fetch cleanly, same
			// contract as status.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			client := adminClient(cmd)
			doc, err := client.Explain(ctx, args[0])
			if err != nil {
				return err
			}
			if format == formatJSON {
				out, err := json.MarshalIndent(doc, "", "  ")
				if err != nil {
					return fmt.Errorf("marshal explain: %w", err)
				}
				_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
				return err
			}
			return runExplainText(cmd.OutOrStdout(), doc, os.Stdout.Fd())
		},
	}
}

// runExplainText renders the document: the styled TTY frame (with
// the bar's ease-in animation) on a terminal, the plain line-based
// rendering everywhere else — pipes and scripts never see escape
// sequences (issue #814; same routing probe as bouine status).
func runExplainText(w interface{ Write([]byte) (int, error) }, doc *bouineapi.ExplainResult, fd uintptr) error {
	out, isTTY := w.(*os.File)
	if !isTTY || !statusIsTTY(fd) {
		return printExplain(w, renderExplainPlain(doc))
	}
	renderer := lipgloss.NewRenderer(out)
	if meta, ok := newExplainBarMeta(doc); ok {
		// The animation is decoration; a failing program run must
		// never lose the document.
		_ = animateExplainBar(renderer, meta)
	}
	st := tuiExplainStylist(renderer, newTuiPalette(renderer))
	return printExplain(w, renderExplain(doc, st))
}

// renderExplainPlain renders the byte-stable version: zero escape
// codes, identical modulo ANSI to the styled frame.
func renderExplainPlain(doc *bouineapi.ExplainResult) string {
	var b strings.Builder
	renderExplainText(&b, doc)
	return b.String()
}

func printExplain(w interface{ Write([]byte) (int, error) }, s string) error {
	_, err := fmt.Fprint(w, s)
	return err
}

// animateExplainBar runs the inline fill animation: the current
// zone's fill eases from 0 to final over ~300 ms via a small
// bubbletea program (inline mode, no alt screen), leaving the final
// frame in the scrollback. The final frame is byte-identical to the
// static render — pinned by test — so the animation cannot change
// what a pipe would have seen.
func animateExplainBar(renderer *lipgloss.Renderer, meta explainBarMeta) error {
	palette := newTuiPalette(renderer)
	prog := tea.NewProgram(&explainBarModel{
		stylist: tuiExplainStylist(renderer, palette),
		meta:    meta,
	}, tea.WithInput(nil))
	_, err := prog.Run()
	return err
}

// explainBarModel eases the bar's fillScale from 0 to 1 across
// explainEaseSteps frames with a cubic ease-out, then quits. Any
// keypress skips straight to the final frame.
type explainBarModel struct {
	stylist explainStylist
	meta    explainBarMeta
	step    int
}

type explainBarTickMsg struct{}

func (m *explainBarModel) Init() tea.Cmd {
	return explainBarTick()
}

func explainBarTick() tea.Cmd {
	return tea.Tick(time.Second/explainEaseFPS, func(time.Time) tea.Msg {
		return explainBarTickMsg{}
	})
}

func (m *explainBarModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case explainBarTickMsg:
		m.step++
		if m.step >= explainEaseSteps {
			return m, tea.Quit
		}
		return m, explainBarTick()
	case tea.KeyMsg:
		m.step = explainEaseSteps - 1
		return m, tea.Quit
	}
	return m, nil
}

// View renders the strip at the eased fill. The final frame (step
// explainEaseSteps-1, scale 1) must byte-equal the static strip —
// the test pins it — so the animation ends exactly where the static
// render is.
func (m *explainBarModel) View() string {
	meta := m.meta
	meta.fillScale = explainEaseOut(float64(m.step) / float64(explainEaseSteps-1))
	return renderExplainBarStrip(meta, m.stylist)
}

// explainEaseOut is a cubic ease-out curve on t in [0, 1]: fast out
// of the gate, settling onto the final fill — the spring feel of
// the charm progress family without the dependency.
func explainEaseOut(t float64) float64 {
	if t < 0 {
		return 0
	}
	if t > 1 {
		return 1
	}
	return 1 - (1-t)*(1-t)*(1-t)
}
