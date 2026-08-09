package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newCompletionCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "completion [shell]",
		Short: "Generate shell completion script",
		Long: "completion generates a shell completion script for bash, " +
			"zsh, or fish. To load completions in your current shell:\n\n" +
			"  bash:  source <(bouine completion bash)\n" +
			"  zsh:   source <(bouine completion zsh)\n" +
			"  fish:  bouine completion fish | source\n\n" +
			"Or add to your shell profile for persistence.",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"bash", "zsh", "fish"},
		RunE: func(cmd *cobra.Command, args []string) error {
			root := cmd.Root()
			switch args[0] {
			case "bash":
				return root.GenBashCompletion(cmd.OutOrStdout())
			case "zsh":
				return root.GenZshCompletion(cmd.OutOrStdout())
			case "fish":
				return root.GenFishCompletion(cmd.OutOrStdout(), true)
			default:
				return fmt.Errorf("unsupported shell %q (bash, zsh, fish)", args[0])
			}
		},
	}
	return c
}
