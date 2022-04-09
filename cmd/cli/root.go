/*
Copyright © 2022 Théotime LEVEQUE

*/
package main

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	raftAddress string
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "bouine",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Purge flags.
	rootCmd.PersistentFlags().StringVarP(&raftAddress, "raft_address", "a", "0.0.0.0:50051", "TCP host+port for this node")
	// Mark flags.
	_ = purgeCmd.MarkFlagRequired("raft_address")
}

func main() {
	Execute()
}
