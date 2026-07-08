// Package main is the bouine CLI entrypoint. It wires together the
// Cobra command tree defined under cmd/bouine/cmd.
//
// During phase 0 only `bouine version` and `bouine serve` (admin-only)
// are wired. Subsequent phases extend the command tree.
package main

import (
	"fmt"
	"os"

	"github.com/bouine-cache/bouine/cmd/bouine/cmd"
)

func main() {
	if err := cmd.Root().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
