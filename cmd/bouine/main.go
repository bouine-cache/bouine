// Package main is the bouine CLI entrypoint. It wires together the
// Cobra command tree defined under cmd/bouine/cmd.
//
// During phase 0 only `bouine version` and `bouine serve` (admin-only)
// are wired. Subsequent phases extend the command tree.
package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/bouine-cache/bouine/cmd/bouine/cmd"
)

func main() {
	if err := cmd.Root().Execute(); err != nil {
		// Emit a structured JSON error line so log shippers can parse it.
		// Raw fmt.Fprintln would inject a non-JSON line into the log stream,
		// breaking structured log pipelines.
		entry := map[string]any{
			"time":  time.Now().UTC().Format(time.RFC3339Nano),
			"level": "ERROR",
			"msg":   "fatal error",
			"error": err.Error(),
		}
		_ = json.NewEncoder(os.Stderr).Encode(entry)
		os.Exit(1)
	}
}
