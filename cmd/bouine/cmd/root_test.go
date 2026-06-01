package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCmdRuns(t *testing.T) {
	t.Parallel()
	root := Root()
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stdout)
	root.SetArgs([]string{"version"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := stdout.String()
	if !strings.HasPrefix(out, "bouine ") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestRootHasExpectedCommands(t *testing.T) {
	t.Parallel()
	want := map[string]bool{
		"version": false,
		"serve":   false,
	}
	for _, c := range Root().Commands() {
		if _, ok := want[c.Name()]; ok {
			want[c.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing subcommand: %s", name)
		}
	}
}
