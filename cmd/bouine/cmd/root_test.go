package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionCmdRuns(t *testing.T) {
	t.Parallel()
	root := Root()
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stdout)
	root.SetArgs([]string{"version"})
	err := root.Execute()
	require.NoError(t, err, "execute")
	out := stdout.String()
	require.True(t, strings.HasPrefix(out, "bouine "))
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
		assert.Truef(t, found, "missing subcommand: %s", name)
	}
}
