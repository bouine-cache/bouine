package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newRemoteCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "remote",
		Short: "List and manage named admin remotes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return listRemotes(cmd)
		},
	}
	use := &cobra.Command{
		Use:   "use <name>",
		Short: "Set current-remote in the remotes file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := remotesFilePath(cmd)
			rf, err := loadRemotesFile(path)
			if err != nil {
				return err
			}
			found := false
			for _, r := range rf.Remotes {
				if r.Name == args[0] {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unknown remote %q", args[0])
			}
			return setCurrentRemote(path, args[0])
		},
	}
	del := &cobra.Command{
		Use:   "delete <name>",
		Short: "Remove a remote from the remotes file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := remotesFilePath(cmd)
			rf, err := loadRemotesFile(path)
			if err != nil {
				return err
			}
			kept := rf.Remotes[:0]
			removed := false
			for _, r := range rf.Remotes {
				if r.Name == args[0] {
					removed = true
					if rf.CurrentRemote == args[0] {
						rf.CurrentRemote = ""
					}
					continue
				}
				kept = append(kept, r)
			}
			if !removed {
				return fmt.Errorf("unknown remote %q", args[0])
			}
			rf.Remotes = kept
			return writeRemotesFile(path, rf)
		},
	}
	c.AddCommand(use, del)
	return c
}

func listRemotes(cmd *cobra.Command) error {
	rf, err := loadRemotesFile(remotesFilePath(cmd))
	if err != nil {
		return err
	}
	if len(rf.Remotes) == 0 {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "no remotes defined")
		return nil
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%-4s %-12s %-30s %s\n", "CUR", "NAME", "SERVER", "LABELS")
	for _, r := range rf.Remotes {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%-4s %-12s %-30s %s [%s]\n",
			currentMark(r.Name == rf.CurrentRemote), r.Name, r.Server, labelsString(r.Labels), credSource(r))
	}
	return nil
}

func currentMark(cur bool) string {
	if cur {
		return "*"
	}
	return ""
}

func credSource(r remoteEntry) string {
	switch {
	case r.TokenEnv != "":
		return "env:" + r.TokenEnv
	case r.TokenFile != "":
		return "file:" + r.TokenFile
	}
	return "none"
}

func labelsString(labels map[string]string) string {
	if len(labels) == 0 {
		return "{}"
	}
	out := "{"
	first := true
	for k, v := range labels {
		if !first {
			out += ","
		}
		first = false
		out += k + "=" + v
	}
	return out + "}"
}

// setCurrentRemote rewrites current-remote in the remotes file while
// preserving comments and key order via yaml.Node.
func setCurrentRemote(path, name string) error {
	if path == "" {
		return fmt.Errorf("no remotes file configured (--remotes-file or $BOUINE_REMOTES_FILE)")
	}
	data, err := readRemotesRaw(path)
	if err != nil {
		return err
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("parse remotes file %s: %w", path, err)
	}
	root := mapping(&node)
	if root == nil {
		node.Kind = yaml.DocumentNode
		node.Content = []*yaml.Node{{Kind: yaml.MappingNode}}
		root = node.Content[0]
	}
	setMappingValue(root, "current-remote", name)
	out, err := yaml.Marshal(&node)
	if err != nil {
		return err
	}
	return writeRemotesRaw(path, out)
}

func writeRemotesFile(path string, rf *remotesFile) error {
	out, err := yaml.Marshal(rf)
	if err != nil {
		return err
	}
	return writeRemotesRaw(path, out)
}

func readRemotesRaw(path string) ([]byte, error) {
	return os.ReadFile(path) //nolint:gosec // operator-configured remotes file path
}

func writeRemotesRaw(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}

func mapping(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		node = node.Content[0]
	}
	if node.Kind == yaml.MappingNode {
		return node
	}
	return nil
}

func setMappingValue(m *yaml.Node, key, value string) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1].Value = value
			return
		}
	}
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Value: value})
}
