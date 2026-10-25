package cmd

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/internal/config"
)

// configDiffRow is one differing leaf between two effective configs.
type configDiffRow struct {
	Path string `json:"path"`
	A    string `json:"a"`
	B    string `json:"b"`
}

var durationType = reflect.TypeOf(time.Duration(0))

// runConfigFileDiff previews a deploy: both files go through
// config.Load (so defaults are filled and both sides are validated),
// then every differing leaf is reported with its YAML path.
func runConfigFileDiff(cmd *cobra.Command, pathA, pathB string) error {
	a, err := config.Load(pathA)
	if err != nil {
		return fmt.Errorf("%s: %w", pathA, err)
	}
	b, err := config.Load(pathB)
	if err != nil {
		return fmt.Errorf("%s: %w", pathB, err)
	}
	rows := diffConfigs(a, b)
	return printResult(cmd, renderConfigDiffRows(rows), rows)
}

// diffConfigs walks two configs in lockstep and returns every
// differing leaf, sorted by path. Paths use YAML field names so they
// match the config file and the hot-knob keys (cluster.ban_ttl).
func diffConfigs(a, b *config.Config) []configDiffRow {
	var rows []configDiffRow
	diffConfigValue(reflect.ValueOf(a), reflect.ValueOf(b), "", &rows)
	sort.Slice(rows, func(i, j int) bool { return rows[i].Path < rows[j].Path })
	return rows
}

// diffConfigValue records differences between two values of the same
// type into rows. Pointer pairs are dereferenced; nil-vs-set is a
// leaf difference.
func diffConfigValue(a, b reflect.Value, path string, rows *[]configDiffRow) {
	if a.Kind() == reflect.Pointer {
		if a.IsNil() != b.IsNil() {
			*rows = append(*rows, configDiffRow{path, formatConfigValue(a), formatConfigValue(b)})
			return
		}
		if a.IsNil() {
			return
		}
		diffConfigValue(a.Elem(), b.Elem(), path, rows)
		return
	}
	switch a.Kind() {
	case reflect.Struct:
		diffConfigStruct(a, b, path, rows)
	case reflect.Slice, reflect.Array:
		diffConfigSlice(a, b, path, rows)
	case reflect.Map:
		diffConfigMap(a, b, path, rows)
	default:
		diffConfigLeaf(a, b, path, rows)
	}
}

func diffConfigStruct(a, b reflect.Value, path string, rows *[]configDiffRow) {
	for i := range a.NumField() {
		field := a.Type().Field(i)
		if !field.IsExported() {
			continue
		}
		name := configFieldName(field)
		if name == "" {
			continue
		}
		diffConfigValue(a.Field(i), b.Field(i), joinConfigPath(path, name), rows)
	}
}

// diffConfigSlice compares element-wise by index; an index present on
// only one side is reported against "<absent>".
func diffConfigSlice(a, b reflect.Value, path string, rows *[]configDiffRow) {
	for i := range max(a.Len(), b.Len()) {
		child := fmt.Sprintf("%s[%d]", path, i)
		switch {
		case i >= a.Len():
			*rows = append(*rows, configDiffRow{child, "<absent>", formatConfigValue(b.Index(i))})
		case i >= b.Len():
			*rows = append(*rows, configDiffRow{child, formatConfigValue(a.Index(i)), "<absent>"})
		default:
			diffConfigValue(a.Index(i), b.Index(i), child, rows)
		}
	}
}

// diffConfigMap compares the union of both key sets, sorted for
// deterministic output; config maps are string-keyed.
func diffConfigMap(a, b reflect.Value, path string, rows *[]configDiffRow) {
	keys := make([]string, 0, a.Len()+b.Len())
	seen := make(map[string]struct{}, a.Len()+b.Len())
	for _, k := range append(a.MapKeys(), b.MapKeys()...) {
		key := fmt.Sprintf("%v", k.Interface())
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		child := joinConfigPath(path, key)
		av := a.MapIndex(reflect.ValueOf(key).Convert(a.Type().Key()))
		bv := b.MapIndex(reflect.ValueOf(key).Convert(b.Type().Key()))
		switch {
		case av.IsValid() && bv.IsValid():
			diffConfigValue(av, bv, child, rows)
		case av.IsValid():
			*rows = append(*rows, configDiffRow{child, formatConfigValue(av), "<absent>"})
		default:
			*rows = append(*rows, configDiffRow{child, "<absent>", formatConfigValue(bv)})
		}
	}
}

func diffConfigLeaf(a, b reflect.Value, path string, rows *[]configDiffRow) {
	if a.Type() == durationType {
		if a.Int() != b.Int() {
			*rows = append(*rows, configDiffRow{
				path, time.Duration(a.Int()).String(), time.Duration(b.Int()).String(),
			})
		}
		return
	}
	if !a.Equal(b) {
		*rows = append(*rows, configDiffRow{path, formatConfigValue(a), formatConfigValue(b)})
	}
}

// formatConfigValue renders one side of a difference; durations are
// humanized (24h0m0s, not nanoseconds) so the output is readable.
func formatConfigValue(v reflect.Value) string {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "<unset>"
		}
		return formatConfigValue(v.Elem())
	}
	if v.Type() == durationType {
		return time.Duration(v.Int()).String()
	}
	return fmt.Sprintf("%v", v.Interface())
}

// configFieldName maps a struct field to its YAML key so diff paths
// match the config file vocabulary. Returns "" for fields excluded
// from serialization ("-").
func configFieldName(field reflect.StructField) string {
	for _, tag := range []string{"yaml", "json"} {
		if name, _, _ := strings.Cut(field.Tag.Get(tag), ","); name != "" {
			return name
		}
	}
	return strings.ToLower(field.Name)
}

func joinConfigPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

func renderConfigDiffRows(rows []configDiffRow) string {
	if len(rows) == 0 {
		return "no differences"
	}
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("%s: %s -> %s", r.Path, r.A, r.B))
	}
	return strings.Join(lines, "\n")
}
