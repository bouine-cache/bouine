package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_BypassOnUserAgent_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
    cache:
      bypass_on_user_agent:
        - ShoppingFeedBot
        - "*Googlebot*"
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	require.Len(t, cfg.Routes, 1)
	assert.Equal(t, []string{"ShoppingFeedBot", "*Googlebot*"}, cfg.Routes[0].Cache.BypassOnUserAgent)
	require.NoError(t, cfg.Validate())
}

func TestParse_BypassOnUserAgent_DefaultEmpty(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	require.Len(t, cfg.Routes, 1)
	assert.Empty(t, cfg.Routes[0].Cache.BypassOnUserAgent, "bypass_on_user_agent must default to empty (off)")
	require.NoError(t, cfg.Validate())
}

func TestValidate_BypassOnUserAgent_Rejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		patterns    []string
		pathSuffix  string
		msgContains string
	}{
		{
			name:        "empty entry",
			patterns:    []string{"Bot", "  "},
			pathSuffix:  "bypass_on_user_agent[1]",
			msgContains: "non-empty",
		},
		{
			name:        "lone star",
			patterns:    []string{"*"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "matches every request",
		},
		{
			name:        "adjacent stars",
			patterns:    []string{"Bot**1"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "empty wildcard",
		},
		{
			name:        "question mark metachar",
			patterns:    []string{"Bot?"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "only supported wildcard",
		},
		{
			name:        "bracket metachar",
			patterns:    []string{"[Bb]ot"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "only supported wildcard",
		},
		{
			name:        "backslash metachar",
			patterns:    []string{`\*Bot`},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "only supported wildcard",
		},
		{
			name:        "non-ascii",
			patterns:    []string{"Böt"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "printable ASCII",
		},
		{
			name:        "oversized pattern",
			patterns:    []string{strings.Repeat("a", 257)},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "capped at 256 bytes",
		},
		{
			name:        "exact duplicate",
			patterns:    []string{"*Bot*", "*Bot*"},
			pathSuffix:  "bypass_on_user_agent[1]",
			msgContains: "duplicate",
		},
		{
			name:        "case-insensitive duplicate",
			patterns:    []string{"*Bot*", "*bot*"},
			pathSuffix:  "bypass_on_user_agent[1]",
			msgContains: "case-insensitive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.Routes[0].Cache.BypassOnUserAgent = tc.patterns
			err := cfg.Validate()
			requireFieldError(t, err, "routes[0].cache."+tc.pathSuffix, tc.msgContains)
		})
	}
}

func TestValidate_BypassOnUserAgent_TooManyEntries(t *testing.T) {
	t.Parallel()
	patterns := make([]string, 17)
	for i := range patterns {
		patterns[i] = "Bot" + string(rune('a'+i))
	}
	cfg := validBase()
	cfg.Routes[0].Cache.BypassOnUserAgent = patterns
	err := cfg.Validate()
	requireFieldError(t, err, "routes[0].cache.bypass_on_user_agent", "capped at 16 entries")
}

func TestValidate_BypassOnUserAgent_Accepted(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache.BypassOnUserAgent = []string{
		"ShoppingFeedBot",       // exact
		"*Googlebot*",           // substring glob
		"  Mozilla/5.0*Feed*  ", // trimmed, multi-segment, '/' literal
		"*Bingbot",              // suffix glob
		"bot",                   // case-variant of Bot is distinct
	}
	require.NoError(t, cfg.Validate())
}
