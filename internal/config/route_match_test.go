package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validRouteMatchCfg builds a Config whose only route carries the given
// match predicate, so every finding is attributable to that predicate.
func validRouteMatchCfg(m RouteMatch) *Config {
	return &Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "app", Targets: []string{"app.example.svc:8080"}},
		},
		Routes: []Route{{Name: "r", Pool: "app", Match: m}},
	}
}

func TestRouteMatch_PathPrefixOnlyStillValid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validRouteMatchCfg(RouteMatch{PathPrefix: "/api/"}).Validate())
}

func TestRouteMatch_WildcardHostAndRegexPathValid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validRouteMatchCfg(RouteMatch{
		Host: "*.staging.example.com",
		Path: `^/[a-z]{2}-[a-z]{2}/l/campaign-.*$`,
	}).Validate())
}

func TestRouteMatch_PathAndPathPrefixMutuallyExclusive(t *testing.T) {
	t.Parallel()
	err := validRouteMatchCfg(RouteMatch{
		PathPrefix: "/api/",
		Path:       `^/api/.*$`,
	}).Validate()
	requireFieldError(t, err, "routes[0].match", "mutually exclusive")
}

func TestRouteMatch_PathMustBeAnchored(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{
		`/campaign-.*`,  // no anchors at all
		`^/campaign-.*`, // start anchor only
		`/campaign-.*$`, // end anchor only
	} {
		err := validRouteMatchCfg(RouteMatch{Path: pattern}).Validate()
		requireFieldError(t, err, "routes[0].match.path", "must be anchored")
	}
}

func TestRouteMatch_InvalidRegexRejected(t *testing.T) {
	t.Parallel()
	err := validRouteMatchCfg(RouteMatch{Path: `^/([unclosed$`}).Validate()
	requireFieldError(t, err, "routes[0].match.path", "not a valid regular expression")
}

func TestRouteMatch_PathPatternSizeCapped(t *testing.T) {
	t.Parallel()
	pattern := "^/" + strings.Repeat("a", MaxRoutePathPatternBytes) + "$"
	err := validRouteMatchCfg(RouteMatch{Path: pattern}).Validate()
	requireFieldError(t, err, "routes[0].match.path", "exceeds")
}

func TestRouteMatch_ControlBytesRejected(t *testing.T) {
	t.Parallel()
	err := validRouteMatchCfg(RouteMatch{Path: "^/a\x00b$"}).Validate()
	requireFieldError(t, err, "routes[0].match.path", "control")
}

func TestRouteMatch_HostPatternValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		host  string
		valid bool
	}{
		{"www.example.com", true},
		{"*.staging.example.com", true},
		{"*.example.com", true},
		{"", true}, // absent host constraint
		{"staging.example.com", true},
		{"*", false},              // matches every host: dead-configs every later route
		{"*.", false},             // no anchor: suffix no real host matches
		{"*a.example.com", false}, // star must align to the label boundary
		{"www.*.example.com", false},
		{"api.*.com", false},
		{"www.example.*", false}, // trailing wildcard: not the leading "*." form
		{"www.example*", false},
		{"a*b.example.com", false},
		{"**.example.com", false},
		{"*.example.*", false},
	}
	for _, tt := range tests {
		err := validRouteMatchCfg(RouteMatch{Host: tt.host, PathPrefix: "/"}).Validate()
		if tt.valid {
			assert.NoError(t, err, tt.host)
		} else {
			requireFieldError(t, err, "routes[0].match.host", "invalid host pattern")
		}
	}
}

func TestRouteMatch_NameDerivation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		m    RouteMatch
		want string
	}{
		{"host+prefix", RouteMatch{Host: "api.example.com", PathPrefix: "/v1"}, "api.example.com:/v1"},
		{"prefix only", RouteMatch{PathPrefix: "/products"}, "/products"},
		{"host+regex", RouteMatch{Host: "www.example.com", Path: `^/l/campaign-.*$`}, "www.example.com:^/l/campaign-.*$"},
		{"regex only", RouteMatch{Path: `^/[a-z]{2}/l/.*$`}, `^/[a-z]{2}/l/.*$`},
		{"catch-all", RouteMatch{}, "_catch-all"},
	}
	for _, tt := range tests {
		cfg := validRouteMatchCfg(tt.m)
		cfg.Routes[0].Name = ""
		require.NoError(t, cfg.Validate(), tt.name)
		assert.Equal(t, tt.want, cfg.Routes[0].Name, tt.name)
	}
}

func TestRouteMatch_YAMLDecode(t *testing.T) {
	t.Parallel()
	yaml := `
listen:
  admin: ":9000"
upstream_pools:
  - name: app
    targets: [app.example.svc:8080]
routes:
  - name: staging
    pool: app
    match:
      host: "*.staging.example.com"
  - name: campaign-pages
    pool: app
    match:
      host: "www.example.com"
      path: "^/[a-z]{2}-[a-z]{2}/l/campaign-.*$"
    cache:
      ttl_override: 60s
`
	var cfg Config
	require.NoError(t, yamlUnmarshal(t, []byte(yaml), &cfg))
	require.NoError(t, cfg.Validate())
	require.Len(t, cfg.Routes, 2)
	assert.Equal(t, "*.staging.example.com", cfg.Routes[0].Match.Host)
	assert.Equal(t, `^/[a-z]{2}-[a-z]{2}/l/campaign-.*$`, cfg.Routes[1].Match.Path)
	assert.Empty(t, cfg.Routes[1].Match.PathPrefix)

	// The strict decoder (KnownFields, used by Load/Parse) rejects a
	// typo'd key instead of silently dropping the predicate.
	bad := strings.Replace(yaml, "path:", "paths:", 1)
	_, err := Parse([]byte(bad))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "paths")
}

func TestRouteMatch_AllFindingsReportedAtOnce(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "app", Targets: []string{"app.example.svc:8080"}},
		},
		Routes: []Route{
			{Name: "r0", Pool: "app", Match: RouteMatch{Host: "www.*.com", PathPrefix: "/a", Path: `^/a$`}},
			{Name: "r1", Pool: "app", Match: RouteMatch{Path: `/unanchored`}},
		},
	}
	var paths []string
	for _, fe := range fieldErrs(t, cfg.Validate()) {
		paths = append(paths, fe.Path)
	}
	assert.ElementsMatch(t, []string{
		"routes[0].match",
		"routes[0].match.host",
		"routes[1].match.path",
	}, paths)
}

func TestRouteMatch_PathLabel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "/api/", RouteMatch{PathPrefix: "/api/"}.PathLabel())
	assert.Equal(t, `^/a.*$`, RouteMatch{Path: `^/a.*$`}.PathLabel())
	// Path wins when both are set in a hand-built struct: Validate
	// rejects that combination, and the display contract is "the
	// predicate that actually selects the route".
	assert.Equal(t, `^/a.*$`, RouteMatch{PathPrefix: "/a", Path: `^/a.*$`}.PathLabel())
	assert.Empty(t, RouteMatch{}.PathLabel())
}
