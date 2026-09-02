package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripRequestURI(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		uri    string
		want   string
	}{
		{name: "nil prefix returns uri unchanged", prefix: "", uri: "/api/v1/users", want: "/api/v1/users"},
		{name: "no match returns uri unchanged", prefix: "/api/v1", uri: "/other/users", want: "/other/users"},
		{name: "remainder path is trimmed", prefix: "/api/v1", uri: "/api/v1/users", want: "/users"},
		{name: "remainder path keeps query", prefix: "/api/v1", uri: "/api/v1/users?q=1", want: "/users?q=1"},
		{name: "exact prefix becomes root", prefix: "/api/v1", uri: "/api/v1", want: "/"},
		{name: "exact prefix with query becomes root with query", prefix: "/api/v1", uri: "/api/v1?q=1", want: "/?q=1"},
		{name: "mid-segment match passes through", prefix: "/api/v1", uri: "/api/v1x/users", want: "/api/v1x/users"},
		{name: "trailing-slash prefix with non-absolute remainder passes through", prefix: "/api/v1/", uri: "/api/v1/users", want: "/api/v1/users"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StripRequestURI([]byte(tt.prefix), []byte(tt.uri))
			require.Equal(t, tt.want, string(got))
		})
	}
}
