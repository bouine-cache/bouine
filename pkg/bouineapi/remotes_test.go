package bouineapi

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_BareHostGetsScheme(t *testing.T) {
	t.Parallel()
	require.Equal(t, "http://127.0.0.1:9000", New("127.0.0.1:9000").BaseURL)
	require.Equal(t, "https://edge.example.net:9000", New("https://edge.example.net:9000").BaseURL)
	require.Equal(t, "http://a:1", New("http://a:1/").BaseURL)
}

func TestFanout_SequentialOrderNeverAborts(t *testing.T) {
	t.Parallel()
	remotes := []Remote{{Name: "a", Server: "a:1"}, {Name: "b", Server: "b:1"}, {Name: "c", Server: "c:1"}}
	var calls []string
	out := Fanout(context.Background(), remotes, func(c *Client) (int, error) {
		calls = append(calls, c.BaseURL)
		if c.BaseURL == "http://b:1" {
			return 0, errors.New("boom")
		}
		return 1, nil
	})
	require.Len(t, out, 3)
	assert.Equal(t, []string{"http://a:1", "http://b:1", "http://c:1"}, calls)
	assert.NoError(t, out[0].Err)
	assert.Equal(t, "boom", out[1].Err.Error())
	assert.NoError(t, out[2].Err)
	assert.Equal(t, "b", out[1].Remote)
}

func TestRemote_MatchesSelector(t *testing.T) {
	t.Parallel()
	r := Remote{Name: "edge", Labels: map[string]string{"tier": "edge", "region": "eu"}}
	assert.True(t, r.MatchesSelector([]string{"tier=edge"}))
	assert.True(t, r.MatchesSelector([]string{"tier=edge", "region=eu"}))
	assert.False(t, r.MatchesSelector([]string{"tier=shield"}))
	assert.False(t, r.MatchesSelector([]string{"tier=edge", "region=us"}))
	assert.False(t, r.MatchesSelector([]string{"noequals"}))
	assert.False(t, r.MatchesSelector([]string{"missing=x"}))
}
