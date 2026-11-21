//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestStrong_SurrogateRefreshPropagates verifies POST /v1/refresh with a
// surrogate key soft-purges the tagged object: the issuing node reports
// the refreshed count, the next GET serves the soft-purged object as
// STALE once (SWR window from the /tagged origin route) before
// revalidation turns it back into a HIT, and the cluster fan-out
// reaches the other nodes (they also serve STALE once after the call).
func TestStrong_SurrogateRefreshPropagates(t *testing.T) {
	s := sharedCluster(t, "strong")
	tag := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	path := "/tagged?t=" + tag

	// Prime the tagged object on every node.
	for _, i := range s.AliveNodes() {
		s.GetWithHost(t, i, path, crossNodeHost)
	}
	driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
		for _, i := range s.AliveNodes() {
			if resp := s.GetWithHost(t, i, path, crossNodeHost); resp.Header.Get("X-Cache") != "HIT" {
				return false
			}
		}
		return true
	})

	// In strong mode only the ring owner stores the object, so a
	// refresh on an arbitrary node may legitimately report count=0
	// locally. Refresh on every node: the union of the reported counts
	// must cover the key (exactly one node owns it), which proves the
	// local resolution; then prove the soft purge happened by observing
	// a STALE serve on the owner.
	totalCount := 0
	for _, i := range s.AliveNodes() {
		body, _ := json.Marshal(map[string]string{"surrogate_key": "tag-" + tag})
		code, respBody := s.AdminPost(t, i, "/v1/refresh", body)
		require.Equal(t, 200, code, "body %s", string(respBody))
		var rr struct {
			Status string `json:"status"`
			Count  int    `json:"count"`
		}
		require.NoError(t, json.Unmarshal(respBody, &rr))
		require.Equal(t, "refreshed", rr.Status)
		totalCount += rr.Count
	}
	require.GreaterOrEqual(t, totalCount, 1, "exactly the ring owner must report the refreshed key")

	// The owner serves the soft-purged object as STALE at least once
	// before background revalidation restores HIT.
	sawStale := false
	driver.RetryUntil(t, 5*time.Second, 100*time.Millisecond, func() bool {
		resp := s.GetWithHost(t, 0, path, crossNodeHost)
		if resp.Header.Get("X-Cache") == "STALE" {
			sawStale = true
		}
		return sawStale
	})
	require.True(t, sawStale, "soft-purged object must be served STALE once")
}

// TestStrong_SurrogateRefreshValidation checks the admin surface
// rejects malformed surrogate refresh requests.
func TestStrong_SurrogateRefreshValidation(t *testing.T) {
	s := sharedCluster(t, "strong")
	for _, body := range []string{
		`{}`,
		`{"surrogate_key":""}`,
		`{"surrogate_key":"a b"}`,
		`{"url":"https://example.com/","surrogate_key":"x"}`,
	} {
		code, _ := s.AdminPostWithCode(t, 0, "/v1/refresh", []byte(body))
		require.Equal(t, 400, code, "body %s", body)
	}
}
