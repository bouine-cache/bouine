package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// parityObj builds an object the way buildObject does: the header map,
// the pre-parsed CacheControl string, and the pre-parsed response flags
// (RespNoCache / RespMustRevalidate) stay consistent, as they are for
// every real stored object.
func parityObj(cc string, ttl time.Duration, age time.Duration) *api.Object {
	obj := freshObj(ttl)
	obj.StoredAt = time.Now().Add(-age)
	if cc != "" {
		obj.Header.Set(header.CacheControl, cc)
		obj.CacheControl = cc
		parsed := ParseCacheControl(cc)
		obj.RespNoCache = parsed.NoCache
		obj.RespMustRevalidate = parsed.MustRevalidate || parsed.ProxyRevalidate
	}
	return obj
}

// TestEvaluateCrossPathParity pins issue #589's contract: the header.Map
// serving path (Evaluate), the fasthttp Peek path (evaluateFast), and the
// RawRequest fast-path core (evaluate + respGateFromObject) must return
// identical decisions for the same request/response directives. The rows
// include the branches the old RawRequest mirror lacked (stale-if-error,
// heuristic freshness, validator-aware no-cache).
func TestEvaluateCrossPathParity(t *testing.T) {
	t.Parallel()
	now := time.Now()

	cases := []struct {
		name    string
		reqCC   string
		obj     *api.Object
		want    Decision
		wantObj bool
	}{
		{
			name:    "fresh hit",
			obj:     parityObj("max-age=60", time.Minute, time.Second),
			want:    Hit,
			wantObj: true,
		},
		{
			name:  "request no-store bypasses",
			reqCC: "no-store",
			obj:   parityObj("max-age=60", time.Minute, time.Second),
			want:  Bypass,
		},
		{
			name:    "response no-cache with validator revalidates",
			obj:     parityObj("no-cache", time.Minute, time.Second),
			want:    Revalidate,
			wantObj: true,
		},
		{
			name: "response no-cache without validator misses",
			obj: func() *api.Object {
				o := parityObj("no-cache", time.Minute, time.Second)
				o.ETag = ""
				return o
			}(),
			want: Miss,
		},
		{
			name:    "request no-cache with validator revalidates",
			reqCC:   "no-cache",
			obj:     parityObj("max-age=60", time.Minute, time.Second),
			want:    Revalidate,
			wantObj: true,
		},
		{
			name:    "stale within max-stale is a stale hit",
			reqCC:   "max-stale=60",
			obj:     parityObj("max-age=1", time.Second, 10*time.Second),
			want:    StaleHit,
			wantObj: true,
		},
		{
			name: "stale within stale-while-revalidate is a stale hit",
			obj: func() *api.Object {
				o := parityObj("max-age=1", time.Second, 10*time.Second)
				o.StaleWhileRevalidate = time.Minute
				return o
			}(),
			want:    StaleHit,
			wantObj: true,
		},
		{
			// Repaired on the fast path by this unification: the old
			// evaluateFromRaw mirror had no SIE branch.
			name: "stale within stale-if-error revalidates first",
			obj: func() *api.Object {
				o := parityObj("max-age=1", time.Second, 10*time.Second)
				o.StaleIfError = 5 * time.Minute
				return o
			}(),
			want:    Revalidate,
			wantObj: true,
		},
		{
			name:    "stale with must-revalidate revalidates",
			obj:     parityObj("max-age=1, must-revalidate", time.Second, 10*time.Second),
			want:    Revalidate,
			wantObj: true,
		},
		{
			name:    "stale with proxy-revalidate revalidates",
			obj:     parityObj("max-age=1, proxy-revalidate", time.Second, 10*time.Second),
			want:    Revalidate,
			wantObj: true,
		},
		{
			// Repaired on the fast path by this unification: the old
			// evaluateFromRaw mirror had no heuristic-freshness branch.
			name: "heuristic-freshness stale object is a stale hit",
			obj: func() *api.Object {
				o := freshObj(time.Second)
				o.StoredAt = now.Add(-10 * time.Second)
				// No explicit freshness: drop max-age from map and field.
				o.Header.Del(header.CacheControl)
				o.CacheControl = ""
				o.RespNoCache = false
				o.RespMustRevalidate = false
				return o
			}(),
			want:    StaleHit,
			wantObj: true,
		},
		{
			name: "heuristic object with explicit Expires revalidates",
			obj: func() *api.Object {
				o := freshObj(time.Second)
				o.StoredAt = now.Add(-10 * time.Second)
				o.Header.Del(header.CacheControl)
				o.CacheControl = ""
				o.Header.Set(header.Expires, "Wed, 21 Oct 2026 07:28:00 GMT")
				return o
			}(),
			want:    Revalidate,
			wantObj: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Path 1: header.Map serving path.
			r := testCtx("GET", "http://example.com/")
			if tc.reqCC != "" {
				r.Request.Header.Set(header.CacheControl, tc.reqCC)
			}
			dMap := Evaluate(requestInfoFromCtx(r), tc.obj, now)

			// Path 2: fasthttp Peek path.
			r2 := testCtx("GET", "http://example.com/")
			if tc.reqCC != "" {
				r2.Request.Header.Set(header.CacheControl, tc.reqCC)
			}
			dFast := evaluateFast(r2, tc.obj, now)

			// Path 3: RawRequest fast-path core, as called by TryHit with
			// directives parsed from CacheControlRaw.
			reqCC := Directives{}
			if tc.reqCC != "" {
				reqCC = ParseCacheControl(tc.reqCC)
			}
			dRaw := evaluate(tc.obj, reqCC, respGateFromObject(tc.obj), now)

			for name, d := range map[string]Disposition{
				"Evaluate(header.Map)": dMap,
				"evaluateFast(Peek)":   dFast,
				"evaluate(fastpath)":   dRaw,
			} {
				require.Equal(t, tc.want, d.Decision, "%s: decision mismatch", name)
				require.Equal(t, tc.wantObj, d.Object != nil, "%s: object presence mismatch", name)
			}
		})
	}
}
