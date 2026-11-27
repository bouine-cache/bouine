package bouineapi

import (
	"context"
	"strings"
)

// Remote names one bouine admin deployment. Server is either a bare
// host:port, an http(s):// URL, or kube://<namespace>/<service>:<port>
// for access through the Kubernetes API-server services/proxy
// subresource.
//
// Stable.
type Remote struct {
	Labels map[string]string
	Name   string
	Server string
	Token  string
	// AllowInsecure permits plaintext http:// writes to non-loopback
	// servers. Off by default; loopback remotes are always allowed.
	AllowInsecure bool
}

// Outcome is the per-remote result of a Fanout call.
//
// Stable.
type Outcome[T any] struct {
	Result T
	Err    error
	Remote string
}

// Fanout runs fn against each remote sequentially, in declaration
// order, and never aborts early: a failed remote is recorded in its
// Outcome and the run continues.
//
// Stable.
func Fanout[T any](ctx context.Context, remotes []Remote, fn func(*Client) (T, error)) []Outcome[T] {
	out := make([]Outcome[T], 0, len(remotes))
	for _, r := range remotes {
		if err := ctx.Err(); err != nil {
			out = append(out, Outcome[T]{Remote: r.Name, Err: err})
			continue
		}
		client := New(r.Server).WithToken(r.Token)
		res, err := fn(client)
		out = append(out, Outcome[T]{Remote: r.Name, Result: res, Err: err})
	}
	return out
}

// MatchesSelector reports whether the remote's labels contain every
// key=value pair in the selector. pairs are "k=v" strings, ANDed.
func (r Remote) MatchesSelector(pairs []string) bool {
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			return false
		}
		if r.Labels[k] != v {
			return false
		}
	}
	return true
}
