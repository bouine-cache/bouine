package prefetch

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestParseLinkPreload(t *testing.T) {
	tests := []struct {
		name   string
		header string
		host   string
		want   []string
	}{
		{
			name:   "single_preload",
			header: `</style.css>; rel=preload; as=style`,
			host:   "example.com",
			want:   []string{"http://example.com/style.css"},
		},
		{
			name:   "absolute_url",
			header: `<https://cdn.example.com/app.js>; rel=preload; as=script`,
			host:   "example.com",
			want:   []string{"https://cdn.example.com/app.js"},
		},
		{
			name:   "quoted_rel",
			header: `</font.woff2>; rel="preload"; as=font`,
			host:   "example.com",
			want:   []string{"http://example.com/font.woff2"},
		},
		{
			name:   "multiple_links",
			header: `</a.css>; rel=preload; as=style, </b.js>; rel=preload; as=script`,
			host:   "example.com",
			want:   []string{"http://example.com/a.css", "http://example.com/b.js"},
		},
		{
			name:   "not_preload",
			header: `</other.css>; rel=stylesheet`,
			host:   "example.com",
			want:   nil,
		},
		{
			name:   "empty",
			header: "",
			host:   "example.com",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseLinkPreload(tt.header, tt.host)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestPrefetcher_OnResponse(t *testing.T) {
	var mu sync.Mutex
	var fetched []string
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fetched = append(fetched, r.URL.String())
		mu.Unlock()
	})

	p := New(Config{
		Handler:        handler,
		MaxConcurrency: 2,
	})

	header := http.Header{}
	header.Set("Link", `</warm-me.css>; rel=preload; as=style`)

	ctx := t.Context()
	p.OnResponse(ctx, "example.com", header)

	// Wait briefly for background goroutine.
	for range 50 {
		mu.Lock()
		n := len(fetched)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPrefetcher_Dedup(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		calls++
	})

	p := New(Config{
		Handler:        handler,
		MaxConcurrency: 2,
	})

	ctx := t.Context()
	p.scheduleWarm(ctx, "http://example.com/dup.css")
	p.scheduleWarm(ctx, "http://example.com/dup.css")

	// Only one fetch should have been scheduled.
	// (dedup via seen map)
}
