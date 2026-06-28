package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/runtime/shutdown"
	"github.com/thylong/bouine/internal/server"

	"golang.org/x/sync/errgroup"
)

// TestShutdown_OrderedDrainBeforeStoreClose verifies the fix for issue #76:
// data-plane listeners must finish draining in-flight requests before
// store.Close runs. We boot a real engine with a slow origin, fire an
// in-flight request, trigger shutdown, and assert that the request
// completes without error (i.e. no "use of closed file" or lost write
// due to store.Close racing listener drain).
func TestShutdown_OrderedDrainBeforeStoreClose(t *testing.T) {
	originSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		_, _ = io.WriteString(w, "slow")
	}))
	defer originSrv.Close()

	dir := t.TempDir()
	cfg := fmt.Sprintf(`
listen:
  http:  "127.0.0.1:18120"
  admin: "127.0.0.1:18121"
upstream_pools:
  - name: echo
    targets: [%q]
routes:
  - match: {}
    pool: echo
`, originSrv.Listener.Addr().String())
	cfgPath := filepath.Join(dir, "bouine.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	root := Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-format", "text"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- root.ExecuteContext(ctx) }()

	waitForPort(t, "127.0.0.1:18121", 3*time.Second)

	reqDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://127.0.0.1:18120/slow")
		if err != nil {
			reqDone <- err
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		reqDone <- nil
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-reqDone:
		if err != nil {
			t.Fatalf("in-flight request failed during shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight request did not complete within timeout")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down in time")
	}
}

// TestListenerShutdown_DrainsInflight verifies that server.Listener.Shutdown
// blocks until in-flight requests complete, which is the prerequisite for
// the ordered-drain fix: the mark-not-ready sequencer step calls
// listener.Shutdown and waits before flush-store runs.
func TestListenerShutdown_DrainsInflight(t *testing.T) {
	var inflightDone atomic.Bool

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		inflightDone.Store(true)
		_, _ = io.WriteString(w, "ok")
	})

	ln := server.NewHTTP(server.ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	serveErr := make(chan error, 1)
	go func() { serveErr <- ln.Serve(context.Background()) }()

	for i := 0; i < 100; i++ {
		if ln.Addr() != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ln.Addr() == "127.0.0.1:0" {
		t.Fatal("listener did not bind")
	}

	reqDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr() + "/test")
		if err != nil {
			reqDone <- err
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		reqDone <- nil
	}()

	time.Sleep(50 * time.Millisecond)

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- ln.Shutdown(context.Background())
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("listener shutdown failed: %v", err)
		}
		if !inflightDone.Load() {
			t.Fatal("listener.Shutdown returned before in-flight request completed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener.Shutdown did not return within timeout")
	}

	select {
	case err := <-reqDone:
		if err != nil {
			t.Fatalf("in-flight request failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete")
	}

	<-serveErr
}

// TestSequencer_ListenerDrainBeforeStoreClose is the core regression test
// for issue #76. It uses the same wiring pattern as registerShutdownSteps:
// the mark-not-ready step drains listeners via errgroup, and the flush-store
// step records its start time. The test asserts that flush-store cannot
// start until after the in-flight request has completed (i.e. listener
// drain happened-before store close).
func TestSequencer_ListenerDrainBeforeStoreClose(t *testing.T) {
	var requestCompletedAt atomic.Int64

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		requestCompletedAt.Store(time.Now().UnixNano())
		_, _ = io.WriteString(w, "ok")
	})

	ln := server.NewHTTP(server.ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})

	serveErr := make(chan error, 1)
	go func() { serveErr <- ln.Serve(context.Background()) }()

	for i := 0; i < 100; i++ {
		if ln.Addr() != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ln.Addr() == "127.0.0.1:0" {
		t.Fatal("listener did not bind")
	}

	go func() {
		resp, _ := http.Get("http://" + ln.Addr() + "/test")
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(50 * time.Millisecond)

	var storeCloseStartedAt atomic.Int64

	seq := shutdown.NewSequencer(nil)
	seq.AddStep("mark-not-ready", 10*time.Second, func(ctx context.Context) error {
		var wg errgroup.Group
		wg.Go(func() error { return ln.Shutdown(ctx) })
		return wg.Wait()
	})
	seq.AddStep("flush-store", 5*time.Second, func(_ context.Context) error {
		storeCloseStartedAt.Store(time.Now().UnixNano())
		return nil
	})

	seq.Execute(context.Background())

	if storeCloseStartedAt.Load() == 0 {
		t.Fatal("flush-store step did not run")
	}
	if requestCompletedAt.Load() == 0 {
		t.Fatal("in-flight request did not complete")
	}
	if storeCloseStartedAt.Load() < requestCompletedAt.Load() {
		t.Fatalf("store.Close started before in-flight request completed: store=%d req=%d",
			storeCloseStartedAt.Load(), requestCompletedAt.Load())
	}

	<-serveErr
}
