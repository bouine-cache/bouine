package supervised

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestGroup_HappyPath(t *testing.T) {
	t.Parallel()
	g := NewGroup(context.Background(), quietLogger())

	var ran atomic.Int32
	g.Go("a", func(_ context.Context) error { ran.Add(1); return nil })
	g.Go("b", func(_ context.Context) error { ran.Add(1); return nil })

	{
		err := g.Wait()
		require.NoErrorf(t, err, "wait: %v", err)
	}
	require.Equal(t, int32(2), ran.Load())
}

func TestGroup_FirstErrorCancelsSiblings(t *testing.T) {
	t.Parallel()
	g := NewGroup(context.Background(), quietLogger())

	first := errors.New("boom")
	g.Go("fail", func(_ context.Context) error { return first })

	cancelled := make(chan struct{})
	g.Go("watch", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			close(cancelled)
			return ctx.Err()
		case <-time.After(2 * time.Second):
			t.Errorf("sibling never cancelled")
			return nil
		}
	})

	err := g.Wait()
	if err == nil || !strings.Contains(err.Error(), "fail") {
		t.Fatalf("want first error wrapped with name; got %v", err)
	}
	<-cancelled
}

func TestGroup_RecoversFromPanic(t *testing.T) {
	t.Parallel()
	g := NewGroup(context.Background(), quietLogger())
	g.Go("kaboom", func(_ context.Context) error { panic("nope") })

	err := g.Wait()
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected panic-wrapped error, got %v", err)
	}
}

func TestGroup_NilLoggerAllowed(t *testing.T) {
	t.Parallel()
	g := NewGroup(context.Background(), nil)
	g.Go("ok", func(_ context.Context) error { return nil })
	{
		err := g.Wait()
		require.NoErrorf(t, err, "wait: %v", err)
	}
}
