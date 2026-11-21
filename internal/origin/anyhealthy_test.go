package origin

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestAnyHealthy_CachedMatchesScan pins issue #939: the cached
// anyHealthy flag (read by HasHealthyTarget) must agree with a
// from-scratch scan of the targets after every transition path.
func TestAnyHealthy_CachedMatchesScan(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:    "t",
		Targets: []string{"a:1", "b:2"},
		Logger:  testLogger(),
	})
	require.NoError(t, err)
	defer p.Close(t.Context())

	scan := func() bool {
		p.mu.RLock()
		defer p.mu.RUnlock()
		for _, tg := range p.targets {
			if tg.healthy.Load() {
				return true
			}
		}
		return false
	}

	assert.True(t, p.HasHealthyTarget())
	assert.Equal(t, scan(), p.HasHealthyTarget())

	// Eject both via the passive path (threshold 1 equivalent).
	for _, tg := range p.Targets() {
		_ = tg
	}
	p.mu.RLock()
	t0, t1 := p.targets[0], p.targets[1]
	p.mu.RUnlock()
	p.MarkHealthy("") // no-op, just exercises the path
	assert.True(t, p.setTargetHealthy(t0, false))
	assert.Equal(t, scan(), p.HasHealthyTarget(), "one still healthy")
	assert.True(t, p.setTargetHealthy(t1, false))
	assert.False(t, p.HasHealthyTarget(), "all ejected -> false")
	assert.Equal(t, scan(), p.HasHealthyTarget())

	// Restore one via MarkHealthy.
	p.MarkHealthy(t0.addr)
	assert.True(t, p.HasHealthyTarget())
	assert.Equal(t, scan(), p.HasHealthyTarget())
}

// TestAnyHealthy_ConcurrentTransitions runs eject/restore from many
// goroutines under -race and asserts the cached flag always matches a
// scan afterwards (issue #939).
func TestAnyHealthy_ConcurrentTransitions(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:    "race",
		Targets: []string{"a:1", "b:2", "c:3"},
		Logger:  testLogger(),
	})
	require.NoError(t, err)
	defer p.Close(t.Context())

	p.mu.RLock()
	targets := append([]*Target(nil), p.targets...)
	p.mu.RUnlock()

	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tg := targets[i%len(targets)]
			if i%2 == 0 {
				p.setTargetHealthy(tg, false)
			} else {
				p.setTargetHealthy(tg, true)
			}
		}(i)
	}
	wg.Wait()

	p.mu.RLock()
	want := false
	for _, tg := range p.targets {
		if tg.healthy.Load() {
			want = true
		}
	}
	p.mu.RUnlock()
	assert.Equal(t, want, p.HasHealthyTarget(), "cached anyHealthy must match final scan")
}
