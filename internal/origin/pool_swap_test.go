package origin

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPoolPickDuringTargetSwap hammers pick() while SetTargets swaps
// the target list underneath — issue #951 acceptance: the element read
// must be under RLock, so this runs clean under -race.
func TestPoolPickDuringTargetSwap(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{Name: "api", Targets: []string{"127.0.0.1:9001", "127.0.0.1:9002"}})
	require.NoError(t, err)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = p.pick()
				}
			}
		}()
	}
	for i := range 200 {
		targets := make([]*Target, 0, 2)
		for _, addr := range []string{fmt.Sprintf("127.0.0.1:%d", 9000+i%2+1), "127.0.0.1:9999"} {
			tt, terr := NewTargetForReload(addr, nil)
			require.NoError(t, terr)
			targets = append(targets, tt)
		}
		p.SetTargets(targets)
	}
	close(stop)
	wg.Wait()
}

// TestPoolSetTargetsReusesTargetByAddr: unchanged addresses keep
// their *Target (eject state survives the swap).
func TestPoolSetTargetsReusesTargetByAddr(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{Name: "api", Targets: []string{"127.0.0.1:9001"}})
	require.NoError(t, err)
	existing := p.TargetByAddr("127.0.0.1:9001")
	require.NotNil(t, existing)
	existing.healthy.Store(false)

	fresh, ferr := NewTargetForReload("127.0.0.1:9002", nil)
	require.NoError(t, ferr)
	p.SetTargets([]*Target{existing, fresh})

	assert.Same(t, existing, p.TargetByAddr("127.0.0.1:9001"), "unchanged addr reuses the *Target object")
	assert.False(t, p.TargetByAddr("127.0.0.1:9001").healthy.Load(), "eject state preserved")
	assert.True(t, p.TargetByAddr("127.0.0.1:9002").healthy.Load(), "new addr starts healthy")
	assert.Equal(t, []string{"127.0.0.1:9001", "127.0.0.1:9002"}, p.TargetAddrs())
}
