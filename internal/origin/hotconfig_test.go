package origin

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPool_HotKnobRoundTrips pins the hot-config surface on Pool:
// the boot config seeds the atomics, the setters swap them live, and
// the getters read the live value.
func TestPool_HotKnobRoundTrips(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:           "api",
		Targets:        []string{"127.0.0.1:18080"},
		Consecutive5xx: 5,
		EjectFor:       time.Minute,
	})
	require.NoError(t, err)

	assert.Equal(t, 5, p.Consecutive5xx())
	assert.Equal(t, time.Minute, p.EjectFor())

	p.SetConsecutive5xx(10)
	assert.Equal(t, 10, p.Consecutive5xx())
	p.SetConsecutive5xx(0)
	assert.Equal(t, 0, p.Consecutive5xx(), "0 disables passive ejection")

	p.SetEjectFor(30 * time.Second)
	assert.Equal(t, 30*time.Second, p.EjectFor())
}

// TestPool_HotKnobConcurrentSwap exercises the atomic swap under the
// race detector: setters and getters from concurrent goroutines must
// never observe a torn value (the pre-hot-config plain fields were a
// data race on the fetch path).
func TestPool_HotKnobConcurrentSwap(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:    "api",
		Targets: []string{"127.0.0.1:18080"},
	})
	require.NoError(t, err)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 200 {
			p.SetConsecutive5xx(i)
			p.SetEjectFor(time.Duration(i) * time.Second)
		}
		close(stop)
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = p.Consecutive5xx()
				_ = p.EjectFor()
			}
		}
	}()
	wg.Wait()
}

// TestEjectReaper_HonorsRuntimeEjectFor verifies a runtime eject_for
// change reaches the running reaper: shrinking the window restores an
// ejected target on the next tick; setting 0 pauses restores.
func TestEjectReaper_HonorsRuntimeEjectFor(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:     "api",
		Targets:  []string{"127.0.0.1:18080"},
		EjectFor: time.Hour, // reaper starts (boot config sets it)
	})
	require.NoError(t, err)
	reaper := NewEjectReaper(p)
	require.NotNil(t, reaper, "boot-set eject_for starts the reaper")

	target := p.targets[0]
	target.healthy.Store(false)
	target.ejectedAt.Store(time.Now().Add(-2 * time.Minute).UnixNano())

	reaper.reap()
	assert.False(t, target.healthy.Load(), "2m-old ejection survives the 1h boot window")

	p.SetEjectFor(time.Minute)
	reaper.reap()
	assert.True(t, target.healthy.Load(), "shrunk window restores the ejected target")

	// Pause: a fresh ejection is never restored while eject_for is 0.
	target.healthy.Store(false)
	target.ejectedAt.Store(time.Now().Add(-time.Hour).UnixNano())
	p.SetEjectFor(0)
	reaper.reap()
	assert.False(t, target.healthy.Load(), "eject_for 0 pauses restores")
}

// TestNewEjectReaper_UnsetIsNil pins the cold-start boundary the
// eject_for knob documents: no boot value, no reaper.
func TestNewEjectReaper_UnsetIsNil(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:    "api",
		Targets: []string{"127.0.0.1:18080"},
	})
	require.NoError(t, err)
	assert.Nil(t, NewEjectReaper(p))
}
