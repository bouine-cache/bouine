//go:build !linux

package platform

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoarseNow(t *testing.T) {
	t.Parallel()
	now := CoarseNow()
	assert.False(t, now.IsZero())
	assert.WithinDuration(t, time.Now(), now, time.Second)
}

func TestSetTCPFastOpen_NoOp(t *testing.T) {
	t.Parallel()
	assert.NoError(t, SetTCPFastOpen(0, 16))
}

func TestSetTCPDeferAccept_NoOp(t *testing.T) {
	t.Parallel()
	assert.NoError(t, SetTCPDeferAccept(0, 1))
}

func TestSetReusePort_Unsupported(t *testing.T) {
	t.Parallel()
	err := SetReusePort(0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errReusePortUnsupported))
}

func TestSetTCPQuickAck_NoOp(t *testing.T) {
	t.Parallel()
	assert.NoError(t, SetTCPQuickAck(0))
}

func TestMadviseSequential_NoOp(t *testing.T) {
	t.Parallel()
	assert.NoError(t, MadviseSequential(make([]byte, 1024)))
}

func TestFadviseRandom_NoOp(t *testing.T) {
	t.Parallel()
	assert.NoError(t, FadviseRandom(0, 0, 1024))
}

func TestFadviseWillNeed_NoOp(t *testing.T) {
	t.Parallel()
	assert.NoError(t, FadviseWillNeed(0, 0, 1024))
}

func TestEffectiveGOMAXPROCS(t *testing.T) {
	t.Parallel()
	assert.Equal(t, runtime.NumCPU(), EffectiveGOMAXPROCS())
}

func TestRaiseFileLimit_NoOp(t *testing.T) {
	t.Parallel()
	got, err := RaiseFileLimit(1024)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), got)
}

func TestPwritev_Unsupported(t *testing.T) {
	t.Parallel()
	n, err := Pwritev(0, [][]byte{[]byte("x")}, 0)
	assert.Equal(t, 0, n)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPwritevUnsupported))
}

func TestReusePortSupported(t *testing.T) {
	t.Parallel()
	assert.False(t, ReusePortSupported)
}

func TestMmapPopulate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, MmapPopulate)
}
