package poll

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestEventuallySucceeds(t *testing.T) {
	var n atomic.Int32
	go func() {
		time.Sleep(5 * time.Millisecond)
		n.Store(1)
	}()
	Eventually(t, 2*time.Second, 2*time.Millisecond, func() bool { return n.Load() == 1 })
}
