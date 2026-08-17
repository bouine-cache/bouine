package warm

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestEstimatedWarmLocHeapBytesAccountsForStructSizes pins the struct
// sizes that EstimatedWarmLocHeapBytes is derived from. The constant is
// rounded up (138 → 160) for alignment and safety margin, so we cannot
// assert exact equality — but we CAN assert that the constant is at
// least the sum of the two struct sizes it must cover (warmLoc + one
// SIEVE entry per indexed key). If warmLoc or evictor.Entry grows a field
// without bumping EstimatedWarmLocHeapBytes, this test fails and the
// warm admission controller no longer silently undercounts its heap
// budget.
//
// Mirrors the hot-tier drift guard in internal/storage/hot_test.go
// (TestObjSize_StructSizeConstantsNotDrifted).
func TestEstimatedWarmLocHeapBytesAccountsForStructSizes(t *testing.T) {
	t.Parallel()
	warmLocSize := int64(unsafe.Sizeof(warmLoc{}))
	sieveEntrySize := int64(unsafe.Sizeof(evictor.Entry[api.Key]{}))
	t.Logf("warmLoc=%d evictor.Entry[api.Key]=%d sum=%d constant=%d",
		warmLocSize, sieveEntrySize, warmLocSize+sieveEntrySize, EstimatedWarmLocHeapBytes)
	// The constant must account for at least the struct bytes plus map
	// overhead (~58 B at load factor 6.5 with 16-byte keys). The map
	// overhead is not a struct we can sizeof, so we assert the constant
	// covers the two struct contributions we can measure and is strictly
	// greater than their sum (the rounding is 160 vs a ~138 basis).
	assert.Greater(t, int64(EstimatedWarmLocHeapBytes), warmLocSize+sieveEntrySize,
		"EstimatedWarmLocHeapBytes must exceed warmLoc+evictor.Entry; if warmLoc grew, bump the constant")
}
