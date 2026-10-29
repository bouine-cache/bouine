package storage

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// banListBenchCaps spans the default cap to a list large enough to
// retain minutes of a ~1k bans/s tag-invalidation storm.
var banListBenchCaps = []int{4096, 16384, 65536, 262144}

// benchSaturatedBanList returns a list filled to capacity with distinct
// surrogate-only bans, the steady state under a tag-invalidation storm.
func benchSaturatedBanList(b *testing.B, capacity int) (*banListState, banPredicate) {
	b.Helper()
	st := &banListState{capacity: capacity}
	pred, err := compileBanPredicate(api.BanExpr{SurrogateKey: "x"})
	if err != nil {
		b.Fatal(err)
	}
	for i := range capacity {
		st.register(api.BanExpr{SurrogateKey: "fill-" + strconv.Itoa(i)}, pred, time.Now())
	}
	return st, pred
}

// BenchmarkBanList_StormRegisterAndHit measures one storm step on a
// saturated list: register a distinct tag ban (dedup, evict the head,
// publish) and serve the next hit, which is where a deferred snapshot
// rebuild would be paid. Cost must not scale with the cap.
func BenchmarkBanList_StormRegisterAndHit(b *testing.B) {
	for _, c := range banListBenchCaps {
		b.Run(fmt.Sprintf("cap=%d", c), func(b *testing.B) {
			st, pred := benchSaturatedBanList(b, c)
			// A pool larger than the cap: a key comes back only after
			// more than cap distinct registrations, i.e. after it was
			// evicted, so every step is a distinct registration (not a
			// refresh) on a saturated list.
			keys := make([]string, c+4096)
			for i := range keys {
				keys[i] = "storm-" + strconv.Itoa(i)
			}
			hit := benchTagObj()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				st.register(api.BanExpr{SurrogateKey: keys[i%len(keys)]}, pred, time.Now())
				_ = st.matches(hit)
			}
		})
	}
}

// BenchmarkBanList_HitTagged measures the clean hit-path check for an
// object carrying three unbanned tags against a saturated list.
func BenchmarkBanList_HitTagged(b *testing.B) {
	for _, c := range banListBenchCaps {
		b.Run(fmt.Sprintf("cap=%d", c), func(b *testing.B) {
			st, _ := benchSaturatedBanList(b, c)
			hit := benchTagObj()
			_ = st.matches(hit)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = st.matches(hit)
			}
		})
	}
}

func benchTagObj() *api.Object {
	o := banTestObj("tagged.example.com", "/page", time.Hour)
	o.SurrogateKeys = []string{"live-a", "live-b", "live-c"}
	return o
}
