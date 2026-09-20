package api

// EvictionAlgorithm selects a cache eviction policy. Values are the
// wire strings used in storage.*_eviction_algorithm config fields; the
// zero value means the documented default (EvictionSieve).
//
// Stable.
type EvictionAlgorithm string

const (
	// EvictionSieve uses the SIEVE visited-bit sweep.
	EvictionSieve EvictionAlgorithm = "sieve"
	// EvictionCachaner uses SIEVE with a 3-bit frequency counter that
	// gives hot objects up to 7 second chances (vs SIEVE's 1) before
	// eviction.
	EvictionCachaner EvictionAlgorithm = "cachaner"
)
