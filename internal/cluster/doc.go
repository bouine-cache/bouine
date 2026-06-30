// Package cluster is the L5 layer. It manages peer discovery via
// hashicorp/memberlist gossip, the consistent-hash ring for request
// routing, peer-fetch (HTTP/2 over mTLS), purge/ban broadcast, and
// anti-entropy reconciliation for full mode (periodic key-set diff
// with backfill via peer-fetch).
package cluster
