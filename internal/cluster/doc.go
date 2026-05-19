// Package cluster is the L6 layer. It manages peer discovery via
// hashicorp/memberlist gossip, the consistent-hash ring for request
// routing, peer-fetch (HTTP/2 over mTLS), purge/ban broadcast, and
// anti-entropy reconciliation.
//
// See PLAN.md §5 for the full design.
package cluster
