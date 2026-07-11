// Package cluster is the L5 layer. It manages peer discovery via
// hashicorp/memberlist gossip, the consistent-hash ring for request
// routing, peer-fetch (HTTP/2 over mTLS), and purge/ban broadcast.
package cluster
