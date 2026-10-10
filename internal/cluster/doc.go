// Package cluster is the L5 layer. It manages peer discovery via
// hashicorp/memberlist gossip, the consistent-hash ring for request
// routing, peer-fetch (HTTP/1.1 over mTLS, ADR-0035), and purge/ban
// broadcast.
package cluster
