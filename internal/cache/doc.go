// Package cache is the L3 cache engine. It implements the RFC 9111
// state machine (cacheability, freshness, Vary, conditionals, SWR/SIE),
// key computation, request collapsing, invalidation (purge, ban,
// refresh), and the fast-path serving pipeline. It consumes the
// storage.Store interface and drives L5 origin fetches.
package cache
