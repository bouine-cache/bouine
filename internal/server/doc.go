// Package server is the L1 data-plane front door. It owns HTTP/1.1
// listeners (TLS and cleartext) and the route-matching router that
// dispatches requests to cache handlers.
package server
