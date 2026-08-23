// Package pprofwrapper wraps net/http/pprof handlers as fasthttp.RequestHandler
// values, isolating the net/http dependency to this package.
package pprofwrapper

import (
	"net/http"
	"net/http/pprof"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// Index returns the pprof index handler as a fasthttp.RequestHandler.
func Index() fasthttp.RequestHandler {
	return fasthttpadaptor.NewFastHTTPHandlerFunc(http.HandlerFunc(pprof.Index))
}

// Map returns a map of pprof sub-handler paths to fasthttp.RequestHandler values.
func Map() map[string]fasthttp.RequestHandler {
	return map[string]fasthttp.RequestHandler{
		"/debug/pprof/cmdline":      fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Cmdline),
		"/debug/pprof/profile":      fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Profile),
		"/debug/pprof/symbol":       fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Symbol),
		"/debug/pprof/trace":        fasthttpadaptor.NewFastHTTPHandlerFunc(pprof.Trace),
		"/debug/pprof/heap":         fasthttpadaptor.NewFastHTTPHandler(pprof.Handler("heap")),
		"/debug/pprof/goroutine":    fasthttpadaptor.NewFastHTTPHandler(pprof.Handler("goroutine")),
		"/debug/pprof/block":        fasthttpadaptor.NewFastHTTPHandler(pprof.Handler("block")),
		"/debug/pprof/mutex":        fasthttpadaptor.NewFastHTTPHandler(pprof.Handler("mutex")),
		"/debug/pprof/threadcreate": fasthttpadaptor.NewFastHTTPHandler(pprof.Handler("threadcreate")),
	}
}
