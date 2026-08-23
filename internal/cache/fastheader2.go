package cache

import (
	"reflect"
	"unsafe"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// noDefaultDateOffset is the byte offset of the unexported noDefaultDate
// field within fasthttp.ResponseHeader. Computed once at package init
// via reflect. Used by setNoDefaultDate to set the field without needing
// a public setter on fasthttp.
var noDefaultDateOffset = func() uintptr {
	t := reflect.TypeOf(fasthttp.ResponseHeader{})
	f, ok := t.FieldByName("noDefaultDate")
	if !ok {
		return 0 // field not found; setNoDefaultDate will be a no-op
	}
	return f.Offset
}()

// setNoDefaultDate sets the unexported noDefaultDate field on a
// fasthttp.ResponseHeader to true. This prevents fasthttp from
// auto-generating a Date header in AppendBytes, which would overwrite
// the stored origin Date set via SetDateRaw.
func setNoDefaultDate(hdr *fasthttp.ResponseHeader) {
	if noDefaultDateOffset == 0 {
		return
	}
	ptr := (*bool)(unsafe.Pointer(uintptr(unsafe.Pointer(hdr)) + noDefaultDateOffset)) //nolint:gosec // G103: controlled unsafe for fasthttp interop
	*ptr = true
}

// getOrComputeFastHeader lazily builds a *fasthttp.ResponseHeader from
// the stored object's headers on the first cache hit, then reuses it on
// subsequent hits via CopyTo. The pre-built header contains only static
// headers (hop-by-hop, internal, Age, X-Cache, X-Cache-Source, Warning,
// and no-cache fields are excluded). Date is included via SetDateRaw.
// noDefaultDate is set to true to prevent fasthttp from auto-adding a Date.
// The result is cached in obj.FastHeader (atomic.Value) for race-safe
// reuse across goroutines.
func getOrComputeFastHeader(obj *api.Object) *fasthttp.ResponseHeader {
	if v := obj.FastHeader.Load(); v != nil {
		return v.(*fasthttp.ResponseHeader)
	}
	hdr := &fasthttp.ResponseHeader{}
	hdr.DisableNormalizing()
	obj.Header.WriteToFastHTTP(hdr)
	if obj.HasDate {
		dateVal := obj.Header.Get(header.Date)
		header.SetDateRaw(hdr, dateVal)
	}
	if obj.HasConnectionList {
		stripConnectionListedHeaders(hdr, obj.Header)
	}
	if obj.HasNoCacheFields {
		stripNoCacheFields(hdr, obj.CacheControl)
	}
	hdr.SetStatusCode(obj.StatusCode)
	setNoDefaultDate(hdr)
	obj.FastHeader.Store(hdr)
	return hdr
}
