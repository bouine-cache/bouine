package storage

import "github.com/bouine-cache/bouine/pkg/header"

// headerMap builds a header.Map from key-value pairs for use in tests.
func headerMap(kvs ...string) header.Map {
	m := header.NewMap(len(kvs) / 2)
	for i := 0; i+1 < len(kvs); i += 2 {
		m.Set(kvs[i], kvs[i+1])
	}
	return m
}
