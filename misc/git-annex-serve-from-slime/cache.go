package main

import (
	"sync"
)

const cacheSize = 8

type cacheEntry struct {
	url  string
	data []byte
}

var cacheMu sync.Mutex
var cache []cacheEntry

func getCached(url string) ([]byte, bool) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	for _, e := range cache {
		if e.url == url {
			return e.data, true
		}
	}

	return nil, false
}

func setCached(url string, data []byte) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	for _, e := range cache {
		if e.url == url {
			return
		}
	}

	if len(cache) == cacheSize {
		copy(cache, cache[1:])
		cache = cache[:len(cache)-1]
	}

	cache = append(cache, cacheEntry{
		url:  url,
		data: data,
	})
}
