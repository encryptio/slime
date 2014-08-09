package main

import (
	"net/http"
	"sync"
)

type Counter struct {
	inner http.Handler
	wg    sync.WaitGroup
}

func CountHTTPRequests(inner http.Handler) *Counter {
	return &Counter{inner: inner}
}

func (c *Counter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.wg.Add(1)
	defer c.wg.Done()
	c.inner.ServeHTTP(w, r)
}

func (c *Counter) Wait() {
	c.wg.Wait()
}
