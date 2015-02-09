package main

import (
	"bufio"
	"errors"
	"log"
	"net"
	"net/http"
	"time"
)

var ErrBadHijack = errors.New("Hijack called on LogRecord, but underlying ResponseWriter is not a Hijacker")

type LogRecord struct {
	http.ResponseWriter

	bytes    int
	code     int
	reqUrl   string
	hijacked bool
}

func (r *LogRecord) Write(p []byte) (int, error) {
	written, err := r.ResponseWriter.Write(p)
	r.bytes += written
	return written, err
}

func (r *LogRecord) WriteHeader(status int) {
	r.code = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *LogRecord) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, ErrBadHijack
	}

	c, buf, err := hj.Hijack()

	if err == nil {
		r.hijacked = true
	}

	return c, buf, err
}

func LogHttpRequests(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		record := &LogRecord{
			ResponseWriter: w,
			code:           200,
			reqUrl:         req.URL.String(),
		}
		started := time.Now()
		defer func() {
			if record.hijacked {
				log.Printf("%s %s %s -> Connection Hijacked for %v",
					req.RemoteAddr, req.Method, record.reqUrl,
					time.Now().Sub(started))
			} else {
				log.Printf("%s %s %s -> %d, %d bytes in %v",
					req.RemoteAddr, req.Method, record.reqUrl, record.code,
					record.bytes, time.Now().Sub(started))
			}
		}()
		inner.ServeHTTP(record, req)
	})
}
