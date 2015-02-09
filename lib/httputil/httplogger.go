package httputil

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"time"
)

type logRecord struct {
	http.ResponseWriter

	bytes    int
	code     int
	reqUrl   string
	hijacked bool
}

func (r *logRecord) Write(p []byte) (int, error) {
	written, err := r.ResponseWriter.Write(p)
	r.bytes += written
	return written, err
}

func (r *logRecord) WriteHeader(status int) {
	r.code = status
	r.ResponseWriter.WriteHeader(status)
}

type hijackableLogRecord struct {
	*logRecord
}

func (r hijackableLogRecord) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj := r.ResponseWriter.(http.Hijacker)
	c, buf, err := hj.Hijack()
	if err == nil {
		r.hijacked = true
	}
	return c, buf, err
}

func LogHttpRequests(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		record := &logRecord{
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

		if _, ok := w.(http.Hijacker); ok {
			inner.ServeHTTP(hijackableLogRecord{record}, req)
		} else {
			inner.ServeHTTP(record, req)
		}
	})
}
