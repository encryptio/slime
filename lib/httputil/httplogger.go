package httputil

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

type stattingReadCloser struct {
	io.ReadCloser
	bytes int
	time  time.Duration
}

func (s *stattingReadCloser) Read(p []byte) (int, error) {
	started := time.Now()
	read, err := s.ReadCloser.Read(p)
	s.time += time.Now().Sub(started)
	s.bytes += read
	return read, err
}

type logRecord struct {
	http.ResponseWriter

	bytes    int
	code     int
	reqUrl   string
	hijacked bool

	time time.Duration
}

func (r *logRecord) Write(p []byte) (int, error) {
	started := time.Now()
	written, err := r.ResponseWriter.Write(p)
	r.time += time.Now().Sub(started)

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
		src := &stattingReadCloser{ReadCloser: req.Body}
		req.Body = src

		record := &logRecord{
			ResponseWriter: w,
			code:           200,
			reqUrl:         req.URL.String(),
		}

		started := time.Now()
		defer func() {
			if record.hijacked {
				log.Printf("%s %s %s -> Connection Hijacked in %v",
					req.RemoteAddr, req.Method, record.reqUrl,
					time.Now().Sub(started))
			} else {
				log.Printf("%s %s %s -> %d, read %d in %v, wrote %d in %v, total time %v",
					req.RemoteAddr, req.Method, record.reqUrl, record.code,
					src.bytes, src.time,
					record.bytes, record.time,
					time.Now().Sub(started))
			}
		}()

		if _, ok := w.(http.Hijacker); ok {
			inner.ServeHTTP(hijackableLogRecord{record}, req)
		} else {
			inner.ServeHTTP(record, req)
		}
	})
}
