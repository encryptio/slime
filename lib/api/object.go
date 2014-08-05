package api

import (
	"net/http"
	"git.encryptio.com/slime/lib/multi"
	"io"
	"io/ioutil"
	"strings"
)

const MaxBodySize = 1024*1024*64

func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if strings.HasSuffix(r.URL.Path, "/") {
			// listing get
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			list, err := h.m.List(r.URL.Path)
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte(err.Error()))
				return
			}

			w.WriteHeader(200)
			for _, fi := range list {
				if fi.IsDir {
					w.Write([]byte("d "))
				} else {
					w.Write([]byte("f "))
				}
				w.Write([]byte(fi.Name))
				w.Write([]byte("\n"))
			}

		} else {
			// file get
			data, err := h.m.Get(r.URL.Path)
			if err != nil {
				w.Header().Set("content-type", "text/plain; charset=utf-8")
				if err == multi.ErrNotFound {
					w.WriteHeader(404)
				} else {
					w.WriteHeader(500)
				}
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Set("content-type", "application/octet-stream")
			w.WriteHeader(200)
			w.Write(data)
		}

	case "PUT":
		rdr := &io.LimitedReader{r.Body, MaxBodySize+1}
		data, err := ioutil.ReadAll(rdr)
		if err != nil {
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		if int64(len(data)) > MaxBodySize {
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte("object too large"))
			return
		}

		if len(data) == 0 {
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(400)
			w.Write([]byte("cannot create zero length object"))
			return
		}

		err = h.m.Set(r.URL.Path, data)
		if err != nil {
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(200)
		w.Write([]byte("ok"))

	case "DELETE":
		err := h.m.Set(r.URL.Path, nil)
		if err != nil {
			w.Header().Set("content-type", "text/plain; charset=utf-8")
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(200)
		w.Write([]byte("ok"))

	default:
		w.Header().Set("allow", "GET, PUT, DELETE")
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(405)
		w.Write([]byte("Bad Method"))
	}
}
