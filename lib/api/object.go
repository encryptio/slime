package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"git.encryptio.com/slime/lib/multi"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

const MaxBodySize = 1024 * 1024 * 64

func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/") {
		h.serveDirectory(w, r)
	} else {
		h.serveFile(w, r)
	}
}

func (h *Handler) serveDirectory(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
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

	default:
		w.Header().Set("allow", "GET")
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(405)
		w.Write([]byte("Bad Method"))
	}
}

func (h *Handler) serveFile(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET", "HEAD":
		var res multi.Result
		var err error
		if r.Method == "GET" {
			res, err = h.m.Get(r.URL.Path)
		} else {
			res, err = h.m.Stat(r.URL.Path)
		}
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
		w.Header().Set("content-length", strconv.FormatInt(res.Length, 10))
		w.Header().Set("x-content-sha256", hex.EncodeToString(res.SHA256[:]))
		w.WriteHeader(200)
		if r.Method == "GET" {
			w.Write(res.Data)
		}

	case "PUT":
		w.Header().Set("content-type", "text/plain; charset=utf-8")

		rdr := &io.LimitedReader{r.Body, MaxBodySize + 1}
		data, err := ioutil.ReadAll(rdr)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		if int64(len(data)) > MaxBodySize {
			w.WriteHeader(400)
			w.Write([]byte("object too large"))
			return
		}

		if len(data) == 0 {
			w.WriteHeader(400)
			w.Write([]byte("cannot create zero length object"))
			return
		}

		if r.Header.Get("x-content-sha256") != "" {
			want, _ := hex.DecodeString(r.Header.Get("x-content-sha256"))
			have := sha256.Sum256(data)
			if !bytes.Equal(want, have[:]) {
				w.WriteHeader(400)
				w.Write([]byte("content does not match x-content-sha256"))
				return
			}
		}

		err = h.m.Set(r.URL.Path, data)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

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
		w.Header().Set("allow", "GET, HEAD, PUT, DELETE")
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(405)
		w.Write([]byte("Bad Method"))
	}
}
