package store

import (
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

const MaxFileSize = 1024 * 1024 * 1024 * 64 // 64MiB

type Server struct {
	store Store
}

func NewServer(s Store) *Server {
	return &Server{s}
}

func (h *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	obj := strings.TrimPrefix(r.URL.Path, "/")

	if len(obj) == 0 {
		h.serveList(w, r)
		return
	}

	switch r.Method {
	case "GET", "HEAD":
		data, err := h.store.Get(obj)
		if err != nil {
			if err == ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Length",
			strconv.FormatInt(int64(len(data)), 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)

		if r.Method == "GET" {
			w.Write(data)
		}

	case "PUT":
		data, err := ioutil.ReadAll(io.LimitReader(r.Body, MaxFileSize))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(data) == MaxFileSize {
			http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
			return
		}

		err = h.store.Set(obj, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.WriteHeader(http.StatusNoContent)

	case "DELETE":
		err := h.store.Delete(obj)
		if err != nil {
			if err == ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
	}
}

func (h *Server) serveList(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}

	qp := r.URL.Query()

	if qp.Get("free") != "" {
		h.serveFree(w, r)
		return
	}

	after := qp.Get("after")

	limit := -1
	if s := qp.Get("limit"); s != "" {
		i, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			http.Error(w, "bad limit argument", http.StatusBadRequest)
			return
		}
		limit = int(i)
	}

	names, err := h.store.List(after, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	newline := []byte("\n")
	for _, name := range names {
		w.Write([]byte(name))
		w.Write(newline)
	}
}

func (h *Server) serveFree(w http.ResponseWriter, r *http.Request) {
	free, err := h.store.FreeSpace()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(strconv.FormatInt(free, 10)))
}
