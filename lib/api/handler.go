package api

import (
	"git.encryptio.com/slime/lib/multi"
	"net/http"
	"strings"
)

type Handler struct {
	m *multi.Multi
}

func NewHandler(m *multi.Multi) *Handler {
	return &Handler{m}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if path == "/config" {
		h.serveConfig(w, r)
		return
	}

	if path == "/scrub" {
		h.serveScrub(w, r)
		return
	}

	if strings.HasPrefix(path, "/obj/") {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/obj")
		h.serveObject(w, r)
		return
	}

	w.Header().Set("content-type", "text/plain; charset=utf-8")
	w.WriteHeader(404)
	w.Write([]byte("No such API"))
}
