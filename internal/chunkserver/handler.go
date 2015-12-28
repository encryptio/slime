package chunkserver

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"time"

	"git.encryptio.com/slime/internal/store"
	"git.encryptio.com/slime/internal/store/storehttp"
	"git.encryptio.com/slime/internal/uuid"
)

type Handler struct {
	stores   []store.Store
	handlers []*storehttp.Server
}

func New(stores []store.Store) (*Handler, error) {
	handlers := make([]*storehttp.Server, len(stores))
	for i, st := range stores {
		handlers[i] = storehttp.NewServer(st)
	}

	h := &Handler{
		stores:   stores,
		handlers: handlers,
	}

	return h, nil
}

// Close is a no-op. Notably, it does not Close any of the stores passed to New.
func (h *Handler) Close() error {
	return nil
}

func (h *Handler) WaitAllAvailable() {
	for {
		done := true
		for _, st := range h.stores {
			if ss, ok := st.(store.SometimesStore); ok && !ss.Available() {
				done = false
				break
			}
		}

		if done {
			return
		}

		time.Sleep(time.Millisecond)
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/uuids":
		h.serveUUIDs(w, r)
	case "/":
		h.serveRoot(w, r)
	default:
		parts := strings.SplitN(r.URL.Path, "/", 3)
		if len(parts) != 3 {
			http.Error(w, "bad url", http.StatusBadRequest)
			return
		}

		uuidStr := parts[1]
		subObject := parts[2]

		uuid, err := uuid.Parse(uuidStr)
		if err != nil {
			http.Error(w, "bad uuid", http.StatusBadRequest)
			return
		}

		var subHandler *storehttp.Server
		for i, st := range h.stores {
			if st.UUID() == uuid {
				subHandler = h.handlers[i]
				break
			}
		}

		if subHandler == nil {
			http.Error(w, "no such uuid", http.StatusNotFound)
			return
		}

		r.URL.Path = "/" + subObject
		subHandler.ServeHTTP(w, r)
	}
}

func (h *Handler) serveUUIDs(w http.ResponseWriter, r *http.Request) {
	var resp bytes.Buffer
	for _, st := range h.stores {
		id := st.UUID()
		if id != [16]byte{} {
			fmt.Fprintf(&resp, "%v\n", uuid.Fmt(id))
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write(resp.Bytes())
}

func (h *Handler) serveRoot(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Howdy, slime chunk server here!\n"))
}
