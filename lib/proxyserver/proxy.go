package proxyserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/multi"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

type Handler struct {
	db         kvl.DB
	dataServer *store.Server
	multi      *multi.Multi
	finder     *multi.Finder
}

func New(db kvl.DB) (*Handler, error) {
	finder, err := multi.NewFinder(db)
	if err != nil {
		return nil, err
	}

	multi, err := multi.NewMulti(db, finder)
	if err != nil {
		finder.Stop()
		return nil, err
	}

	return &Handler{
		db:         db,
		dataServer: store.NewServer(multi),
		multi:      multi,
		finder:     finder,
	}, nil
}

func (h *Handler) Stop() {
	h.multi.Stop()
	h.finder.Stop()
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/data/") {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/data")
		h.dataServer.ServeHTTP(w, r)
		return
	}

	switch r.URL.Path {
	case "/redundancy":
		h.serveRedundancy(w, r)
	case "/stores":
		h.serveStores(w, r)
	case "/":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello from slime proxy server!"))
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (h *Handler) serveRedundancy(w http.ResponseWriter, r *http.Request) {
	redundancy := struct {
		Need  int `json:"need"`
		Total int `json:"total"`
	}{}

	switch r.Method {
	case "GET":
		redundancy.Need, redundancy.Total = h.multi.GetRedundancy()

		w.Header().Set("content-type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(redundancy)
	case "POST":
		err := json.NewDecoder(r.Body).Decode(&redundancy)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = h.multi.SetRedundancy(redundancy.Need, redundancy.Total)
		if err != nil {
			status := http.StatusInternalServerError
			if _, ok := err.(multi.BadConfigError); ok {
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
	}
}

type storesResponseEntry struct {
	UUID      string    `json:"uuid"`
	URL       string    `json:"url"`
	Name      string    `json:"name"`
	Dead      bool      `json:"dead"`
	Connected bool      `json:"connected"`
	LastSeen  time.Time `json:"last_seen"`
	Free      int64     `json:"free,omitempty"`
	Error     string    `json:"error,omitempty"`
}

func (h *Handler) serveStores(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		rdr := bufio.NewReader(r.Body)
		for {
			line, err := rdr.ReadString('\n')
			if line == "" && err != nil {
				if err == io.EOF {
					break
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			line = strings.TrimSuffix(line, "\n")

			err = h.finder.Scan(line)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(fmt.Sprintf("Couldn't scan %v: %v", line, err)))
				return
			}
		}
	} else if r.Method != "GET" {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}

	stores := h.finder.Stores()

	ret := make([]storesResponseEntry, 0, 10)
	_, err := h.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		ret = ret[:0]

		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		locs, err := layer.AllLocations()
		if err != nil {
			return nil, err
		}

		for _, loc := range locs {
			st, connected := stores[loc.UUID]

			var free int64
			var errorField string
			if connected {
				free, err = st.FreeSpace()
				if err != nil {
					errorField = err.Error()
					free = 0
				}
			}

			ret = append(ret, storesResponseEntry{
				UUID:      uuid.Fmt(loc.UUID),
				URL:       loc.URL,
				Name:      loc.Name,
				Dead:      loc.Dead,
				Connected: connected,
				LastSeen:  time.Unix(loc.LastSeen, 0).UTC(),
				Free:      free,
				Error:     errorField,
			})
		}

		return nil, nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ret)
}
