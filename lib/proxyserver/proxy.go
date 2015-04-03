package proxyserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/store/multi"
	"git.encryptio.com/slime/lib/store/storehttp"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

type Handler struct {
	db         kvl.DB
	dataServer *storehttp.Server
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
		dataServer: storehttp.NewServer(multi),
		multi:      multi,
		finder:     finder,
	}, nil
}

func (h *Handler) Stop() {
	h.multi.Close()
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
		// do nothing

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

	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
	}

	redundancy.Need, redundancy.Total = h.multi.GetRedundancy()

	w.Header().Set("content-type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(redundancy)
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

type storesRequest struct {
	Operation string `json:"operation"`
	URL       string `json:"url,omitempty"`
	UUID      string `json:"uuid,omitempty"`
}

func (h *Handler) serveStores(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		var req storesRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		switch req.Operation {
		case "scan":
			err = h.finder.Scan(req.URL)
			if err != nil {
				http.Error(w, fmt.Sprintf("Couldn't scan %v: %v", req.URL, err),
					http.StatusBadRequest)
				return
			}

		case "dead":
			id, err := uuid.Parse(req.UUID)
			if err != nil {
				http.Error(w, "Couldn't parse UUID", http.StatusBadRequest)
				return
			}

			_, err = h.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
				layer, err := meta.Open(ctx)
				if err != nil {
					return nil, err
				}

				loc, err := layer.GetLocation(id)
				if err != nil {
					return nil, err
				}

				if loc == nil {
					return nil, kvl.ErrNotFound
				}

				loc.Dead = true

				err = layer.SetLocation(*loc)
				if err != nil {
					return nil, err
				}

				return nil, nil
			})
			if err != nil {
				if err == kvl.ErrNotFound {
					http.Error(w, "No store with that UUID",
						http.StatusBadRequest)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		case "delete":
			id, err := uuid.Parse(req.UUID)
			if err != nil {
				http.Error(w, "Couldn't parse UUID", http.StatusBadRequest)
				return
			}

			st := h.finder.StoreFor(id)
			if st != nil {
				http.Error(w, "UUID currently connected", http.StatusBadRequest)
				return
			}

			_, err = h.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
				layer, err := meta.Open(ctx)
				if err != nil {
					return nil, err
				}

				loc, err := layer.GetLocation(id)
				if err != nil {
					return nil, err
				}

				if loc == nil {
					return nil, kvl.ErrNotFound
				}

				err = layer.DeleteLocation(*loc)
				if err != nil {
					return nil, err
				}

				return nil, nil
			})
			if err != nil {
				if err == kvl.ErrNotFound {
					http.Error(w, "No store with that UUID",
						http.StatusBadRequest)
					return
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		default:
			http.Error(w, "unsupported operation", http.StatusBadRequest)
			return
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
