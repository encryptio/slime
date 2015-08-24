package proxyserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"git.encryptio.com/slime/lib/httputil"
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

func New(db kvl.DB, scrubbers int) (*Handler, error) {
	finder, err := multi.NewFinder(db)
	if err != nil {
		return nil, err
	}

	multi, err := multi.NewMulti(db, finder, scrubbers)
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
		httputil.RespondJSONError(w, "not found", http.StatusNotFound)
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
			httputil.RespondJSONError(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = h.multi.SetRedundancy(redundancy.Need, redundancy.Total)
		if err != nil {
			status := http.StatusInternalServerError
			if _, ok := err.(multi.BadConfigError); ok {
				status = http.StatusBadRequest
			}
			httputil.RespondJSONError(w, err.Error(), status)
			return
		}

	default:
		w.Header().Set("Allow", "GET, POST")
		httputil.RespondJSONError(w, "bad method", http.StatusMethodNotAllowed)
		return
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
			httputil.RespondJSONError(w, err.Error(), http.StatusBadRequest)
			return
		}

		switch req.Operation {
		case "rescan":
			err = h.finder.Rescan()
			if err != nil {
				httputil.RespondJSONError(w, fmt.Sprintf("Couldn't rescan: %v", err),
					http.StatusInternalServerError)
				return
			}

		case "scan":
			err = h.finder.Scan(req.URL)
			if err != nil {
				httputil.RespondJSONError(w, fmt.Sprintf("Couldn't scan %v: %v", req.URL, err),
					http.StatusBadRequest)
				return
			}

		case "dead", "undead":
			id, err := uuid.Parse(req.UUID)
			if err != nil {
				httputil.RespondJSONError(w, "Couldn't parse UUID", http.StatusBadRequest)
				return
			}

			err = h.db.RunTx(func(ctx kvl.Ctx) error {
				layer, err := meta.Open(ctx)
				if err != nil {
					return err
				}

				loc, err := layer.GetLocation(id)
				if err != nil {
					return err
				}

				if loc == nil {
					return kvl.ErrNotFound
				}

				loc.Dead = req.Operation == "dead"

				return layer.SetLocation(*loc)
			})
			if err != nil {
				if err == kvl.ErrNotFound {
					httputil.RespondJSONError(w, "No store with that UUID",
						http.StatusBadRequest)
					return
				}
				httputil.RespondJSONError(w, err.Error(), http.StatusInternalServerError)
				return
			}

		case "delete":
			id, err := uuid.Parse(req.UUID)
			if err != nil {
				httputil.RespondJSONError(w, "Couldn't parse UUID", http.StatusBadRequest)
				return
			}

			st := h.finder.StoreFor(id)
			if st != nil {
				httputil.RespondJSONError(w, "UUID currently connected", http.StatusBadRequest)
				return
			}

			err = h.db.RunTx(func(ctx kvl.Ctx) error {
				layer, err := meta.Open(ctx)
				if err != nil {
					return err
				}

				loc, err := layer.GetLocation(id)
				if err != nil {
					return err
				}

				if loc == nil {
					return kvl.ErrNotFound
				}

				return layer.DeleteLocation(*loc)
			})
			if err != nil {
				if err == kvl.ErrNotFound {
					httputil.RespondJSONError(w, "No store with that UUID",
						http.StatusBadRequest)
					return
				}
				httputil.RespondJSONError(w, err.Error(), http.StatusInternalServerError)
				return
			}

		default:
			httputil.RespondJSONError(w, "unsupported operation", http.StatusBadRequest)
			return
		}
	} else if r.Method != "GET" {
		w.Header().Set("Allow", "GET, POST")
		httputil.RespondJSONError(w, "bad method", http.StatusMethodNotAllowed)
		return
	}

	finderEntries := h.finder.Stores()

	ret := make([]storesResponseEntry, 0, 10)
	err := h.db.RunReadTx(func(ctx kvl.Ctx) error {
		ret = ret[:0]

		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		locs, err := layer.AllLocations()
		if err != nil {
			return err
		}

		for _, loc := range locs {
			fe, connected := finderEntries[loc.UUID]

			ret = append(ret, storesResponseEntry{
				UUID:      uuid.Fmt(loc.UUID),
				URL:       loc.URL,
				Name:      loc.Name,
				Dead:      loc.Dead,
				Connected: connected,
				LastSeen:  time.Unix(loc.LastSeen, 0).UTC(),
				Free:      fe.Free,
			})
		}

		return nil
	})
	if err != nil {
		httputil.RespondJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ret)
}
