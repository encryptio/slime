package chunkserver

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"
)

var ScanInterval = 15 * time.Second

const MaxFileSize = 1024 * 1024 * 1024 * 64 // 64MiB

type Handler struct {
	dirs         []string
	sleepPerFile time.Duration
	sleepPerByte time.Duration

	stop chan struct{}

	mu             sync.RWMutex
	serveLocations map[[16]byte]store.Store
}

func New(dirs []string, sleepPerFile, sleepPerByte time.Duration) (*Handler, error) {
	h := &Handler{
		dirs:           dirs,
		sleepPerFile:   sleepPerFile,
		sleepPerByte:   sleepPerByte,
		stop:           make(chan struct{}),
		serveLocations: make(map[[16]byte]store.Store, len(dirs)),
	}
	go h.scanUntilFull()
	return h, nil
}

func (h *Handler) Stop() {
	select {
	case <-h.stop:
	default:
		close(h.stop)
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

		h.serveObject(w, r, uuid, subObject)
	}
}

func (h *Handler) serveUUIDs(w http.ResponseWriter, r *http.Request) {
	var resp bytes.Buffer
	h.mu.RLock()
	for k := range h.serveLocations {
		fmt.Fprintf(&resp, "%v\n", uuid.Fmt(k))
	}
	h.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	w.Write(resp.Bytes())
}

func (h *Handler) serveRoot(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Hello there!"))
}

func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request,
	uuid [16]byte, obj string) {

	h.mu.RLock()
	loc, ok := h.serveLocations[uuid]
	h.mu.RUnlock()
	if !ok {
		http.Error(w, "no such uuid", http.StatusNotFound)
		return
	}

	if len(obj) == 0 {
		http.Error(w, "no object name given", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET", "HEAD":
		data, err := loc.Get(obj)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, err.Error(), 404)
				return
			}
			http.Error(w, err.Error(), 500)
			return
		}

		w.Header().Set("Content-Length",
			strconv.FormatInt(int64(len(data)), 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)

		if r.Method == "GET" {
			w.Write(data)
		}

	case "PUT":
		data, err := ioutil.ReadAll(io.LimitReader(r.Body, MaxFileSize))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		if len(data) == MaxFileSize {
			http.Error(w, "file too large", 413)
			return
		}

		err = loc.Set(obj, data)
		if err != nil {
			http.Error(w, err.Error(), 500)
		}

		w.WriteHeader(204)

	case "DELETE":
		err := loc.Delete(obj)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", 404)
				return
			}
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)

	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) scanUntilFull() {
	found := make(map[string]struct{}, len(h.dirs))
	for {
		for _, dir := range h.dirs {
			if _, ok := found[dir]; ok {
				continue
			}

			select {
			case <-h.stop:
				return
			default:
			}

			log.Printf("Trying to open dirstore %v...", dir)
			ds, err := store.OpenDirStore(dir)
			if err != nil {
				log.Printf("Couldn't open dirstore %v: %v\n", dir, err)
				continue
			}

			log.Printf("Activating dirstore at %v with uuid %v",
				dir, uuid.Fmt(ds.UUID()))

			h.mu.Lock()
			h.serveLocations[ds.UUID()] = ds
			h.mu.Unlock()

			go h.repeatHashcheck(ds)

			found[dir] = struct{}{}
		}

		if len(found) == len(h.dirs) {
			return
		}

		select {
		case <-time.After(ScanInterval):
		case <-h.stop:
			return
		}
	}
}

func (h *Handler) repeatHashcheck(ds *store.DirStore) {
	for {
		good, bad := ds.Hashcheck(h.sleepPerFile, h.sleepPerByte, h.stop)
		log.Printf("Finished hash check on %v: %v good, %v bad\n",
			uuid.Fmt(ds.UUID()), good, bad)

		select {
		case <-time.After(60 * time.Second):
		case <-h.stop:
			return
		}
	}
}
