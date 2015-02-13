package chunkserver

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"
)

var ScanInterval = 15 * time.Second

type Handler struct {
	dirs         []string
	sleepPerFile time.Duration
	sleepPerByte time.Duration

	stop     chan struct{}
	scanning chan struct{}

	mu             sync.RWMutex
	serveLocations map[[16]byte]*store.Server
}

func New(dirs []string, sleepPerFile, sleepPerByte time.Duration) (*Handler, error) {
	h := &Handler{
		dirs:           dirs,
		sleepPerFile:   sleepPerFile,
		sleepPerByte:   sleepPerByte,
		stop:           make(chan struct{}),
		scanning:       make(chan struct{}),
		serveLocations: make(map[[16]byte]*store.Server, len(dirs)),
	}
	go h.scanUntilFull()
	return h, nil
}

func (h *Handler) Stop() {
	h.mu.Lock()

	select {
	case <-h.stop:
	default:
		close(h.stop)
	}

	h.mu.Unlock()
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

		h.mu.RLock()
		subHandler, ok := h.serveLocations[uuid]
		h.mu.RUnlock()
		if !ok {
			http.Error(w, "no such uuid", http.StatusNotFound)
			return
		}

		r.URL.Path = "/" + subObject
		subHandler.ServeHTTP(w, r)
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
	w.Write([]byte("Howdy, slime chunk server here!"))
}

func (h *Handler) scanUntilFull() {
	defer close(h.scanning)

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

			ds, err := store.OpenDirStore(dir)
			if err != nil {
				log.Printf("Couldn't open dirstore %v: %v\n", dir, err)
				continue
			}

			h.mu.Lock()
			h.serveLocations[ds.UUID()] = store.NewServer(ds)
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
		if bad != 0 {
			log.Printf("Finished hash check on %v: %v good, %v bad\n",
				uuid.Fmt(ds.UUID()), good, bad)
		}

		select {
		case <-time.After(60 * time.Second):
		case <-h.stop:
			return
		}
	}
}
