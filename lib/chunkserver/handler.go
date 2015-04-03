package chunkserver

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"git.encryptio.com/slime/lib/store/storedir"
	"git.encryptio.com/slime/lib/store/storehttp"
	"git.encryptio.com/slime/lib/uuid"
)

var ScanInterval = 30 * time.Second

type Handler struct {
	dirs        []string
	perFileWait time.Duration
	perByteWait time.Duration

	stop chan struct{}

	c      *sync.Cond
	loaded map[[16]byte]loadedDir
}

type loadedDir struct {
	dir     string
	handler *storehttp.Server
	store   *storedir.Directory
}

func New(dirs []string, perFileWait, perByteWait time.Duration) (*Handler, error) {
	h := &Handler{
		dirs:        dirs,
		perFileWait: perFileWait,
		perByteWait: perByteWait,
		stop:        make(chan struct{}),
		c:           sync.NewCond(&sync.Mutex{}),
		loaded:      make(map[[16]byte]loadedDir, len(dirs)),
	}

	go h.scanLoop()

	return h, nil
}

func (h *Handler) Stop() {
	h.c.L.Lock()

	select {
	case <-h.stop:
	default:
		close(h.stop)
	}

	h.c.L.Unlock()
}

func (h *Handler) WaitScanDone() {
	h.c.L.Lock()
	for len(h.loaded) != len(h.dirs) {
		h.c.Wait()
	}
	h.c.L.Unlock()
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

		h.c.L.Lock()
		subHandler, ok := h.loaded[uuid]
		h.c.L.Unlock()
		if !ok {
			http.Error(w, "no such uuid", http.StatusNotFound)
			return
		}

		r.URL.Path = "/" + subObject
		subHandler.handler.ServeHTTP(w, r)
	}
}

func (h *Handler) serveUUIDs(w http.ResponseWriter, r *http.Request) {
	h.c.L.Lock()
	var resp bytes.Buffer
	for k := range h.loaded {
		fmt.Fprintf(&resp, "%v\n", uuid.Fmt(k))
	}
	h.c.L.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write(resp.Bytes())
}

func (h *Handler) serveRoot(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Howdy, slime chunk server here!\n"))
}

func (h *Handler) scanLoop() {
	defer func() {
		h.c.L.Lock()
		for id, ldir := range h.loaded {
			delete(h.loaded, id)
			ldir.store.Close()
		}
		h.c.L.Unlock()
	}()

	for {
		for _, dir := range h.dirs {
			select {
			case <-h.stop:
				return
			default:
			}

			h.c.L.Lock()
			found := false
			var ldir loadedDir
			for _, ldir = range h.loaded {
				if ldir.dir == dir {
					found = true
					break
				}
			}
			h.c.L.Unlock()

			if found {
				if !ldir.store.StillValid() {
					log.Printf("Directory store at %v (%v) is no longer valid, removing",
						dir, uuid.Fmt(ldir.store.UUID()))
					h.c.L.Lock()
					delete(h.loaded, ldir.store.UUID())
					h.c.Broadcast()
					h.c.L.Unlock()
					ldir.store.Close()
				}
			} else {
				ds, err := storedir.OpenDirectory(dir, h.perFileWait, h.perByteWait)
				if err != nil {
					log.Printf("Couldn't open directory store %v: %v", dir, err)
					continue
				}

				ldir = loadedDir{
					dir:     dir,
					handler: storehttp.NewServer(ds),
					store:   ds,
				}

				h.c.L.Lock()
				h.loaded[ds.UUID()] = ldir
				h.c.Broadcast()
				h.c.L.Unlock()
			}
		}

		select {
		case <-time.After(ScanInterval):
		case <-h.stop:
			return
		}
	}
}
