package storehttp

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/encryptio/slime/internal/retry"
	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/uuid"
)

var errBadIfMatchFormat = errors.New("bad format for if-match header value")

// MaxFileSize is the maximum size to accept in a Server request.
const MaxFileSize = 1024 * 1024 * 64 // 64MiB

// A Server is an http.Handler which serves a Store with the standard HTTP
// interface, suitable for use by Client.
//
// A Server responds to the following requests:
//     GET /key - retrieve key contents
//     HEAD /key - retrieve metadata (sha256, length)
//     PUT /key - set key contents
//     DELETE /key - remove a key
//     GET /?mode=list&after=xx&limit=nn - list keys, after and limit are
//                                         optional.
//     GET /?mode=free - get the number of free bytes
//     GET /?mode=uuid - get the uuid
//     GET /?mode=name - get the name
//
// The X-Content-SHA256 header is used to verify the hash of PUT'd content
// and is sent in responses.
//
// If an If-Match header is present on a PUT request, the server will return 412
// if the existing value does not have the given ETag/SHA256, and will
// atomically swap if it does. The special ETag "nonexistent" will only match
// nonexistent values.
type Server struct {
	store      store.Store
	rangeStore store.RangeReadStore // nil if unsupported
}

// NewServer creates a Server out of a Store256.
func NewServer(s store.Store) *Server {
	h := &Server{store: s}
	h.rangeStore, _ = s.(store.RangeReadStore)
	return h
}

func parseIfMatch(ifMatch string) (store.CASV, error) {
	ifMatch = strings.Trim(ifMatch, `" `)

	switch ifMatch {
	case "":
		return store.AnyV, nil
	case "nonexistent":
		return store.MissingV, nil
	default:
		casBytes, err := hex.DecodeString(ifMatch)
		if err != nil || len(casBytes) != 32 {
			return store.CASV{}, errBadIfMatchFormat
		}

		var cas [32]byte
		copy(cas[:], casBytes)

		return store.CASV{
			Present: true,
			SHA256:  cas,
		}, nil
	}
}

func parseRange(rang string) (int, int, bool) {
	if !strings.HasPrefix(rang, "bytes=") {
		return 0, 0, false
	}
	rang = strings.TrimPrefix(rang, "bytes=")

	// We accept bytes=START- and bytes=START-END, but no other forms.

	parts := strings.SplitN(rang, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	start64, err := strconv.ParseInt(parts[0], 10, 0)
	if err != nil {
		return 0, 0, false
	}
	start := int(start64)

	var length int
	if parts[1] == "" {
		length = -1
	} else {
		length64, err := strconv.ParseInt(parts[1], 10, 0)
		if err != nil {
			return 0, 0, false
		}
		length = int(length64)
	}

	return start, length, true
}

func (h *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	obj := strings.TrimPrefix(r.URL.Path, "/")

	if len(obj) == 0 {
		h.serveRoot(w, r)
		return
	}

	switch r.Method {
	case "GET":
		h.serveObjectGet(w, r, obj)
	case "HEAD":
		h.serveObjectHead(w, r, obj)
	case "PUT":
		h.serveObjectPut(w, r, obj)
	case "DELETE":
		h.serveObjectDelete(w, r, obj)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
	}
}

func (h *Server) serveRoot(w http.ResponseWriter, r *http.Request) {
	qp := r.URL.Query()
	mode := qp.Get("mode")
	switch mode {
	case "list":
		h.serveList(w, r)
	case "free":
		h.serveFree(w, r)
	case "name":
		h.serveName(w, r)
	case "", "uuid":
		h.serveUUID(w, r)
	default:
		http.Error(w, "no such query mode", http.StatusBadRequest)
	}
}

func (h *Server) serveList(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}

	qp := r.URL.Query()

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

	names, err := h.store.List(after, limit, nil)
	if err != nil {
		log.Printf("Couldn't List(): %v", err)
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
	free, err := h.store.FreeSpace(nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(strconv.FormatInt(free, 10)))
}

func (h *Server) serveUUID(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(uuid.Fmt(h.store.UUID())))
}

func (h *Server) serveName(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(h.store.Name()))
}

func (h *Server) serveObjectGet(w http.ResponseWriter, r *http.Request, obj string) {
	if theirEtags := r.Header.Get("if-none-match"); theirEtags != "" {
		st, err := h.store.Stat(obj, nil)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			log.Printf("Couldn't Stat(%#v): %v", obj, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		etag := `"` + hex.EncodeToString(st.SHA256[:]) + `"`

		matched := false
		for _, s := range strings.Split(theirEtags, ",") {
			s = strings.TrimSpace(s)
			if s == "*" || s == etag {
				matched = true
			}
		}

		if matched {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// The race between the Get and the above Stat is benign; we might
	// miss an opportunity to cache, but we'll never return an incorrect
	// result.

	var data []byte
	var st store.Stat
	var err error
	usingRange := false

	start, length, ok := parseRange(r.Header.Get("Range"))
	if ok && h.rangeStore != nil {
		usingRange = true
		data, st, err = h.rangeStore.GetPartial(obj, start, length, nil)
	} else {
		data, st, err = h.store.Get(obj, nil)
	}

	if err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		log.Printf("Couldn't Get(%#v): %v", obj, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if usingRange && len(data) == 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("*/%v", st.Size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length",
		strconv.FormatInt(int64(len(data)), 10))
	if usingRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %v-%v/%v",
			start, start+len(data)-1, st.Size))
	} else {
		w.Header().Set("X-Content-SHA256", hex.EncodeToString(st.SHA256[:]))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(st.SHA256[:])+`"`)

	if usingRange {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Write(data)
}

func (h *Server) serveObjectHead(w http.ResponseWriter, r *http.Request, obj string) {
	st, err := h.store.Stat(obj, nil)
	if err != nil {
		if err == store.ErrNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		log.Printf("Couldn't Stat(%#v): %v", obj, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if st.Size >= 0 {
		w.Header().Set("Content-Length",
			strconv.FormatInt(st.Size, 10))
	}

	var zeroes [32]byte
	if zeroes != st.SHA256 {
		w.Header().Set("X-Content-SHA256",
			hex.EncodeToString(st.SHA256[:]))
		w.Header().Set("ETag",
			`"`+hex.EncodeToString(st.SHA256[:])+`"`)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
}

func (h *Server) serveObjectPut(w http.ResponseWriter, r *http.Request, obj string) {
	if r.ContentLength > MaxFileSize {
		http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
		return
	}

	hash := sha256.New()
	tee := io.TeeReader(io.LimitReader(r.Body, MaxFileSize+1), hash)
	data, err := ioutil.ReadAll(tee)
	if err != nil {
		log.Printf("Couldn't read object body in PUT: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(data) == MaxFileSize+1 {
		http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
		return
	}

	var haveHash [32]byte
	hash.Sum(haveHash[:0])

	if want := r.Header.Get("X-Content-SHA256"); want != "" {
		wantBytes, err := hex.DecodeString(want)
		if err != nil || len(wantBytes) != 32 {
			http.Error(w, "bad format for x-content-sha256",
				http.StatusBadRequest)
			return
		}

		var wantHash [32]byte
		copy(wantHash[:], wantBytes)

		if wantHash != haveHash {
			http.Error(w, "hash mismatch", http.StatusBadRequest)
			return
		}
	}

	from, err := parseIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.store.CAS(obj, from, store.CASV{
		Present: true,
		SHA256:  haveHash,
		Data:    data,
	}, nil)
	if err != nil {
		if err == store.ErrCASFailure {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		log.Printf("Couldn't CAS(%#v): %v", obj, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Server) serveObjectDelete(w http.ResponseWriter, r *http.Request, obj string) {
	doRetry := true
	retr := retry.New(10)
	for retr.Next() {
		doRetry = false

		from, err := parseIfMatch(r.Header.Get("If-Match"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if from.Any {
			// TODO: make the CAS interface rich enough to handle this
			// without Stat
			st, err := h.store.Stat(obj, nil)
			if err != nil {
				if err == store.ErrNotFound {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				log.Printf("Couldn't Stat(%#v): %v", obj, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			from = store.CASV{Present: true, SHA256: st.SHA256}
			doRetry = true
		}

		err = h.store.CAS(obj, from, store.CASV{Present: false}, nil)
		if err != nil {
			if err == store.ErrCASFailure {
				if doRetry {
					continue
				} else {
					http.Error(w, err.Error(),
						http.StatusPreconditionFailed)
					return
				}
			}
			log.Printf("Couldn't CAS(%#v): %v", obj, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
		return
	}

	http.Error(w, "too many retries", http.StatusInternalServerError)
}
