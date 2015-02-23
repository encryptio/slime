package storehttp

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"git.encryptio.com/slime/lib/retry"
	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/uuid"
)

var errBadIfMatchFormat = errors.New("bad format for if-match header value")

// MaxFileSize is the maximum size to accept in a Server request.
const MaxFileSize = 1024 * 1024 * 1024 * 64 // 64MiB

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
	store store.Store
}

// NewServer creates a Server out of a Store256.
func NewServer(s store.Store) *Server {
	return &Server{s}
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

func (h *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	obj := strings.TrimPrefix(r.URL.Path, "/")

	if len(obj) == 0 {
		h.serveRoot(w, r)
		return
	}

	switch r.Method {
	case "GET":
		if theirEtags := r.Header.Get("if-none-match"); theirEtags != "" {
			st, err := h.store.Stat(obj)
			if err != nil {
				if err == store.ErrNotFound {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
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

		// The race between this Get and the above Stat is benign; we might
		// miss an opportunity to cache, but we'll never return an incorrect
		// result.

		data, hash, err := h.store.Get(obj)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Length",
			strconv.FormatInt(int64(len(data)), 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Content-SHA256", hex.EncodeToString(hash[:]))
		w.Header().Set("ETag", `"`+hex.EncodeToString(hash[:])+`"`)
		w.WriteHeader(http.StatusOK)

		w.Write(data)

	case "HEAD":
		st, err := h.store.Stat(obj)
		if err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
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

	case "PUT":
		if r.ContentLength > MaxFileSize {
			http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
			return
		}

		data, err := ioutil.ReadAll(io.LimitReader(r.Body, MaxFileSize+1))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(data) == MaxFileSize+1 {
			http.Error(w, "file too large", http.StatusRequestEntityTooLarge)
			return
		}

		haveHash := sha256.Sum256(data)

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
		})
		if err != nil {
			if err == store.ErrCASFailure {
				http.Error(w, err.Error(), http.StatusPreconditionFailed)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.WriteHeader(http.StatusNoContent)

	case "DELETE":
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
				st, err := h.store.Stat(obj)
				if err != nil {
					if err == store.ErrNotFound {
						http.Error(w, "not found", http.StatusNotFound)
						return
					}
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				from = store.CASV{Present: true, SHA256: st.SHA256}
				doRetry = true
			}

			err = h.store.CAS(obj, from,
				store.CASV{Present: false})
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
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusNoContent)
			return
		}

		http.Error(w, "too many retries", http.StatusInternalServerError)

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

func (h *Server) serveUUID(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(uuid.Fmt(h.store.UUID())))
}

func (h *Server) serveName(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(h.store.Name()))
}
