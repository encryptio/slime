package api

import (
	"net/http"
	"encoding/json"
	"log"
	"io"
	"git.encryptio.com/slime/lib/multi"
)

func (h *Handler) serveConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		w.Header().Set("content-type", "application/json; charset=utf-8")
		w.WriteHeader(200)
		err := json.NewEncoder(w).Encode(h.m.GetConfig())
		if err != nil {
			log.Printf("Couldn't serialize config: %v", err)
		}

	case "PUT":
		w.Header().Set("content-type", "text/plain; charset=utf-8")

		rdr := &io.LimitedReader{r.Body, MaxBodySize+1}

		var config multi.Config
		err := json.NewDecoder(rdr).Decode(&config)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		if h.m.GetConfig().Version >= config.Version {
			w.WriteHeader(409)
			w.Write([]byte("new version is not greater than current version"))
			return
		}

		err = h.m.SetConfig(config)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(200)
		w.Write([]byte("ok"))

	default:
		w.Header().Set("allow", "GET, PUT")
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		w.WriteHeader(405)
		w.Write([]byte("Bad Method"))
	}
}
