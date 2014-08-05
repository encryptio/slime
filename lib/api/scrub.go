package api

import (
	"net/http"
	"encoding/json"
	"log"
)

func (h *Handler) serveScrub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(200)
	err := json.NewEncoder(w).Encode(h.m.GetScrubStats())
	if err != nil {
		log.Printf("Couldn't serialize scrub stats: %v", err)
	}
}
