package httputil

import (
	"encoding/json"
	"net/http"
)

func RespondJSON(w http.ResponseWriter, body interface{}, code int) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(body)
}

func RespondJSONError(w http.ResponseWriter, err string, code int) {
	RespondJSON(w, map[string]string{"error": err}, code)
}
