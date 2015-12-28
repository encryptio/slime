package httputil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func mustRequest(method, url, secret string) *http.Request {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}
	if secret != "" {
		req.Header.Set("X-Secret", secret)
	}
	return req
}

func TestSecret(t *testing.T) {
	calls := 0
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(200)
	})

	tests := []struct {
		s          Secret
		req        *http.Request
		resCode    int
		shouldCall bool
	}{
		{Secret{"", inner}, mustRequest("GET", "/", ""), 200, true},
		{Secret{"", inner}, mustRequest("GET", "/", "asdf"), 403, false},
		{Secret{"asdf", inner}, mustRequest("GET", "/", ""), 403, false},
		{Secret{"asdf", inner}, mustRequest("GET", "/", "wrong"), 403, false},
		{Secret{"asdf", inner}, mustRequest("GET", "/", "asdf"), 200, true},
	}

	for _, test := range tests {
		w := httptest.NewRecorder()

		beforeCalls := calls
		test.s.ServeHTTP(w, test.req)

		if w.Code != test.resCode {
			t.Errorf("%v on %v responded %v, wanted %v",
				test.req, test.s, w.Code, test.resCode)
		}

		called := beforeCalls != calls
		if test.shouldCall != called {
			t.Errorf("%v on %v called incorrectly, got %v, wanted %v",
				test.req, test.s, called, test.shouldCall)
		}
	}
}
