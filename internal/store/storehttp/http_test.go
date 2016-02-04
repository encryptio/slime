package storehttp

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/encryptio/slime/internal/store"
	"github.com/encryptio/slime/internal/store/storetests"
	"github.com/encryptio/slime/internal/uuid"
)

func TestHTTPCommon(t *testing.T) {
	mock := storetests.NewMockStore(0)

	srv := httptest.NewServer(NewServer(mock))
	defer srv.Close()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}

	storetests.TestStore(t, client)

	if client.UUID() != mock.UUID() {
		t.Errorf("client UUID %v does not match directory UUID %v",
			uuid.Fmt(client.UUID()), uuid.Fmt(mock.UUID()))
	}
}

func TestHTTPTooBig(t *testing.T) {
	mock := storetests.NewMockStore(0)

	handler := NewServer(mock)

	data := make([]byte, MaxFileSize+1000)

	w := httptest.NewRecorder()
	req, err := http.NewRequest("PUT", "/thing", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Couldn't create request: %v", err)
	}

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("Wanted response code %v, but got %v",
			http.StatusRequestEntityTooLarge, w.Code)
	}
}

func TestHTTPRange(t *testing.T) {
	mock := storetests.NewMockStore(0)

	srv := NewServer(mock)

	storetests.ShouldCAS(t, mock, "key", store.AnyV, store.DataV([]byte("hello world!")))

	tests := []struct {
		RangeRequest   string
		Body           string
		RangeResponse  string
		NotSatisfiable bool
	}{
		{"bytes=5-", " world!", "bytes 5-11/12", false},
		{"bytes=5-12", " world!", "bytes 5-11/12", false},
		{"bytes=11-", "!", "bytes 11-11/12", false},
		{"bytes=0-", "hello world!", "bytes 0-11/12", false},
		{"bytes=1-1", "e", "bytes 1-1/12", false},
		{"bytes=12-", "", "", true},
		{"bytes=12341234-44", "", "", true},
	}

	for _, test := range tests {
		w := httptest.NewRecorder()
		r, err := http.NewRequest("GET", "/key", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Range", test.RangeRequest)
		srv.ServeHTTP(w, r)

		if test.NotSatisfiable {
			if w.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Errorf("Wanted code %#v in response to %#v, got %#v",
					http.StatusRequestedRangeNotSatisfiable, test.RangeRequest,
					w.Code)
			}
		} else {
			if w.Body.String() != test.Body {
				t.Errorf("Wanted %#v in response to %#v, got %#v",
					test.Body, test.RangeRequest, w.Body.String())
			}
			if w.HeaderMap.Get("Content-Range") != test.RangeResponse {
				t.Errorf(`Wanted Content-Range %#v in response to %#v, got %#v`,
					test.RangeResponse, test.RangeRequest,
					w.HeaderMap.Get("Content-Range"))
			}
		}
	}
}

type noverifyRecorder struct {
	store.Store
	noverify []bool
}

func (n *noverifyRecorder) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	n.noverify = append(n.noverify, opts.NoVerify)
	return n.Store.Get(key, opts)
}

func TestHTTPNoVerify(t *testing.T) {
	mock := storetests.NewMockStore(0)
	recorder := &noverifyRecorder{mock, nil}

	srv := httptest.NewServer(NewServer(recorder))
	defer srv.Close()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}

	pattern := []bool{false, true, false}
	for _, nv := range pattern {
		client.Get("key", store.GetOptions{NoVerify: nv})
	}

	if !reflect.DeepEqual(recorder.noverify, pattern) {
		t.Fatalf("Wanted noverify pattern %#v, but got %#v", pattern, recorder.noverify)
	}
}
