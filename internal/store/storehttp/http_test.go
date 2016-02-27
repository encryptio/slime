package storehttp

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

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
		{"bytes=4-4", "o", "bytes 4-4/12", false},
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

type cancelRecorder struct {
	store.Store
	mu      sync.Mutex
	cancels []<-chan struct{}
}

func (c *cancelRecorder) Get(key string, opts store.GetOptions) ([]byte, store.Stat, error) {
	c.mu.Lock()
	c.cancels = append(c.cancels, opts.Cancel)
	c.mu.Unlock()
	return c.Store.Get(key, opts)
}

func (c *cancelRecorder) Cancels() []<-chan struct{} {
	c.mu.Lock()
	out := make([]<-chan struct{}, len(c.cancels))
	copy(out, c.cancels)
	c.mu.Unlock()
	return out
}

func (c *cancelRecorder) ClearCancels() {
	c.mu.Lock()
	c.cancels = nil
	c.mu.Unlock()
}

func TestHTTPCancellation(t *testing.T) {
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	mock := storetests.NewMockStore(0)
	recorder := &cancelRecorder{Store: mock}

	srv := httptest.NewServer(NewServer(recorder))
	defer srv.Close()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}

	recorder.ClearCancels()

	mock.SetBlocked(true)
	defer mock.SetBlocked(false)

	// Start a request
	clientCancel := make(chan struct{})
	clientErr := make(chan error, 1)
	go func() {
		_, _, err := client.Get("key", store.GetOptions{Cancel: clientCancel})
		clientErr <- err
	}()

	// Wait until the request reaches the server
	var serverCancel <-chan struct{}
	for {
		select {
		case <-timeout.C:
			t.Fatalf("Timed out while waiting for server to recieve request")
		case <-time.After(5 * time.Millisecond):
		}

		cancels := recorder.Cancels()
		if len(cancels) == 0 {
			// Request hasn't reached the server yet
			continue
		}
		if len(cancels) == 1 {
			serverCancel = cancels[0]
			break
		}
		t.Fatalf("Got multiple cancel channels")
	}

	// Close the client request
	close(clientCancel)

	// Wait for the client to respond to the cancellation
	select {
	case err := <-clientErr:
		if err != store.ErrCancelled {
			t.Errorf("Client Get returned err %v after canceling, wanted %v",
				err, store.ErrCancelled)
		}
	case <-timeout.C:
		t.Fatalf("Client Get did not return after cancellation")
	}

	// Wait for the server to respond to the cancellation
LOOP:
	for {
		select {
		case <-serverCancel:
			break LOOP
		case <-timeout.C:
			t.Fatalf("Timed out waiting for server cancel")
		}
	}
}

func TestHTTPConditionalGetLastModified(t *testing.T) {
	mock := storetests.NewMockStore(0)
	srv := NewServer(mock)

	storetests.ShouldCAS(t, mock, "key", store.AnyV, store.DataV([]byte("some data")))

	mkReq := func(ims string) *http.Request {
		req, err := http.NewRequest("GET", "/key", nil)
		if err != nil {
			t.Fatalf("Couldn't create request: %v", err)
		}
		if ims != "" {
			req.Header.Set("If-Modified-Since", ims)
		}
		return req
	}

	// First request: no special headers
	resp := httptest.NewRecorder()
	req := mkReq("")
	srv.ServeHTTP(resp, req)
	if resp.Code != 200 {
		t.Fatalf("Couldn't GET /key, status code %v", resp.Code)
	}

	lmStr := resp.HeaderMap.Get("Last-Modified")
	if lmStr == "" {
		t.Fatalf("Last-Modified header was not returned from GET")
	}

	lm, err := time.Parse(http.TimeFormat, lmStr)
	if err != nil {
		t.Fatalf("Couldn't parse Last-Modified %#v: %v", lmStr, err)
	}

	// Second request: matching If-Modified-Since time
	resp = httptest.NewRecorder()
	req = mkReq(lm.Format(http.TimeFormat))
	srv.ServeHTTP(resp, req)
	if resp.Code != 304 {
		t.Fatalf("Equal If-Modified-Since response did not return partial, got status %v",
			resp.Code)
	}

	// Third request: If-Modified-Since time in the past
	resp = httptest.NewRecorder()
	req = mkReq(lm.Add(-time.Second).Format(http.TimeFormat))
	srv.ServeHTTP(resp, req)
	if resp.Code != 200 {
		t.Fatalf("Past If-Modified-Since response did not return full, got status %v",
			resp.Code)
	}

	// Fourth request: If-Modified-Since time in the future
	resp = httptest.NewRecorder()
	req = mkReq(lm.Add(time.Second).Format(http.TimeFormat))
	srv.ServeHTTP(resp, req)
	if resp.Code != 304 {
		t.Fatalf("Equal If-Modified-Since response did not return partial, got status %v",
			resp.Code)
	}
}
