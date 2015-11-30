package storehttp

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storetests"
)

func TestClientCancel(t *testing.T) {
	mock := storetests.NewMockStore(0)

	handler := NewServer(mock)
	var serverSending chan struct{}
	serverWait := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case serverSending <- struct{}{}:
		default:
		}

		<-serverWait

		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	startBlocking := make(chan struct{})
	startedBlocking := make(chan struct{})
	go func() {
		defer close(startedBlocking)

		for {
			select {
			case <-startBlocking:
				return
			case serverWait <- struct{}{}:
			}
		}
	}()

	client, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't initialize client: %v", err)
	}
	defer client.Close()

	close(startBlocking)
	<-startedBlocking

	serverSending = make(chan struct{})
	cancel := make(chan struct{})
	clientGet := make(chan error)

	go func() {
		_, _, err := client.Get("key", cancel)
		clientGet <- err
	}()

	<-serverSending // wait until the client has asked the server
	close(cancel)   // then cancel

	select {
	case err = <-clientGet:
	case <-time.After(time.Second):
		t.Fatalf("client cancel did not return early")
	}
	if err != store.ErrCancelled {
		t.Errorf("cancelled client get did not return ErrCancelled (got %v)", err)
	}

	close(serverWait)

	_, _, err = client.Get("key", nil)
	if err != store.ErrNotFound {
		t.Errorf("client did not recover from a cancelled request")
	}
}

func TestClientBadSHA(t *testing.T) {
	corrupt := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Query().Get("mode") {
		case "uuid":
			w.WriteHeader(200)
			w.Write([]byte("4c47e705-c248-4bf8-b9a0-50ce7b6fb444"))
		case "name":
			w.WriteHeader(200)
			w.Write([]byte("testing server"))
		default:
			data := []byte("this is the data")
			h := sha256.Sum256(data)

			if corrupt {
				h[0]++
			}

			w.Header().Set("X-Content-SHA256", hex.EncodeToString(h[:]))
			w.WriteHeader(200)
			w.Write(data)
		}
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL + "/")
	if err != nil {
		t.Fatalf("Couldn't create client: %v", err)
	}
	defer c.Close()

	_, _, err = c.Get("key", nil)
	if err != nil {
		t.Fatalf("Couldn't GET key: %v", err)
	}

	corrupt = true

	_, _, err = c.Get("key", nil)
	if _, ok := err.(HashMismatchError); !ok {
		t.Fatalf("Wanted hash mismatch error on corrupt get, got err %v", err)
	}
}
