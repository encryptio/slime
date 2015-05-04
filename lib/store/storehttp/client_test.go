package storehttp

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"git.encryptio.com/slime/lib/store"
	"git.encryptio.com/slime/lib/store/storedir"
)

func TestClientCancel(t *testing.T) {
	ds, tmpDir := storedir.MakeTestingDirectory(t)
	defer os.RemoveAll(tmpDir)

	handler := NewServer(ds)
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
